//------------------------------------------------------------------------------
// This file is part of XrdRedis: A Redis-like server implementation
//
// Copyright (c) 2016 by European Organization for Nuclear Research (CERN)
// Author: Georgios Bitzes <georgios.bitzes@cern.ch>
// File Date: May 2016
//------------------------------------------------------------------------------
// XRootD is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// XRootD is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with XRootD.  If not, see <http://www.gnu.org/licenses/>.
//------------------------------------------------------------------------------

#include "XrdRedisRocksDB.hh"
#include "XrdRedisUtil.hh"

#include <iostream>
#include <stdlib.h>
#include <sstream>
#include <climits>

#include <rocksdb/merge_operator.h>

using namespace rocksdb;

static XrdRedisStatus status_convert(const rocksdb::Status &st) {
  return XrdRedisStatus(st.code(), st.ToString());
}

static XrdRedisStatus OK() {
  return XrdRedisStatus(rocksdb::Status::kOk);
}

enum RedisType {
  kString = 'a',
  kHash,
  kSet
};

// it's rare to have to escape a key, most don't contain #
// so don't make a copy, just change the existing string
static void escape(std::string &str) {
  char replacement[3];
  replacement[0] = '|';
  replacement[1] = '#';
  replacement[2] = '\0';

  size_t pos = 0;
  while((pos = str.find('#', pos)) != std::string::npos) {
    str.replace(pos, 1, replacement);
    pos += 2;
  }
}

// given a rocksdb key (might also contain a field),
// extract the original redis key
static std::string extract_key(std::string &tkey) {
  std::string key;
  key.reserve(tkey.size());

  for(size_t i = 1; i < tkey.size(); i++) {
    // escaped hash?
    if(i != tkey.size() - 1 && tkey[i] == '|' && tkey[i+1] == '#') {
      key.append(1, '#');
      i++;
      continue;
    }
    // boundary?
    if(tkey[i] == '#') {
      break;
    }

    key.append(1, tkey[i]);
  }

  return key;
}

static std::string translate_key(const RedisType type, const std::string &key) {
  std::string escaped = key;
  escape(escaped);

  return std::string(1, type) + escaped;
}

static std::string translate_key(const RedisType type, const std::string &key, const std::string &field) {
  std::string translated = translate_key(type, key) + "#" + field;
  return translated;
}

bool my_strtoll(const std::string &str, int64_t &ret) {
  char *endptr = NULL;
  ret = strtoll(str.c_str(), &endptr, 10);
  if(endptr != str.c_str() + str.size() || ret == LLONG_MIN || ret == LONG_LONG_MAX) {
    return false;
  }
  return true;
}

// merge operator for additions to provide atomic incrby
class Int64AddOperator : public AssociativeMergeOperator {
public:
  virtual bool Merge(const Slice& key, const Slice* existing_value, const Slice& value,
                     std::string* new_value, Logger* logger) const override {
    // there's no decent way to do error reporting to the client
    // inside a rocksdb Merge operator, partially also because
    // the method is applied asynchronously and might not be run
    // until the next Get on this key!!
    //
    // ignore all errors and return true, without modifying the value.
    // returning false here corrupts the key entirely!
    // all sanity checking should be done in the client code calling merge

    // assuming 0 if no existing value
    int64_t existing = 0;
    if(existing_value) {
      if(!my_strtoll(existing_value->ToString(), existing)) {
        *new_value = existing_value->ToString();
        return true;
      }
    }

    int64_t oper;
    if(!my_strtoll(value.ToString(), oper)) {
      // this should not happen under any circumstances..
      *new_value = existing_value->ToString();
      return true;
    }

    int64_t newval = existing + oper;
    std::stringstream ss;
    ss << newval;
    *new_value = ss.str();
    return true;
  }

  virtual const char* Name() const override {
    return "Int64AddOperator";
  }
};

XrdRedisStatus XrdRedisRocksDB::initialize(const std::string &filename) {
  std::cout << "constructing redis rocksdb backend" << std::endl;
  rocksdb::Options options;
  options.merge_operator.reset(new Int64AddOperator);
  options.create_if_missing = true;
  rocksdb::Status status = rocksdb::DB::Open(options, filename, &db);
  return status_convert(status);
}

XrdRedisRocksDB::XrdRedisRocksDB() {
  db = nullptr;
}

XrdRedisRocksDB::~XrdRedisRocksDB() {
  if(db) {
    std::cout << "Closing connection to rocksdb" << std::endl;
    delete db;
    db = nullptr;
  }
}

XrdRedisStatus XrdRedisRocksDB::hget(const std::string &key, const std::string &field, std::string &value) {
  std::string tkey = translate_key(kHash, key, field);
  return status_convert(db->Get(rocksdb::ReadOptions(), tkey, &value));
}

XrdRedisStatus XrdRedisRocksDB::hexists(const std::string &key, const std::string &field) {
  std::string tkey = translate_key(kHash, key, field);

  std::string value;
  return status_convert(db->Get(rocksdb::ReadOptions(), tkey, &value));
}

// strncmp should NOT be used as the strings can contain embedded null bytes
bool startswith(const std::string &str, const std::string &prefix) {
  if(prefix.size() > str.size()) return false;

  for(size_t i = 0; i < prefix.size(); i++) {
    if(str[i] != prefix[i]) return false;
  }
  return true;
}

XrdRedisStatus XrdRedisRocksDB::hkeys(const std::string &key, std::vector<std::string> &keys) {
  std::string tkey = translate_key(kHash, key) + "#";
  auto iter = db->NewIterator(rocksdb::ReadOptions());
  for(iter->Seek(tkey); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    if(!startswith(tmp, tkey)) break;
    keys.push_back(std::string(tmp.begin()+tkey.size(), tmp.end()));
  }
  return OK();
}

XrdRedisStatus XrdRedisRocksDB::hgetall(const std::string &key, std::vector<std::string> &res) {
  std::string tkey = translate_key(kHash, key) + "#";
  auto iter = db->NewIterator(rocksdb::ReadOptions());
  for(iter->Seek(tkey); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    if(!startswith(tmp, tkey)) break;
    res.push_back(std::string(tmp.begin()+tkey.size(), tmp.end()));
    res.push_back(iter->value().ToString());
  }
  return OK();
}

XrdRedisStatus XrdRedisRocksDB::hset(const std::string &key, const std::string &field, const std::string &value) {
  std::string tkey = translate_key(kHash, key, field);
  rocksdb::Status st = db->Put(rocksdb::WriteOptions(), tkey, value);
  return status_convert(st);
}

XrdRedisStatus XrdRedisRocksDB::hincrby(const std::string &key, const std::string &field, const std::string &incrby, int64_t &result) {
  std::string tkey = translate_key(kHash, key, field);

  int64_t tmp;
  if(!my_strtoll(incrby, tmp)) {
    return XrdRedisStatus(rocksdb::Status::kNotSupported, "value is not an integer or out of range");
  }

  rocksdb::Status st = db->Merge(WriteOptions(), tkey, incrby);
  if(!st.ok()) return status_convert(st);

  std::string value;
  st = db->Get(rocksdb::ReadOptions(), tkey, &value);

  if(!my_strtoll(value, result)) {
    // This can occur under two circumstances: The value in tkey was not an
    // integer in the first place, and the Merge operation had no effects on it.

    // It could also happen if the Merge operation was successful, but then
    // afterwards another request came up and set tkey to a non-integer.
    // Even in this case the redis semantics are not violated - we just pretend
    // this request was processed after the other thread modified the key to a
    // non-integer.

    return XrdRedisStatus(rocksdb::Status::kNotSupported, "hash value is not an integer");
  }

  // RACE CONDITION: An OK() can be erroneous in the following scenario:
  // original value was "aaa"
  // HINCRBY called to increase by 1
  // Merge operation failed and did not modify the value at all
  // Another thread came by and set the value to "5"
  // Now, this thread sees an integer and thinks its merge operation was successful,
  // happily reporting "5" to the user.
  //
  // Unfortunately, the semantics of rocksdb makes this very difficult to avoid
  // without an extra layer of synchronization on top of it..

  return OK();
}

XrdRedisStatus XrdRedisRocksDB::hdel(const std::string &key, const std::string &field) {
  std::string tkey = translate_key(kHash, key, field);

  // race condition
  std::string value;
  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), tkey, &value);
  if(!st.ok()) return status_convert(st);

  return status_convert(db->Delete(rocksdb::WriteOptions(), tkey));
}

XrdRedisStatus XrdRedisRocksDB::hlen(const std::string &key, size_t &len) {
  len = 0;

  std::string tkey = translate_key(kHash, key) + "#";
  auto iter = db->NewIterator(rocksdb::ReadOptions());
  for(iter->Seek(tkey); iter->Valid(); iter->Next()) {
    if(!startswith(iter->key().ToString(), tkey)) break;
    len++;
  }

  return OK();
}

XrdRedisStatus XrdRedisRocksDB::hvals(const std::string &key, std::vector<std::string> &vals) {
  std::string tkey = translate_key(kHash, key) + "#";
  auto iter = db->NewIterator(rocksdb::ReadOptions());
  for(iter->Seek(tkey); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    if(!startswith(tmp, tkey)) break;
    vals.push_back(tmp);
  }
  return OK();
}

XrdRedisStatus XrdRedisRocksDB::sadd(const std::string &key, const std::string &element, int &added) {
  std::string tkey = translate_key(kSet, key, element);

  std::string tmp;
  rocksdb::Status st = db->Get(ReadOptions(), tkey, &tmp);
  if(st.IsNotFound()) {
    added++;
    return status_convert(db->Put(WriteOptions(), tkey, "1"));
  }

  return status_convert(st);
}

XrdRedisStatus XrdRedisRocksDB::sismember(const std::string &key, const std::string &element) {
  std::string tkey = translate_key(kSet, key, element);

  std::string tmp;
  return status_convert(db->Get(ReadOptions(), tkey, &tmp));
}

XrdRedisStatus XrdRedisRocksDB::srem(const std::string &key, const std::string &element) {
  std::string tkey = translate_key(kSet, key, element);

  // race condition
  std::string value;
  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), tkey, &value);
  if(!st.ok()) return status_convert(st);

  return status_convert(db->Delete(rocksdb::WriteOptions(), tkey));
}

XrdRedisStatus XrdRedisRocksDB::smembers(const std::string &key, std::vector<std::string> &members) {
  std::string tkey = translate_key(kSet, key) + "#";

  auto iter = db->NewIterator(rocksdb::ReadOptions());
  for(iter->Seek(tkey); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    if(!startswith(tmp, tkey)) break;
    members.push_back(std::string(tmp.begin()+tkey.size(), tmp.end()));
  }
  return OK();
}

XrdRedisStatus XrdRedisRocksDB::scard(const std::string &key, size_t &count) {
  std::string tkey = translate_key(kSet, key) + "#";
  count = 0;

  auto iter = db->NewIterator(rocksdb::ReadOptions());
  for(iter->Seek(tkey); iter->Valid(); iter->Next()) {
    if(!startswith(iter->key().ToString(), tkey)) break;
    count++;
  }
  return OK();
}

XrdRedisStatus XrdRedisRocksDB::set(const std::string& key, const std::string& value) {
  std::string tkey = translate_key(kString, key);

  rocksdb::Status st = db->Put(rocksdb::WriteOptions(), tkey, value);
  if(!st.ok()) return status_convert(st);
  return OK();
}

XrdRedisStatus XrdRedisRocksDB::get(const std::string &key, std::string &value) {
  std::string tkey = translate_key(kString, key);

  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), tkey, &value);
  if(!st.ok()) return status_convert(st);
  return OK();
}


// if 0 keys are found matching prefix, it'll return kOk, not kNotFound!!
XrdRedisStatus XrdRedisRocksDB::remove_all_with_prefix(const std::string &prefix) {
  std::string tmp;

  auto iter = db->NewIterator(rocksdb::ReadOptions());
  for(iter->Seek(prefix); iter->Valid(); iter->Next()) {
    std::string key = iter->key().ToString();
    if(!startswith(key, prefix)) break;
    // if(strncmp(key.c_str(), prefix.c_str(), prefix.size()) != 0) break;
    rocksdb::Status st = db->Delete(WriteOptions(), key);
    if(!st.ok()) return status_convert(st);
  }
  return OK();
}

XrdRedisStatus XrdRedisRocksDB::del(const std::string &key) {
  std::string tmp;

  // is it a string?
  std::string str_key = translate_key(kString, key);
  XrdRedisStatus st = status_convert(db->Get(rocksdb::ReadOptions(), str_key, &tmp));
  if(st.ok()) return remove_all_with_prefix(str_key);

  // is it a hash?
  std::string hash_key = translate_key(kHash, key) + "#";
  auto iter = db->NewIterator(rocksdb::ReadOptions());
  iter->Seek(hash_key);
  if(iter->Valid() && startswith(iter->key().ToString(), hash_key)) {
    return remove_all_with_prefix(hash_key);
  }

  // is it a set?
  std::string set_key = translate_key(kSet, key) + "#";
  iter = db->NewIterator(rocksdb::ReadOptions());
  iter->Seek(set_key);
  if(iter->Valid() && startswith(iter->key().ToString(), set_key)) {
    return remove_all_with_prefix(set_key);
  }

  return XrdRedisStatus(rocksdb::Status::kNotFound, "");
}

XrdRedisStatus XrdRedisRocksDB::exists(const std::string &key) {
  std::string tmp;

  // is it a string?
  std::string str_key = translate_key(kString, key);
  XrdRedisStatus st = status_convert(db->Get(rocksdb::ReadOptions(), str_key, &tmp));
  if(st.ok() || !st.IsNotFound()) return st;

  // is it a hash?
  std::string hash_key = translate_key(kHash, key) + "#";
  auto iter = db->NewIterator(rocksdb::ReadOptions());
  iter->Seek(hash_key);
  if(iter->Valid() && startswith(iter->key().ToString(), hash_key)) {
    return OK();
  }

  // is it a set?
  std::string set_key = translate_key(kSet, key) + "#";
  iter = db->NewIterator(rocksdb::ReadOptions());
  iter->Seek(set_key);
  if(iter->Valid() && startswith(iter->key().ToString(), set_key)) {
    return OK();
  }

  return XrdRedisStatus(rocksdb::Status::kNotFound, "");
}

XrdRedisStatus XrdRedisRocksDB::keys(const std::string &pattern, std::vector<std::string> &result) {
  bool allkeys = (pattern.length() == 1 && pattern[0] == '*');
  auto iter = db->NewIterator(rocksdb::ReadOptions());
  std::string previous;
  for(iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string tmp = iter->key().ToString();
    std::string redis_key = extract_key(tmp);

    if(redis_key != previous) {
      if(allkeys || XrdRedis_stringmatchlen(pattern.c_str(), pattern.length(),
                                            redis_key.c_str(), redis_key.length(), 0)) {
        result.push_back(redis_key);
      }
    }
    previous = redis_key;
  }

  return OK();
}

XrdRedisStatus XrdRedisRocksDB::flushall() {
  return remove_all_with_prefix("");
}
