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

static void escape(std::string &str, char target) {
  char replacement[3];
  replacement[0] = '\\';
  replacement[1] = target;
  replacement[2] = '\0';

  size_t pos = 0;
  while((pos = str.find(target, pos)) != std::string::npos) {
    str.replace(pos, 1, replacement);
    pos += 2;
  }
}

static std::string translate_key(const RedisType type, const std::string &key) {
  std::string escaped = key;
  escape(escaped, '#');

  return std::string(1, type) + escaped;
}

static std::string translate_key(const RedisType type, const std::string &key, const std::string &field) {
  std::string translated = translate_key(type, key) + "#" + field;
  std::cout << translated << std::endl;
  return translated;
}

XrdRedisRocksDB::XrdRedisRocksDB(const std::string &filename) {
  std::cout << "constructing redis rocksdb backend" << std::endl;
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status status = rocksdb::DB::Open(options, filename, &db);
  assert(status.ok());
}

XrdRedisRocksDB::~XrdRedisRocksDB() {
  std::cout << "Closing connection to rocksdb" << std::endl;
  delete db;
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

XrdRedisStatus XrdRedisRocksDB::hkeys(const std::string &key, std::vector<std::string> &keys) {
  std::string tkey = translate_key(kHash, key) + "#";
  auto iter = db->NewIterator(rocksdb::ReadOptions());
  for(iter->Seek(tkey); iter->Valid(); iter->Next()) {
    if(strncmp(iter->key().data(), tkey.c_str(), tkey.size()) != 0) break;
    keys.push_back(iter->key().data() + tkey.size());
  }
  return OK();
}

XrdRedisStatus XrdRedisRocksDB::hgetall(const std::string &key, std::vector<std::string> &res) {
  std::string tkey = translate_key(kHash, key) + "#";
  auto iter = db->NewIterator(rocksdb::ReadOptions());
  for(iter->Seek(tkey); iter->Valid(); iter->Next()) {
    if(strncmp(iter->key().data(), tkey.c_str(), tkey.size()) != 0) break;
    res.push_back(iter->key().data() + tkey.size());
    res.push_back(iter->value().ToString());
  }
  return OK();
}

XrdRedisStatus XrdRedisRocksDB::hset(const std::string &key, const std::string &field, const std::string &value) {
  std::string tkey = translate_key(kHash, key, field);
  rocksdb::Status st = db->Put(rocksdb::WriteOptions(), tkey, value);
  return status_convert(st);
}

bool XrdRedisRocksDB::hincrby(const std::string &key, const std::string &field, long long incrby, long long &result) {
  long long num = 0;
  XrdRedisStatus st = this->hexists(key, field);
  if(st.ok()) {
    std::string value;
    this->hget(key, field, value);
    char *endptr = NULL;
    num = strtoll(value.c_str(), &endptr, 10);
    if(*endptr != '\0' || num == LLONG_MIN || num == LONG_LONG_MAX) {
      return false;
    }
  }

  result = num + incrby;
  std::stringstream ss;
  ss << result;
  this->hset(key, field, ss.str());
  return true;
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
    if(strncmp(iter->key().data(), tkey.c_str(), tkey.size()) != 0) break;
    len++;
  }

  return OK();
}

XrdRedisStatus XrdRedisRocksDB::hvals(const std::string &key, std::vector<std::string> &vals) {
  std::string tkey = translate_key(kHash, key) + "#";
  auto iter = db->NewIterator(rocksdb::ReadOptions());
  for(iter->Seek(tkey); iter->Valid(); iter->Next()) {
    if(strncmp(iter->key().data(), tkey.c_str(), tkey.size()) != 0) break;
    vals.push_back(iter->value().ToString());
  }
  return OK();
}

int XrdRedisRocksDB::sadd(const std::string &key, const std::string &element) {
  int count = 0;

  if(store[key].find(element) == store[key].end()) {
    count++;
    store[key][element] = 1;
  }

  return count;
}

bool XrdRedisRocksDB::sismember(const std::string &key, const std::string &element) {
  return store[key].find(element) != store[key].end();
}

int XrdRedisRocksDB::srem(const std::string &key, const std::string &element) {
  int count = 0;

  if(store[key].find(element) != store[key].end()) {
    store[key].erase(element);
    count++;
  }
  return count;
}

std::vector<std::string> XrdRedisRocksDB::smembers(const std::string &key) {
  return std::vector<std::string>();
  // return hkeys(key);
}

int XrdRedisRocksDB::scard(const std::string &key) {
  return store[key].size();
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

XrdRedisStatus XrdRedisRocksDB::del(const std::string &key) {
  XrdRedisStatus st = this->exists(key);
  if(!st.ok()) return st;
  return status_convert(db->Delete(rocksdb::WriteOptions(), key));
}

XrdRedisStatus XrdRedisRocksDB::exists(const std::string &key) {
  std::string value;
  return status_convert(db->Get(rocksdb::ReadOptions(), key, &value));
}

std::vector<std::string> XrdRedisRocksDB::keys(const std::string &pattern) {
  std::vector<std::string> ret;

  bool allkeys = (pattern[0] == '*' && pattern.length() == 1);
  for(std::map<std::string, std::map<std::string, std::string> >::iterator it = store.begin(); it != store.end(); it++) {
    const std::string &key = it->first;
    if(allkeys || XrdRedis_stringmatchlen(pattern.c_str(), pattern.length(),
                                          key.c_str(), key.length(), 0)) {
      ret.push_back(key);
    }
  }
  return ret;
}
