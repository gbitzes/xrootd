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
  return XrdRedisStatus();
}

XrdRedisRocksDB::XrdRedisRocksDB(const std::string &filename) {
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status status = rocksdb::DB::Open(options, filename, &db);
  assert(status.ok());
}

std::string XrdRedisRocksDB::hget(const std::string &key, const std::string &field) {
  return store[key][field];
}

bool XrdRedisRocksDB::hexists(const std::string &key, const std::string &field) {
  return store[key].find(field) != store[key].end();
}

std::vector<std::string> XrdRedisRocksDB::hkeys(const std::string &key) {
  std::vector<std::string> ret;
  for(std::map<std::string, std::string>::iterator it = store[key].begin(); it != store[key].end(); it++) {
    ret.push_back(it->first);
  }
  return ret;
}

std::vector<std::string> XrdRedisRocksDB::hgetall(const std::string &key) {
  std::vector<std::string> ret;
  for(std::map<std::string, std::string>::iterator it = store[key].begin(); it != store[key].end(); it++) {
    ret.push_back(it->first);
    ret.push_back(it->second);
  }
  return ret;
}

void XrdRedisRocksDB::hset(const std::string &key, const std::string &field, const std::string &value) {
  store[key][field] = value;
}

bool XrdRedisRocksDB::hincrby(const std::string &key, const std::string &field, long long incrby, long long &result) {
  long long num = 0;
  if(this->hexists(key, field)) {
    const std::string &value = this->hget(key, field);
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

int XrdRedisRocksDB::hdel(const std::string &key, const std::string &field) {
  int count = 0;
  if(store[key].find(field) != store[key].end()) {
    store[key].erase(field);
    count++;
  }
  return count;
}

int XrdRedisRocksDB::hlen(const std::string &key) {
  return store[key].size();
}

std::vector<std::string> XrdRedisRocksDB::hvals(const std::string &key) {
  std::vector<std::string> ret;
  for(std::map<std::string, std::string>::iterator it = store[key].begin(); it != store[key].end(); it++) {
    ret.push_back(it->second);
  }
  return ret;
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
  return hkeys(key);
}

int XrdRedisRocksDB::scard(const std::string &key) {
  return store[key].size();
}

XrdRedisStatus XrdRedisRocksDB::set(const std::string& key, const std::string& value) {
  rocksdb::Status st = db->Put(rocksdb::WriteOptions(), key, value);
  if(!st.ok()) return status_convert(st);
  return OK();
}

XrdRedisStatus XrdRedisRocksDB::get(const std::string &key, std::string &value) {
  rocksdb::Status st = db->Get(rocksdb::ReadOptions(), key, &value);
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
