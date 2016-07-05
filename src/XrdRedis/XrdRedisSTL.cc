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

#include "XrdRedisSTL.hh"
#include "XrdRedisUtil.hh"

#include <iostream>
#include <stdlib.h>
#include <sstream>
#include <climits>

static XrdRedisStatus OK() {
  return XrdRedisStatus(rocksdb::Status::kOk);
}

XrdRedisStatus XrdRedisSTL::hget(const std::string &key, const std::string &field, std::string &value) {
  value = store[key][field];
  return OK();
}

XrdRedisStatus XrdRedisSTL::hexists(const std::string &key, const std::string &field) {
  if(store[key].find(field) != store[key].end()) return OK();
  return XrdRedisStatus(rocksdb::Status::kNotFound, "");
}

XrdRedisStatus XrdRedisSTL::hkeys(const std::string &key, std::vector<std::string> &keys) {
  for(std::map<std::string, std::string>::iterator it = store[key].begin(); it != store[key].end(); it++) {
    keys.push_back(it->first);
  }
  return OK();
}

XrdRedisStatus XrdRedisSTL::hgetall(const std::string &key, std::vector<std::string> &res) {
  for(std::map<std::string, std::string>::iterator it = store[key].begin(); it != store[key].end(); it++) {
    res.push_back(it->first);
    res.push_back(it->second);
  }
  return OK();
}

XrdRedisStatus XrdRedisSTL::hset(const std::string &key, const std::string &field, const std::string &value) {
  store[key][field] = value;
  return OK();
}

XrdRedisStatus XrdRedisSTL::hincrby(const std::string &key, const std::string &field, const std::string &incrby, int64_t &result) {
  return OK();
  // long long num = 0;
  //
  // XrdRedisStatus st = this->hexists(key, field);
  // if(st.ok()) {
  //   std::string value;
  //   this->hget(key, field, value);
  //   char *endptr = NULL;
  //   num = strtoll(value.c_str(), &endptr, 10);
  //   if(*endptr != '\0' || num == LLONG_MIN || num == LONG_LONG_MAX) {
  //     return false;
  //   }
  // }
  //
  // result = num + incrby;
  // std::stringstream ss;
  // ss << result;
  // this->hset(key, field, ss.str());
  // return true;
}

XrdRedisStatus XrdRedisSTL::hdel(const std::string &key, const std::string &field) {
  if(store[key].find(field) != store[key].end()) {
    store[key].erase(field);
    return OK();
  }
  return XrdRedisStatus(rocksdb::Status::kNotFound, "");
}

XrdRedisStatus XrdRedisSTL::hlen(const std::string &key, size_t &len) {
  len = store[key].size();
  return OK();
}

XrdRedisStatus XrdRedisSTL::hvals(const std::string &key, std::vector<std::string> &vals) {
  for(std::map<std::string, std::string>::iterator it = store[key].begin(); it != store[key].end(); it++) {
    vals.push_back(it->second);
  }
  return OK();
}

XrdRedisStatus XrdRedisSTL::sadd(const std::string &key, const std::string &element, int64_t &added) {
  if(store[key].find(element) == store[key].end()) {
    added++;
    store[key][element] = 1;
  }

  return OK();
}

XrdRedisStatus XrdRedisSTL::sismember(const std::string &key, const std::string &element) {
  bool exists = store[key].find(element) != store[key].end();
  if(exists) return OK();
  return XrdRedisStatus(rocksdb::Status::kNotFound, "");
}

XrdRedisStatus XrdRedisSTL::srem(const std::string &key, const std::string &element) {
  if(store[key].find(element) != store[key].end()) {
    store[key].erase(element);
    return OK();
  }
  return XrdRedisStatus(rocksdb::Status::kNotFound, "");
}

XrdRedisStatus XrdRedisSTL::smembers(const std::string &key, std::vector<std::string> &members) {
  hkeys(key, members);
  return OK();
}

XrdRedisStatus XrdRedisSTL::scard(const std::string &key, size_t &count) {
  count = store[key].size();
  return OK();
}

XrdRedisStatus XrdRedisSTL::set(const std::string& key, const std::string& value) {
  hset(key, "\0", value);
  return OK();
}

XrdRedisStatus XrdRedisSTL::get(const std::string &key, std::string &value) {
  this->hget(key, "\0", value);
  return OK();
}

XrdRedisStatus XrdRedisSTL::del(const std::string &key) {
  if(store.find(key) != store.end()) {
    store.erase(key);
    return OK();
  }
  return XrdRedisStatus(rocksdb::Status::kNotFound, "");
}

XrdRedisStatus XrdRedisSTL::exists(const std::string &key) {
  if(store.find(key) != store.end()) return OK();
  return XrdRedisStatus(rocksdb::Status::kNotFound, "");
}

XrdRedisStatus XrdRedisSTL::keys(const std::string &pattern, std::vector<std::string> &result) {
  bool allkeys = (pattern[0] == '*' && pattern.length() == 1);
  for(std::map<std::string, std::map<std::string, std::string> >::iterator it = store.begin(); it != store.end(); it++) {
    const std::string &key = it->first;
    if(allkeys || XrdRedis_stringmatchlen(pattern.c_str(), pattern.length(),
                                          key.c_str(), key.length(), 0)) {
        result.push_back(key);
    }
  }
  return OK();
}

XrdRedisStatus XrdRedisSTL::flushall() {
  for(auto it = store.begin(); it != store.end(); it++) {
    it->second.clear();
  }
  return OK();
}
