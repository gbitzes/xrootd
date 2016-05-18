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

std::string XrdRedisSTL::hget(const std::string &key, const std::string &field) {
  return store[key][field];
}

bool XrdRedisSTL::hexists(const std::string &key, const std::string &field) {
  return store[key].find(field) != store[key].end();
}

std::vector<std::string> XrdRedisSTL::hkeys(const std::string &key) {
  std::vector<std::string> ret;
  for(std::map<std::string, std::string>::iterator it = store[key].begin(); it != store[key].end(); it++) {
    ret.push_back(it->first);
  }
  return ret;
}

std::vector<std::string> XrdRedisSTL::hgetall(const std::string &key) {
  std::vector<std::string> ret;
  for(std::map<std::string, std::string>::iterator it = store[key].begin(); it != store[key].end(); it++) {
    ret.push_back(it->first);
    ret.push_back(it->second);
  }
  return ret;
}

void XrdRedisSTL::hset(const std::string &key, const std::string &field, const std::string &value) {
  store[key][field] = value;
}

bool XrdRedisSTL::hincrby(const std::string &key, const std::string &field, long long incrby, long long &result) {
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

int XrdRedisSTL::hdel(const std::string &key, const std::string &field) {
  int count = 0;
  if(store[key].find(field) != store[key].end()) {
    store[key].erase(field);
    count++;
  }
  return count;
}

int XrdRedisSTL::hlen(const std::string &key) {
  return store[key].size();
}

std::vector<std::string> XrdRedisSTL::hvals(const std::string &key) {
  std::vector<std::string> ret;
  for(std::map<std::string, std::string>::iterator it = store[key].begin(); it != store[key].end(); it++) {
    ret.push_back(it->second);
  }
  return ret;
}

int XrdRedisSTL::sadd(const std::string &key, const std::string &element) {
  int count = 0;

  if(store[key].find(element) == store[key].end()) {
    count++;
    store[key][element] = 1;
  }

  return count;
}

bool XrdRedisSTL::sismember(const std::string &key, const std::string &element) {
  return store[key].find(element) != store[key].end();
}

int XrdRedisSTL::srem(const std::string &key, const std::string &element) {
  int count = 0;

  if(store[key].find(element) != store[key].end()) {
    store[key].erase(element);
    count++;
  }
  return count;
}

std::vector<std::string> XrdRedisSTL::smembers(const std::string &key) {
  return hkeys(key);
}

int XrdRedisSTL::scard(const std::string &key) {
  return store[key].size();
}

void XrdRedisSTL::set(const std::string& key, const std::string& value) {
  hset(key, "\0", value);
}

std::string XrdRedisSTL::get(const std::string &key) {
  return hget(key, "\0");
}

int XrdRedisSTL::del(const std::string &key) {
  if(store.find(key) != store.end()) {
    store.erase(key);
    return 1;
  }
  return 0;
}

bool XrdRedisSTL::exists(const std::string &key) {
  return store.find(key) != store.end();
}

std::vector<std::string> XrdRedisSTL::keys(const std::string &pattern) {
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
