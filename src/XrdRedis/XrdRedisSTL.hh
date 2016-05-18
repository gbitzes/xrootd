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

#ifndef __XRDREDIS_STL_H__
#define __XRDREDIS_STL_H__

#include "XrdRedisBackend.hh"
#include <map>

class XrdRedisSTL : public XrdRedisBackend {
public:
  void hset(const std::string &key, const std::string &field, const std::string &value);
  std::string hget(const std::string &key, const std::string &field);
  bool hexists(const std::string &key, const std::string &field);
  std::vector<std::string> hkeys(const std::string &key);
  std::vector<std::string> hgetall(const std::string &key);
  bool hincrby(const std::string &key, const std::string &field, long long incrby, long long &result);
  int hdel(const std::string &key, const std::string &field);
  int hlen(const std::string &key);
  std::vector<std::string> hvals(const std::string &key);

  void set(const std::string &key, const std::string &value);
  std::string get(const std::string &key);
  bool exists(const std::string &key);
  int del(const std::string &key);
  std::vector<std::string> keys(const std::string &pattern);

  int sadd(const std::string &key, const std::string &element);
  bool sismember(const std::string &key, const std::string &element);
  int srem(const std::string &key, const std::string &element);
  std::vector<std::string> smembers(const std::string &key);
  int scard(const std::string &key);
private:
  std::map<std::string, std::map<std::string, std::string> > store;
};

#endif
