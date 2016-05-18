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

#ifndef __XRDREDIS_BACKEND_H__
#define __XRDREDIS_BACKEND_H__

#include <string>
#include <vector>

/******************************************************************************/
/*                               D e f i n e s                                */
/******************************************************************************/

// interface for a redis backend
class XrdRedisBackend {
public:
  virtual void set(const std::string &key, const std::string &value) = 0;
  virtual std::string get(const std::string &key) = 0;
  virtual bool exists(const std::string &key) = 0;
  virtual int del(const std::string &key) = 0;

  virtual void hset(const std::string &key, const std::string &field, const std::string &value) = 0;
  virtual std::string hget(const std::string &key, const std::string &field) = 0;
  virtual bool hexists(const std::string &key, const std::string &field) = 0;
  virtual std::vector<std::string> hkeys(const std::string &key) = 0;
  virtual std::vector<std::string> hgetall(const std::string &key) = 0;
  virtual bool hincrby(const std::string &key, const std::string &field, long long incrby, long long &result) = 0;

  virtual int sadd(const std::string &key, const std::string &element) = 0;
  virtual bool sismember(const std::string &key, const std::string &element) = 0;
  virtual int srem(const std::string &key, const std::string &element) = 0;
  virtual std::vector<std::string> smembers(const std::string &key) = 0;
  virtual int scard(const std::string &key) = 0;

};



#endif
