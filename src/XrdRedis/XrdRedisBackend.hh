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
#include <rocksdb/status.h>
#include <iostream>

/******************************************************************************/
/*                               D e f i n e s                                */
/******************************************************************************/

class XrdRedisStatus {
public:
  XrdRedisStatus(int code, const std::string &err) : code_(code), error_(err) {
  }

  XrdRedisStatus(int code) : code_(code) {}
  XrdRedisStatus() {}

  std::string ToString() const {
    return error_;
  }

  int code() const {
    return code_;
  }

  bool ok() const {
    return code_ == rocksdb::Status::kOk;
    // return error_.empty();
  }

  bool IsNotFound() const {
    return code_ == rocksdb::Status::kNotFound;
  }

private:
  int code_;
  std::string error_;
};

// interface for a redis backend
class XrdRedisBackend {
public:
  virtual XrdRedisStatus set(const std::string &key, const std::string &value) = 0;
  virtual XrdRedisStatus get(const std::string &key, std::string &value) = 0;
  virtual XrdRedisStatus exists(const std::string &key) = 0;
  virtual XrdRedisStatus del(const std::string &key) = 0;
  virtual XrdRedisStatus keys(const std::string &pattern, std::vector<std::string> &result) = 0;

  virtual XrdRedisStatus hset(const std::string &key, const std::string &field, const std::string &value) = 0;
  virtual XrdRedisStatus hget(const std::string &key, const std::string &field, std::string &value) = 0;
  virtual XrdRedisStatus hexists(const std::string &key, const std::string &field) = 0;
  virtual XrdRedisStatus hkeys(const std::string &key, std::vector<std::string> &keys) = 0;
  virtual XrdRedisStatus hgetall(const std::string &key, std::vector<std::string> &res) = 0;
  virtual XrdRedisStatus hincrby(const std::string &key, const std::string &field, const std::string &incrby, int64_t &result) = 0;
  virtual XrdRedisStatus hdel(const std::string &key, const std::string &field) = 0;
  virtual XrdRedisStatus hlen(const std::string &key, size_t &len) = 0;
  virtual XrdRedisStatus hvals(const std::string &key, std::vector<std::string> &vals) = 0;

  virtual XrdRedisStatus sadd(const std::string &key, const std::string &element, int64_t &added) = 0;
  virtual XrdRedisStatus sismember(const std::string &key, const std::string &element) = 0;
  virtual XrdRedisStatus srem(const std::string &key, const std::string &element) = 0;
  virtual XrdRedisStatus smembers(const std::string &key, std::vector<std::string> &members) = 0;
  virtual XrdRedisStatus scard(const std::string &key, size_t &count) = 0;

  virtual XrdRedisStatus flushall() = 0;
  virtual XrdRedisStatus ping() = 0;

  virtual ~XrdRedisBackend() {}
};



#endif
