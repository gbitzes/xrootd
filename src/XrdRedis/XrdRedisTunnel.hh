//------------------------------------------------------------------------------
// This file is part of XrdRedis: A Redis-like server implementation
//
// Copyright (c) 2016 by European Organization for Nuclear Research (CERN)
// Author: Georgios Bitzes <georgios.bitzes@cern.ch>
// File Date: July 2016
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

#ifndef __XRDREDIS_TUNNEL_H__
#define __XRDREDIS_TUNNEL_H__

#include "XrdRedisBackend.hh"
#include "XrdRedisQueue.hh"
#include <hiredis.h>
#include <mutex>
#include <memory>

typedef std::shared_ptr<redisReply> redisReplyPtr;

class XrdRedisConnection {
public:
  XrdRedisConnection(std::string _ip, int _port);
  ~XrdRedisConnection();

  XrdRedisStatus ensureConnected();
  void clearConnection();
  redisContext* ctx;

  XrdRedisStatus received_unexpected_reply(const redisReplyPtr reply);
  XrdRedisStatus received_null_reply();

  template <class ...Args>
  redisReplyPtr execute(const Args & ... args);
private:
  std::string ip;
  int port;
};

class XrdRedisConnectionPool {
public:
  XrdRedisConnection* acquire();
  void release(XrdRedisConnection*);

  XrdRedisConnectionPool(std::string _ip, int _port, size_t _size);
private:
  XrdRedisConnectionPool(const XrdRedisConnectionPool&) = delete;
  XrdRedisConnectionPool& operator=(const XrdRedisConnectionPool&) = delete;

  XrdRedisQueue<XrdRedisConnection*> connections;
  XrdRedisConnection* create();

  std::string ip;
  int port;
  size_t size;
};

class XrdRedisConnectionGrabber {
public:
  XrdRedisConnectionGrabber(XrdRedisConnectionPool &pool);
  ~XrdRedisConnectionGrabber();

  XrdRedisConnection* operator->() const {
    return conn;
  }
private:
  XrdRedisConnectionGrabber(const XrdRedisConnectionGrabber&) = delete;
  XrdRedisConnectionGrabber& operator=(const XrdRedisConnectionGrabber&) = delete;

  XrdRedisConnectionPool &pool;
  XrdRedisConnection *conn;
};

class XrdRedisTunnel : public XrdRedisBackend {
public:

  XrdRedisStatus hset(const std::string &key, const std::string &field, const std::string &value);
  XrdRedisStatus hget(const std::string &key, const std::string &field, std::string &value);
  XrdRedisStatus hexists(const std::string &key, const std::string &field);
  XrdRedisStatus hkeys(const std::string &key, std::vector<std::string> &keys);
  XrdRedisStatus hgetall(const std::string &key, std::vector<std::string> &res);
  XrdRedisStatus hincrby(const std::string &key, const std::string &field, const std::string &incrby, int64_t &result);
  XrdRedisStatus hdel(const std::string &key, const std::string &field);
  XrdRedisStatus hlen(const std::string &key, size_t &len);
  XrdRedisStatus hvals(const std::string &key, std::vector<std::string> &vals);

  XrdRedisStatus set(const std::string &key, const std::string &value);
  XrdRedisStatus get(const std::string &key, std::string &value);
  XrdRedisStatus exists(const std::string &key);
  XrdRedisStatus del(const std::string &key);
  XrdRedisStatus keys(const std::string &pattern, std::vector<std::string> &result);

  XrdRedisStatus sadd(const std::string &key, const std::string &element, int64_t &added);
  XrdRedisStatus sismember(const std::string &key, const std::string &element);
  XrdRedisStatus srem(const std::string &key, const std::string &element);
  XrdRedisStatus smembers(const std::string &key, std::vector<std::string> &members);
  XrdRedisStatus scard(const std::string &key, size_t &count);

  XrdRedisStatus ping();
  XrdRedisStatus flushall();

  XrdRedisTunnel(const std::string &ip, const int port, size_t nconnections=10);
  ~XrdRedisTunnel();
private:
  std::string ip;
  int port;

  XrdRedisConnectionPool pool;

  template <class ...Args>
  XrdRedisStatus expect_list(std::vector<std::string> &vec, const std::string &cmd, const Args & ... args);

  template <class IntegerType, class ...Args>
  XrdRedisStatus expect_integer(IntegerType &value, const std::string &cmd, const Args & ... args);

  template <class ...Args>
  XrdRedisStatus expect_str(std::string &value, const std::string &cmd, const Args & ... args);

  template <class ...Args>
  XrdRedisStatus expect_ok(const std::string &cmd, const Args & ... args);

  template <class ...Args>
  XrdRedisStatus expect_pong(const std::string &cmd, const Args & ... args);

  template <class ...Args>
  XrdRedisStatus expect_exists(const std::string &cmd, const Args & ... args);
};

#endif
