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
#include <hiredis.h>
#include <mutex>
#include <memory>

class XrdRedisTunnel : public XrdRedisBackend {
public:
  typedef std::shared_ptr<redisReply> redisReplyPtr;

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

  XrdRedisStatus flushall();

  XrdRedisTunnel(const std::string &ip, const int port);
  ~XrdRedisTunnel();
private:
  std::string ip;
  int port;

  redisContext *ctx;
  std::mutex mtx;

  XrdRedisStatus ensureConnected();
  void clearConnection();

  XrdRedisStatus received_unexpected_reply(const redisReplyPtr reply);
  XrdRedisStatus received_null_reply();
  XrdRedisStatus expect_ok(const redisReplyPtr reply);
  XrdRedisStatus expect_str(const redisReplyPtr reply, std::string &value);
  XrdRedisStatus expect_size(const redisReplyPtr reply, size_t &value);
  XrdRedisStatus expect_int64(const redisReplyPtr reply, int64_t &value);
  XrdRedisStatus expect_exists(const redisReplyPtr reply);
  XrdRedisStatus expect_list(const redisReplyPtr reply, std::vector<std::string> &vec);

  redisReplyPtr forward(const char *fmt);
  redisReplyPtr forward(const char *fmt, const std::string &s1);
  redisReplyPtr forward(const char *fmt, const std::string &s1, const std::string &s2);
  redisReplyPtr forward(const char *fmt, const std::string &s1, const std::string &s2, const std::string &s3);
};

#endif
