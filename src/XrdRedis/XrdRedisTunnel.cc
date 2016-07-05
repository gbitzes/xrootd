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

#include "XrdRedisTunnel.hh"
#include <sstream>

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()
//
#define LOCK_AND_CONNECT() std::lock_guard<std::mutex> lock(this->mtx); \
                         { XrdRedisStatus st = this->ensureConnected(); \
                           if(!st.ok()) return st; }

static XrdRedisStatus OK() {
  return XrdRedisStatus(rocksdb::Status::kOk);
}

XrdRedisStatus XrdRedisTunnel::received_unexpected_reply(const redisReplyPtr reply) {
  return XrdRedisStatus(rocksdb::Status::kAborted, "Unexpected reply");
}

XrdRedisStatus XrdRedisTunnel::received_null_reply() {
  std::string err = SSTR("Context error when talking to " << ip << ":" << port << ": " << ctx->errstr);
  return XrdRedisStatus(
    rocksdb::Status::kAborted,
    err
  );
}

typedef XrdRedisTunnel::redisReplyPtr redisReplyPtr;

XrdRedisStatus XrdRedisTunnel::expect_ok(const redisReplyPtr reply) {
  if(reply == NULL) return received_null_reply();

  if(reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") == 0) {
    return OK();
  }
  return received_unexpected_reply(reply);
}

XrdRedisStatus XrdRedisTunnel::expect_str(const redisReplyPtr reply, std::string &value) {
  if(reply == NULL) return received_null_reply();

  if(reply->type == REDIS_REPLY_STRING) {
    value = std::string(reply->str, reply->len);
    return OK();
  }
  else if(reply->type == REDIS_REPLY_NIL) {
    return XrdRedisStatus(rocksdb::Status::kNotFound, "");
  }

  return received_unexpected_reply(reply);
}

XrdRedisStatus XrdRedisTunnel::expect_size(const redisReplyPtr reply, size_t &value) {
  if(reply == NULL) return received_null_reply();

  if(reply->type == REDIS_REPLY_INTEGER) {
    value = reply->integer;
    return OK();
  }
  return received_unexpected_reply(reply);
}

XrdRedisStatus XrdRedisTunnel::expect_int64(const redisReplyPtr reply, int64_t &value) {
  if(reply == NULL) return received_null_reply();

  if(reply->type == REDIS_REPLY_INTEGER) {
    value = reply->integer;
    return OK();
  }
  return received_unexpected_reply(reply);
}


XrdRedisStatus XrdRedisTunnel::expect_exists(const redisReplyPtr reply) {
  if(reply == NULL) return received_null_reply();

  if(reply->type == REDIS_REPLY_INTEGER) {
    if(reply->integer == 0) return XrdRedisStatus(rocksdb::Status::kNotFound, "");
    if(reply->integer == 1) return OK();
  }

  return received_unexpected_reply(reply);
}

XrdRedisStatus XrdRedisTunnel::expect_list(const redisReplyPtr reply, std::vector<std::string> &vec) {
  if(reply == NULL) return received_null_reply();

  if(reply->type == REDIS_REPLY_ARRAY) {
    for(size_t i = 0; i < reply->elements; i++) {
      if(reply->element[i]->type != REDIS_REPLY_STRING) return received_unexpected_reply(reply);
      vec.push_back(std::string(reply->element[i]->str, reply->element[i]->len));
    }
    return OK();
  }
  return received_unexpected_reply(reply);
}

static redisReplyPtr redisCommandPtr(redisContext *ctx, const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  redisReply *reply = (redisReply*) redisvCommand(ctx, fmt, args);
  va_end(args);

  return redisReplyPtr(reply, freeReplyObject);
}

XrdRedisStatus XrdRedisTunnel::ensureConnected() {
  if(ctx && ctx->err) clearConnection();

  if(!ctx) {
    ctx = redisConnect(ip.c_str(), port);
  }

  if(!ctx) {
    return XrdRedisStatus(rocksdb::Status::kAborted, "Can't allocate hiredis context");
  }

  if(ctx->err) {
    XrdRedisStatus ret = XrdRedisStatus(rocksdb::Status::kAborted, SSTR("Error connecting to " << ip << ":" << port << ". " << ctx->errstr));
    clearConnection();
    return ret;
  }

  return OK();
}

void XrdRedisTunnel::clearConnection() {
  if(ctx) {
    redisFree(ctx);
    ctx = nullptr;
  }
}

XrdRedisTunnel::XrdRedisTunnel(const std::string &ip_, const int port_): ip(ip_), port(port_) {
  ctx = nullptr;
}

XrdRedisTunnel::~XrdRedisTunnel() {
  clearConnection();
}

XrdRedisStatus XrdRedisTunnel::set(const std::string &key, const std::string &value) {
  LOCK_AND_CONNECT();
  return expect_ok(forward("SET %b %b", key, value));
}

XrdRedisStatus XrdRedisTunnel::get(const std::string &key, std::string &value) {
  LOCK_AND_CONNECT();
  return expect_str(forward("GET %b", key), value);
}

XrdRedisStatus XrdRedisTunnel::exists(const std::string &key) {
  LOCK_AND_CONNECT();
  return expect_exists(forward("EXISTS %b", key));
}

XrdRedisStatus XrdRedisTunnel::del(const std::string &key) {
  LOCK_AND_CONNECT();
  return expect_ok(forward("DEL %b", key));
}

XrdRedisStatus XrdRedisTunnel::keys(const std::string &pattern, std::vector<std::string> &result) {
  LOCK_AND_CONNECT();
  return expect_list(forward("KEYS %b", pattern), result);
}

XrdRedisStatus XrdRedisTunnel::sadd(const std::string &key, const std::string &element, int64_t &added) {
  LOCK_AND_CONNECT();
  return expect_int64(forward("SADD %b %b", key, element), added);
}

XrdRedisStatus XrdRedisTunnel::sismember(const std::string &key, const std::string &element) {
  LOCK_AND_CONNECT();
  return expect_exists(forward("SISMEMBER %b %b", key, element));
}

XrdRedisStatus XrdRedisTunnel::srem(const std::string &key, const std::string &element) {
  LOCK_AND_CONNECT();
  return expect_exists(forward("SREM %b %b", key, element));
}

XrdRedisStatus XrdRedisTunnel::smembers(const std::string &key, std::vector<std::string> &members) {
  LOCK_AND_CONNECT();
  return expect_list(forward("SMEMBERS %b", key), members);
}

XrdRedisStatus XrdRedisTunnel::scard(const std::string &key, size_t &count) {
  LOCK_AND_CONNECT();
  return expect_size(forward("SCARD %b", key), count);
}

XrdRedisStatus XrdRedisTunnel::flushall() {
  LOCK_AND_CONNECT();
  return expect_ok(forward("FLUSHALL"));
}

XrdRedisStatus XrdRedisTunnel::hset(const std::string &key, const std::string &field, const std::string &value) {
  LOCK_AND_CONNECT();
  // TODO: change interface so both check and set are performed in this function, not redisProtocol!!!!!!
  int64_t tmp;
  return expect_int64(forward("HSET %b %b %b", key, field, value), tmp);
}

XrdRedisStatus XrdRedisTunnel::hget(const std::string &key, const std::string &field, std::string &value) {
  LOCK_AND_CONNECT();
  return expect_str(forward("HGET %b %b", key, field), value);
}

XrdRedisStatus XrdRedisTunnel::hexists(const std::string &key, const std::string &field) {
  LOCK_AND_CONNECT();
  return expect_exists(forward("HEXISTS %b %b", key, field));
}

XrdRedisStatus XrdRedisTunnel::hkeys(const std::string &key, std::vector<std::string> &keys) {
  LOCK_AND_CONNECT();
  return expect_list(forward("HKEYS %b", key), keys);
}

XrdRedisStatus XrdRedisTunnel::hgetall(const std::string &key, std::vector<std::string> &res) {
  LOCK_AND_CONNECT();
  return expect_list(forward("HGETALL %b", key), res);
}

XrdRedisStatus XrdRedisTunnel::hincrby(const std::string &key, const std::string &field, const std::string &incrby, int64_t &result) {
  LOCK_AND_CONNECT();
  return expect_int64(forward("HINCRBY %b %b %b", key, field, incrby), result);
}

XrdRedisStatus XrdRedisTunnel::hdel(const std::string &key, const std::string &field) {
  LOCK_AND_CONNECT();
  return expect_ok(forward("HDEL %b %b", key, field));
}

XrdRedisStatus XrdRedisTunnel::hlen(const std::string &key, size_t &len) {
  LOCK_AND_CONNECT();
  return expect_size(forward("HLEN %b", key), len);
}

XrdRedisStatus XrdRedisTunnel::hvals(const std::string &key, std::vector<std::string> &vals) {
  LOCK_AND_CONNECT();
  return expect_list(forward("HVALS %b", key), vals);
}

// a variadic template function could be used, but needs crazy template metaprogramming
// to juggle the c_str() and size() arguments.
// It's probably simpler this way.
redisReplyPtr XrdRedisTunnel::forward(const char *fmt) {
  return redisCommandPtr(ctx, fmt);
}

redisReplyPtr XrdRedisTunnel::forward(const char *fmt, const std::string &s1) {
  return redisCommandPtr(ctx, fmt, s1.c_str(), s1.size());
}

redisReplyPtr XrdRedisTunnel::forward(const char *fmt, const std::string &s1, const std::string &s2) {
  return redisCommandPtr(ctx, fmt, s1.c_str(), s1.size(), s2.c_str(), s2.size());
}

redisReplyPtr XrdRedisTunnel::forward(const char *fmt, const std::string &s1, const std::string &s2, const std::string &s3) {
  return redisCommandPtr(ctx, fmt, s1.c_str(), s1.size(), s2.c_str(), s2.size(), s3.c_str(), s3.size());
}
