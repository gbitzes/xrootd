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

#define RUN_COMMAND(reply, conn, pool, cmd, args)               \
XrdRedisConnectionGrabber conn(pool);                           \
{ XrdRedisStatus st = conn->ensureConnected();                  \
  if(!st.ok()) return st; }                                     \
redisReplyPtr reply = conn->execute(cmd, args...);              \
if(reply == NULL) return conn->received_null_reply();           \

XrdRedisConnectionGrabber::XrdRedisConnectionGrabber(XrdRedisConnectionPool &_pool)
: pool(_pool), conn(pool.acquire()) { }

XrdRedisConnectionGrabber::~XrdRedisConnectionGrabber() {
  pool.release(conn);
}

static XrdRedisStatus OK() {
  return XrdRedisStatus(rocksdb::Status::kOk);
}

XrdRedisConnection::~XrdRedisConnection() {
  clearConnection();
}

XrdRedisConnection::XrdRedisConnection(std::string _ip, int _port)
: ctx(nullptr), ip(_ip), port(_port) {
  std::cout << "Creating connection.." << std::endl;
}

void XrdRedisConnection::clearConnection() {
  if(ctx) {
    redisFree(ctx);
    ctx = nullptr;
  }
}

XrdRedisStatus XrdRedisConnection::ensureConnected() {
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

template <class ...Args>
redisReplyPtr XrdRedisConnection::execute(const Args & ... args) {
  const char *cstr[] = { (args.c_str())... };
  size_t sizes[] = { (args.size())... };

  redisReply *reply = (redisReply*) redisCommandArgv(ctx, sizeof...(args), cstr, sizes);

  return redisReplyPtr(reply, freeReplyObject);
}

XrdRedisConnectionPool::XrdRedisConnectionPool(std::string _ip, int _port, size_t _size)
: ip(_ip), port(_port), size(_size) {

  for(size_t i = 0; i < _size; i++) {
    connections.enqueue(create());
  }
}

XrdRedisConnection* XrdRedisConnectionPool::create() {
  return new XrdRedisConnection(ip, port);
}

XrdRedisConnection* XrdRedisConnectionPool::acquire() {
  return connections.dequeue();
}

void XrdRedisConnectionPool::release(XrdRedisConnection *conn) {
  connections.enqueue(conn);
}

XrdRedisStatus XrdRedisConnection::received_unexpected_reply(const redisReplyPtr reply) {
  return XrdRedisStatus(rocksdb::Status::kAborted, "Unexpected reply");
}

XrdRedisStatus XrdRedisConnection::received_null_reply() {
  std::string err = SSTR("Context error when talking to " << ip << ":" << port << ": " << ctx->errstr);
  return XrdRedisStatus(
    rocksdb::Status::kAborted,
    err
  );
}

template <class ...Args>
XrdRedisStatus XrdRedisTunnel::expect_ok(const std::string &cmd, const Args & ... args) {
  RUN_COMMAND(reply, conn, pool, cmd, args);

  if(reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") == 0) {
    return OK();
  }
  return conn->received_unexpected_reply(reply);
}

template <class ...Args>
XrdRedisStatus XrdRedisTunnel::expect_str(std::string &value, const std::string &cmd, const Args & ... args) {
  RUN_COMMAND(reply, conn, pool, cmd, args);

  if(reply->type == REDIS_REPLY_STRING) {
    value = std::string(reply->str, reply->len);
    return OK();
  }
  else if(reply->type == REDIS_REPLY_NIL) {
    return XrdRedisStatus(rocksdb::Status::kNotFound, "");
  }

  return conn->received_unexpected_reply(reply);
}

template <class IntegerType, class ...Args>
XrdRedisStatus XrdRedisTunnel::expect_integer(IntegerType &value, const std::string &cmd, const Args & ... args) {
  RUN_COMMAND(reply, conn, pool, cmd, args);

  if(reply->type == REDIS_REPLY_INTEGER) {
    value = (IntegerType) reply->integer;
    return OK();
  }
  return conn->received_unexpected_reply(reply);
}

template <class ...Args>
XrdRedisStatus XrdRedisTunnel::expect_list(std::vector<std::string> &vec, const std::string &cmd, const Args & ... args) {
  RUN_COMMAND(reply, conn, pool, cmd, args);

  if(reply->type == REDIS_REPLY_ARRAY) {
    for(size_t i = 0; i < reply->elements; i++) {
      if(reply->element[i]->type != REDIS_REPLY_STRING) return conn->received_unexpected_reply(reply);
      vec.push_back(std::string(reply->element[i]->str, reply->element[i]->len));
    }
    return OK();
  }
  return conn->received_unexpected_reply(reply);
}

template <class ...Args>
XrdRedisStatus XrdRedisTunnel::expect_pong(const std::string &cmd, const Args & ... args) {
  RUN_COMMAND(reply, conn, pool, cmd, args);

  if(reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "PONG") == 0) {
    return OK();
  }
  return conn->received_unexpected_reply(reply);
}

template <class ...Args>
XrdRedisStatus XrdRedisTunnel::expect_exists(const std::string &cmd, const Args & ... args) {
  RUN_COMMAND(reply, conn, pool, cmd, args);

  if(reply->type == REDIS_REPLY_INTEGER) {
    if(reply->integer == 0) return XrdRedisStatus(rocksdb::Status::kNotFound, "");
    if(reply->integer == 1) return OK();
  }

  return conn->received_unexpected_reply(reply);
}

XrdRedisTunnel::XrdRedisTunnel(const std::string &ip_, const int port_, size_t nconnections)
: ip(ip_), port(port_), pool(ip_, port_, nconnections) {

}

XrdRedisTunnel::~XrdRedisTunnel() {
}

XrdRedisStatus XrdRedisTunnel::set(const std::string &key, const std::string &value) {
  return expect_ok("SET", key, value);
}

XrdRedisStatus XrdRedisTunnel::get(const std::string &key, std::string &value) {
  return expect_str(value, "GET", key);
}

XrdRedisStatus XrdRedisTunnel::exists(const std::string &key) {
  return expect_exists("EXISTS", key);
}

XrdRedisStatus XrdRedisTunnel::del(const std::string &key) {
  return expect_ok("DEL", key);
}

XrdRedisStatus XrdRedisTunnel::keys(const std::string &pattern, std::vector<std::string> &result) {
  return expect_list(result, "KEYS", pattern);
}

XrdRedisStatus XrdRedisTunnel::sadd(const std::string &key, const std::string &element, int64_t &added) {
  return expect_integer(added, "SADD", key, element);
}

XrdRedisStatus XrdRedisTunnel::sismember(const std::string &key, const std::string &element) {
  return expect_exists("SISMEMBER", key, element);
}

XrdRedisStatus XrdRedisTunnel::srem(const std::string &key, const std::string &element) {
  return expect_exists("SREM", key, element);
}

XrdRedisStatus XrdRedisTunnel::smembers(const std::string &key, std::vector<std::string> &members) {
  return expect_list(members, "SMEMBERS", key);
}

XrdRedisStatus XrdRedisTunnel::scard(const std::string &key, size_t &count) {
  return expect_integer(count, "SCARD", key);
}

XrdRedisStatus XrdRedisTunnel::ping() {
  return expect_pong("PING");
}

XrdRedisStatus XrdRedisTunnel::flushall() {
  return expect_ok("FLUSHALL");
}

XrdRedisStatus XrdRedisTunnel::hset(const std::string &key, const std::string &field, const std::string &value) {
  int64_t tmp;
  return expect_integer(tmp, "HSET", key, field, value);
}

XrdRedisStatus XrdRedisTunnel::hget(const std::string &key, const std::string &field, std::string &value) {
  return expect_str(value, "HGET", key, field);
}

XrdRedisStatus XrdRedisTunnel::hexists(const std::string &key, const std::string &field) {
  return expect_exists("HEXISTS", key, field);
}

XrdRedisStatus XrdRedisTunnel::hkeys(const std::string &key, std::vector<std::string> &keys) {
  return expect_list(keys, "HKEYS", key);
}

XrdRedisStatus XrdRedisTunnel::hgetall(const std::string &key, std::vector<std::string> &res) {
  return expect_list(res, "HGETALL", key);
}

XrdRedisStatus XrdRedisTunnel::hincrby(const std::string &key, const std::string &field, const std::string &incrby, int64_t &result) {
  return expect_integer(result, "HINCRBY", key, field, incrby);
}

XrdRedisStatus XrdRedisTunnel::hdel(const std::string &key, const std::string &field) {
  return expect_ok("HDEL", key, field);
}

XrdRedisStatus XrdRedisTunnel::hlen(const std::string &key, size_t &len) {
  return expect_integer(len, "HLEN", key);
}

XrdRedisStatus XrdRedisTunnel::hvals(const std::string &key, std::vector<std::string> &vals) {
  return expect_list(vals, "HVALS", key);
}
