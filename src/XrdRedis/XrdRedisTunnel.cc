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
#include <future>
#include <signal.h>
#include <chrono>
#include <sys/eventfd.h>
#include <poll.h>
#include <unistd.h>

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

// void my_event_loop(redisAsyncContext **async, std::string ip, int port) {
//   // signal(SIGPIPE, SIG_IGN);
//
//   // struct ev_loop *loop = ev_loop_new(EVFLAG_AUTO);
//   // uv_loop_t* loop = uv_default_loop();
//   (*async) = redisAsyncConnect(ip.c_str(), port);
//   if(redisLibevAttach(EV_DEFAULT_ *async) != REDIS_OK) {
//     std::cout << "could not attach to event loop..." << std::endl;
//   }
//
//   redisAsyncSetConnectCallback(*async, connectCallback);
//   redisAsyncSetDisconnectCallback(*async, disconnectCallback);
//
//   std::cout << "ev_run, no loop" << std::endl;
//   ev_run(EV_DEFAULT_ 0);
//
//   // int i = 0;
//   // while(true) {
//   //   ev_run(EV_DEFAULT_ EVRUN_NOWAIT);
//   //   std::this_thread::sleep_for(std::chrono::seconds(1));
//   // }
//
//   // while(i <= 10) {
//   //   i++;
//   //   std::cout << "loop, wait 1 sec" << std::endl;
//   // }
//
//
//   // while(true) {
//   //   ev_run(loop, EVRUN_NOWAIT);
//   //   std::cout << "tick" << std::endl;
//   // }
//
//   // struct ev_loop *loop = ev_loop_new(EVFLAG_NOINOTIFY);
//   // struct ev_loop *loop = ev_loop_new(EVFLAG_AUTO);
//   // struct event_base *base = event_base_new();
//
//
//   // while(true) {
//   //   std::cout << "before ev_run" << std::endl;
//   //   ev_run(loop, 0);
//   //   std::cout << "tick" << std::endl;
//   // }
//
//   // redisLibeventAttach(async, base);
//   // while(true) {
//   //   event_base_loop(base, 0);
//   //   std::cout << "tick" << std::endl;
//   // }
// }

void connectCallback(const redisAsyncContext *c, int status) {
    if (status != REDIS_OK) {
        // printf("Connect callback - error: %s\n", c->errstr);
        return;
    }
    printf("Connected...\n");
}

void disconnectCallback(const redisAsyncContext *c, int status) {
    if (status != REDIS_OK) {
        printf("Disconnect callback - error: %s\n", c->errstr);
        return;
    }
    printf("Disconnected...\n");
}

static void add_write_callback(void *privdata) {
  XrdRedisConnection *conn = (XrdRedisConnection*) privdata;
  conn->notifyWrite();
}

static void del_write_callback(void *privdata) {
  XrdRedisConnection *conn = (XrdRedisConnection*) privdata;
  conn->removeWriteNotification();
}

void XrdRedisConnection::notifyWrite() {
  // std::cout << "in notify write" << std::endl;
  int64_t tmp = 1;
  if(write(write_event_fd, &tmp, sizeof(tmp)) != sizeof(tmp)) {
    std::cout << "could not notify write" << std::endl;
  }
}

void XrdRedisConnection::removeWriteNotification() {
  // std::cout << "in remove write notification" << std::endl;
  int64_t tmp;
  if(read(write_event_fd, &tmp, sizeof(tmp)) != sizeof(tmp)) {
    std::cout << "could not remove write notification" << std::endl;
  }
}

void ignore_callback(redisAsyncContext *c, void *reply, void *privdata) { }

void XrdRedisConnection::connect() {
  std::unique_lock<std::mutex> lock(async_mutex);
  // if(async) redisAsyncFree(async);
  // TODO: figure out what I have to do to free the async context

  async = redisAsyncConnect(ip.c_str(), port);

  // redisAsyncSetConnectCallback(async, connectCallback);
  // redisAsyncSetDisconnectCallback(async, disconnectCallback);

  async->ev.addWrite = add_write_callback;
  async->ev.delWrite = del_write_callback;
  async->ev.data = this;

  if(write_event_fd >= 0) {
    close(write_event_fd);
  }
  write_event_fd = eventfd(0, EFD_NONBLOCK);

  lock.unlock();
  if(!handshakeCommand.empty()) {
    executeAsyncFuture(handshakeCommand);
  }

  // const char *cstr[1];
  // size_t sizes[1];
  //
  // std::string cmd("PING");
  // cstr[0] = cmd.c_str();
  // sizes[0] = cmd.size();

  // redisAsyncCommandArgv(async, ignore_callback, NULL, 1, cstr, sizes);
}

void XrdRedisConnection::eventLoop() {
  while(true) {
    this->connect();

    struct pollfd polls[2];
    polls[0].fd = write_event_fd;
    polls[0].events = POLLIN;

    polls[1].fd = async->c.fd;
    polls[1].events = POLLIN;

    while(true) {
      poll(polls, 2, -1);

      std::unique_lock<std::mutex> lock(async_mutex);

      if(async->err) {
        break;
      }

      if(polls[0].revents != 0) {
        redisAsyncHandleWrite(async);
      }
      else if(polls[1].revents != 0) {
        redisAsyncHandleRead(async);
      }
    }

    // dropped connection, wait before retrying
    std::chrono::milliseconds backoff(1000);
    std::this_thread::sleep_for(backoff);
  }
}

XrdRedisConnection::XrdRedisConnection(std::string _ip, int _port)
: ctx(nullptr), ip(_ip), port(_port) {
  std::cout << "Creating connection to " << ip << ":" << port << std::endl;

  async = nullptr;
  write_event_fd = -1;
  std::cout << "handshakeCommand before clear: " << handshakeCommand.size() << std::endl;
  handshakeCommand.clear();
  std::cout << "handshakeCommand after clear: " << handshakeCommand.size() << std::endl;

  std::thread th(&XrdRedisConnection::eventLoop, this);
  th.detach();


//   redox::Redox rrr;
//
//   std::cout << "before rdx.connect" << std::endl;
//   try {
//   if(!rrr.connect("localhost", 7777)) {
//     std::cout << "redox: could not connect!" << std::endl;
//   }
// }
//   catch(...) {
//     std::cout << "ded" << std::endl;
//   }
//   std::cout << "survived rdx.connect" << std::endl;

  // struct ev_loop *loop = EV_DEFAULT;
  // std::cout << "async object: " << async << std::endl;
  // if (async->err) {
  //   printf("Error: %s\n", async->errstr);
  //   // handle error
  // }
  // else {
  //   printf("no error when creating async connection\n");
  // }

  // std::thread th(my_event_loop, &async, ip, port);
  // th.detach();
}

void XrdRedisConnection::clearConnection() {
  if(ctx) {
    redisFree(ctx);
    ctx = nullptr;
  }
}

void XrdRedisConnection::ensureConnectedAsync() {
  if(async && async->err) {
    redisAsyncFree(async);
    async = nullptr;
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

redisReply* dupReplyObject(redisReply* reply) {
    redisReply* r = (redisReply*)calloc(1, sizeof(*r));
    memcpy(r, reply, sizeof(*r));
    if(REDIS_REPLY_ERROR==reply->type || REDIS_REPLY_STRING==reply->type || REDIS_REPLY_STATUS==reply->type) //copy str
    {
        r->str = (char*)malloc(reply->len+1);
        memcpy(r->str, reply->str, reply->len);
        r->str[reply->len] = '\0';
    }
    else if(REDIS_REPLY_ARRAY==reply->type) //copy array
    {
        r->element = (redisReply**)calloc(reply->elements, sizeof(redisReply*));
        memset(r->element, 0, r->elements*sizeof(redisReply*));
        for(uint32_t i=0; i<reply->elements; ++i)
        {
            if(NULL!=reply->element[i])
            {
                if( NULL == (r->element[i] = dupReplyObject(reply->element[i])) )
                {
                    //clone child failed, free current reply, and return NULL
                        freeReplyObject(r);
                    return NULL;
                }
            }
        }
    }
    return r;
}

void async_future_callback(redisAsyncContext *c, void *reply, void *privdata) {
  redisReply *rreply = (redisReply*) reply;
  std::promise<redisReplyPtr> *promise = (std::promise<redisReplyPtr>*) privdata;

  if(reply) {
    promise->set_value(redisReplyPtr(dupReplyObject(rreply), freeReplyObject));
  }
  else {
    promise->set_value(redisReplyPtr());
  }
  delete promise;
}

std::future<redisReplyPtr> XrdRedisConnection::executeAsyncFuture(XrdRedisRequest &req) {
  const char *cstr[req.size()];
  size_t sizes[req.size()];

  for(size_t i = 0; i < req.size(); i++) {
    cstr[i] = req[i]->c_str();
    sizes[i] = req[i]->size();
  }

  std::unique_lock<std::mutex> lock(async_mutex);

  if(async && !async->err) {
    std::promise<redisReplyPtr>* prom = new std::promise<redisReplyPtr>();
    redisAsyncCommandArgv(async, async_future_callback, prom, req.size(), cstr, sizes);
    return prom->get_future();
  }

  std::promise<redisReplyPtr> prom;
  prom.set_value(redisReplyPtr());
  return prom.get_future();
}

template <class ...Args>
int XrdRedisConnection::executeAsync(redisCallbackFn *fn, void *data, const Args & ... args) {
  const char *cstr[] = { (args.c_str())... };
  size_t sizes[] = { (args.size())... };

  std::unique_lock<std::mutex> lock(async_mutex);
  return redisAsyncCommandArgv(async, fn, data, sizeof...(args), cstr, sizes);
}

XrdRedisConnectionPool::XrdRedisConnectionPool(std::string _ip, int _port, size_t _size)
: ip(_ip), port(_port), size(_size) {

  // for(size_t i = 0; i < _size; i++) {
  //   connections.enqueue(create());
  // }
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

// template <class ...Args>
// XrdRedisStatus XrdRedisTunnel::expect_ok(const std::string &cmd, const Args & ... args) {
//   RUN_COMMAND(reply, conn, pool, cmd, args);
//
//   if(reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") == 0) {
//     return OK();
//   }
//   return conn->received_unexpected_reply(reply);
// }
//
// template <class ...Args>
// XrdRedisStatus XrdRedisTunnel::expect_str(std::string &value, const std::string &cmd, const Args & ... args) {
//   RUN_COMMAND(reply, conn, pool, cmd, args);
//
//   if(reply->type == REDIS_REPLY_STRING) {
//     value = std::string(reply->str, reply->len);
//     return OK();
//   }
//   else if(reply->type == REDIS_REPLY_NIL) {
//     return XrdRedisStatus(rocksdb::Status::kNotFound, "");
//   }
//
//   return conn->received_unexpected_reply(reply);
// }
//
// template <class IntegerType, class ...Args>
// XrdRedisStatus XrdRedisTunnel::expect_integer(IntegerType &value, const std::string &cmd, const Args & ... args) {
//   RUN_COMMAND(reply, conn, pool, cmd, args);
//
//   if(reply->type == REDIS_REPLY_INTEGER) {
//     value = (IntegerType) reply->integer;
//     return OK();
//   }
//   return conn->received_unexpected_reply(reply);
// }
//
// template <class ...Args>
// XrdRedisStatus XrdRedisTunnel::expect_list(std::vector<std::string> &vec, const std::string &cmd, const Args & ... args) {
//   RUN_COMMAND(reply, conn, pool, cmd, args);
//
//   if(reply->type == REDIS_REPLY_ARRAY) {
//     for(size_t i = 0; i < reply->elements; i++) {
//       if(reply->element[i]->type != REDIS_REPLY_STRING) return conn->received_unexpected_reply(reply);
//       vec.push_back(std::string(reply->element[i]->str, reply->element[i]->len));
//     }
//     return OK();
//   }
//   return conn->received_unexpected_reply(reply);
// }
//
// void expect_pong_callback(redisAsyncContext *c, void *reply, void *privdata) {
//   // std::cout << "in expect pong callback" << std::endl;
//   redisReply *rreply = (redisReply*) reply;
//   std::promise<XrdRedisStatus> *promise = (std::promise<XrdRedisStatus>*) privdata;
//
//   if(rreply->type == REDIS_REPLY_STATUS && strcmp(rreply->str, "PONG") == 0) {
//     promise->set_value(OK());
//     return;
//   }
//
//   promise->set_value(XrdRedisStatus(rocksdb::Status::kAborted, "Unexpected reply"));
// }
//
// template <class ...Args>
// XrdRedisStatus XrdRedisTunnel::expect_pong(const std::string &cmd, const Args & ... args) {
//   RUN_COMMAND(reply, conn, pool, cmd, args);
//
//   if(reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "PONG") == 0) {
//     return OK();
//   }
//   return conn->received_unexpected_reply(reply);
// }
//
// template <class ...Args>
// XrdRedisStatus XrdRedisTunnel::expect_exists(const std::string &cmd, const Args & ... args) {
//   RUN_COMMAND(reply, conn, pool, cmd, args);
//
//   if(reply->type == REDIS_REPLY_INTEGER) {
//     if(reply->integer == 0) return XrdRedisStatus(rocksdb::Status::kNotFound, "");
//     if(reply->integer == 1) return OK();
//   }
//
//   return conn->received_unexpected_reply(reply);
// }

std::future<redisReplyPtr> XrdRedisTunnel::executeAsync(XrdRedisRequest &req) {
  return my_conn.executeAsyncFuture(req);
}

XrdRedisTunnel::XrdRedisTunnel(const std::string &ip_, const int port_, size_t nconnections)
: ip(ip_), port(port_), /*pool(ip_, port_, nconnections),*/ my_conn(ip_, port_) {

}

XrdRedisTunnel::~XrdRedisTunnel() {
}

XrdRedisStatus XrdRedisTunnel::set(const std::string &key, const std::string &value) {
  return OK();
  // return expect_ok("SET", key, value);
}

XrdRedisStatus XrdRedisTunnel::get(const std::string &key, std::string &value) {
  return OK();
  // return expect_str(value, "GET", key);
}

XrdRedisStatus XrdRedisTunnel::exists(const std::string &key) {
  return OK();
  // return expect_exists("EXISTS", key);
}

XrdRedisStatus XrdRedisTunnel::del(const std::string &key) {
  return OK();
  // return expect_ok("DEL", key);
}

XrdRedisStatus XrdRedisTunnel::keys(const std::string &pattern, std::vector<std::string> &result) {
  return OK();
  // return expect_list(result, "KEYS", pattern);
}

XrdRedisStatus XrdRedisTunnel::sadd(const std::string &key, const std::string &element, int64_t &added) {
  return OK();
  // return expect_integer(added, "SADD", key, element);
}

XrdRedisStatus XrdRedisTunnel::sismember(const std::string &key, const std::string &element) {
  return OK();
  // return expect_exists("SISMEMBER", key, element);
}

XrdRedisStatus XrdRedisTunnel::srem(const std::string &key, const std::string &element) {
  return OK();
  // return expect_exists("SREM", key, element);
}

XrdRedisStatus XrdRedisTunnel::smembers(const std::string &key, std::vector<std::string> &members) {
  return OK();
  // return expect_list(members, "SMEMBERS", key);
}

XrdRedisStatus XrdRedisTunnel::scard(const std::string &key, size_t &count) {
  return OK();
  // return expect_integer(count, "SCARD", key);
}

template <class ...Args>
XrdRedisStatus XrdRedisTunnel::executeAsync(redisCallbackFn *fn, const Args & ... args) {
  std::promise<XrdRedisStatus> st;
  int ret = my_conn.executeAsync(fn, &st, args...);
  if(ret != REDIS_OK) {
    std::cout << "ERROR: async not OK" << std::endl;
    return XrdRedisStatus(rocksdb::Status::kNotFound, "error pushing async operation");
  }
  return st.get_future().get();
}

void XrdRedisConnection::setHandshake(XrdRedisRequest &req) {
  handshakeCommand = req;
  executeAsyncFuture(handshakeCommand);
}

void XrdRedisTunnel::setHandshake(XrdRedisRequest &req) {
  my_conn.setHandshake(req);
}

// redisCallbackFn *fn, void *data, const Args & ... args);
//
//   template <class ...Args>
//   int executeAsync(



XrdRedisStatus XrdRedisTunnel::ping() {
  return OK();
  // return this->executeAsync(expect_pong_callback, std::string("PING"));
  // return expect_pong("PING");


  // std::promise<XrdRedisStatus> st;
  // int ret = my_conn.executeAsync(expect_pong_callback, &st, std::string("PING"));
  // // std::cout << "waiting on the promise.." << std::endl;
  // XrdRedisStatus ss = st.get_future().get();
  // // std::cout << "promise fulfilled" << std::endl;
  // return ss;
}

XrdRedisStatus XrdRedisTunnel::flushall() {
  return OK();
  // return expect_ok("FLUSHALL");
}

XrdRedisStatus XrdRedisTunnel::hset(const std::string &key, const std::string &field, const std::string &value) {
  return OK();
  // int64_t tmp;
  // return expect_integer(tmp, "HSET", key, field, value);
}

XrdRedisStatus XrdRedisTunnel::hget(const std::string &key, const std::string &field, std::string &value) {
  return OK();
  // return expect_str(value, "HGET", key, field);
}

XrdRedisStatus XrdRedisTunnel::hexists(const std::string &key, const std::string &field) {
  return OK();
  // return expect_exists("HEXISTS", key, field);
}

XrdRedisStatus XrdRedisTunnel::hkeys(const std::string &key, std::vector<std::string> &keys) {
  return OK();
  // return expect_list(keys, "HKEYS", key);
}

XrdRedisStatus XrdRedisTunnel::hgetall(const std::string &key, std::vector<std::string> &res) {
  return OK();
  // return expect_list(res, "HGETALL", key);
}

XrdRedisStatus XrdRedisTunnel::hincrby(const std::string &key, const std::string &field, const std::string &incrby, int64_t &result) {
  return OK();
  // return expect_integer(result, "HINCRBY", key, field, incrby);
}

XrdRedisStatus XrdRedisTunnel::hdel(const std::string &key, const std::string &field) {
  return OK();
  // return expect_ok("HDEL", key, field);
}

XrdRedisStatus XrdRedisTunnel::hlen(const std::string &key, size_t &len) {
  return OK();
  // return expect_integer(len, "HLEN", key);
}

XrdRedisStatus XrdRedisTunnel::hvals(const std::string &key, std::vector<std::string> &vals) {
  return OK();
  // return expect_list(vals, "HVALS", key);
}
