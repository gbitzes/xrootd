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

#ifndef __XRDREDIS_REPLICATOR_H__
#define __XRDREDIS_REPLICATOR_H__

#include "XrdRedisBackend.hh"

/******************************************************************************/
/*                               D e f i n e s                                */
/******************************************************************************/

class XrdRedisReplicator : public XrdRedisBackend {
public:
  XrdRedisStatus set(const std::string &key, const std::string &value);
  XrdRedisStatus get(const std::string &key, std::string &value);
  XrdRedisStatus exists(const std::string &key);
  XrdRedisStatus del(const std::string &key);
  XrdRedisStatus keys(const std::string &pattern, std::vector<std::string> &result);

  XrdRedisStatus hset(const std::string &key, const std::string &field, const std::string &value);
  XrdRedisStatus hget(const std::string &key, const std::string &field, std::string &value);
  XrdRedisStatus hexists(const std::string &key, const std::string &field);
  XrdRedisStatus hkeys(const std::string &key, std::vector<std::string> &keys);
  XrdRedisStatus hgetall(const std::string &key, std::vector<std::string> &res);
  XrdRedisStatus hincrby(const std::string &key, const std::string &field, const std::string &incrby, int64_t &result);
  XrdRedisStatus hdel(const std::string &key, const std::string &field);
  XrdRedisStatus hlen(const std::string &key, size_t &len);
  XrdRedisStatus hvals(const std::string &key, std::vector<std::string> &vals);

  XrdRedisStatus sadd(const std::string &key, const std::string &element, int64_t &added);
  XrdRedisStatus sismember(const std::string &key, const std::string &element);
  XrdRedisStatus srem(const std::string &key, const std::string &element);
  XrdRedisStatus smembers(const std::string &key, std::vector<std::string> &members);
  XrdRedisStatus scard(const std::string &key, size_t &count);

  XrdRedisStatus ping();
  XrdRedisStatus flushall();

  XrdRedisReplicator(XrdRedisBackend *primary_, std::vector<XrdRedisBackend*> replicas_);
  ~XrdRedisReplicator();
private:
  struct ReplicaState {
    int64_t revision;
    XrdRedisBackend *backend;
    bool online;
  };

  struct JournalEntry {
    int64_t revision;
    std::string command;
    std::vector<std::string> arguments;
  };

  ReplicaState primary;
  std::vector<ReplicaState> replicas;

  // static XrdRedisStatus applyUpdate(ReplicaState &state, const JournalEntry &entry);
};



#endif
