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

#ifndef __XRDREDIS_COMMANDS_H__
#define __XRDREDIS_COMMANDS_H__

#include <map>

enum class XrdRedisCommand {
  PING,
  FLUSHALL,

  GET,
  SET,
  EXISTS,
  DEL,
  KEYS,

  HGET,
  HSET,
  HEXISTS,
  HKEYS,
  HGETALL,
  HINCRBY,
  HDEL,
  HLEN,
  HVALS,
  HSCAN,

  SADD,
  SISMEMBER,
  SREM,
  SMEMBERS,
  SCARD,
  SSCAN,

  RAFT_HANDSHAKE,
  RAFT_APPEND_ENTRY,
  RAFT_INFO,
  RAFT_RECONFIGURE,
  RAFT_REQUEST_VOTE,
  RAFT_PANIC,
  RAFT_FETCH
};

extern std::map<std::string, XrdRedisCommand> redis_cmd_map;

#endif
