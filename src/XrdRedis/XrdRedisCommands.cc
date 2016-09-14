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

#include "XrdRedisCommands.hh"
#include <iostream>

std::map<std::string, XrdRedisCommand> redis_cmd_map;

struct cmdMapInit {
  cmdMapInit() {
    redis_cmd_map["ping"] = XrdRedisCommand::PING;
    redis_cmd_map["flushall"] = XrdRedisCommand::FLUSHALL;

    redis_cmd_map["get"] = XrdRedisCommand::GET;
    redis_cmd_map["set"] = XrdRedisCommand::SET;
    redis_cmd_map["exists"] = XrdRedisCommand::EXISTS;
    redis_cmd_map["del"] =  XrdRedisCommand::DEL;
    redis_cmd_map["keys"] =  XrdRedisCommand::KEYS;

    redis_cmd_map["hget"] = XrdRedisCommand::HGET;
    redis_cmd_map["hset"] =  XrdRedisCommand::HSET;
    redis_cmd_map["hexists"] = XrdRedisCommand::HEXISTS;
    redis_cmd_map["hkeys"] = XrdRedisCommand::HKEYS;
    redis_cmd_map["hgetall"] = XrdRedisCommand::HGETALL;
    redis_cmd_map["hincrby"] = XrdRedisCommand::HINCRBY;
    redis_cmd_map["hdel"] = XrdRedisCommand::HDEL;
    redis_cmd_map["hlen"] = XrdRedisCommand::HLEN;
    redis_cmd_map["hvals"] = XrdRedisCommand::HVALS;
    redis_cmd_map["hscan"] = XrdRedisCommand::HSCAN;

    redis_cmd_map["sadd"] = XrdRedisCommand::SADD;
    redis_cmd_map["sismember"] = XrdRedisCommand::SISMEMBER;
    redis_cmd_map["srem"] = XrdRedisCommand::SREM;
    redis_cmd_map["smembers"] = XrdRedisCommand::SMEMBERS;
    redis_cmd_map["scard"] = XrdRedisCommand::SCARD;
    redis_cmd_map["sscan"] = XrdRedisCommand::SSCAN;

    redis_cmd_map["raft_handshake"] = XrdRedisCommand::RAFT_HANDSHAKE;
    redis_cmd_map["raft_append_entry"] = XrdRedisCommand::RAFT_APPEND_ENTRY;
    redis_cmd_map["raft_info"] = XrdRedisCommand::RAFT_INFO;
    redis_cmd_map["raft_request_vote"] = XrdRedisCommand::RAFT_REQUEST_VOTE;
    redis_cmd_map["raft_panic"] = XrdRedisCommand::RAFT_PANIC;
    redis_cmd_map["raft_fetch"] = XrdRedisCommand::RAFT_FETCH;
  }
} cmd_map_init;
