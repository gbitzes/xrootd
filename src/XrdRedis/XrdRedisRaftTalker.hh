//------------------------------------------------------------------------------
// This file is part of XrdRedis: A Redis-like server implementation
//
// Copyright (c) 2016 by European Organization for Nuclear Research (CERN)
// Author: Georgios Bitzes <georgios.bitzes@cern.ch>
// File Date: September 2016
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

#ifndef __XRDREDIS_RAFT_TALKER_H__
#define __XRDREDIS_RAFT_TALKER_H__

#include "XrdRedisTunnel.hh"
#include "XrdRedisCommon.hh"
#include <atomic>
#include <future>

class XrdRedisRaftTalker {
public:
  XrdRedisRaftTalker(const RaftServer &srv);
  ~XrdRedisRaftTalker();

  std::future<redisReplyPtr> sendHandshake(RaftClusterID id);
  std::future<redisReplyPtr> sendHeartbeat(RaftTerm term, RaftServerID leaderId, LogIndex prevIndex,
                                         RaftTerm prevTerm, LogIndex commit);
  std::future<redisReplyPtr> sendRequestVote(RaftTerm term, int64_t candidateId, LogIndex lastIndex, RaftTerm lastTerm);
private:
  RaftServer target;
  XrdRedisTunnel tunnel;
};

#endif
