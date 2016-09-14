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

#include "XrdRedisRaftTalker.hh"

XrdRedisRaftTalker::XrdRedisRaftTalker(const RaftServer &srv)
: target(srv), tunnel(srv.hostname, srv.port) {

}

XrdRedisRaftTalker::~XrdRedisRaftTalker() {

}

std::future<redisReplyPtr> XrdRedisRaftTalker::sendHandshake(const RaftClusterID &id, const std::vector<RaftServer> &participants) {
  XrdRedisRequest req;
  req.emplace_back(new std::string("RAFT_HANDSHAKE"));
  req.emplace_back(new std::string(id));

  std::ostringstream ss;
  for(size_t i = 0; i < participants.size(); i++) {
    ss << participants[i].hostname << ":" << participants[i].port;
    if(i != participants.size()-1) ss << ",";
  }

  req.emplace_back(new std::string(ss.str()));
  return tunnel.executeAsync(req);
}

std::future<redisReplyPtr> XrdRedisRaftTalker::sendAppendEntries(RaftTerm term, RaftServerID leaderId, LogIndex prevIndex,
                                         RaftTerm prevTerm, LogIndex commit, XrdRedisRequest &cmd, RaftTerm entryTerm) {
  XrdRedisRequest req;
  req.emplace_back(new std::string("RAFT_APPEND_ENTRY"));
  req.emplace_back(new std::string(SSTR(term)));
  req.emplace_back(new std::string(SSTR(leaderId)));
  req.emplace_back(new std::string(SSTR(prevIndex)));
  req.emplace_back(new std::string(SSTR(prevTerm)));
  req.emplace_back(new std::string(SSTR(commit)));
  req.emplace_back(new std::string(SSTR(entryTerm)));

  for(size_t i = 0; i < cmd.size(); i++) {
    req.emplace_back(cmd[i]);
  }

  return tunnel.executeAsync(req);
}

std::future<redisReplyPtr> XrdRedisRaftTalker::sendHeartbeat(RaftTerm term, RaftServerID leaderId, LogIndex prevIndex,
                                         RaftTerm prevTerm, LogIndex commit) {
  XrdRedisRequest req;
  req.emplace_back(new std::string("RAFT_APPEND_ENTRY"));
  req.emplace_back(new std::string(SSTR(term)));
  req.emplace_back(new std::string(SSTR(leaderId)));
  req.emplace_back(new std::string(SSTR(prevIndex)));
  req.emplace_back(new std::string(SSTR(prevTerm)));
  req.emplace_back(new std::string(SSTR(commit)));
  req.emplace_back(new std::string(SSTR(term)));
  req.emplace_back(new std::string("HEARTBEAT"));
  return tunnel.executeAsync(req);
}

std::future<redisReplyPtr> XrdRedisRaftTalker::sendRequestVote(RaftTerm term, int64_t candidateId, LogIndex lastIndex, RaftTerm lastTerm) {
  XrdRedisRequest req;
  req.emplace_back(new std::string("RAFT_REQUEST_VOTE"));
  req.emplace_back(new std::string(SSTR(term)));
  req.emplace_back(new std::string(SSTR(candidateId)));
  req.emplace_back(new std::string(SSTR(lastIndex)));
  req.emplace_back(new std::string(SSTR(lastTerm)));
  return tunnel.executeAsync(req);
}

std::future<redisReplyPtr> XrdRedisRaftTalker::sendPanic() {
  XrdRedisRequest req;
  req.emplace_back(new std::string("RAFT_PANIC"));
  return tunnel.executeAsync(req);
}
