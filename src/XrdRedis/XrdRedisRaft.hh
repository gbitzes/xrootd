//------------------------------------------------------------------------------
// This file is part of XrdRedis: A Redis-like server implementation
//
// Copyright (c) 2016 by European Organization for Nuclear Research (CERN)
// Author: Georgios Bitzes <georgios.bitzes@cern.ch>
// File Date: August 2016
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

#ifndef __XRDREDIS_RAFT_H__
#define __XRDREDIS_RAFT_H__

#include "XrdRedisJournal.hh"
#include "XrdRedisRaftTalker.hh"
#include <atomic>
#include <thread>

class ScopedAdder {
public:
  ScopedAdder(std::atomic<int> &target_, int value_ = 1) : target(target_), value(value_) {
    target += value;
  }

  ~ScopedAdder() {
    target -= value;
  }

private:
  std::atomic<int> &target;
  int value;
};

enum class RaftState {
  follower = 0,
  candidate = 1,
  leader = 2,
  shutdown = 3
};

class XrdRedisRaft {
public:
  XrdRedisRaft(XrdRedisBackend *journalStore, XrdRedisBackend *smachine, RaftClusterID id, RaftServer myself);

  std::vector<std::string> appendEntries(RaftTerm term, RaftServerID leaderId, LogIndex prevIndex, RaftTerm prevTerm,
                               XrdRedisRequest &req, RaftTerm entryTerm, LogIndex commit);

  std::vector<std::string> requestVote(RaftTerm term, int64_t candidateId, LogIndex lastIndex, RaftTerm lastTerm);

  std::vector<std::string> info();
  std::vector<std::string> fetch(LogIndex index);

  RaftClusterID getClusterID() {
    return journal.getClusterID();
  }

  XrdRedisStatus configureParticipants(std::vector<RaftServer> &reps);
  void panic();

  XrdRedisStatus pushUpdate(XrdRedisRequest &req);
private:
  std::condition_variable logUpdates;
  size_t quorumThreshold;

  std::vector<XrdRedisRaftTalker*> talkers;

  std::atomic<int> requestsInFlight{0};
  std::chrono::steady_clock::time_point lastAppend;

  RaftServerID leader{-1};

  std::atomic<RaftState> raftState{RaftState::follower};
  XrdRedisJournal2 journal;
  // XrdRedisBackend *journal; // we might want to change this in the future
  XrdRedisBackend *stateMachine;
  std::vector<RaftServer> participants;
  RaftServer myself;
  RaftServerID myselfID;

  void stateTransition(RaftState newstate);
  void monitor();
  void monitorFollower(RaftServerID machine);
  void updateRandomTimeout();
  void becomeLeader();
  void performElection();


  size_t processVotes(std::vector<std::future<redisReplyPtr>> &replies);

  void triggerPanic();

  void transition(RaftState newstate, RaftTerm newterm, RaftServerID newvotedfor, RaftServerID newleader);
  std::mutex transitionMutex;

  void updateTermIfNecessary(RaftTerm term, RaftServerID leader);
  void declareTerm(RaftTerm newTerm, RaftServerID newLeader);

  std::thread monitorThread;

  std::chrono::milliseconds heartbeatInterval{75};
  std::chrono::milliseconds timeoutLow{200};
  std::chrono::milliseconds timeoutHigh{300};

  std::chrono::milliseconds randomTimeout;

};

#endif
