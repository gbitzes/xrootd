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
#include "XrdRedisSpinlock.hh"
#include <atomic>
#include <thread>
#include <map>

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

struct RaftStatus {
  RaftState state;
  RaftTerm term;
  RaftServerID leader;
};

// synchronization class to determine when it is safe to send an ACK to the client
// class QuorumTracker {
// public:
//   QuorumTracker() {}
//
//   void init(size_t nparticipants, size_t quorumThreshold) {
//     this->nparticipants = nparticipants;
//     this->quorumThreshold = quorumThreshold;
//
//     for(size_t i = 0; i < nparticipants; i++) {
//       nextIndex.push_back(i);
//     }
//      lowestWithQuorum = -1;
//   }
//
//   // notify the tracker that 'machine' has acked all entries until nextIndex
//   void notifyNextIndex(RaftServerId machine, LogIndex nextIndex);
//
//   // wait until a quorum of machines have acked 'index'
//   bool waitForQuorum(LogIndex index);
//
//   // no longer a leader, release any threads waiting
//   void releaseAll();
// private:
//   std::vector<LogIndex> nextIndex;
//   std::map<LogIndex, std::condition_variable> vars;
//
//   LogIndex lowestWithQuorum;
//
//   size_t quorumThreshold;
//   size_t nparticipants;
// };

struct AppendEntriesReply {
  RaftTerm term = -1;
  LogIndex logSize = -1;
  bool success = false;
  bool online = false;
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

  std::future<redisReplyPtr> pushUpdate(XrdRedisRequest &req);

  std::string getLeader();
private:
  std::mutex pendingRepliesMutex;
  std::map<LogIndex, std::promise<redisReplyPtr>> pendingReplies;


  // RaftTerm currentTerm;

  XrdRedisStatus append(RaftTerm prevTerm, LogIndex prevIndex, XrdRedisRequest &cmd, RaftTerm entryTerm);
  void applyCommits();

  std::mutex updating;

  std::mutex logUpdatedMutex;
  std::condition_variable logUpdated;
  size_t quorumThreshold;

  // tracks how up-to-date the log of each follower is - only used during leadership
  std::mutex matchIndexMutex;
  std::vector<LogIndex> matchIndex;
  void updateMatchIndex(RaftServerID machine, LogIndex index);

  std::vector<XrdRedisRaftTalker*> talkers;

  std::chrono::steady_clock::time_point lastAppend;

  RaftServerID leader{-1};

  std::atomic<RaftState> raftState{RaftState::follower};
  XrdRedisJournal journal;
  XrdRedisBackend *stateMachine;
  std::vector<RaftServer> participants;
  RaftServer myself;
  RaftServerID myselfID;

  void stateTransition(RaftState newstate);
  void monitor();
  void monitorFollower(RaftServerID machine);
  void monitorLeader();
  void updateRandomTimeout();
  void becomeLeader();
  void performElection();

  bool decideOnVoteRequest(RaftTerm term, int64_t candidateId, LogIndex lastIndex, RaftTerm lastTerm);

  AppendEntriesReply pipelineAppendEntries(RaftServerID machine, LogIndex nextIndex, RaftTerm prevTerm, int pipelineLength);

  size_t processVotes(std::vector<std::future<redisReplyPtr>> &replies);

  void triggerPanic();
  void clearPendingReplies();

  void transition(RaftState newstate, RaftTerm newterm, RaftServerID newvotedfor, RaftServerID newleader);
  std::mutex transitionMutex;

  void updateTermIfNecessary(RaftTerm term, RaftServerID leader);
  void declareTerm(RaftTerm newTerm, RaftServerID newLeader);

  std::thread monitorThread;

  std::chrono::milliseconds heartbeatInterval{200};
  std::chrono::milliseconds timeoutLow{800};
  std::chrono::milliseconds timeoutHigh{1000};

  std::chrono::milliseconds randomTimeout;

};

#endif
