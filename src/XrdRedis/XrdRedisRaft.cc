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

#include "XrdRedisRaft.hh"
#include "XrdRedisUtil.hh"
#include <random>

static XrdRedisStatus OK() {
  return XrdRedisStatus(rocksdb::Status::kOk);
}

XrdRedisRaft::XrdRedisRaft(XrdRedisBackend *journalStore, XrdRedisBackend *smachine, RaftClusterID id, RaftServer me)
: journal(journalStore, id), stateMachine(smachine), participants{me}, myself(me) {

  lastAppend = std::chrono::steady_clock::now();
  updateRandomTimeout();

  monitorThread = std::thread(&XrdRedisRaft::monitor, this);
}

std::vector<std::string> parseRaftResponse(redisReplyPtr &reply) {
  std::vector<std::string> ret;

  if(reply == nullptr) return ret;

  if(reply->type == REDIS_REPLY_STRING || reply->type == REDIS_REPLY_ERROR) {
    ret.emplace_back(reply->str, reply->len);
    return ret;
  }

  if(reply->type == REDIS_REPLY_ARRAY) {
    for(size_t i = 0; i < reply->elements; i++) {
      ret.emplace_back(reply->element[i]->str, reply->element[i]->len);
    }
  }

  return ret;
}

bool is_positive_vote(redisReplyPtr &reply) {
  if(reply == nullptr) return false;
  if(reply->type != REDIS_REPLY_ARRAY) return false;

  for(size_t i = 0; i < reply->elements; i++) {
    if(std::string(reply->element[i]->str, reply->element[i]->len) == "VOTE-GRANTED") {
      return std::string(reply->element[i+1]->str, reply->element[i+1]->len) == "TRUE";
    }
  }
  return false;
}

void XrdRedisRaft::performElection() {
  RaftTerm newTerm = journal.getCurrentTerm()+1;

  journal.setCurrentTerm(newTerm);
  journal.setVotedFor(myselfID);

  std::vector<std::future<redisReplyPtr>> replies;
  for(size_t i = 0; i < talkers.size(); i++) {
    talkers[i]->sendHandshake(journal.getClusterID());
    replies.push_back(talkers[i]->sendRequestVote(newTerm, myselfID, 0, 0)); // TODO
  }

  size_t acks = 1;
  for(size_t i = 0; i < talkers.size(); i++) {
    redisReplyPtr r = replies[i].get();
    if(r) {
      if(is_positive_vote(r)) acks++;
      std::cout << std::string(r->str, r->len) << std::endl;
    }
  }

  if(acks >= participants.size()/2 + 1) {
    stateTransition(RaftState::leader);
    leader = myselfID;
    std::cout << "RAFT: election round for " << newTerm << " successful with " << acks << " votes, long may I reign" << std::endl;
  }
  else {
    std::cout << "RAFT: election round for " << newTerm << " failed, only " << acks << " votes. :-(" << std::endl;
    std::this_thread::sleep_for(randomTimeout);
  }
}

void XrdRedisRaft::updateRandomTimeout() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(timeoutLow.count(), timeoutHigh.count());
  randomTimeout = std::chrono::milliseconds(dist(gen));
  std::cout << "RAFT: setting random timeout to " << randomTimeout.count() << "ms" << std::endl;
}

void XrdRedisRaft::monitor() {
  // things are a bit volatile in the beginning, maybe the request processing
  // threads have not started yet. Avoid being too agressive into becoming
  // a candidate, and wait a bit at the beginning
  std::this_thread::sleep_for(std::chrono::seconds(1));

  while(raftState != RaftState::shutdown) {
    // make sure I'm receiving heartbeats from the leader
    while(raftState == RaftState::follower) {
      std::this_thread::sleep_for(randomTimeout);
      if(std::chrono::steady_clock::now() - lastAppend > randomTimeout) {
        stateTransition(RaftState::candidate);
      }
    }

    while(raftState == RaftState::candidate) {
      performElection();
    }

    // I'm leader - send heartbeats
    while(raftState == RaftState::leader) {
      for(size_t i = 0; i < talkers.size(); i++) {
        talkers[i]->sendHandshake(journal.getClusterID());
        std::future<redisReplyPtr> reply = talkers[i]->sendHeartbeat(journal.getCurrentTerm(), myselfID, 6, 2, 0);
      }
      std::this_thread::sleep_for(timeoutLow);
    }

    updateRandomTimeout();
  }
}

XrdRedisStatus XrdRedisRaft::configureParticipants(std::vector<RaftServer> &reps) {
  bool found_me = false;
  for(size_t i = 0; i < reps.size(); i++) {
    if(reps[i].hostname == myself.hostname && reps[i].port == myself.port) {
      found_me = true;
      myselfID = i;
      break;
    }
  }

  if(!found_me) {
    return XrdRedisStatus(rocksdb::Status::kCorruption, "received a reconfigure request which doesn't include myself");
  }

  participants = reps;
  talkers.clear();

  for(size_t i = 0; i < participants.size(); i++) {
    if(participants[i].hostname == myself.hostname && participants[i].port == myself.port) continue;
    talkers.push_back(new XrdRedisRaftTalker(participants[i]));
  }

  return OK();
}

static std::string stateToString(RaftState state) {
  if(state == RaftState::leader) return "leader";
  if(state == RaftState::candidate) return "candidate";
  if(state == RaftState::follower) return "follower";
  if(state == RaftState::shutdown) return "shutdown";
  return "unknown";
}

void XrdRedisRaft::stateTransition(RaftState newstate) {
  if(newstate == raftState) return;

  std::cout << "RAFT: state transition: " << stateToString(raftState) << " => " << stateToString(newstate) << std::endl;
  raftState = newstate;
}

void XrdRedisRaft::declareTerm(RaftTerm newTerm, RaftServerID newLeader) {
  if(newLeader != -1) {
    std::cout << "RAFT: recognizing leader " << newLeader << " for term " << newTerm << std::endl;
  }
}

void XrdRedisRaft::updateTermIfNecessary(RaftTerm term, RaftServerID newLeader) {
  if(term > journal.getCurrentTerm()) {
    std::cout << "RAFT: transitioning from term " << journal.getCurrentTerm() << " to " << term << std::endl;
    journal.setCurrentTerm(term);
    journal.setVotedFor(newLeader);
    leader = newLeader;
    stateTransition(RaftState::follower);
    declareTerm(journal.getCurrentTerm(), newLeader);
  }
  else if(leader == -1) {
    leader = newLeader;
    declareTerm(journal.getCurrentTerm(), newLeader);
  }
}

std::vector<std::string> XrdRedisRaft::appendEntries(RaftTerm term, RaftServerID leaderId, LogIndex prevIndex, RaftTerm prevTerm,
                                           XrdRedisRequest &req, LogIndex commit) {


  ScopedAdder inflight(requestsInFlight);
  std::vector<std::string> ret;

  RaftTerm current = journal.getCurrentTerm();

  if(current != term) {
    std::cout << "First heartbeat for " << term << " from server " << leaderId << std::endl;
  }

  ret.emplace_back("TERM");
  ret.emplace_back(SSTR(current));

  if(term < current) {
    ret.emplace_back("OUTCOME FAIL");
    ret.emplace_back("My raft term is newer");
    return ret;
  }

  updateTermIfNecessary(term, leaderId);

  if(leader != leaderId) {
    std::cout << "SERIOUS WARNING: received appendEntries from unrecognized leader!!" << std::endl;
    ret.emplace_back("OUTCOME FAIL");
    ret.emplace_back("You are not the current leader!");
  }

  lastAppend = std::chrono::steady_clock::now();

  if(!journal.entryExists(prevTerm, prevIndex)) {
    ret.emplace_back("OUTCOME FAIL");
    ret.emplace_back("Log entry mismatch");
    return ret;
  }

  XrdRedisStatus st = journal.append(prevTerm, prevIndex, req);

  ret.emplace_back("OUTCOME");
  if(!st.ok()) {
    ret.emplace_back("FAIL");
    ret.emplace_back(st.ToString());
  }
  else {
    ret.emplace_back("OK");
  }

  return ret;
}

std::vector<std::string> XrdRedisRaft::requestVote(RaftTerm term, int64_t candidateId, LogIndex lastIndex, RaftTerm lastTerm) {
  std::vector<std::string> ret;
  updateTermIfNecessary(term, -1);

  ret.emplace_back("TERM");
  ret.emplace_back(SSTR(journal.getCurrentTerm()));

  bool granted = journal.requestVote(term, candidateId, lastIndex, lastTerm);

  ret.emplace_back("VOTE-GRANTED");
  if(granted) {
    ret.emplace_back("TRUE");
  }
  else {
    ret.emplace_back("FALSE");
  }

  std::cout << "Answering " << granted << " to requestVote for term " << term
            << " and candidate " << candidateId << "." << std::endl;
  return ret;
}

std::vector<std::string> XrdRedisRaft::info() {
  std::vector<std::string> ret;
  ret.emplace_back("CLUSTER-ID");
  ret.emplace_back(SSTR(journal.getClusterID()));

  ret.emplace_back("STATE");
  ret.emplace_back(stateToString(raftState));

  ret.emplace_back("TERM");
  ret.emplace_back(SSTR(journal.getCurrentTerm()));

  ret.emplace_back("LEADER");
  ret.emplace_back(SSTR(leader));

  ret.emplace_back("RANDOM-TIMEOUT");
  ret.emplace_back(SSTR(randomTimeout.count()));

  ret.emplace_back("LOG-SIZE");
  ret.emplace_back(SSTR(journal.getLogSize()));

  ret.emplace_back("REQUESTS-IN-FLIGHT");
  ret.emplace_back(SSTR(requestsInFlight));

  ret.emplace_back("LAST-CONTACT");
  ret.emplace_back(SSTR(
    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - lastAppend).count()
  ));

  ret.emplace_back("PARTICIPANTS");

  std::ostringstream ss;
  for(size_t i = 0; i < participants.size(); i++) {
    ss << participants[i].hostname << ":" << participants[i].port;
    if(i != participants.size()-1) ss << ",";
  }
  ret.emplace_back(ss.str());

  ret.emplace_back("MYSELF");
  ret.emplace_back(SSTR(myself.hostname << ":" << myself.port));

  ret.emplace_back("MY-ID");
  ret.emplace_back(SSTR(myselfID));
  return ret;
}
