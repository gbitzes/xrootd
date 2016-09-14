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

void XrdRedisRaft::transition(RaftState newstate, RaftTerm newterm, RaftServerID newvotedfor, RaftServerID newleader) {
  std::lock_guard<std::mutex> lock(transitionMutex);

  // if(raftState != newstate) {
  //   std::cout << "RAFT: state transition: " << stateToString(raftState) << " => " << stateToString(newstate) << std::endl;
  //   raftState = newstate;
  // }
  //
  // if(raftTerm != newterm) {
  //   std::cout << "RAFT: transitioning from term " << journal.getCurrentTerm() << " to " << newterm << std::endl;
  //   raftTerm = newterm;
  //   journal.setCurrentTerm(newterm);
  // }
  //
  // if(newvotedfor != -1) {
  //   std::cout << "RAFT: setting votedFor for term " << journal.getCurrentTerm() << " to machine " << newvotedfor << std::endl;
  //   journal.setVotedFor(newvotedfor);
  // }
  //
  // if(newLeader != -1) {
  //   std::cout << "RAFT: recognizing leader " << newleader << " for term " << journal.getCurrentTerm() << std::endl;
  // }
  // leader = newleader;
}

static std::string redis_reply_to_str(redisReply *element) {
  if(element->type != REDIS_REPLY_STRING) return std::string();
  return std::string(element->str, element->len);
}

static std::pair<RaftTerm, bool> processVote(redisReplyPtr &reply) {
  bool answer = false;
  RaftTerm term = -1;

  if(reply == nullptr) return {term, answer};
  if(reply->type != REDIS_REPLY_ARRAY) return {term, answer};

  for(size_t i = 0; i < reply->elements; i++) {
    if(redis_reply_to_str(reply->element[i]) == "VOTE-GRANTED") {
      answer = (redis_reply_to_str(reply->element[i+1]) == "TRUE");
      i += 1;
    }
    else if(redis_reply_to_str(reply->element[i]) == "TERM") {
      my_strtoll(redis_reply_to_str(reply->element[i+1]), term);
    }
  }

  return {term, answer};
}

size_t XrdRedisRaft::processVotes(std::vector<std::future<redisReplyPtr>> &replies) {
  size_t votes = 1;

  for(size_t i = 0; i < replies.size(); i++) {
    redisReplyPtr reply = replies[i].get();
    std::pair<RaftTerm, bool> outcome = processVote(reply);
    if(outcome.first != -1) {
      updateTermIfNecessary(outcome.first, -1);
    }

    if(outcome.second) votes++;
  }
  return votes;
}

void XrdRedisRaft::performElection() {
  RaftTerm newTerm = journal.getCurrentTerm()+1;

  journal.setCurrentTerm(newTerm);
  journal.setVotedFor(myselfID);

  std::vector<std::future<redisReplyPtr>> replies;
  for(size_t i = 0; i < talkers.size(); i++) {
    if(!talkers[i]) continue;

    talkers[i]->sendHandshake(journal.getClusterID(), participants);
    replies.push_back(talkers[i]->sendRequestVote(newTerm, myselfID, 0, 0)); // TODO
  }

  size_t acks = processVotes(replies);

  if(raftState != RaftState::candidate) {
    // no longer a candidate, abort. We most likely received information about a newer term in the meantime
    std::cout << "RAFT: election round for " << newTerm << " interrupted after receiving " << acks << " votes. " << std::endl;
    return;
  }

  if(acks >= quorumThreshold) {
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

static std::pair<RaftTerm, bool> processAppendEntriesReply(redisReplyPtr &reply) {
  bool success = false;
  RaftTerm term = -1;

  if(reply == nullptr) return {term, success};
  if(reply->type != REDIS_REPLY_ARRAY) return {term, success};

  for(size_t i = 0; i < reply->elements; i++) {
    if(redis_reply_to_str(reply->element[i]) == "OUTCOME") {
      success = (redis_reply_to_str(reply->element[i+1]) == "OK");
      i += 1;
    }
    else if(redis_reply_to_str(reply->element[i]) == "TERM") {
      my_strtoll(redis_reply_to_str(reply->element[i+1]), term);
    }
  }

  return {term, success};
}

// I'm a master, send updates / heartbeats to a specific follower
void XrdRedisRaft::monitorFollower(RaftServerID machine) {
  assert(follower != myselfID);
  std::cout << "starting monitoring thread for " << machine << std::endl;

  LogIndex nextIndex = journal.getLogSize();

  while(raftState == RaftState::leader) {
    talkers[machine]->sendHandshake(journal.getClusterID(), participants);

    RaftTerm prevTerm;
    XrdRedisRequest tmp;
    journal.fetch(nextIndex-1, prevTerm, tmp);

    std::future<redisReplyPtr> fut;
    redisReplyPtr reply;

    std::pair<RaftTerm, bool> outcome;
    if(nextIndex == journal.getLogSize()) {
      fut = talkers[machine]->sendHeartbeat(journal.getCurrentTerm(), myselfID, nextIndex-1, prevTerm, 0);
      reply = fut.get();
      outcome = processAppendEntriesReply(reply);
    }
    else {
      RaftTerm entryTerm;
      XrdRedisRequest entry;
      journal.fetch(nextIndex, entryTerm, entry);
      fut = talkers[machine]->sendAppendEntries(journal.getCurrentTerm(), myselfID, nextIndex-1, prevTerm, 0, entry, entryTerm);
      reply = fut.get();
      outcome = processAppendEntriesReply(reply);
      if(outcome.second) nextIndex++;
    }

    if(!outcome.second && nextIndex > 1 && reply) {
      // BUG: only decrement on entry mismatch
      nextIndex--;
    }

    updateTermIfNecessary(outcome.first, -1);
    std::this_thread::sleep_for(timeoutLow);
  }
  std::cout << "shutting down monitoring thread for " << machine << std::endl;
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
      std::vector<std::thread> monitors;

      for(size_t i = 0; i < participants.size(); i++) {
        if( (RaftServerID) i != myselfID) {
          monitors.emplace_back(&XrdRedisRaft::monitorFollower, this, i);
        }
      }

      for(size_t i = 0; i < monitors.size(); i++) {
        monitors[i].join();
      }
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
    if(participants[i].hostname == myself.hostname && participants[i].port == myself.port) {
      talkers.push_back(nullptr);
      continue;
    }

    talkers.push_back(new XrdRedisRaftTalker(participants[i]));
  }

  quorumThreshold = (participants.size()/2) + 1;
  std::cout << "RAFT: setting quorum threshold to " << quorumThreshold << std::endl;
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

void XrdRedisRaft::panic() {
  std::cout << "RAFT: PANIC mode!!! Advancing term by 100 and triggering re-election." << std::endl;
  journal.setCurrentTerm(journal.getCurrentTerm() + 100);
  stateTransition(RaftState::candidate);
}

void XrdRedisRaft::triggerPanic() {
  std::cout << "RAFT: Triggering cluster-wide PANIC to force re-election." << std::endl;
  for(size_t i = 0; i < talkers.size(); i++) {
    if(!talkers[i]) continue;
    talkers[i]->sendPanic();
  }
  panic();
}

XrdRedisStatus XrdRedisRaft::pushUpdate(XrdRedisRequest &req) {
  journal.leaderAppend(req);
  return OK();
}

std::vector<std::string> XrdRedisRaft::appendEntries(RaftTerm term, RaftServerID leaderId, LogIndex prevIndex, RaftTerm prevTerm,
                                           XrdRedisRequest &req, RaftTerm entryTerm, LogIndex commit) {


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
    ret.emplace_back("OUTCOME");
    ret.emplace_back("FAIL");
    ret.emplace_back("You are not the current leader!");
    this->triggerPanic();
  }

  lastAppend = std::chrono::steady_clock::now();

  if(!journal.entryExists(prevTerm, prevIndex)) {
    ret.emplace_back("OUTCOME");
    ret.emplace_back("FAIL");
    ret.emplace_back("Log entry mismatch");
    return ret;
  }

  XrdRedisStatus st = journal.append(prevTerm, prevIndex, req, entryTerm);

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
  assert(candidateId != myselfID);

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

// for interactive debugging purposes only
std::vector<std::string> XrdRedisRaft::fetch(LogIndex index) {
  std::vector<std::string> ret;

  RaftTerm term;
  XrdRedisRequest req;

  XrdRedisStatus st = journal.fetch(index, term, req);
  if(!st.ok()) return {};

  ret.emplace_back("TERM");
  ret.emplace_back(SSTR(term));

  for(size_t i = 0; i < req.size(); i++) {
    ret.emplace_back(*req[i]);
  }

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
