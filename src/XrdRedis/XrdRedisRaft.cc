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
#include "Xrd/XrdLink.hh"

#include <random>
#include <algorithm>
#include <set>

static XrdRedisStatus OK() {
  return XrdRedisStatus(rocksdb::Status::kOk);
}

XrdRedisRaft::XrdRedisRaft(XrdRedisBackend *journalStore, XrdRedisBackend *smachine, RaftClusterID id, RaftServer me)
: journal(journalStore, id), stateMachine(smachine), participants{me}, myself(me) {

  lastAppend = std::chrono::steady_clock::now();
  updateRandomTimeout();

  monitorThread = std::thread(&XrdRedisRaft::monitor, this);
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
    std::future_status reply_status = replies[i].wait_for(std::chrono::milliseconds(300));
    if(reply_status != std::future_status::ready) continue;

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

  journal.statusUpdate(newTerm, myselfID);

  LogIndex lastIndex = journal.getLogSize() - 1;
  RaftTerm lastTerm;
  journal.fetchTerm(lastIndex, lastTerm);

  std::vector<std::future<redisReplyPtr>> replies;
  for(size_t i = 0; i < talkers.size(); i++) {
    if(!talkers[i]) continue;
    replies.push_back(talkers[i]->sendRequestVote(newTerm, myselfID, lastIndex, lastTerm));
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
    if(raftState != RaftState::candidate) return;
  }
}

void XrdRedisRaft::updateRandomTimeout() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dist(timeoutLow.count(), timeoutHigh.count());
  randomTimeout = std::chrono::milliseconds(dist(gen));
  std::cout << "RAFT: setting random timeout to " << randomTimeout.count() << "ms" << std::endl;
}

static AppendEntriesReply processAppendEntriesReply(redisReplyPtr &reply) {
  AppendEntriesReply ret;

  if(reply == nullptr) return ret;
  if(reply->type != REDIS_REPLY_ARRAY) {
    std::cout << "RAFT critical error: " << " processAppendEntriesReply got a non-array reply" << std::endl;
    return ret;
  }

  ret.online = true;
  for(size_t i = 0; i < reply->elements; i++) {
    // std::cout << redis_reply_to_str(reply->element[i]) << std::endl;
    if(redis_reply_to_str(reply->element[i]) == "OUTCOME") {
      ret.success = (redis_reply_to_str(reply->element[i+1]) == "OK");
    }
    else if(redis_reply_to_str(reply->element[i]) == "TERM") {
      my_strtoll(redis_reply_to_str(reply->element[i+1]), ret.term);
    }
    else if(redis_reply_to_str(reply->element[i]) == "LOG-SIZE") {
      my_strtoll(redis_reply_to_str(reply->element[i+1]), ret.logSize);
    }
  }

  return ret;
}

std::string XrdRedisRaft::getLeader() {
  if(leader >= 0) return SSTR(participants[leader].hostname << ":" << participants[leader].port);
  return "";
}

// rapid-fire multiple append entries
AppendEntriesReply XrdRedisRaft::pipelineAppendEntries(RaftServerID machine, LogIndex nextIndex, RaftTerm prevTerm, int pipelineLength) {
  LogIndex startIndex = nextIndex;
  LogIndex lastIndex = std::min(journal.getLogSize(), nextIndex + pipelineLength);
  RaftTerm currentTerm = journal.getCurrentTerm();

  std::vector<std::future<redisReplyPtr>> fut;

  for(LogIndex index = startIndex; index < lastIndex; index++) {
    RaftTerm entryTerm;
    XrdRedisRequest entry;
    journal.fetch(index, entryTerm, entry);
    fut.push_back(talkers[machine]->sendAppendEntries(currentTerm, myselfID, index-1, prevTerm, journal.getCommitIndex(), entry, entryTerm));
    prevTerm = entryTerm;
  }

  AppendEntriesReply outcome;
  for(size_t i = 0; i < fut.size(); i++) {
    redisReplyPtr reply = fut[i].get();
    outcome = processAppendEntriesReply(reply);

    if(!outcome.success) {
      break;
    }

    updateMatchIndex(machine, nextIndex+i);
  }

  return outcome;
}

// I'm a master, send updates / heartbeats to a specific follower
void XrdRedisRaft::monitorFollower(RaftServerID machine) {
  std::unique_lock<std::mutex> lock(logUpdatedMutex, std::defer_lock);
  assert(follower != myselfID);

  LogIndex nextIndex = journal.getLogSize();

  int pipelineLength = 1;

  while(raftState == RaftState::leader) {
    RaftTerm prevTerm;
    XrdRedisRequest tmp;
    journal.fetch(nextIndex-1, prevTerm, tmp);

    AppendEntriesReply outcome;
    if(nextIndex == journal.getLogSize()) {
      std::future<redisReplyPtr> fut = talkers[machine]->sendHeartbeat(journal.getCurrentTerm(), myselfID, nextIndex-1, prevTerm, journal.getCommitIndex());
      redisReplyPtr reply = fut.get();
      outcome = processAppendEntriesReply(reply);

      if(outcome.online && outcome.success) {
        updateMatchIndex(machine, outcome.logSize - 1);
      }
    }
    else {
      outcome = pipelineAppendEntries(machine, nextIndex, prevTerm, pipelineLength);
    }

    if(outcome.online && !outcome.success && nextIndex <= outcome.logSize) {
      nextIndex--;
    }
    else if(outcome.online && outcome.logSize > 0) {
      nextIndex = outcome.logSize;
    }

    if(!outcome.success) pipelineLength = 1;
    if(outcome.success && pipelineLength < 30000) {
      pipelineLength *= 2;
    }

    updateTermIfNecessary(outcome.term, -1);

    // bool sleepUntilNextRound = true;

    if(outcome.online && nextIndex >= journal.getLogSize()) {
      lock.lock();
      logUpdated.wait_for(lock, heartbeatInterval);
      lock.unlock();
    }
    else if(!outcome.online) {
      std::this_thread::sleep_for(heartbeatInterval);
    }

    // if(outcome.online && nextIndex < journal.getLogSize()) {
    //   sleepUntilNextRound = false;
    // }
    //
    // if(sleepUntilNextRound) {
    //   lock.lock();
    //   logUpdated.wait_for(lock, heartbeatInterval);
    //   lock.unlock();
    // }
  }
}

// I'm a follower, monitor what the leader is doing
void XrdRedisRaft::monitorLeader() {
  while(raftState == RaftState::follower) {
    this->applyCommits();

    std::this_thread::sleep_for(randomTimeout);
    if(std::chrono::steady_clock::now() - lastAppend > randomTimeout) {
      std::cout << "FOLLOWER: timeout" << std::endl;
      stateTransition(RaftState::candidate);
    }
  }
}

void XrdRedisRaft::updateMatchIndex(RaftServerID machine, LogIndex index) {
  std::unique_lock<std::mutex> lock(matchIndexMutex);

  matchIndex[machine] = index;

  std::vector<LogIndex> sortedMatchIndex = matchIndex;
  std::sort(sortedMatchIndex.begin(), sortedMatchIndex.end());
  size_t threshold = participants.size() - quorumThreshold;
  journal.setCommitIndex(sortedMatchIndex[threshold]);
  this->applyCommits();
}

void XrdRedisRaft::monitor() {
  // things are a bit volatile in the beginning, maybe the request processing
  // threads have not started yet. Avoid being too agressive into becoming
  // a candidate, and wait a bit at the beginning
  std::this_thread::sleep_for(std::chrono::seconds(3));
  struct sched_param params;
  params.sched_priority = 50;

  while(raftState != RaftState::shutdown) {
    // make sure I'm receiving heartbeats from the leader
    while(raftState == RaftState::follower) {
      monitorLeader();
    }

    while(raftState == RaftState::candidate) {
      performElection();
    }

    // I'm leader - launch one thread for every follower so as to keep them up-to-date
    // and send heartbeats when necessary
    while(raftState == RaftState::leader) {
      matchIndex.clear();
      for(size_t i = 0; i < participants.size(); i++) {
        matchIndex.push_back(0);
      }
      updateMatchIndex(myselfID, journal.getLogSize()-1);

      std::vector<std::thread> monitors;

      for(size_t i = 0; i < participants.size(); i++) {
        if( (RaftServerID) i != myselfID) {
          monitors.emplace_back(&XrdRedisRaft::monitorFollower, this, i);
          pthread_setschedparam(monitors[monitors.size()-1].native_handle(), SCHED_FIFO, &params);
        }
      }

      for(size_t i = 0; i < monitors.size(); i++) {
        monitors[i].join();
      }

      clearPendingReplies();
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

    talkers.push_back(new XrdRedisRaftTalker(participants[i], journal.getClusterID(), participants));
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
    journal.statusUpdate(term, newLeader);
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
  journal.statusUpdate(journal.getCurrentTerm() + 100, -1);
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

static redisReplyPtr redis_reply_err(const std::string &err) {
  redisReply *r = (redisReply*) calloc(1, sizeof(redisReply));
  r->type = REDIS_REPLY_ERROR;
  r->len = err.size();
  r->str = (char*) malloc( (r->len+1) * sizeof(char) );
  strcpy(r->str, err.c_str());
  return redisReplyPtr(r, freeReplyObject);
}

// std::future<redisReplyPtr> XrdRedisRaft::pushUpdate(XrdRedisRequest &req, XrdLink *lp) {
void XrdRedisRaft::pushUpdate(XrdRedisRequest &req, XrdLink *lp) {
  // if(raftState != RaftState::leader) {
  //   std::promise<redisReplyPtr> reply;
  //   reply.set_value(redis_reply_err(SSTR("MOVED 0 " << getLeader())));
  //   return reply.get_future();
  // }

  std::lock_guard<std::mutex> lock(updating);

  LogIndex index = journal.append(req);

  // std::promise<redisReplyPtr> promise;
  // std::future<redisReplyPtr> ret = promise.get_future();

  std::unique_lock<std::mutex> lock2(pendingRepliesMutex);
  pendingReplies.emplace(std::make_pair(index, lp)); // std::move(promise)));
  // std::cout << "pendingReplies size: " << pendingReplies.size() << std::endl;
  lock2.unlock();

  updateMatchIndex(myselfID, index);
  logUpdated.notify_all();

  // return std::move(ret);
}

static int Send(const redisReplyPtr &reply, XrdLink *Link) {
  if(reply->type == REDIS_REPLY_STATUS) {
    Link->Send("+", 1);
    Link->Send(reply->str, reply->len);
    return Link->Send("\r\n", 2);
  }
  else if(reply->type == REDIS_REPLY_ERROR) {
    Link->Send("-", 1);
    Link->Send(reply->str, reply->len);
    return Link->Send("\r\n", 2);
  }

  std::cout << "unknown reply type" << std::endl;
  std::terminate();
}


static redisReplyPtr redis_reply_ok() {
  redisReply *r = (redisReply*) calloc(1, sizeof(redisReply));
  r->type = REDIS_REPLY_STATUS;
  r->len = 2;
  r->str = (char*) malloc( (r->len+1) * sizeof(char));
  strcpy(r->str, "OK");
  return redisReplyPtr(r, freeReplyObject);
}

void XrdRedisRaft::clearPendingReplies() {
  std::lock_guard<std::mutex> lock(pendingRepliesMutex);

  for(auto it = pendingReplies.begin(); it != pendingReplies.end(); it++) {
    Send(redis_reply_err("unavailable"), it->second);
    // it->second.set_value(redis_reply_err("unavailable"));
    pendingReplies.erase(it);
  }
}


void XrdRedisRaft::applyCommits() {
  std::unique_lock<std::mutex> lock(pendingRepliesMutex, std::defer_lock);

  while(journal.getLastApplied() < journal.getCommitIndex() && journal.getCommitIndex() < journal.getLogSize()) {
    // std::cout << "commiting " << journal.getLastApplied()+1 << " to state machine" << std::endl;

    XrdRedisRequest cmd;
    RaftTerm term;

    journal.fetch(journal.getLastApplied()+1, term, cmd);
    lock.lock();
    auto it = pendingReplies.find(journal.getLastApplied()+1);
    lock.unlock();

    if(*cmd[0] == "SET" || *cmd[0] == "set") {
      stateMachine->set(*cmd[1], *cmd[2]);
      if(it != pendingReplies.end()) {
        Send(redis_reply_ok(), it->second);
        // it->second.set_value(redis_reply_ok());
        lock.lock();
        pendingReplies.erase(it);
        lock.unlock();
      }
    }
    else {
      std::terminate();
    }

    journal.setLastApplied(journal.getLastApplied()+1);
  }
}

XrdRedisStatus XrdRedisRaft::append(RaftTerm prevTerm, LogIndex prevIndex, XrdRedisRequest &cmd, RaftTerm entryTerm) {
  // entry already exists?
  if(prevIndex+1 < journal.getLogSize()) {
    // TODO if entry has same raft index, maybe it's a duplicate message and we don't need to delete anything
    // TODO take care of pendingReplies

    std::cout << "RAFT: conflict, removing log entries [" << prevIndex+1 << "," << journal.getLogSize()-1 << "]" << std::endl;

    if(prevIndex+1 <= journal.getCommitIndex()) {
      std::cout << "RAFT stop-the-world error: tried to remove commited entries. Bye. (commitIndex = " << journal.getCommitIndex() << ")" << std::endl;
      std::terminate();
    }

    // std::cout << "asked to remove entries from " << prevIndex+1 << " to " << journal.getLogSize()-1 << std::endl;
    // std::cout << "getLogSize: " << journal.getLogSize() << std::endl;
    // std::cout << "prevIndex: " << prevIndex << std::endl;
    // std::terminate();

    journal.removeEntries(prevIndex+1, journal.getLogSize()-1);
    journal.setLogSize(prevIndex);
  }

  // don't add anything to the log if it's only a heartbeat
  if(cmd.size() == 1 && strcasecmp(cmd[0]->c_str(), "HEARTBEAT") == 0) {
    return OK();
  }

  XrdRedisStatus st = journal.rawAppend(entryTerm, prevIndex+1, cmd);
  if(!st.ok()) return st;

  st = journal.setLogSize(prevIndex + 2);
  if(!st.ok()) return st;

  return OK();
}

std::vector<std::string> XrdRedisRaft::appendEntries(RaftTerm term, RaftServerID leaderId, LogIndex prevIndex, RaftTerm prevTerm,
                                           XrdRedisRequest &req, RaftTerm entryTerm, LogIndex commit) {
  std::vector<std::string> ret;

  RaftTerm current = journal.getCurrentTerm();

  if(current != term) {
    std::cout << "First heartbeat for " << term << " from server " << leaderId << std::endl;
  }

  ret.emplace_back("TERM");
  ret.emplace_back(SSTR(current));

  if(term < current) {
    ret.emplace_back("LOG-SIZE");
    ret.emplace_back(SSTR(journal.getLogSize()));

    ret.emplace_back("OUTCOME");
    ret.emplace_back("FAIL");
    ret.emplace_back("My raft term is newer");
    return ret;
  }

  updateTermIfNecessary(term, leaderId);

  if(leader != leaderId) {
    std::cout << "SERIOUS WARNING: received appendEntries from unrecognized leader!!" << std::endl;
    ret.emplace_back("LOG-SIZE");
    ret.emplace_back(SSTR(journal.getLogSize()));

    ret.emplace_back("OUTCOME");
    ret.emplace_back("FAIL");
    ret.emplace_back("You are not the current leader!");
    this->triggerPanic();
    return ret;
  }

  lastAppend = std::chrono::steady_clock::now();

  // std::cout << "checking if entry with index " << prevIndex << " has term " << prevTerm << std::endl;
  // std::cout << "logSize: " << journal.getLogSize() << std::endl;

  if(!journal.entryExists(prevTerm, prevIndex)) {
    ret.emplace_back("LOG-SIZE");
    ret.emplace_back(SSTR(journal.getLogSize()));

    ret.emplace_back("OUTCOME");
    ret.emplace_back("FAIL");
    ret.emplace_back("Log entry mismatch");
    return ret;
  }

  XrdRedisStatus st = this->append(prevTerm, prevIndex, req, entryTerm);

  ret.emplace_back("LOG-SIZE");
  ret.emplace_back(SSTR(journal.getLogSize()));

  ret.emplace_back("OUTCOME");

  // std::cout << "append outcome: " << st.ok() << std::endl;

  if(!st.ok()) {
    ret.emplace_back("FAIL");
    ret.emplace_back(st.ToString());
  }
  else {
    ret.emplace_back("OK");
    journal.setCommitIndex(commit);
  }

  return ret;
}


bool XrdRedisRaft::decideOnVoteRequest(RaftTerm term, int64_t candidateId, LogIndex lastIndex, RaftTerm lastTerm) {
  if(journal.getCurrentTerm() > term) {
    std::cout << "I know of a newer term, rejecting request vote." << std::endl;
    return false;
  }

  if(journal.getCurrentTerm() == term && journal.getVotedFor() != -1 && journal.getVotedFor() != candidateId) {
    std::cout << "I've already voted for this term for " << journal.getVotedFor() << ", rejecting request vote." << std::endl;
    return false;
  }

  RaftTerm myPrevTerm;
  XrdRedisRequest cmd;
  journal.fetch(journal.getLogSize()-1, myPrevTerm, cmd);

  if(myPrevTerm > lastTerm) {
    std::cout << "RAFT: rejecting vote from " << candidateId << " because my log is more up-to-date: term of last entry " << myPrevTerm << " vs " << lastTerm << std::endl;
    return false;
  }

  if(journal.getLogSize()-1 > lastIndex) {
    std::cout << "RAFT: rejecting vote from " << candidateId << " because my log is more up-to-date: index of last entry " << journal.getLogSize()-1 << " vs " << lastIndex << std::endl;
    return false;
  }

  // remember that we've voted for this candidate
  journal.statusUpdate(journal.getCurrentTerm(), candidateId);
  return true;
}

std::vector<std::string> XrdRedisRaft::requestVote(RaftTerm term, RaftServerID candidateId, LogIndex lastIndex, RaftTerm lastTerm) {
  assert(candidateId != myselfID);

  std::vector<std::string> ret;
  updateTermIfNecessary(term, -1);

  ret.emplace_back("TERM");
  ret.emplace_back(SSTR(journal.getCurrentTerm()));

  bool granted = decideOnVoteRequest(term, candidateId, lastIndex, lastTerm);

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

  ret.emplace_back("COMMIT-INDEX");
  ret.emplace_back(SSTR(journal.getCommitIndex()));

  ret.emplace_back("LAST-APPLIED");
  ret.emplace_back(SSTR(journal.getLastApplied()));

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
