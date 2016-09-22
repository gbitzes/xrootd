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

#include "XrdRedisJournal.hh"
#include "XrdRedisUtil.hh"

static XrdRedisStatus OK() {
  return XrdRedisStatus(rocksdb::Status::kOk);
}

XrdRedisStatus XrdRedisJournal::retrieve(const std::string &key, int64_t &value) {
  std::string tmp;
  XrdRedisStatus st = storage->get(key, tmp);
  if(!st.ok()) return XrdRedisStatus(rocksdb::Status::kCorruption, SSTR("unable to retrieve " << key));

  if(!my_strtoll(tmp, value)) {
    return XrdRedisStatus(rocksdb::Status::kCorruption, SSTR("unable to parse " << key << ": " << quotes(value)));
  }
  return OK();
}

XrdRedisJournal::XrdRedisJournal(XrdRedisBackend *store, RaftClusterID id) : storage(store), clusterID(id) {
  // make sure the cluster's ID matches, should catch misconfiguration errors
  RaftClusterID clust;
  XrdRedisStatus st = storage->get("RAFT_CLUSTER_ID", clust);
  if(!st.ok()) throw st;

  if(clust != clusterID) {
    throw XrdRedisStatus(rocksdb::Status::kCorruption, SSTR("unexpected cluster ID " << quotes(clust)));
  }

  // cool, we're joining the correct cluster.. retrieve the raft state from when this server crashed
  st = retrieve("RAFT_CURRENT_TERM", currentTerm);
  if(!st.ok()) throw st;

  st = retrieve("RAFT_VOTED_FOR", votedFor);
  if(!st.ok()) throw st;

  st = retrieve("RAFT_LOG_SIZE", logSize);
  if(!st.ok()) throw st;

  st = retrieve("RAFT_LAST_APPLIED", lastApplied);
  if(!st.ok()) throw st;

  commitIndex = lastApplied;

  if(logSize == 0) {
    XrdRedisRequest req;
    req.emplace_back(std::make_shared<std::string>("PING"));
    st = rawAppend(-1, 0, req);
    if(!st.ok()) throw st;

    st = setLogSize(logSize+1);
    if(!st.ok()) throw st;
  }
}

bool XrdRedisJournal::requestVote(RaftTerm term, int64_t candidateId, LogIndex lastIndex, RaftTerm lastTerm) {
  if(currentTerm > term) {
    std::cout << "I know of a newer term, rejecting request vote." << std::endl;
    return false;
  }

  if(currentTerm == term && votedFor != -1 && votedFor != candidateId) {
    std::cout << "I've already voted for this term for " << votedFor << ", rejecting request vote." << std::endl;
    return false;
  }

  RaftTerm myPrevTerm;
  XrdRedisRequest cmd;
  fetch(logSize-1, myPrevTerm, cmd);

  if(myPrevTerm > lastTerm) {
    std::cout << "RAFT: rejecting vote from " << candidateId << " because my log is more up-to-date: term of last entry " << myPrevTerm << " vs " << lastTerm << std::endl;
    return false;
  }

  if(logSize-1 > lastIndex) {
    std::cout << "RAFT: rejecting vote from " << candidateId << " because my log is more up-to-date: index of last entry " << logSize-1 << " vs " << lastIndex << std::endl;
    return false;
  }

  this->setVotedFor(candidateId);
  return true;
}

XrdRedisStatus XrdRedisJournal::setCurrentTerm(RaftTerm term) {
  XrdRedisStatus st = storage->set("RAFT_CURRENT_TERM", SSTR(term));
  if(st.ok()) currentTerm = term;
  else return st;

  votedFor = -1;
  return storage->set("RAFT_VOTED_FOR", "-1");
}

static redisReplyPtr redis_reply_ok() {
  redisReply *r = (redisReply*) calloc(1, sizeof(redisReply));
  r->type = REDIS_REPLY_STATUS;
  r->len = 2;
  r->str = (char*) malloc( (r->len+1) * sizeof(char));
  strcpy(r->str, "OK");
  return redisReplyPtr(r, freeReplyObject);
}

void XrdRedisJournal::applyCommits() {
  std::unique_lock<std::mutex> lock(pendingRepliesMutex, std::defer_lock);

  while(lastApplied < commitIndex && commitIndex < logSize) {
    // std::cout << "commiting " << lastApplied+1 << " to state machine" << std::endl;

    XrdRedisRequest cmd;
    RaftTerm term;

    fetch(lastApplied+1, term, cmd);
    lock.lock();
    auto it = pendingReplies.find(lastApplied+1);
    lock.unlock();

    if(*cmd[0] == "SET" || *cmd[0] == "set") {
      storage->set(*cmd[1], *cmd[2]);
      if(it != pendingReplies.end()) {
        it->second.set_value(redis_reply_ok());
      }
    }
    else {
      std::terminate();
    }

    lock.lock();
    if(it != pendingReplies.end()) {
      pendingReplies.erase(it);
    }
    lock.unlock();

    setLastApplied(lastApplied+1);
  }
}

XrdRedisStatus XrdRedisJournal::setVotedFor(RaftServerID vote) {
  XrdRedisStatus st = storage->set("RAFT_VOTED_FOR", SSTR(vote));
  if(st.ok()) votedFor = vote;
  return st;
}

XrdRedisStatus XrdRedisJournal::setLastApplied(LogIndex index) {
  XrdRedisStatus st = storage->set("RAFT_LAST_APPLIED", SSTR(index));
  if(st.ok()) lastApplied = index;
  return st;
}

XrdRedisJournal::~XrdRedisJournal() {
  delete storage;
}

// check whether there's an entry in the log and has the specified term
bool XrdRedisJournal::entryExists(RaftTerm term, LogIndex revision) {
  std::string tmp;
  XrdRedisStatus st = storage->get(SSTR("REVISION_" << revision), tmp);
  if(!st.ok()) {
    std::cout << "Raft event - log entry " << revision << " does not exist" << std::endl;
    return false;
  }

  int64_t trm;
  memcpy(&trm, tmp.c_str(), sizeof(trm));
  // std::cout << "LogEntry " << revision << " has term " << trm << std::endl;

  if(trm != term) {
    std::cout << "Raft event - log entry " << revision << " has different term than the leader expects, " << trm << " vs " << term << std::endl;
  }

  return term == trm;
}


void XrdRedisJournal::removeInconsistent(LogIndex start) {
  // TODO: take care of pendingReplies
  std::cout << "Major raft event: remove inconsistent entries, from " << start << " to the end, " << logSize << std::endl;
  for(LogIndex i = start; i < logSize; i++) {
    XrdRedisStatus st = storage->del(SSTR("REVISION_" << i));
    if(!st.ok()) {
      std::cout << "WARNING: unable to delete inconsistent entry in raft journal. Continuing anyway." << std::endl;
    }
  }
  setLogSize(start);
}

XrdRedisStatus XrdRedisJournal::setLogSize(const LogIndex newsize) {
  XrdRedisStatus st = storage->set("RAFT_LOG_SIZE", SSTR(newsize));
  if(st.ok()) logSize = newsize;
  return st;
}

void XrdRedisJournal::setCommitIndex(LogIndex index) {
  commitIndex = index;
}

static int64_t fetch_int_from_string(const char *pos) {
  int64_t result;
  memcpy(&result, pos, sizeof(result));
  return result;
}

static bool deserializeRedisRequest(const std::string &data, RaftTerm &term, XrdRedisRequest &cmd) {
  term = fetch_int_from_string(data.c_str());

  const char *pos = data.c_str() + sizeof(term);
  const char *end = data.c_str() + data.size();

  while(pos < end) {
    int64_t len = fetch_int_from_string(pos);
    pos += sizeof(len);

    cmd.emplace_back(new std::string(pos, len));
    pos += len;
  }

  return true;
}

static void append_int_to_string(int64_t source, std::ostringstream &target) {
  std::string tmp(sizeof(source), '0');
  memcpy(&tmp[0], &source, sizeof(source));
  target << tmp;
}

static std::string serializeRedisRequest(RaftTerm term, const XrdRedisRequest &cmd) {
  std::ostringstream ss;
  append_int_to_string(term, ss);

  for(size_t i = 0; i < cmd.size(); i++) {
    append_int_to_string(cmd[i]->size(), ss);
    ss << *cmd[i];
  }

  return ss.str();
}

std::pair<LogIndex, std::future<redisReplyPtr>> XrdRedisJournal::leaderAppend(XrdRedisRequest &req) {
  LogIndex index = logSize;
  rawAppend(currentTerm, index, req);
  setLogSize(logSize+1);

  std::lock_guard<std::mutex> lock(pendingRepliesMutex);
  auto resp = pendingReplies.emplace(std::make_pair(index, std::promise<redisReplyPtr>()));
  return {index, resp.first->second.get_future()};
}

XrdRedisStatus XrdRedisJournal::rawAppend(RaftTerm term, LogIndex index, XrdRedisRequest &cmd) {
  // std::cout << "rawAppend - term " << term << " index " << index << ": ";
  // for(size_t i = 0; i < cmd.size(); i++) {
  //   std::cout << *cmd[i] << " ";
  // }
  // std::cout << std::endl;

  XrdRedisStatus st = storage->set(SSTR("REVISION_" << index), serializeRedisRequest(term, cmd));
  // std::cout << st.ToString() << std::endl;
  return st;
}

XrdRedisStatus XrdRedisJournal::fetchTerm(LogIndex index, RaftTerm &term) {
  std::string data;
  XrdRedisStatus st = storage->get(SSTR("REVISION_" << index), data);
  // TODO: investigate whether I can only retrieve the first few bytes of a value in rocksdb
  if(!st.ok()) return st;

  term = fetch_int_from_string(data.c_str());
  return st;
}

XrdRedisStatus XrdRedisJournal::fetch(LogIndex index, RaftTerm &term, XrdRedisRequest &cmd) {
  std::string data;
  XrdRedisStatus st = storage->get(SSTR("REVISION_" << index), data);
  if(!st.ok()) return st;

  deserializeRedisRequest(data, term, cmd);
  return OK();
}

XrdRedisStatus XrdRedisJournal::append(RaftTerm prevTerm, LogIndex prevIndex, XrdRedisRequest &cmd, RaftTerm entryTerm) {
  // entry already exists?
  if(logSize > prevIndex+1) {
    // TODO verify log entries have not been commited yet (very serious error)
    // TODO if entry has same raft index, maybe it's a duplicate message and we don't need to delete anything
    removeInconsistent(prevIndex+1);
  }

  // don't add anything to the log if it's only a heartbeat
  if(cmd.size() == 1 && strcasecmp(cmd[0]->c_str(), "HEARTBEAT") == 0) {
    return OK();
  }

  XrdRedisStatus st = rawAppend(entryTerm, prevIndex+1, cmd);
  if(!st.ok()) return st;

  st = setLogSize(prevIndex + 2);
  if(!st.ok()) return st;

  return OK();
}



// XrdRedisStatus XrdRedisJournal2::append(XrdRedisCommand& cmd, RaftTerm term, LogIndex revision) {
//    std::lock_guard<std::mutex> lock(m);
//
//   last_index++;
//   XrdRedisStatus st = storage->set(SSTR("REVISION_" << last_index), cmd.toString());
//   return st;
//
//   // if(!st.ok()) return st;
//   // revision might have been updated in the meantime, don't use my_revision
//   // rev = my_revision;
//   // return store->set("GLOBAL_REVISION", SSTR(revision));
// }
