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
    req.emplace_back("PING");
    // req.emplace_back(std::make_shared<std::string>("PING"));
    st = rawAppend(-1, 0, req);
    if(!st.ok()) throw st;

    st = setLogSize(logSize+1);
    if(!st.ok()) throw st;
  }
}

// major event, should happen very rarely
XrdRedisStatus XrdRedisJournal::statusUpdate(RaftTerm term, RaftServerID vote) {
  std::lock_guard<std::mutex> lock(statusUpdateMutex);

  XrdRedisStatus st = storage->set("RAFT_CURRENT_TERM", SSTR(term));
  if(st.ok()) {
    currentTerm = term;
  }
  else {
    return st;
  }

  // even if we crash here, having a wrong votedFor will mean this node will refuse to
  // vote for any other node only for this term. Not a problem

  votedFor = vote;
  return storage->set("RAFT_VOTED_FOR", SSTR(vote));
}

XrdRedisStatus XrdRedisJournal::setLastApplied(LogIndex index) {
  XrdRedisStatus st = storage->set("RAFT_LAST_APPLIED", SSTR(index));
  if(st.ok()) lastApplied = index;
  return st;
}

RaftTerm XrdRedisJournal::getCurrentTerm() {
  return currentTerm;

  RaftTerm tmp;
  retrieve("RAFT_CURRENT_TERM", tmp);
  return tmp;
}

XrdRedisJournal::~XrdRedisJournal() {
  delete storage;
}

// check whether there's an entry in the log and has the specified term
bool XrdRedisJournal::entryExists(RaftTerm term, LogIndex revision) {
  if(logSize <= revision) {
    return false;
  }

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

// remove all entries in [start, end] inclusive
void XrdRedisJournal::removeEntries(LogIndex start, LogIndex end) {
  // TODO: take care of pendingReplies
  // std::cout << "Major raft event: remove inconsistent entries, from " << start << " to the end, " << logSize << std::endl;

  for(LogIndex i = start; i <= end; i++) {
    XrdRedisStatus st = storage->del(SSTR("REVISION_" << i));
  }

  // for(LogIndex i = start; i < logSize; i++) {
  //   XrdRedisStatus st = storage->del(SSTR("REVISION_" << i));
  //   if(!st.ok()) {
  //     std::cout << "WARNING: unable to delete inconsistent entry in raft journal. Continuing anyway." << std::endl;
  //   }
  // }
  // setLogSize(start);
}

XrdRedisStatus XrdRedisJournal::setLogSize(const LogIndex newsize) {
  XrdRedisStatus st = storage->set("RAFT_LOG_SIZE", std::to_string(newsize));
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

    cmd.emplace_back(pos, len); // new std::string(pos, len));
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
    append_int_to_string(cmd[i].size(), ss);
    ss << cmd[i];
  }

  return ss.str();
}

LogIndex XrdRedisJournal::append(XrdRedisRequest &req) {
  std::lock_guard<std::mutex> lock(appendMutex);

  LogIndex index = logSize;
  rawAppend(currentTerm, index, req);
  setLogSize(logSize+1);

  return index;
}

XrdRedisStatus XrdRedisJournal::rawAppend(RaftTerm term, LogIndex index, XrdRedisRequest &cmd) {
  return storage->set(SSTR("REVISION_" << index), serializeRedisRequest(term, cmd));
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

// XrdRedisStatus XrdRedisJournal::append(RaftTerm prevTerm, LogIndex prevIndex, XrdRedisRequest &cmd, RaftTerm entryTerm) {
//   // entry already exists?
//   if(logSize > prevIndex+1) {
//     // TODO verify log entries have not been commited yet (very serious error)
//     // TODO if entry has same raft index, maybe it's a duplicate message and we don't need to delete anything
//     removeInconsistent(prevIndex+1);
//   }
//
//   // don't add anything to the log if it's only a heartbeat
//   if(cmd.size() == 1 && strcasecmp(cmd[0]->c_str(), "HEARTBEAT") == 0) {
//     return OK();
//   }
//
//   XrdRedisStatus st = rawAppend(entryTerm, prevIndex+1, cmd);
//   if(!st.ok()) return st;
//
//   st = setLogSize(prevIndex + 2);
//   if(!st.ok()) return st;
//
//   return OK();
// }
