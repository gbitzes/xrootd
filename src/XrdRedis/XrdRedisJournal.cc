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

XrdRedisStatus XrdRedisJournal2::retrieve(const std::string &key, int64_t &value) {
  std::string tmp;
  XrdRedisStatus st = storage->get(key, tmp);
  if(!st.ok()) return XrdRedisStatus(rocksdb::Status::kCorruption, SSTR("unable to retrieve " << key));

  if(!my_strtoll(tmp, value)) {
    return XrdRedisStatus(rocksdb::Status::kCorruption, SSTR("unable to parse " << key << ": " << quotes(value)));
  }
  return OK();
}

XrdRedisJournal2::XrdRedisJournal2(XrdRedisBackend *store, RaftClusterID id) : storage(store), clusterID(id) {
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

  if(logSize == 0) {
    XrdRedisRequest req;
    req.emplace_back(std::make_shared<std::string>("PING"));
    st = rawAppend(-1, 0, req);
    if(!st.ok()) throw st;

    st = setLogSize(logSize+1);
    if(!st.ok()) throw st;
  }
}

bool XrdRedisJournal2::requestVote(RaftTerm term, int64_t candidateId, LogIndex lastIndex, RaftTerm lastTerm) {
  if(currentTerm > term) {
    std::cout << "I know of a newer term, rejecting request vote." << std::endl;
    return false;
  }

  if(currentTerm == term && votedFor != -1 && votedFor != candidateId) {
    std::cout << "I've already voted for this term for " << votedFor << ", rejecting request vote." << std::endl;
    return false;
  }

  this->setVotedFor(candidateId);
  return true;
}

XrdRedisStatus XrdRedisJournal2::setCurrentTerm(RaftTerm term) {
  XrdRedisStatus st = storage->set("RAFT_CURRENT_TERM", SSTR(term));
  if(st.ok()) currentTerm = term;
  else return st;

  votedFor = -1;
  return storage->set("RAFT_VOTED_FOR", "-1");
}

XrdRedisStatus XrdRedisJournal2::setVotedFor(RaftServerID vote) {
  XrdRedisStatus st = storage->set("RAFT_VOTED_FOR", SSTR(vote));
  if(st.ok()) votedFor = vote;
  return st;
}

XrdRedisJournal2::~XrdRedisJournal2() {
  delete storage;
}

// check whether there's an entry in the log and has the specified term
bool XrdRedisJournal2::entryExists(RaftTerm term, LogIndex revision) {
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


void XrdRedisJournal2::removeInconsistent(LogIndex start) {
  std::cout << "Major raft event: remove inconsistent entries, from " << start << " to the end, " << logSize << std::endl;
  for(LogIndex i = start; i < logSize; i++) {
    XrdRedisStatus st = storage->del(SSTR("REVISION_" << i));
    if(!st.ok()) {
      std::cout << "WARNING: unable to delete inconsistent entry in raft journal. Continuing anyway." << std::endl;
    }
  }
  setLogSize(start);
}

XrdRedisStatus XrdRedisJournal2::setLogSize(const LogIndex newsize) {
  XrdRedisStatus st = storage->set("RAFT_LOG_SIZE", SSTR(newsize));
  if(st.ok()) logSize = newsize;
  return st;
}

static std::string serializeRedisRequest(RaftTerm term, const XrdRedisRequest &cmd) {
  std::ostringstream ss;
  std::string trm(sizeof(RaftTerm), '0');
  memcpy(&trm[0], &term, sizeof(trm));
  std::cout << "raft term: " << term << std::endl;
  ss << trm << cmd[0]->c_str();
  // should probably do something less retarded
  for(size_t i = 1; i < cmd.size(); i++) {
    ss << " " << cmd[i]->c_str();
  }
  std::cout << "serialized command: " << ss.str() << std::endl;
  std::cout << "serialized command: " << ss.str().c_str()+sizeof(trm) << std::endl;
  return ss.str();
}

XrdRedisStatus XrdRedisJournal2::rawAppend(RaftTerm term, LogIndex index, XrdRedisRequest &cmd) {
  std::cout << "raw append, term: " << term << std::endl;
  std::cout << "appending raw command: " << cmd[0]->c_str() << std::endl;
  return storage->set(SSTR("REVISION_" << index), serializeRedisRequest(term, cmd));
}

XrdRedisStatus XrdRedisJournal2::append(RaftTerm prevTerm, LogIndex prevIndex, XrdRedisRequest &cmd) {
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

  XrdRedisStatus st = rawAppend(currentTerm, prevIndex+1, cmd);
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

// XrdRedisStatus XrdRedisJournal2::fetch(LogIndex revision, XrdRedisCommand &cmd) {
//   std::string tmp;
//   XrdRedisStatus st = storage->get(SSTR("REVISION_" << revision), tmp);
//   if(!st.ok()) return st;
//   cmd.fromString(tmp);
//   return OK();
// }
