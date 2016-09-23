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

#ifndef __XRDREDIS_JOURNAL_H__
#define __XRDREDIS_JOURNAL_H__

#include "XrdRedisCommon.hh"
#include "XrdRedisBackend.hh"
#include "XrdRedisTunnel.hh"

#include <mutex>
#include <map>
#include <future>

class XrdRedisJournal {
private:
  struct JournalEntry {
    RaftTerm term;
    LogIndex index;
    XrdRedisRequest command;
  };

  XrdRedisBackend *storage;
  RaftTerm current_term;
  LogIndex last_index;

  std::mutex m;

  // should always be backed-up to stable storage - here stored only for convenience
  RaftTerm currentTerm = -1;
  RaftServerID votedFor = -1;

  LogIndex logSize = -1;
  RaftTerm termOfLastEntry = -1;

  RaftClusterID clusterID = "uninitialized";
  LogIndex lastApplied = 0;

  // transient value, can always be inferred from stable storage
  LogIndex commitIndex = 0;

  XrdRedisStatus retrieve(const std::string &key, int64_t &value);
public:
  std::mutex statusUpdateMutex;
  XrdRedisStatus statusUpdate(RaftTerm term, RaftServerID vote);


  XrdRedisStatus setLastApplied(LogIndex index);
  void removeEntries(LogIndex start, LogIndex end);
  XrdRedisStatus rawAppend(RaftTerm term, LogIndex index, XrdRedisRequest &cmd);
  XrdRedisStatus setLogSize(const LogIndex newsize);

  XrdRedisJournal(XrdRedisBackend *store, RaftClusterID id);
  ~XrdRedisJournal();

  RaftTerm getCurrentTerm();

  RaftServerID getVotedFor() {
    return votedFor;
  }

  LogIndex getCommitIndex() {
    return commitIndex;
  }

  LogIndex getLastApplied() {
    return lastApplied;
  }

  LogIndex getLogSize() {
    return logSize;
  }

  RaftTerm getTermOfLastEntry() {
    return termOfLastEntry;
  }

  RaftClusterID getClusterID() {
    return clusterID;
  }

  XrdRedisStatus progressTerm(RaftTerm term, RaftServerID vote);
  void setCommitIndex(LogIndex index);

  bool entryExists(RaftTerm term, LogIndex revision);

  std::mutex appendMutex;
  LogIndex append(XrdRedisRequest &req);

  XrdRedisStatus fetchTerm(LogIndex index, RaftTerm &term);
  XrdRedisStatus fetch(LogIndex index, RaftTerm &term, XrdRedisRequest &cmd);
};

#endif
