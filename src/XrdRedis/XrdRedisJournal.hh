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
#include <mutex>

class XrdRedisJournal2 {
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
  void removeInconsistent(LogIndex start);

  XrdRedisStatus setLogSize(const LogIndex newsize);
  XrdRedisStatus setLastApplied(LogIndex index);

  XrdRedisStatus rawAppend(RaftTerm term, LogIndex index, XrdRedisRequest &cmd);
public:
  XrdRedisJournal2(XrdRedisBackend *store, RaftClusterID id);
  ~XrdRedisJournal2();

  RaftTerm getCurrentTerm() {
    return currentTerm;
  }

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

  XrdRedisStatus setCurrentTerm(RaftTerm term);
  XrdRedisStatus setVotedFor(RaftServerID server);
  void setCommitIndex(LogIndex index);

  bool entryExists(RaftTerm term, LogIndex revision);

  XrdRedisStatus append(RaftTerm prevTerm, LogIndex prevIndex, XrdRedisRequest &cmd, RaftTerm entryTerm);
  bool requestVote(RaftTerm term, int64_t candidateId, LogIndex lastIndex, RaftTerm lastTerm);

  XrdRedisStatus fetchTerm(LogIndex index, RaftTerm &term);
  XrdRedisStatus fetch(LogIndex index, RaftTerm &term, XrdRedisRequest &cmd);
  std::pair<LogIndex, RaftTerm> leaderAppend(XrdRedisRequest &req);

  void applyCommits();

  // XrdRedisStatus create(const std::string &filename);
  // XrdRedisStatus initialize(const std::string &filename);

  // XrdRedisStatus append(XrdRedisRequest& cmd, RaftTerm term, LogIndex revision);
};

// private:
//   std::atomic<int64_t> revision;
//   XrdRedisBackend *store;
// public:
//   struct JournalEntry {
//     std::vector<std::string> items;
//
//     JournalEntry() {}
//
//     JournalEntry(const std::string &s1, const std::string &s2) {
//       items.emplace_back(s1);
//       items.emplace_back(s2);
//     }
//
//     std::string toString() const {
//       std::ostringstream ss;
//       ss << items[0];
//       for(size_t i = 1; i < items.size(); i++) {
//         ss << " " << items[i];
//       }
//       return ss.str();
//     }
//
//     void fromString(const std::string &str) {
//       items = split(str, " ");
//     }
//   };
//
//   XrdRedisStatus pushUpdate(const JournalEntry &entry, int64_t &revision);
//   XrdRedisStatus fetch(int64_t revision, JournalEntry &entry);
//
//   int64_t getRevision() {
//     return revision;
//   }
//
//   XrdRedisJournal(XrdRedisBackend *backend);
//   XrdRedisStatus initialize();
// };






#endif
