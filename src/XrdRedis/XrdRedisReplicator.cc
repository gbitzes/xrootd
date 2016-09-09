//------------------------------------------------------------------------------
// This file is part of XrdRedis: A Redis-like server implementation
//
// Copyright (c) 2016 by European Organization for Nuclear Research (CERN)
// Author: Georgios Bitzes <georgios.bitzes@cern.ch>
// File Date: July 2016
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

#include "XrdRedisReplicator.hh"
#include "XrdRedisUtil.hh"
#include <future>
#include <chrono>

static XrdRedisStatus OK() {
  return XrdRedisStatus(rocksdb::Status::kOk);
}

XrdRedisJournal::XrdRedisJournal(XrdRedisBackend *backend) : store(backend) {

}

XrdRedisStatus XrdRedisJournal::initialize() {
  std::string tmp;
  XrdRedisStatus st = store->get("GLOBAL_REVISION", tmp);
  if(!st.ok()) return st;
  int64_t tmp_rev;

  if(!my_strtoll(tmp, tmp_rev)) {
    return XrdRedisStatus(rocksdb::Status::kNotFound, "unable to retrieve revision number");
  }
  revision = tmp_rev;
  std::cout << "Journal initialized with GLOBAL REVISION set to " << revision << std::endl;
  return OK();
}

XrdRedisStatus XrdRedisJournal::pushUpdate(const JournalEntry &entry, int64_t &rev) {
  int64_t my_revision = ++revision;
  XrdRedisStatus st = store->set(SSTR("REVISION_" << my_revision), entry.toString());
  if(!st.ok()) return st;

  // revision might have been updated in the meantime, don't use my_revision
  rev = my_revision;
  return store->set("GLOBAL_REVISION", SSTR(revision));
}

XrdRedisStatus XrdRedisJournal::fetch(int64_t revision, JournalEntry &entry) {
  std::string tmp;
  XrdRedisStatus st = store->get(SSTR("REVISION_" << revision), tmp);
  if(!st.ok()) return st;
  entry.fromString(tmp);
  return OK();
}

XrdRedisReplicator::XrdRedisReplicator(XrdRedisBackend *primary_, std::vector<XrdRedisBackend*> replicas_)
: primary(primary_), journal(primary_) {

  XrdRedisStatus st = journal.initialize();
  if(!st.ok()) {
    std::cout << "journal initialization failed: " << st.ToString() << std::endl;
  }

  for(int i = 0; i < 10; i++) {
    std::thread th(&XrdRedisReplicator::taskExecutor, this);
    th.detach();
  }

  for(size_t i = 0; i < replicas_.size(); i++) {
    replicas.emplace_back(replicas_[i]);
  }

  std::thread th(&XrdRedisReplicator::monitorReplicas, this);
  th.detach();
}

XrdRedisReplicator::~XrdRedisReplicator() {
  delete primary.backend;
  for(auto& item : replicas) {
    delete item.backend;
  }
}

void XrdRedisReplicator::taskExecutor() {
  std::cout << "Starting task executor.." << std::endl;
  while(true) {
    std::packaged_task<XrdRedisStatus()> task = std::move(work_queue.dequeue());
    task();
  }
}

void XrdRedisReplicator::monitorReplicas() {
  while(true) {
    for(size_t i = 0; i < replicas.size(); i++) {
      if(!replicas[i].online) {
        XrdRedisStatus st = this->bringOnline(i, false);
        std::cout << st.ToString() << std::endl;
      }
    }

    std::this_thread::sleep_for(std::chrono::seconds(2));
  }
}

XrdRedisStatus XrdRedisReplica::getRevision(int64_t &revision) {
  std::string tmp;
  XrdRedisStatus st = backend->get("GLOBAL_REVISION", tmp);
  if(!st.ok()) return st;

  if(!my_strtoll(tmp, revision)) {
    return XrdRedisStatus(rocksdb::Status::kNotFound, "unable to retrieve revision number");
  }

  cached_revision = revision;
  return OK();
}

XrdRedisStatus XrdRedisReplica::setRevision(int64_t revision) {
  XrdRedisStatus st = backend->set("GLOBAL_REVISION", SSTR(revision));
  if(!st.ok()) return st;

  cached_revision = revision;
  return OK();
}


XrdRedisStatus XrdRedisReplicator::bringOnline(size_t nrep, bool resilver) {
  std::cout << "Trying to bring replica #" << nrep << " online, resilver = " << resilver << std::endl;
  XrdRedisStatus st = replicas[nrep].backend->ping();
  if(!st.ok()) {
    std::cout << "Replica does not reply to pings." << std::endl;
    return st;
  }

  int64_t current_revision = -1;

  if(!resilver) {
    st = replicas[nrep].getRevision(current_revision);
    if(st.IsNotFound()) {
      st = replicas[nrep].backend->flushall();
      if(!st.ok()) return st;
      st = replicas[nrep].setRevision(-1);
      if(!st.ok()) return st;
    }
    else if(!st.ok()) {
      std::cout << "Could not get revision of replica #" << nrep << std::endl;
      return st;
    }
  }
  else {
    st = replicas[nrep].backend->flushall();
    if(!st.ok()) return st;
    st = replicas[nrep].setRevision(-1);
    if(!st.ok()) return st;
  }

  while(current_revision < journal.getRevision()) {
    current_revision++;
    if(journal.getRevision() - current_revision < 100 || current_revision % 1000 == 0) {
      std::cout << "Advancing replica #" << nrep << " to revision #" << current_revision << std::endl;
    }

    XrdRedisJournal::JournalEntry entry;
    XrdRedisStatus st = journal.fetch(current_revision, entry);
    if(!st.ok()) {
      std::cout << "journal entry not found" << std::endl;
      return st;
    }

    if(entry.items[0] == "SET") {
      std::string current_value;
      XrdRedisStatus st = primary.backend->get(entry.items[1], current_value);
      if(!st.ok()) return st;

      st = replicas[nrep].backend->set(entry.items[1], current_value);
      if(!st.ok()) return st;
    } else {
      std::cout << "journal corruption" << std::endl;
      return XrdRedisStatus(rocksdb::Status::kNotFound, "journal corruption, internal error");
    }
  }

  replicas[nrep].setRevision(current_revision-1);
  replicas[nrep].online = true;
  // TODO: must lock journal at the very last step
  std::cout << "Replica #" << nrep << " brought back up successfully." << std::endl;
  return OK();
}

// template <class FunctionType, class ...Args>
// XrdRedisStatus XrdRedisReplicator::broadcast(FunctionType f, XrdRedisBackend* backend, const Args & ... args) {
//   std::vector<std::future<XrdRedisStatus>> futures;
//   for(size_t i = 0; i < replicas.size(); i++) {
//     if(!replicas[i].online) continue;
//
//     std::packaged_task<XrdRedisStatus()> task(std::bind(&XrdRedisBackend::set, replicas[i].backend, key, value));
//     futures.push_back(task.get_future());
//     work_queue.enqueue(std::move(task));
//   }
//
//   for(size_t i = 0; i < futures.size(); i++) {
//     XrdRedisStatus st = futures[i].get();
//     if(!st.ok()) {
//       // TODO: mark offline
//     }
//   }
//
//   return st;
// }

XrdRedisStatus XrdRedisReplicator::set(const std::string &key, const std::string &value) {
  // XrdRedisJournal::JournalEntry entry("SET", key);

  // int64_t revision;
  // XrdRedisStatus st = journal.pushUpdate(entry, revision);
  // if(!st.ok()) return st;

  XrdRedisStatus st = primary.backend->set(key, value);
  if(!st.ok()) return st;

  std::vector<std::future<XrdRedisStatus>> futures;
  for(size_t i = 0; i < replicas.size(); i++) {
    if(!replicas[i].online) continue;

    // XrdRedisStatus st = replicas[i].backend->set(key, value);

    std::packaged_task<XrdRedisStatus()> task(std::bind(&XrdRedisBackend::set, replicas[i].backend, key, value));
    futures.push_back(task.get_future());
    work_queue.enqueue(std::move(task));
  }

  for(size_t i = 0; i < futures.size(); i++) {
    XrdRedisStatus st = futures[i].get();
    if(!st.ok()) {
      std::cout << "Replica #" << i << " went offline" << std::endl;
      replicas[i].online = false;
    }
    // replicas[i].setRevision(revision);
  }

  return st;
}

XrdRedisStatus XrdRedisReplicator::get(const std::string &key, std::string &value) {
  return primary.backend->get(key, value);
}

XrdRedisStatus XrdRedisReplicator::exists(const std::string &key) {
  return primary.backend->exists(key);
}

XrdRedisStatus XrdRedisReplicator::del(const std::string &key) {
  return primary.backend->del(key);
}

XrdRedisStatus XrdRedisReplicator::keys(const std::string &pattern, std::vector<std::string> &result) {
  return primary.backend->keys(pattern, result);
}

XrdRedisStatus XrdRedisReplicator::hset(const std::string &key, const std::string &field, const std::string &value) {
  return primary.backend->hset(key, field, value);
}

XrdRedisStatus XrdRedisReplicator::hget(const std::string &key, const std::string &field, std::string &value) {
  return primary.backend->hget(key, field, value);
}

XrdRedisStatus XrdRedisReplicator::hexists(const std::string &key, const std::string &field) {
  return primary.backend->hexists(key, field);
}

XrdRedisStatus XrdRedisReplicator::hkeys(const std::string &key, std::vector<std::string> &keys) {
  return primary.backend->hkeys(key, keys);
}

XrdRedisStatus XrdRedisReplicator::hgetall(const std::string &key, std::vector<std::string> &res) {
  return primary.backend->hgetall(key, res);
}

XrdRedisStatus XrdRedisReplicator::hincrby(const std::string &key, const std::string &field, const std::string &incrby, int64_t &result) {

  return primary.backend->hincrby(key, field, incrby, result);
}

XrdRedisStatus XrdRedisReplicator::hdel(const std::string &key, const std::string &field) {
  return primary.backend->hdel(key, field);
}

XrdRedisStatus XrdRedisReplicator::hlen(const std::string &key, size_t &len) {
  return primary.backend->hlen(key, len);
}

XrdRedisStatus XrdRedisReplicator::hvals(const std::string &key, std::vector<std::string> &vals) {
  return primary.backend->hvals(key, vals);
}

XrdRedisStatus XrdRedisReplicator::sadd(const std::string &key, const std::string &element, int64_t &added) {
  return primary.backend->sadd(key, element, added);
}

XrdRedisStatus XrdRedisReplicator::sismember(const std::string &key, const std::string &element) {
  return primary.backend->sismember(key, element);
}

XrdRedisStatus XrdRedisReplicator::srem(const std::string &key, const std::string &element) {
  return primary.backend->srem(key, element);
}

XrdRedisStatus XrdRedisReplicator::smembers(const std::string &key, std::vector<std::string> &members) {
  return primary.backend->smembers(key, members);
}

XrdRedisStatus XrdRedisReplicator::scard(const std::string &key, size_t &count) {
  return primary.backend->scard(key, count);
}

XrdRedisStatus XrdRedisReplicator::flushall() {
  return primary.backend->flushall();
}

XrdRedisStatus XrdRedisReplicator::ping() {
  return primary.backend->ping();
}
