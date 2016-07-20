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

XrdRedisReplicator::XrdRedisReplicator(XrdRedisBackend *primary_, std::vector<XrdRedisBackend*> replicas_) {
  primary.backend = primary_;
  for(auto item : replicas_) {
    ReplicaState replica;
    replica.backend = item;
    replicas.push_back(replica);
  }
}

XrdRedisReplicator::~XrdRedisReplicator() {
  delete primary.backend;
  for(auto item : replicas) {
    delete item.backend;
  }
}

// XrdRedisStatus XrdRedisReplicator::applyUpdate(ReplicaState &state, const JournalEntry &entry) {
//   // if(entry.command ==)
//
//
// }

XrdRedisStatus XrdRedisReplicator::set(const std::string &key, const std::string &value) {
  XrdRedisStatus st = primary.backend->set(key, value);
  for(auto replica : replicas) {
    replica.backend->set(key, value);
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
