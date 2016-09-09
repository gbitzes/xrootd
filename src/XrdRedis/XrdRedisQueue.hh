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

#ifndef __XRDREDIS_QUEUE_H__
#define __XRDREDIS_QUEUE_H__

#include <queue>
#include <mutex>
#include <condition_variable>

template<class T>
class XrdRedisQueue {
public:
  XrdRedisQueue() { }

  void enqueue(T t) {
    std::lock_guard<std::mutex> lock(mutex);
    queue.push(std::move(t));
    cond.notify_one();
  }

  T dequeue() {
    std::unique_lock<std::mutex> lock(mutex);
    while(queue.empty())
    {
      cond.wait(lock);
    }
    T val = std::move(queue.front());
    queue.pop();
    return val;
  }

private:
  std::queue<T> queue;
  std::mutex mutex;
  std::condition_variable cond;
};

#endif
