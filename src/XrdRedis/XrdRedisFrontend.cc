//------------------------------------------------------------------------------
// This file is part of XrdRedis: A Redis-like server implementation
//
// Copyright (c) 2016 by European Organization for Nuclear Research (CERN)
// Author: Georgios Bitzes <georgios.bitzes@cern.ch>
// File Date: September 2016
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

#include <algorithm>
#include "XrdRedisFrontend.hh"
#include "XrdRedisCommands.hh"

// XrdRedisFrontend::XrdRedisFrontend(XrdRedisBackend *backend, XrdRedisRaft *raft) {
//   this->backend = backend;
//   this->raft = raft;
// }

int XrdRedisFrontend::execute(XrdRedisRequest &request, XrdLink *lp) {
  std::lock_guard<std::mutex> lock(raftMutex);

  // to lower
  std::transform(request[0].begin(), request[0].end(), request[0].begin(), ::tolower);

  std::string command = request[0];
  std::map<std::string, XrdRedisCommand>::iterator cmd = redis_cmd_map.find(command);

  if(cmd == redis_cmd_map.end()) {
    return SendErr(lp, SSTR("unknown command '" << request[0] << "'"));
  }


  return SendPong(lp);
}



int XrdRedisFrontend::Send(XrdLink *lp, const std::string &str) {
  return lp->Send(str.c_str(), str.length());
}

int XrdRedisFrontend::SendPong(XrdLink *lp) {
  return Send(lp, "+PONG\r\n");
}

int XrdRedisFrontend::SendErr(XrdLink *lp, const std::string &msg) {
  return Send(lp, SSTR("-ERR " << msg << "\r\n"));
}

int XrdRedisFrontend::SendErr(XrdLink *lp, const XrdRedisStatus &st) {
  return Send(lp, SSTR("-ERR " << st.ToString() << "\r\n"));
}
