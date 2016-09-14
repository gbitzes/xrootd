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

#ifndef __XRDREDIS_FRONTEND_H__
#define __XRDREDIS_FRONTEND_H__

#include "Xrd/XrdLink.hh"

#include "XrdRedisCommon.hh"
#include "XrdRedisBackend.hh"
#include "XrdRedisRaft.hh"

class XrdRedisFrontend {
public:
  // XrdRedisFrontend(XrdRedisBackend *backend, XrdRedisRaft *raft);

  int execute(XrdRedisRequest &req, XrdLink *lp);
private:
  int dispatchRedis(XrdRedisRequest &req, XrdLink *lp);
  int dispatchRaft(XrdRedisRequest &req, XrdLink *lp);

  XrdRedisBackend *backend;
  XrdRedisRaft *raft;

  // int SendNumber(XrdLink *lp, int number);
  // int SendScanResp(XrdLink *lp, const std::string &marker, const std::vector<std::string> &arr);
  // int SendArray(XrdLink *lp, const std::vector<std::string> &arr);
  // int SendString(XrdLink *lp, const std::string &str);
  // int Send(const std::stringstream& sstream);

  int Send(XrdLink *lp, const std::string &str);
  int SendPong(XrdLink *lp);
  int SendErr(XrdLink *lp, const std::string &msg);
  int SendErr(XrdLink *lp, const XrdRedisStatus &st);

  std::mutex raftMutex;
  bool authorized_for_raft = false;

// int XrdRedisProtocol::SendErrArgs(const std::string &cmd) {
//   return Send(SSTR("-ERR wrong number of arguments for '" << cmd << "' command\r\n"));
// }
//
// int XrdRedisProtocol::SendErr(const std::string &msg) {
//   return Send(SSTR("-ERR " << msg << "\r\n"));
// }
//
// int XrdRedisProtocol::SendErr(const XrdRedisStatus &st) {
//   return Send(SSTR("-ERR " << st.ToString() << "\r\n"));
// }
//
// int XrdRedisProtocol::SendNull() {
//   return Send("$-1\r\n");
// }
//
// int XrdRedisProtocol::SendPong() {
//   return Send("+PONG\r\n");
// }
//
// int XrdRedisProtocol::SendOK() {
//   return Send("+OK\r\n");
// }

};

#endif
