//------------------------------------------------------------------------------
// This file is part of XrdRedis: A Redis-like server implementation
//
// Copyright (c) 2016 by European Organization for Nuclear Research (CERN)
// Author: Georgios Bitzes <georgios.bitzes@cern.ch>
// File Date: May 2016
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

#ifndef __XRDREDIS_PROTOCOL_H__
#define __XRDREDIS_PROTOCOL_H__

/** @file  XrdRedisProtocol.hh
 * @brief  A Redis-like server implementation
 * @author Georgios Bitzes
 * @date   May 2016
 */

#include "Xrd/XrdProtocol.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "Xrd/XrdObject.hh"
#include "Xrd/XrdLink.hh"
#include "Xrd/XrdBuffer.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucStream.hh"

#include "XrdRedisTrace.hh"
#include "XrdRedisBackend.hh"
#include "XrdRedisRocksDB.hh"
#include "XrdRedisRaft.hh"
#include "XrdRedisFrontend.hh"

#include <sstream>
#include <vector>
#include <deque>
#include <chrono>

/******************************************************************************/
/*                               D e f i n e s                                */
/******************************************************************************/

#ifndef __GNUC__
#define __attribute__(x)
#endif

class XrdRedisProtocol : public XrdProtocol {
public:
  /// Read and apply the configuration
  static int Configure(char *parms, XrdProtocol_Config *pi);
  static int xtrace(XrdOucStream &Config);
  static int Config(const char *ConfigFN);
  // static int xdb(XrdOucStream &Config);
  static int fetch_config(XrdOucStream &Config, const std::string &msg, std::string &dest);

  /// Implementation of XrdProtocol interface
  XrdProtocol *Match(XrdLink *lp);
  int Process(XrdLink *lp);
  void Recycle(XrdLink *lp=0,int consec=0,const char *reason=0);
  int Stats(char *buff, int blen, int do_sync=0);

  /// Implementation of XrdJob interface
  void DoIt();

  /// Construction / destruction
  XrdRedisProtocol();
  virtual ~XrdRedisProtocol();

  static XrdObjectQ<XrdRedisProtocol> ProtStack;
private:
  bool authorized_for_raft = false;
  std::chrono::steady_clock::time_point last_raft_handshake;

  /// The link we are bound to
  XrdLink *Link;

  static XrdSysError           eDest;     // Error message handler
  int ReadLine();

  std::chrono::steady_clock::time_point prev_process;

  XrdRedisRequest request;
  // std::vector<string_cptr> request2;
  // std::vector<std::string> request;
  int request_size;
  int current_element;
  int element_size;

  int nrequests;

  // buffer for ReadInteger
  std::string current_integer;

  int ReadRequest(XrdLink *lp);
  int ReadElement(XrdLink *lp, std::string &str);
  int ReadInteger(XrdLink *lp, char prefix);
  int ReadString(XrdLink *lp, int nbytes, std::string &str);

  /// Parse and process a command
  int ProcessRequest(XrdLink *lp);


  /// deque of buffers used to read and write requests
  /// We always append - once a buffer is full, we allocate a new one
  /// Once the contents of a buffer have been parsed, we release it
  /// The pool manager will take care of re-using the memory and preventing
  /// unnecessary allocations

  std::deque<XrdBuffer*> buffers;
  size_t position_read; // always points to the buffer at the front
  size_t position_write; // always points to the buffer at the end
  const size_t buffer_size = 1024 * 32;

  /// can I read len bytes from buffers?
  /// will attempt to Recv if not enough data is ready yet
  int canConsume(size_t len, XrdLink *lp);
  /// read len bytes from buffers - must call canConsume previously
  void consume(size_t len, std::string &str, XrdLink *lp);

  // read bytes from link
  int readFromLink(XrdLink *link);

  /// Parse and process a command
  int ProcessCommand(XrdOucString& tmpline);

  /// Send a response
  int SendNumber(int number);
  int SendArray(const std::vector<std::string> &arr);
  int Send(std::stringstream& sstream);
  int Send(std::string str);
  int SendErrArgs(const std::string &cmd);
  int SendNull();
  int SendString(const std::string &str);
  int SendErr(const std::string &msg);
  int SendErr(const XrdRedisStatus &st);
  int SendScanResp(const std::string &marker, const std::vector<std::string> &err);
  int SendPong();
  int SendOK();

protected:
  static XrdBuffManager *BPool; // Buffer manager
  static XrdRedisBackend *backend;
  static XrdRedisRaft *raft;
  static XrdRedisFrontend *frontend;

  static std::string dbpath;
  static std::string replicas;
  static std::string myself;
  static std::string primary;
  static std::string tunnel;
  static RaftClusterID clusterID;

  static std::chrono::steady_clock::time_point last_raft_config_update;
  static std::vector<RaftServer> raftServers;

  static int readWait;

  void Reset();
};

#endif
