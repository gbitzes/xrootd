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

#include "XrdRedisTrace.hh"
#include "XrdRedisBackend.hh"

#include <sstream>
#include <vector>

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
  /// The link we are bound to
  XrdLink *Link;

  static XrdSysError           eDest;     // Error message handler
  int ReadLine();

  std::vector<std::string> request;
  int request_size;
  int current_element;
  int element_size;
  int buff_position;

  int ReadRequest(XrdLink *lp);
  int ReadElement(XrdLink *lp);
  int ReadInteger(XrdLink *lp, char prefix);
  int ReadString(XrdLink *lp, int nbytes);

  /// Parse and process a command
  int ProcessRequest(XrdLink *lp);




  /// Circular Buffer used to read requests
  XrdBuffer *myBuff;
  /// The circular pointers
  char *myBuffStart, *myBuffEnd;

  /// A nice var to hold the current header line
  XrdOucString tmpline;

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

protected:
  static XrdBuffManager *BPool; // Buffer manager
  static XrdRedisBackend *backend;


  static int readWait;

  void Reset();




};

#endif
