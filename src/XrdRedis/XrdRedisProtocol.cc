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

#include "XrdVersion.hh"
#include "XrdRedisProtocol.hh"
#include "XrdRedisSTL.hh"
#include "XrdRedisRocksDB.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include <stdlib.h>
#include <algorithm>


/******************************************************************************/
/*                               G l o b a l s                                */
/******************************************************************************/

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

XrdSysError XrdRedisProtocol::eDest(0, "redis");

const char *XrdRedisTraceID = "XrdRedis";
XrdOucTrace *XrdRedisTrace = 0;

XrdBuffManager *XrdRedisProtocol::BPool = 0; // Buffer manager
XrdRedisBackend *XrdRedisProtocol::backend = 0;
int XrdRedisProtocol::readWait = 0;

enum CmdType {
  CMD_PING,

  CMD_GET,
  CMD_SET,
  CMD_EXISTS,
  CMD_DEL,
  CMD_KEYS,

  CMD_HGET,
  CMD_HSET,
  CMD_HEXISTS,
  CMD_HKEYS,
  CMD_HGETALL,
  CMD_HINCRBY,
  CMD_HDEL,
  CMD_HLEN,
  CMD_HVALS,
  CMD_HSCAN,

  CMD_SADD,
  CMD_SISMEMBER,
  CMD_SREM,
  CMD_SMEMBERS,
  CMD_SCARD,
  CMD_SSCAN
};

std::map<std::string, CmdType> cmdMap;

struct cmdMapInit {
  cmdMapInit() {
    cmdMap["ping"] = CMD_PING;

    cmdMap["get"] = CMD_GET;
    cmdMap["set"] = CMD_SET;
    cmdMap["exists"] = CMD_EXISTS;
    cmdMap["del"] = CMD_DEL;
    cmdMap["keys"] = CMD_KEYS;

    cmdMap["hget"] = CMD_HGET;
    cmdMap["hset"] = CMD_HSET;
    cmdMap["hexists"] = CMD_HEXISTS;
    cmdMap["hkeys"] = CMD_HKEYS;
    cmdMap["hgetall"] = CMD_HGETALL;
    cmdMap["hincrby"] = CMD_HINCRBY;
    cmdMap["hdel"] = CMD_HDEL;
    cmdMap["hlen"] = CMD_HLEN;
    cmdMap["hvals"] = CMD_HVALS;
    cmdMap["hscan"] = CMD_HSCAN;

    cmdMap["sadd"] = CMD_SADD;
    cmdMap["sismember"] = CMD_SISMEMBER;
    cmdMap["srem"] = CMD_SREM;
    cmdMap["smembers"] = CMD_SMEMBERS;
    cmdMap["scard"] = CMD_SCARD;
    cmdMap["sscan"] = CMD_SSCAN;
  }
} cmdMapInit;

/******************************************************************************/
/*            P r o t o c o l   M a n a g e m e n t   S t a c k s             */
/******************************************************************************/

XrdObjectQ<XrdRedisProtocol>
XrdRedisProtocol::ProtStack("ProtStack",
        "redis protocol anchor");

/******************************************************************************/
/*                X r d R e d i s P r o t o c o l    C l a s s                */
/******************************************************************************/
/******************************************************************************/
/*                           C o n s t r u c t o r                            */
/******************************************************************************/

XrdRedisProtocol::XrdRedisProtocol()
: XrdProtocol("Redis protocol handler") {
  myBuff = 0;

  request_size = 0;
  buff_position = 0;
  element_size = 0;

  Reset();
}

/******************************************************************************/
/*                                 M a t c h                                  */
/******************************************************************************/

XrdProtocol* XrdRedisProtocol::Match(XrdLink *lp) {
  XrdRedisProtocol *rp;
  if (!(rp = ProtStack.Pop())) rp = new XrdRedisProtocol();

  // Bind the protocol to the link and return the protocol
  rp->Link = lp;
  return rp;
}

#define TRACELINK lp
int XrdRedisProtocol::Process(XrdLink *lp) {
  TRACEI(DEBUG, " Process. lp:" << lp);

  if (!myBuff || !myBuff->buff || !myBuff->bsize) {
    TRACE(ALL, " Process. No buffer available. Internal error.");
    return -1;
  }

  int rc = ReadRequest(lp);
  if(rc == 0) return 1;
  if(rc < 0) return rc;

  request_size = 0;
  buff_position = 0;

  rc = ProcessRequest(lp);
  request.clear();

  return rc;
}

int XrdRedisProtocol::ReadRequest(XrdLink *lp) {
  // resumable function to read a request
  // returns 1 if we have a new command to process
  //         0 on slow link
  //         negative on error

  if(request_size == 0) {
    int reqsize = ReadInteger(lp, '*');
    if(reqsize <= 0) return reqsize;

    request_size = reqsize;
    element_size = 0;
    current_element = 0;
    TRACEI(DEBUG, "Received size of array: " << request_size);
  }

  for( ; current_element < request_size; current_element++) {
    int rc = ReadElement(lp);
    if(rc <= 0) return rc;
    request.push_back(std::string(myBuff->buff, rc));
    element_size = 0;
    buff_position = 0;
  }

  TRACEI(DEBUG, "Received command:");
  for(unsigned i = 0; i < request.size(); i++) {
    TRACEI(DEBUG, request[i]);
  }

  return 1;
}

int XrdRedisProtocol::ReadString(XrdLink *lp, int nbytes) {
  int toread = nbytes - buff_position + 2;
  TRACEI(DEBUG, "To read: " << toread);
  int rlen = Link->Recv(myBuff->buff + buff_position, toread, readWait);
  if(rlen <= 0) return rlen;

  toread -= rlen;
  buff_position += rlen;
  if(toread > 0) return 0; // slow link

  if(myBuff->buff[buff_position - 2] != '\r') {
    TRACEI(ALL, "Protocol error, expected \\r, received " << myBuff->buff[buff_position-2]);
    return -1;
  }
  if(myBuff->buff[buff_position - 1] != '\n') {
    TRACEI(ALL, "Protocol error, expected \\n, received " << myBuff->buff[buff_position-1]);
    return -1;
  }

  TRACEI(DEBUG, "Got string: " << myBuff->buff);
  return nbytes;
}

int XrdRedisProtocol::ReadElement(XrdLink *lp) {
  TRACEI(DEBUG, "Element size: " << element_size);
  if(element_size == 0) {
    int elsize = ReadInteger(lp, '$');
    if(elsize <= 0) return elsize;
    element_size = elsize;
    buff_position = 0;
  }
  return ReadString(lp, element_size);
}

int XrdRedisProtocol::ReadInteger(XrdLink *lp, char prefix) {
  // read a single integer
  char prev = '\0';

  while(prev != '\n') {
    int rlen = Link->Recv(myBuff->buff + buff_position, 1, readWait);
    if(rlen <= 0) return rlen;

    prev = myBuff->buff[buff_position];
    TRACEI(DEBUG, "Received byte: '" << prev << "'" << " " << (int) prev);
    buff_position++;
  }

  if(myBuff->buff[buff_position-2] != '\r') {
    TRACEI(ALL, "Protocol error, received \\n without preceeding \\r");
    return -1;
  }

  if(myBuff->buff[0] != prefix) {
    TRACEI(ALL, "Protocol error, expected an integer with preceeding '" << prefix << "', received '" << myBuff->buff[0] << "' instead");
    return -1;
  }

  myBuff->buff[buff_position-2] = '\0';

  char *endptr;
  long num = strtol(myBuff->buff+1, &endptr, 10);
  if(*endptr != '\0' || num == LONG_MIN || num == LONG_MAX) {
    TRACEI(ALL, "Protocol error, received an invalid integer");
    return -1;
  }

  buff_position = 0;

  return num;
}

int XrdRedisProtocol::ProcessRequest(XrdLink *lp) {
  // to lower
  std::transform(request[0].begin(), request[0].end(), request[0].begin(), ::tolower);

  std::string command = request[0];
  TRACEI(DEBUG, "in process request, command: '" << command << "'");
  TRACEI(DEBUG, "cmdMap size: " << cmdMap.size());
  std::map<std::string, CmdType>::iterator cmd = cmdMap.find(command);


  if(cmd == cmdMap.end()) {
    return SendErr(SSTR("unknown command '" << request[0] << "'"));
  }

  XrdRedisStatus st;

  switch(cmd->second) {
    case CMD_PING: {
      if(request.size() > 2) return SendErrArgs(command);

      if(request.size() == 1) return SendPong();
      if(request.size() == 2) return SendString(request[1]);
    }
    case CMD_GET: {
      if(request.size() != 2) return SendErrArgs(command);

      std::string value;
      st = backend->get(request[1], value);
      if(st.IsNotFound()) return SendNull();
      if(!st.ok()) return SendErr(st);
      return SendString(value);
    }
    case CMD_SET: {
      if(request.size() != 3) return SendErrArgs(command);

      st = backend->set(request[1], request[2]);
      if(!st.ok()) return SendErr(st);
      return Send("+OK\r\n");
    }
    case CMD_EXISTS: {
      if(request.size() <= 1) return SendErrArgs(command);

      int count = 0;
      for(unsigned i = 1; i < request.size(); i++) {
        st = backend->exists(request[i]);
        if(st.ok()) count++;
        else if(!st.IsNotFound()) SendErr(st);

      }
      return SendNumber(count);
    }
    case CMD_DEL: {
      if(request.size() <= 1) return SendErrArgs(command);

      int count = 0;
      for(unsigned i = 1; i < request.size(); i++) {
        st = backend->del(request[i]);
        if(st.ok()) count++;
        else if(!st.IsNotFound()) return SendErr(st);
      }
      return SendNumber(count);
    }
    case CMD_KEYS: {
      if(request.size() != 2) return SendErrArgs(command);

      std::vector<std::string> ret;
      XrdRedisStatus st = backend->keys(request[1], ret);
      if(!st.ok()) return SendErr(st);
      return SendArray(ret);
    }
    case CMD_HGET: {
      if(request.size() != 3) return SendErrArgs(command);

      std::string value;
      st = backend->hget(request[1], request[2], value);
      if(st.IsNotFound()) SendNull();
      else if(!st.ok()) return SendErr(st);

      return SendString(value);
    }
    case CMD_HSET: {
      if(request.size() != 4) return SendErrArgs(command);

      // Mild race condition here.. if the key doesn't exist, but another thread modifies
      // it in the meantime the user gets a response of 1, not 0

      XrdRedisStatus existed = backend->hexists(request[1], request[2]);
      if(!existed.ok() && !existed.IsNotFound()) return SendErr(st);

      XrdRedisStatus st = backend->hset(request[1], request[2], request[3]);
      if(!st.ok()) return SendErr(st);

      if(existed.ok()) return SendNumber(0);
      return SendNumber(1);
    }
    case CMD_HEXISTS: {
      return SendErr("not implemented");
    }
    case CMD_HKEYS: {
      if(request.size() != 2) return SendErrArgs(command);

      std::vector<std::string> keys;
      XrdRedisStatus st = backend->hkeys(request[1], keys);
      if(!st.ok()) return SendErr(st);

      return SendArray(keys);
    }
    case CMD_HGETALL: {
      if(request.size() != 2) return SendErrArgs(command);

      std::vector<std::string> arr;
      XrdRedisStatus st = backend->hgetall(request[1], arr);
      if(!st.ok()) return SendErr(st);

      return SendArray(arr);
    }
    case CMD_HINCRBY: {
      if(request.size() != 4) return SendErrArgs(command);

      int64_t ret = 0;
      XrdRedisStatus st = backend->hincrby(request[1], request[2], request[3], ret);
      if(!st.ok()) return SendErr(st);
      return SendNumber(ret);
    }
    case CMD_HDEL: {
      if(request.size() <= 2) return SendErrArgs(command);

      int count = 0;
      for(unsigned i = 2; i < request.size(); i++) {
        XrdRedisStatus st = backend->hdel(request[1], request[i]);
        if(st.ok()) count++;
        else if(!st.IsNotFound()) return SendErr(st);
      }
      return SendNumber(count);
    }
    case CMD_HLEN: {
      if(request.size() != 2) return SendErrArgs(command);
      size_t len;
      XrdRedisStatus st = backend->hlen(request[1], len);
      if(!st.ok()) return SendErr(st);

      return SendNumber(len);
    }
    case CMD_HVALS: {
      if(request.size() != 2) return SendErrArgs(command);
      std::vector<std::string> vals;
      XrdRedisStatus st = backend->hvals(request[1], vals);
      return SendArray(vals);
    }
    case CMD_HSCAN: {
      if(request.size() != 3) return SendErrArgs(command);
      if(request[2] != "0") return SendErr("invalid cursor");

      std::vector<std::string> arr;
      XrdRedisStatus st = backend->hgetall(request[1], arr);
      if(!st.ok()) return SendErr(st);

      return SendScanResp("0", arr);
    }
    case CMD_SADD: {
      if(request.size() <= 2) return SendErrArgs(command);

      int count = 0;
      for(unsigned i = 2; i < request.size(); i++) {
        XrdRedisStatus st = backend->sadd(request[1], request[i], count);
        if(!st.ok()) return SendErr(st);
      }
      return SendNumber(count);
    }
    case CMD_SISMEMBER: {
      if(request.size() != 3) return SendErrArgs(command);

      XrdRedisStatus st = backend->sismember(request[1], request[2]);
      if(st.ok()) return SendNumber(1);
      if(st.IsNotFound()) return SendNumber(0);
      return SendErr(st);
    }
    case CMD_SREM: {
      if(request.size() <= 2) return SendErrArgs(command);

      int count = 0;
      for(unsigned i = 2; i < request.size(); i++) {
        XrdRedisStatus st = backend->srem(request[1], request[i]);
        if(st.ok()) count++;
        else if(!st.IsNotFound()) return SendErr(st);
      }
      return SendNumber(count);
    }
    case CMD_SMEMBERS: {
      if(request.size() != 2) return SendErrArgs(command);
      std::vector<std::string> members;
      XrdRedisStatus st = backend->smembers(request[1], members);
      if(!st.ok()) return SendErr(st);
      return SendArray(members);
    }
    case CMD_SCARD: {
      if(request.size() != 2) return SendErrArgs(command);
      size_t count;
      XrdRedisStatus st = backend->scard(request[1], count);
      if(!st.ok()) return SendErr(st);
      return SendNumber(count);
    }
    case CMD_SSCAN: {
      if(request.size() != 3) return SendErrArgs(command);
      if(request[2] != "0") return SendErr("invalid cursor");
      std::vector<std::string> members;
      XrdRedisStatus st = backend->smembers(request[1], members);
      if(!st.ok()) return SendErr(st);
      return SendScanResp("0", members);
    }
    default: {
      return SendErr("an unknown error occurred when dispatching the command");
    }
  }
}

int XrdRedisProtocol::SendNumber(int number) {
  return Send(SSTR(":" << number << "\r\n"));
}

int XrdRedisProtocol::SendScanResp(const std::string &marker, const std::vector<std::string> &arr) {
  std::stringstream ss;
  ss << "*2\r\n";
  ss << "$" << marker.length() << "\r\n";
  ss << marker << "\r\n";

  ss << "*" << arr.size() << "\r\n";
  for(std::vector<std::string>::const_iterator it = arr.begin(); it != arr.end(); it++) {
    ss << "$" << it->length() << "\r\n";
    ss << *it << "\r\n";
  }
  return Send(ss);
}

int XrdRedisProtocol::SendArray(const std::vector<std::string> &arr) {
  std::stringstream ss;
  ss << "*" << arr.size() << "\r\n";
  for(std::vector<std::string>::const_iterator it = arr.begin(); it != arr.end(); it++) {
    ss << "$" << it->length() << "\r\n";
    ss << *it << "\r\n";
  }
  return Send(ss);
}

int XrdRedisProtocol::SendString(const std::string &str) {
  return Send(SSTR("$" << str.length() << "\r\n" << str << "\r\n"));
}


int XrdRedisProtocol::Send(std::string str) {
  return Link->Send(str.c_str(), str.length());
}

int XrdRedisProtocol::Send(std::stringstream& sstream) {
  return Send(sstream.str());
}

int XrdRedisProtocol::SendErrArgs(const std::string &cmd) {
  return Send(SSTR("-ERR wrong number of arguments for '" << cmd << "' command\r\n"));
}

int XrdRedisProtocol::SendErr(const std::string &msg) {
  return Send(SSTR("-ERR " << msg << "\r\n"));
}

int XrdRedisProtocol::SendErr(const XrdRedisStatus &st) {
  return Send(SSTR("-ERR " << st.ToString() << "\r\n"));
}

int XrdRedisProtocol::SendNull() {
  return Send("$-1\r\n");
}

int XrdRedisProtocol::SendPong() {
  return Send("+PONG\r\n");

}




void XrdRedisProtocol::Reset() {
  if (!myBuff) {
    myBuff = BPool->Obtain(1024 * 1024);
  }
  myBuffStart = myBuffEnd = myBuff->buff;
}

void XrdRedisProtocol::Recycle(XrdLink *lp,int consec,const char *reason) {

}

int XrdRedisProtocol::Stats(char *buff, int blen, int do_sync) {
  return 0;
}

void XrdRedisProtocol::DoIt() {

}

// copied from XrdHttp
int XrdRedisProtocol::xtrace(XrdOucStream & Config) {

  char *val;

  static struct traceopts {
    const char *opname;
    int opval;
  } tropts[] = {
    {"all", TRACE_ALL},
    {"emsg", TRACE_EMSG},
    {"debug", TRACE_DEBUG},
    {"fs", TRACE_FS},
    {"login", TRACE_LOGIN},
    {"mem", TRACE_MEM},
    {"stall", TRACE_STALL},
    {"redirect", TRACE_REDIR},
    {"request", TRACE_REQ},
    {"response", TRACE_RSP}
  };
  int i, neg, trval = 0, numopts = sizeof (tropts) / sizeof (struct traceopts);

  if (!(val = Config.GetWord())) {
    eDest.Emsg("config", "trace option not specified");
    return 1;
  }
  while (val) {
    if (!strcmp(val, "off")) trval = 0;
    else {
      if ((neg = (val[0] == '-' && val[1]))) val++;
      for (i = 0; i < numopts; i++) {
        if (!strcmp(val, tropts[i].opname)) {
          if (neg) trval &= ~tropts[i].opval;
          else trval |= tropts[i].opval;
          break;
        }
      }
      if (i >= numopts)
        eDest.Emsg("config", "invalid trace option", val);
    }
    val = Config.GetWord();
  }
  XrdRedisTrace->What = trval;
  return 0;
}

#define TS_Xeq(x,m) (!strcmp(x,var)) GoNo = m(Config)

int XrdRedisProtocol::Config(const char *ConfigFN) {
  XrdOucEnv myEnv;
  XrdOucStream Config(&eDest, getenv("XRDINSTANCE"), &myEnv, "=====> ");
  char *var;
  int cfgFD, GoNo, NoGo = 0, ismine;

  // Open and attach the config file
  //
  if ((cfgFD = open(ConfigFN, O_RDONLY, 0)) < 0)
    return eDest.Emsg("Config", errno, "open config file", ConfigFN);
  Config.Attach(cfgFD);

  // Process items
  //
  while ((var = Config.GetMyFirstWord())) {
    if ((ismine = !strncmp("redis.", var, 6)) && var[6]) var += 6;
    // else if ((ismine = !strcmp("all.export", var))) var += 4;
    // else if ((ismine = !strcmp("all.pidpath", var))) var += 4;

    if (ismine) {
           if TS_Xeq("trace", xtrace);
      else {
        eDest.Say("Config warning: ignoring unknown directive '", var, "'.");
        Config.Echo();
        continue;
      }
      if (GoNo) {
        Config.Echo();
        NoGo = 1;
      }
    }
  }
  return NoGo;
}

int XrdRedisProtocol::Configure(char *parms, XrdProtocol_Config * pi) {
  BPool = pi->BPool;

  // Copy out the special info we want to use at top level
  //
  eDest.logger(pi->eDest->logger());
  XrdRedisTrace = new XrdOucTrace(&eDest);
  XrdRedisTrace->What = TRACE_ALL;

  // readWait = 30000;
  readWait = 100;


  // Process items
  //
  // while ((var = Config.GetMyFirstWord())) {
  //   if ((ismine = !strncmp("http.", var, 5)) && var[5]) var += 5;
  //   else if ((ismine = !strcmp("all.export", var))) var += 4;
  //   else if ((ismine = !strcmp("all.pidpath", var))) var += 4;
  //
  //   if (ismine) {
  //          if TS_Xeq("trace", xtrace);
  //     else if TS_Xeq("cert", xsslcert);
  //     else if TS_Xeq("key", xsslkey);
  //     else if TS_Xeq("cadir", xsslcadir);
  //     else if TS_Xeq("gridmap", xgmap);
  //     else if TS_Xeq("cafile", xsslcafile);
  //     else if TS_Xeq("secretkey", xsecretkey);
  //     else if TS_Xeq("desthttps", xdesthttps);
  //     else if TS_Xeq("secxtractor", xsecxtractor);
  //     else if TS_Xeq("selfhttps2http", xselfhttps2http);
  //     else if TS_Xeq("embeddedstatic", xembeddedstatic);
  //     else if TS_Xeq("listingredir", xlistredir);
  //     else if TS_Xeq("staticredir", xstaticredir);
  //     else if TS_Xeq("staticpreload", xstaticpreload);
  //     else if TS_Xeq("listingdeny", xlistdeny);
  //     else {
  //       eDest.Say("Config warning: ignoring unknown directive '", var, "'.");
  //       Config.Echo();
  //       continue;
  //     }
  //     if (GoNo) {
  //
  //       Config.Echo();
  //       NoGo = 1;
  //     }
  //   }
  // }

  backend = new XrdRedisRocksDB("/home/gbitzes/redisdb");
  return 1;
}

XrdRedisProtocol::~XrdRedisProtocol() {

}
