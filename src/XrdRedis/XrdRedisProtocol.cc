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

#include "XrdRedisCommands.hh"
#include "XrdVersion.hh"
#include "XrdRedisProtocol.hh"
#include "XrdRedisSTL.hh"
#include "XrdRedisRocksDB.hh"
#include "XrdRedisTunnel.hh"
#include "XrdRedisReplicator.hh"
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
std::string XrdRedisProtocol::dbpath;
std::string XrdRedisProtocol::tunnel;
std::string XrdRedisProtocol::primary;
int XrdRedisProtocol::readWait = 0;


// std::map<std::string, CmdType> cmdMap;
//
// struct cmdMapInit {
//   cmdMapInit() {
//     cmdMap["ping"] = CMD_PING;
//     cmdMap["flushall"] = CMD_FLUSHALL;
//
//     cmdMap["get"] = CMD_GET;
//     cmdMap["set"] = CMD_SET;
//     cmdMap["exists"] = CMD_EXISTS;
//     cmdMap["del"] = CMD_DEL;
//     cmdMap["keys"] = CMD_KEYS;
//
//     cmdMap["hget"] = CMD_HGET;
//     cmdMap["hset"] = CMD_HSET;
//     cmdMap["hexists"] = CMD_HEXISTS;
//     cmdMap["hkeys"] = CMD_HKEYS;
//     cmdMap["hgetall"] = CMD_HGETALL;
//     cmdMap["hincrby"] = CMD_HINCRBY;
//     cmdMap["hdel"] = CMD_HDEL;
//     cmdMap["hlen"] = CMD_HLEN;
//     cmdMap["hvals"] = CMD_HVALS;
//     cmdMap["hscan"] = CMD_HSCAN;
//
//     cmdMap["sadd"] = CMD_SADD;
//     cmdMap["sismember"] = CMD_SISMEMBER;
//     cmdMap["srem"] = CMD_SREM;
//     cmdMap["smembers"] = CMD_SMEMBERS;
//     cmdMap["scard"] = CMD_SCARD;
//     cmdMap["sscan"] = CMD_SSCAN;
//   }
// } cmdMapInit;

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

  request_size = 0;
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

  if(buffers.size() == 0 || !buffers[0]->buff || !buffers[0]->bsize) {
    TRACE(ALL, " Process. No buffer available. Internal error.");
    return -1;
  }

  while(true) {
    int rc = ReadRequest(lp);

    if(rc == 0) return 1;
    if(rc < 0) return rc;

    request_size = 0;

    rc = ProcessRequest(lp);
    request.clear();
  }


  // return rc;
}

int XrdRedisProtocol::ReadRequest(XrdLink *lp) {
  // resumable function to read a request
  // returns 1 if we have a new command to process
  //         0 on slow link
  //         negative on error

  if(request_size == 0) {
    TRACEI(DEBUG, " Before read integer");
    int reqsize = ReadInteger(lp, '*');
    TRACEI(DEBUG, " After read integer: " << reqsize);
    if(reqsize <= 0) return reqsize;

    request_size = reqsize;
    element_size = 0;
    current_element = 0;
    TRACEI(DEBUG, "Received size of array: " << request_size);
  }

  for( ; current_element < request_size; current_element++) {
    std::string str;
    int rc = ReadElement(lp, str);
    if(rc <= 0) return rc;

    request.push_back(str);
    element_size = 0;
  }

  TRACEI(DEBUG, "Received command:");
  for(unsigned i = 0; i < request.size(); i++) {
    TRACEI(DEBUG, request[i]);
  }

  return 1;
}

int XrdRedisProtocol::ReadString(XrdLink *lp, int nbytes, std::string &str) {
  int rlen = canConsume(nbytes+2, lp);
  TRACEI(DEBUG, "in ReadString. canConsume: " << rlen);
  if(rlen <= 0) return rlen;

  consume(nbytes+2, str, lp);
  for(size_t i = 0; i < str.size(); i++) {
    TRACEI(DEBUG, "ReadString: consumed byte " << (int) str[i]);
  }

  TRACEI(DEBUG, "ReadString: consume returned string with size " << str.size() << "'" << str << "'");

  if(str[str.size()-2] != '\r') {
    TRACEI(ALL, "Protocol error, expected \\r, received " << str[str.size()-2]);
    return -1;
  }

  if(str[str.size()-1] != '\n') {
    TRACEI(ALL, "Protocol error, expected \\n, received " << str[str.size()-1]);
    return -1;
  }

  str.erase(str.begin()+str.size()-2, str.end());
  TRACEI(DEBUG, "Got string: " << str);
  return rlen;
}

int XrdRedisProtocol::ReadElement(XrdLink *lp, std::string &str) {
  TRACEI(DEBUG, "Element size: " << element_size);
  if(element_size == 0) {
    int elsize = ReadInteger(lp, '$');
    if(elsize <= 0) return elsize;
    element_size = elsize;
  }
  return ReadString(lp, element_size, str);
}

int XrdRedisProtocol::readFromLink(XrdLink *lp) {
  int total_bytes = 0;
  while(true) {
    // how many bytes can I write to the end of the last buffer?
    int available_space = buffer_size - position_write;

    // non-blocking read
    int rlen = Link->Recv(buffers.back()->buff + position_write, available_space, 0);
    if(rlen < 0) return rlen; // an error occured, let Process deal with it

    total_bytes += rlen;
    // we asked for available_space bytes, we got fewer. Means no more data to read
    if(rlen < available_space) {
      position_write += rlen;
      return total_bytes;
    }

    // we have more data to read, but no more space. Need to allocate buffer
    buffers.push_back(BPool->Obtain(buffer_size));
    position_write = 0;
  }
}

int XrdRedisProtocol::canConsume(size_t len, XrdLink *lp) {
  // we have n buffers, thus n*buffer_size bytes to read
  size_t available_bytes = buffers.size() * buffer_size;

  // .. minus, of course, the read and write markers for the first and last buffers
  available_bytes -= position_read;
  available_bytes -= buffer_size - position_write;
  if(available_bytes >= len) return available_bytes;

  // since we don't have enough bytes, try to read from the link
  int rlink = readFromLink(lp);
  if(rlink < 0) return rlink; // an error occurred, propagate to Process

  available_bytes += rlink;
  if(available_bytes >= len) return available_bytes;
  return 0;
}

void XrdRedisProtocol::consume(size_t len, std::string &str, XrdLink *lp) {
  str.clear();
  str.reserve(len);

  size_t remaining = len;
  // we assume there's enough space..
  while(remaining > 0) {
    TRACEI(DEBUG, "reading from buffer: " << (int)buffers.front()->buff[0] <<
                " " << (int) buffers.front()->buff[1] << " " << (int) buffers.front()->buff[2]);
    // how many bytes to read from current buffer?
    size_t available_bytes = buffer_size - position_read;
    if(available_bytes >= remaining) {
      available_bytes = remaining;
    }
    remaining -= available_bytes;

    // add them
    TRACEI(DEBUG, "Appending " << available_bytes << " bytes to str");
    str.append(buffers.front()->buff + position_read, available_bytes);
    position_read += available_bytes;

    if(position_read >= buffer_size) {
      TRACEI(DEBUG, "An entire buffer has been consumed, releasing");
      // an entire buffer has been consumed
      BPool->Release(buffers.front());
      buffers.pop_front();
      position_read = 0;
    }
  }
}

int XrdRedisProtocol::ReadInteger(XrdLink *lp, char prefix) {
  std::string prev;

  while(prev[0] != '\n') {
    int rlen = canConsume(1, lp);
    if(rlen <= 0) return rlen;

    consume(1, prev, lp);
    current_integer.append(prev);

    TRACEI(DEBUG, "Received byte: '" << prev << "'" << " " << (int) prev[0]);
    TRACEI(DEBUG, "current_integer: '" << current_integer << "'");
  }

  if(current_integer[current_integer.size()-2] != '\r') {
    TRACEI(ALL, "Protocol error, received \\n without preceeding \\r");
    return -1;
  }

  if(current_integer[0] != prefix) {
    TRACEI(ALL, "Protocol error, expected an integer with preceeding '" << prefix << "', received '" << current_integer[0] << "' instead");
    return -1;
  }

  current_integer.erase(current_integer.size()-2, 2);

  char *endptr;
  long num = strtol(current_integer.c_str()+1, &endptr, 10);
  if(*endptr != '\0' || num == LONG_MIN || num == LONG_MAX) {
    TRACEI(ALL, "Protocol error, received an invalid integer");
    return -1;
  }

  current_integer = "";
  return num;
}

int XrdRedisProtocol::ProcessRequest(XrdLink *lp) {
  // to lower
  std::transform(request[0].begin(), request[0].end(), request[0].begin(), ::tolower);

  std::string command = request[0];
  TRACEI(DEBUG, "in process request, command: '" << command << "'");
  TRACEI(DEBUG, "cmdMap size: " << redis_cmd_map.size());
  std::map<std::string, XrdRedisCommand>::iterator cmd = redis_cmd_map.find(command);


  if(cmd == redis_cmd_map.end()) {
    return SendErr(SSTR("unknown command '" << request[0] << "'"));
  }

  switch(cmd->second) {
    case XrdRedisCommand::PING: {
      if(request.size() > 2) return SendErrArgs(command);

      if(request.size() == 1) {
        XrdRedisStatus st = backend->ping();
        if(st.ok()) return SendPong();
        return SendErr(st);
      }
      if(request.size() == 2) return SendString(request[1]);
    }
    case XrdRedisCommand::FLUSHALL: {
      if(request.size() != 1) return SendErrArgs(command);
      XrdRedisStatus st = backend->flushall();
      if(!st.ok()) return SendErr(st);
      return SendOK();
    }
    case XrdRedisCommand::GET: {
      if(request.size() != 2) return SendErrArgs(command);

      std::string value;
      XrdRedisStatus st = backend->get(request[1], value);
      if(st.IsNotFound()) return SendNull();
      if(!st.ok()) return SendErr(st);
      return SendString(value);
    }
    case XrdRedisCommand::SET: {
      if(request.size() != 3) return SendErrArgs(command);

      XrdRedisStatus st = backend->set(request[1], request[2]);
      if(!st.ok()) return SendErr(st);
      return SendOK();
    }
    case XrdRedisCommand::EXISTS: {
      if(request.size() <= 1) return SendErrArgs(command);

      int count = 0;
      for(unsigned i = 1; i < request.size(); i++) {
        XrdRedisStatus st = backend->exists(request[i]);
        if(st.ok()) count++;
        else if(!st.IsNotFound()) SendErr(st);

      }
      return SendNumber(count);
    }
    case XrdRedisCommand::DEL: {
      if(request.size() <= 1) return SendErrArgs(command);

      int count = 0;
      for(unsigned i = 1; i < request.size(); i++) {
        XrdRedisStatus st = backend->del(request[i]);
        if(st.ok()) count++;
        else if(!st.IsNotFound()) return SendErr(st);
      }
      return SendNumber(count);
    }
    case XrdRedisCommand::KEYS: {
      if(request.size() != 2) return SendErrArgs(command);

      std::vector<std::string> ret;
      XrdRedisStatus st = backend->keys(request[1], ret);
      if(!st.ok()) return SendErr(st);
      return SendArray(ret);
    }
    case XrdRedisCommand::HGET: {
      if(request.size() != 3) return SendErrArgs(command);

      std::string value;
      XrdRedisStatus st = backend->hget(request[1], request[2], value);
      if(st.IsNotFound()) SendNull();
      else if(!st.ok()) return SendErr(st);

      return SendString(value);
    }
    case XrdRedisCommand::HSET: {
      if(request.size() != 4) return SendErrArgs(command);

      // Mild race condition here.. if the key doesn't exist, but another thread modifies
      // it in the meantime the user gets a response of 1, not 0

      XrdRedisStatus existed = backend->hexists(request[1], request[2]);
      if(!existed.ok() && !existed.IsNotFound()) return SendErr(existed);

      XrdRedisStatus st = backend->hset(request[1], request[2], request[3]);
      if(!st.ok()) return SendErr(st);

      if(existed.ok()) return SendNumber(0);
      return SendNumber(1);
    }
    case XrdRedisCommand::HEXISTS: {
      return SendErr("not implemented");
    }
    case XrdRedisCommand::HKEYS: {
      if(request.size() != 2) return SendErrArgs(command);

      std::vector<std::string> keys;
      XrdRedisStatus st = backend->hkeys(request[1], keys);
      if(!st.ok()) return SendErr(st);

      return SendArray(keys);
    }
    case XrdRedisCommand::HGETALL: {
      if(request.size() != 2) return SendErrArgs(command);

      std::vector<std::string> arr;
      XrdRedisStatus st = backend->hgetall(request[1], arr);
      if(!st.ok()) return SendErr(st);

      return SendArray(arr);
    }
    case XrdRedisCommand::HINCRBY: {
      if(request.size() != 4) return SendErrArgs(command);

      int64_t ret = 0;
      XrdRedisStatus st = backend->hincrby(request[1], request[2], request[3], ret);
      if(!st.ok()) return SendErr(st);
      return SendNumber(ret);
    }
    case XrdRedisCommand::HDEL: {
      if(request.size() <= 2) return SendErrArgs(command);

      int count = 0;
      for(unsigned i = 2; i < request.size(); i++) {
        XrdRedisStatus st = backend->hdel(request[1], request[i]);
        if(st.ok()) count++;
        else if(!st.IsNotFound()) return SendErr(st);
      }
      return SendNumber(count);
    }
    case XrdRedisCommand::HLEN: {
      if(request.size() != 2) return SendErrArgs(command);
      size_t len;
      XrdRedisStatus st = backend->hlen(request[1], len);
      if(!st.ok()) return SendErr(st);

      return SendNumber(len);
    }
    case XrdRedisCommand::HVALS: {
      if(request.size() != 2) return SendErrArgs(command);
      std::vector<std::string> vals;
      XrdRedisStatus st = backend->hvals(request[1], vals);
      return SendArray(vals);
    }
    case XrdRedisCommand::HSCAN: {
      if(request.size() != 3) return SendErrArgs(command);
      if(request[2] != "0") return SendErr("invalid cursor");

      std::vector<std::string> arr;
      XrdRedisStatus st = backend->hgetall(request[1], arr);
      if(!st.ok()) return SendErr(st);

      return SendScanResp("0", arr);
    }
    case XrdRedisCommand::SADD: {
      if(request.size() <= 2) return SendErrArgs(command);

      int64_t count = 0;
      for(unsigned i = 2; i < request.size(); i++) {
        int64_t tmp = 0;
        XrdRedisStatus st = backend->sadd(request[1], request[i], tmp);
        if(!st.ok()) return SendErr(st);
        count += tmp;
      }
      return SendNumber(count);
    }
    case XrdRedisCommand::SISMEMBER: {
      if(request.size() != 3) return SendErrArgs(command);

      XrdRedisStatus st = backend->sismember(request[1], request[2]);
      if(st.ok()) return SendNumber(1);
      if(st.IsNotFound()) return SendNumber(0);
      return SendErr(st);
    }
    case XrdRedisCommand::SREM: {
      if(request.size() <= 2) return SendErrArgs(command);

      int count = 0;
      for(unsigned i = 2; i < request.size(); i++) {
        XrdRedisStatus st = backend->srem(request[1], request[i]);
        if(st.ok()) count++;
        else if(!st.IsNotFound()) return SendErr(st);
      }
      return SendNumber(count);
    }
    case XrdRedisCommand::SMEMBERS: {
      if(request.size() != 2) return SendErrArgs(command);
      std::vector<std::string> members;
      XrdRedisStatus st = backend->smembers(request[1], members);
      if(!st.ok()) return SendErr(st);
      return SendArray(members);
    }
    case XrdRedisCommand::SCARD: {
      if(request.size() != 2) return SendErrArgs(command);
      size_t count;
      XrdRedisStatus st = backend->scard(request[1], count);
      if(!st.ok()) return SendErr(st);
      return SendNumber(count);
    }
    case XrdRedisCommand::SSCAN: {
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

int XrdRedisProtocol::SendOK() {
  return Send("+OK\r\n");
}




void XrdRedisProtocol::Reset() {
  for(size_t i = 0; i < buffers.size(); i++) {
    BPool->Release(buffers[i]);
  }
  buffers.clear();
  buffers.push_back(BPool->Obtain(buffer_size));

  position_read = 0;
  position_write = 0;
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
#define FETCH(x,dest) (!strcmp(x,var)) GoNo = fetch_config(Config, x, dest)

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
           else if FETCH("primary", primary);
           else if FETCH("tunnel", tunnel);
           else if FETCH("db", dbpath);
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

int XrdRedisProtocol::fetch_config(XrdOucStream &Config, const std::string &msg, std::string &dest) {
  char *val;
  if (!(val = Config.GetWord())) {
    eDest.Emsg("Config", SSTR(msg << " option not specified").c_str()); return 1;
  }

  dest = val;
  return 0;
}

static std::vector<std::string> split(std::string data, std::string token) {
    std::vector<std::string> output;
    size_t pos = std::string::npos;
    do {
        pos = data.find(token);
        output.push_back(data.substr(0, pos));
        if(std::string::npos != pos)
            data = data.substr(pos + token.size());
    } while (std::string::npos != pos);
    return output;
}

int XrdRedisProtocol::Configure(char *parms, XrdProtocol_Config * pi) {
  BPool = pi->BPool;

  // Copy out the special info we want to use at top level
  //
  eDest.logger(pi->eDest->logger());
  XrdRedisTrace = new XrdOucTrace(&eDest);
  XrdRedisTrace->What = TRACE_ALL;

  eDest.Emsg("Config", "redis: configuring");

  // readWait = 30000;
  readWait = 100;

  eDest.Emsg("Config", "in Configure");

  char* rdf;
  rdf = (parms && *parms ? parms : pi->ConfigFN);
  if (rdf && Config(rdf)) return 0;

  if(primary.empty()) {
    eDest.Emsg("Config", "redis.primary not specified, unable to continue");
    return 0;
  }

  // configure primary datastore
  if(primary == "rocksdb") {
    if(dbpath.empty()) {
      eDest.Emsg("Config", "redis.dbpath required when the primary datastore is rocksdb");
      return 0;
    }

    XrdRedisRocksDB *rocksdb = new XrdRedisRocksDB();
    XrdRedisStatus st = rocksdb->initialize(dbpath);
    if(!st.ok()) {
      eDest.Emsg("Config", SSTR("error while opening the db: " << st.ToString()).c_str());
      return 0;
    }

    backend = rocksdb;
  }
  else if(primary == "tunnel") {
    if(tunnel.empty()) {
      eDest.Emsg("Config", "redis.tunnel required when the primary datastore is tunnelled");
      return 0;
    }

    std::vector<std::string> components = split(tunnel, ":");
    if(components.size() != 2) {
      eDest.Emsg("Config", "malformed redis.tunnel. Syntax: ip:port");
      return 0;
    }

    std::string ip = components[0];

    char *endptr;
    long port = strtol(components[1].c_str(), &endptr, 10);
    if(*endptr != '\0' || port == LONG_MIN || port == LONG_MAX) {
      eDest.Emsg("Config", "malformed ip in redis.tunnel: not a number");
      return -1;
    }

    backend = new XrdRedisTunnel(ip, port);
  }
  else {
    eDest.Emsg("Config", "unknown option for redis.primary, unable to continue");
    return 0;
  }

  std::vector<XrdRedisBackend*> replicas;
  backend = new XrdRedisReplicator(backend, replicas);

  return 1;
}

XrdRedisProtocol::~XrdRedisProtocol() {

}
