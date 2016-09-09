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

#ifndef __XRDREDIS_COMMON_H__
#define __XRDREDIS_COMMON_H__

#include <sstream>

#define SSTR(message) static_cast<std::ostringstream&>(std::ostringstream().flush() << message).str()

#include <cstdint>
#include <vector>
#include <memory>

using RaftTerm = int64_t;
using LogIndex = int64_t;
using RaftClusterID = std::string;
using RaftServerID = int64_t;
using string_ptr  = std::shared_ptr<std::string>;
using XrdRedisRequest = std::vector<string_ptr>;



struct RaftServer {
  std::string hostname;
  int port;
};


#endif
