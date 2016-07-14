include( XRootDCommon )

#-------------------------------------------------------------------------------
# Modules
#-------------------------------------------------------------------------------
set( LIB_XRD_REDIS       XrdRedis-${PLUGIN_VERSION} )

#-------------------------------------------------------------------------------
# Shared library version
#-------------------------------------------------------------------------------

if( ENABLE_REDIS )
  #-------------------------------------------------------------------------------
  # Statically link to rocksdb
  #-------------------------------------------------------------------------------

  if(NOT DEFINED ROCKSDB_PATH)
    message(FATAL_ERROR "Error: cmake variable ROCKSDB_PATH not set")
  endif()

  include_directories(${ROCKSDB_PATH}/include)
  link_directories(${ROCKSDB_PATH})

  #-------------------------------------------------------------------------------
  # Statically link to hiredis
  #-------------------------------------------------------------------------------

  if(NOT DEFINED HIREDIS_INCLUDE_DIRS)
    message(FATAL_ERROR "Error: cmake variable HIREDIS_INCLUDE_DIRS not set")
  endif()

  if(NOT DEFINED HIREDIS_LIBRARIES)
    message(FATAL_ERROR "Error: cmake variable HIREDIS_LIBRARIES not set")
  endif()

  include_directories(${HIREDIS_INCLUDE_DIRS})

  #-----------------------------------------------------------------------------
  # The XrdRedis library
  #-----------------------------------------------------------------------------

  add_definitions(--std=c++11)

  add_library(
    ${LIB_XRD_REDIS}
    MODULE
    XrdRedis/XrdRedisPlugin.cc
    XrdRedis/XrdRedisSTL.cc         XrdRedis/XrdRedisSTL.hh
    XrdRedis/XrdRedisRocksDB.cc     XrdRedis/XrdRedisRocksDB.hh
    XrdRedis/XrdRedisProtocol.cc    XrdRedis/XrdRedisProtocol.hh
    XrdRedis/XrdRedisUtil.cc        XrdRedis/XrdRedisUtil.hh
    XrdRedis/XrdRedisTunnel.cc      XrdRedis/XrdRedisTunnel.hh
    XrdRedis/XrdRedisReplicator.cc  XrdRedis/XrdRedisReplicator.hh)

  target_link_libraries(
    ${LIB_XRD_REDIS}
    XrdServer
    XrdUtils
    rocksdb
    ${HIREDIS_LIBRARIES})

  set_target_properties(
    ${LIB_XRD_REDIS}
    PROPERTIES
    INTERFACE_LINK_LIBRARIES ""
    LINK_INTERFACE_LIBRARIES "" )

  #-----------------------------------------------------------------------------
  # Install
  #-----------------------------------------------------------------------------
  install(
    TARGETS ${LIB_XRD_REDIS}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} )

endif()
