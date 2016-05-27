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

  set(ROCKS_DB_PATH /home/gbitzes/rocksdb-4.5.1/)
  include_directories(${ROCKS_DB_PATH}/include)
  link_directories(${ROCKS_DB_PATH})

  #-----------------------------------------------------------------------------
  # The XrdRedis library
  #-----------------------------------------------------------------------------

  add_definitions(--std=c++11)

  add_library(
    ${LIB_XRD_REDIS}
    MODULE
    XrdRedis/XrdRedisPlugin.cc
    XrdRedis/XrdRedisSTL.cc       XrdRedis/XrdRedisSTL.hh
    XrdRedis/XrdRedisRocksDB.cc   XrdRedis/XrdRedisRocksDB.hh
    XrdRedis/XrdRedisProtocol.cc  XrdRedis/XrdRedisProtocol.hh
    XrdRedis/XrdRedisUtil.cc      XrdRedis/XrdRedisUtil.hh)

  target_link_libraries(
    ${LIB_XRD_REDIS}
    XrdServer
    XrdUtils
    rocksdb)

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
