INCLUDE_DIRECTORIES(
  ${CMAKE_CURRENT_SOURCE_DIR}

  ${izenelib_INCLUDE_DIRS}
  ${Boost_INCLUDE_DIRS}
  ${TokyoCabinet_INCLUDE_DIRS}
  ${Glog_INCLUDE_DIRS}
  )

MESSAGE(STATUE "include path: ${INCLUDE_DIRECTORIES}")

FILE(GLOB log_server_process_SRC
  "*.cpp"
  "common/*.cpp"
  "log-server/*.cpp"
  )
ADD_DEFINITIONS("-fno-strict-aliasing")
ADD_EXECUTABLE(LogServerProcess ${log_server_process_SRC})

FIND_PACKAGE(izenelib REQUIRED COMPONENTS
  febird
  izene_util
  luxio
  leveldb
  json
  am
  compressor
  aggregator
  msgpack
  zookeeper
  snappy
  sf1r
  )


TARGET_LINK_LIBRARIES(LogServerProcess
  ${izenelib_LIBRARIES}

  #external
  ${Boost_LIBRARIES}
  ${Glog_LIBRARIES}
  ${TokyoCabinet_LIBRARIES}
  ${CMAKE_THREAD_LIBS_INIT}
  ${SYS_LIBS}
  )

INSTALL(TARGETS
  LogServerProcess
  RUNTIME DESTINATION bin
  COMPONENT sf1r_logserver_apps)
