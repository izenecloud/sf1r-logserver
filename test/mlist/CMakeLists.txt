INCLUDE_DIRECTORIES(
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/process
  ${Boost_INCLUDE_DIRS}
  ${Glog_INCLUDE_DIRS}
  ${izenelib_INCLUDE_DIRS}
  ${TokyoCabinet_INCLUDE_DIRS}
  )
MESSAGE(STATUE "include path: ${INCLUDE_DIRECTORIES}")
SET(libs
  ${izenelib_LIBRARIES}
  #external
  ${Boost_LIBRARIES}
  ${TokyoCabinet_LIBRARIES}
  ${Glog_LIBRARIES}
)
SET(Boost_USE_STATIC_LIBS OFF)
FIND_PACKAGE(Boost ${Boost_FIND_VERSION}
  COMPONENTS unit_test_framework)

IF(Boost_FOUND AND Boost_UNIT_TEST_FRAMEWORK_FOUND)
  INCLUDE_DIRECTORIES(
    ${Boost_INCLUDE_DIRS}
  )

ADD_EXECUTABLE(t_mon_list
    t_mon_list.cpp
  )
TARGET_LINK_LIBRARIES(t_mon_list
    ${libs}
  )

ADD_EXECUTABLE(t_import_data
  t_import_data.cpp
  )
TARGET_LINK_LIBRARIES(t_import_data
  ${libs}
  ${MYSQL_LIBRARIES}
  )

ADD_EXECUTABLE(t_import_scd
  t_import_scd.cpp
  )
TARGET_LINK_LIBRARIES(t_import_scd
  ${libs}
  ${MYSQL_LIBRARIES}
  )

ENDIF(Boost_FOUND AND Boost_UNIT_TEST_FRAMEWORK_FOUND)
