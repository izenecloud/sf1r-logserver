CMAKE_MINIMUM_REQUIRED(VERSION 2.6)
GET_FILENAME_COMPONENT(SF1RLOGSERVER_ROOT "${CMAKE_SOURCE_DIR}" PATH)

##################################################
# Find extra cmake modules dirs
#
# You can clone all modules from https://ssl.izenesoft.cn/projects/cmake
#
# Clone it and set environment variable EXTRA_CMAKE_MODULES_DIRS point to it.
#
SET(ENV_EXTRA_CMAKE_MODULES_DIRS $ENV{EXTRA_CMAKE_MODULES_DIRS})
IF(ENV_EXTRA_CMAKE_MODULES_DIRS)
  SET(EXTRA_CMAKE_MODULES_DIRS ${EXTRA_CMAKE_MODULES_DIRS} ${ENV_EXTRA_CMAKE_MODULES_DIRS})
ENDIF(ENV_EXTRA_CMAKE_MODULES_DIRS)
IF(NOT EXTRA_CMAKE_MODULES_DIRS)
  MESSAGE(FATAL_ERROR
    "Cannot find EXTRA_CMAKE_MODULES_DIRS.
1. Please clone it from https://ssl.izenesoft.cn/cmake.
2. Then set EXTRA_CMAKE_MODULES_DIRS through environment variable

       export EXTRA_CMAKE_MODULES_DIRS=/path/to/cmake

   You can put it in your .bashrc so it is loaded automatically.

   or cmake options

       cmake  -DEXTRA_CMAKE_MODULES_DIRS=/path/to/cmake (other options)
")
ENDIF(NOT EXTRA_CMAKE_MODULES_DIRS)
SET(CMAKE_MODULE_PATH ${EXTRA_CMAKE_MODULES_DIRS})

# force out-of-source build
IF (CMAKE_BINARY_DIR STREQUAL CMAKE_SOURCE_DIR)
  MESSAGE(FATAL_ERROR "In source build is not allowed")
ENDIF (CMAKE_BINARY_DIR STREQUAL CMAKE_SOURCE_DIR)

##################################################
# Project and some options
#####
PROJECT(sf1r-logserver)
OPTION(VERBOSE "print detailed configure messages" ON)
OPTION(ENABLE_TEST "build test applications?" ON)
IF(ENABLE_TEST)
  ENABLE_TESTING()
ENDIF(ENABLE_TEST)


##################################################
# BUILD TYPE & OPTIONS
#
# Put this section just under PROJECT
#####

# Use BUILD_FLAGS_HAS_BEEN_CUSTOMIZED as guard.
#
# The options are only customized in first run. User can edit cache to
# set their own options. If it's required to customized these in cmake
# command line options, please also add
# "-DBUILD_FLAGS_HAS_BEEN_CUSTOMIZED".
IF (CMAKE_COMPILER_IS_GNUCXX)
  INCLUDE(MacroSetBuildTypes)

  MACRO_SET_BUILD_TYPES(
    RelWithDebInfo
    "-march=native -mtune=native -fopenmp -O2 -g -Wno-deprecated -DNDEBUG"
    "Flags used by the compiler during Release with Debug Info builds."

    Release
    "-march=native -mtune=native -fopenmp -O3 -Wno-deprecated -DNDEBUG"
    "Flags used by the compiler during release builds"

    MinSizeRel
    "-march=native -mtune=native -fopenmp -Os -Wno-deprecated -DNDEBUG"
    "Flags used by the compiler during release minsize builds."

    Debug
    "-march=native -mtune=native -fopenmp -g -O0 -fno-reorder-blocks -fno-schedule-insns -fno-inline -Wno-deprecated"
    "Flags used by the compiler during debug builds."

    Profile
    "-march=native -mtune=native -fopenmp -g3 -fno-inline -ftest-coverage -fprofile-arcs -Wno-deprecated"
    "Profile build type"
    )
ENDIF (CMAKE_COMPILER_IS_GNUCXX)

INCLUDE(MacroSetDefaultBuildType)
MACRO_SET_DEFAULT_BUILD_TYPE(RelWithDebInfo)

INCLUDE(SetCompilerWarningAll)
# Write to cache
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}" CACHE STRING
  "Flags used by the compiler during all build types." FORCE)
SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}" CACHE STRING
  "Flags used by the compiler during all build types." FORCE)

# build shared libs by default
OPTION(BUILD_SHARED_LIBS "Build shared libraries?" ON)

##################################################
# Check Dependencies
####
INCLUDE(SetPkgConfigPath)
INCLUDE(ConfigureChecks.cmake)

##################################################
# Set options for packaging
#####
STRING(TOLOWER ${CMAKE_BUILD_TYPE} buildtype)
INCLUDE(Packaging.cmake)

##################################################
# Sub Directories
#####

# library dir
LINK_DIRECTORIES(
  ${izenelib_LIBRARY_DIRS}
  ${idmlib_LIBRARY_DIRS}
)

# set output directory
SET(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${SF1RLOGSERVER_ROOT}/lib")
SET(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${SF1RLOGSERVER_ROOT}/lib")
SET(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${SF1RLOGSERVER_ROOT}/bin")

ADD_SUBDIRECTORY(process)

IF(ENABLE_TEST)
  ADD_SUBDIRECTORY("${SF1RLOGSERVER_ROOT}/test" test)
ENDIF(ENABLE_TEST)


##################################################
# Config
#####
SET(LOCAL_HOST_USER_NAME $ENV{USER})
IF(NOT LOCAL_HOST_USER_NAME)
  SET(LOCAL_HOST_USER_NAME $ENV{LOGNAME})
ENDIF(NOT LOCAL_HOST_USER_NAME)
IF(NOT LOCAL_HOST_USER_NAME)
  SET(LOCAL_HOST_USER_NAME $ENV{USERNAME})
ENDIF(NOT LOCAL_HOST_USER_NAME)


##################################################
# Messages
#####
IF(VERBOSE)

  MESSAGE(STATUS "========================================")
  MESSAGE(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
  STRING(TOUPPER "${CMAKE_BUILD_TYPE}" CMAKE_BUILD_TYPE_UPPER)
  MESSAGE(STATUS "  corresponding flags: ${CMAKE_CXX_FLAGS_${CMAKE_BUILD_TYPE_UPPER}}")
  MESSAGE(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
  IF(${buildtype} STREQUAL "cobrarestrict"  OR ${buildtype} STREQUAL "cobrarestrictrelease" )
      MESSAGE(STATUS "LICENSE LEVEL = ${UPPER_LICENSE_LEVEL}")
      MESSAGE(STATUS "LICENSE FLAGS = ${LICENSE_FLAGS}")
  ELSE(${buildtype} STREQUAL "cobrarestrict"  OR ${buildtype} STREQUAL "cobrarestrictrelease" )
      MESSAGE(STATUS "LICENSE LEVEL = NO LICENSE RESTRICTION")
  ENDIF(${buildtype} STREQUAL "cobrarestrict"  OR ${buildtype} STREQUAL "cobrarestrictrelease" )
  MESSAGE(STATUS "----------------------------------------")

  IF(izenelib_FOUND)
    MESSAGE(STATUS "izenelib:")
    MESSAGE(STATUS "  header: ${izenelib_INCLUDE_DIRS}")
    MESSAGE(STATUS "  lib: ${izenelib_LIBRARIES}")
  ELSE(izenelib_FOUND)
    MESSAGE(STATUS "izenelib: not found")
  ENDIF(izenelib_FOUND)

  IF(idmlib_FOUND)
    MESSAGE(STATUS "idmlib:")
    MESSAGE(STATUS "  header: ${idmlib_INCLUDE_DIRS}")
    MESSAGE(STATUS "  lib: ${idmlib_LIBRARIES}")
  ELSE(idmlib_FOUND)
    MESSAGE(STATUE "idmlib: not found")
  ENDIF(idmlib_FOUND)

  IF(MYSQL_FOUND)
    MESSAGE(STATUS "MySQL:")
    MESSAGE(STATUS " header: ${MYSQL_INCLUDE_DIR}")
    MESSAGE(STATUS " libraries: ${MYSQL_LIBRARIES}")
  ELSE(MYSQL_FOUND)
    MESSAGE(STATUS "MySQL: not found")
  ENDIF(MYSQL_FOUND)

  IF(Boost_FOUND)
    MESSAGE(STATUS "BOOST:")
    MESSAGE(STATUS "  header: ${Boost_INCLUDE_DIR}")
    MESSAGE(STATUS "  version: ${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION}.${Boost_SUBMINOR_VERSION}")
  ELSE(Boost_FOUND)
    MESSAGE(STATUS "BOOST: not found")
  ENDIF(Boost_FOUND)

  IF(TokyoCabinet_FOUND)
    MESSAGE(STATUS "TokyoCabine:")
    MESSAGE(STATUS "  header: ${TokyoCabinet_INCLUDE_DIRS}")
    MESSAGE(STATUS "  version: ${TokyoCabinet_VERSION}")
  ELSE(TokyoCabinet_FOUND)
    MESSAGE(STATUS "TokyoCabinet: not found")
  ENDIF(TokyoCabinet_FOUND)

  IF(Glog_FOUND)
    MESSAGE(STATUS "Google-glog:")
    MESSAGE(STATUS "  header: ${Glog_INCLUDE_DIRS}")
  ELSE(Glog_FOUND)
    MESSAGE(STATUS "Google-glog: not found")
  ENDIF(Glog_FOUND)

  IF(Threads_FOUND)
    MESSAGE(STATUS "Threads Library: ${CMAKE_THREAD_LIBS_INIT}")
  ELSE(Threads_FOUND)
    MESSAGE(STATUS "Threads Library: none")
  ENDIF(Threads_FOUND)

  MESSAGE(STATUS "========================================")

ENDIF(VERBOSE)
