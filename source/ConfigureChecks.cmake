##################################################
# Headers
#####
INCLUDE(CheckIncludeFile)

# int types
CHECK_INCLUDE_FILE(inttypes.h HAVE_INTTYPES_H)
CHECK_INCLUDE_FILE(stdint.h HAVE_STDINT_H)
CHECK_INCLUDE_FILE(sys/types.h HAVE_SYS_TYPES_H)
CHECK_INCLUDE_FILE(sys/stat.h HAVE_SYS_STAT_H)
CHECK_INCLUDE_FILE(stddef.h HAVE_STDDEF_H)

# signal.h
CHECK_INCLUDE_FILE(signal.h HAVE_SIGNAL_H)

# ext hash
CHECK_INCLUDE_FILE(ext/hash_map HAVE_EXT_HASH_MAP)

##################################################
# Our Proprietary Libraries
#####

FIND_PACKAGE(izenelib REQUIRED COMPONENTS
  izene_util
  febird
  leveldb
  jemalloc
  json
  am
  compressor
  msgpack
  zookeeper
  snappy
  luxio
  aggregator
  sf1r
  )
FIND_PACKAGE(idmlib REQUIRED)
FIND_PACKAGE(TokyoCabinet 1.4.29 REQUIRED)
FIND_PACKAGE(MySQL REQUIRED)
##################################################
# Other Libraries
#####

FIND_PACKAGE(Threads REQUIRED)

SET(Boost_ADDITIONAL_VERSIONS 1.53)
FIND_PACKAGE(Boost 1.47 REQUIRED
  COMPONENTS
  system
  program_options
  thread
  regex
  date_time
  serialization
  filesystem
  unit_test_framework
  iostreams
  )

FIND_PACKAGE(Glog REQUIRED)

##################################################
# Doxygen
#####
FIND_PACKAGE(Doxygen)
IF(DOXYGEN_DOT_EXECUTABLE)
  OPTION(USE_DOT "use dot in doxygen?" FLASE)
ENDIF(DOXYGEN_DOT_EXECUTABLE)

SET(USE_DOT_YESNO NO)
IF(USE_DOT)
  SET(USE_DOT_YESNO YES)
ENDIF(USE_DOT)

set(SYS_LIBS
  m rt dl z
)

