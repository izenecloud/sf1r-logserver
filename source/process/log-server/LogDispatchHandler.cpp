#include "LogDispatchHandler.h"

#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/bimap.hpp>
#include <boost/bimap/set_of.hpp>
#include <boost/bimap/multiset_of.hpp>

#include <glog/logging.h>

namespace bfs = boost::filesystem;

namespace sf1r
{
using namespace izenelib::driver;
using namespace izenelib::net::sf1r;

////////////////////////////////////////////////////////////////////////////////
/// LogDispatchHandler

void LogDispatchHandler::Init()
{
    // call back for drum dispatcher
    // collections of which log need to be updated
    driverCollections_ = LogServerCfg::get()->getDriverCollections();
    storageBaseDir_ = LogServerCfg::get()->getStorageBaseDir();
}

void LogDispatchHandler::Flush()
{
}

void LogDispatchHandler::InitSf1DriverClient_(
    const std::string& host, 
    uint32_t port)
{
    if (!sf1DriverClient_ || (sf1Host_ != host || sf1Port_ != port))
    {
        sf1Host_ = host;
        sf1Port_ = port;
        izenelib::net::sf1r::Sf1Config sf1Conf;
        std::string sf1Address = host + ":" + boost::lexical_cast<std::string>(port);
        sf1DriverClient_.reset(new Sf1Driver(sf1Address, sf1Conf));
    }
}

}
