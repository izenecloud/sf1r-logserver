#include "LogServerCfg.h"

#include <util/string/StringUtils.h>

#include <iostream>
#include <fstream>

#include <boost/lexical_cast.hpp>
#include <boost/tokenizer.hpp>
#include <boost/filesystem.hpp>

namespace bfs = boost::filesystem;

namespace sf1r
{

static const char* DEFAULT_STORAGE_BASE_DIR = "log_server_storage";
static const unsigned int DEFAULT_RPC_REQUEST_QUEUE_SIZE = 32768;
static const unsigned int DEFAULT_FLASH_CHECK_INTERVAL = 60; // seconds
static const unsigned int DEFAULT_THREAD_NUM = 30;
static const unsigned int MAX_THREAD_NUM = 1024;

LogServerCfg::LogServerCfg()
    : rpcPort_(0)
    , driverPort_(0)
{
    import_from_database_=false;
}

LogServerCfg::~LogServerCfg()
{
}

bool LogServerCfg::parse(const std::string& cfgFile)
{
    cfgFile_ = cfgFile;
    return parseCfgFile_(cfgFile);
}

bool LogServerCfg::parseCfgFile_(const std::string& cfgFile)
{
    try
    {
        if (!bfs::exists(cfgFile) || !bfs::is_regular_file(cfgFile))
        {
            std::cerr <<"\""<<cfgFile<< "\" is not existed or not a regular file." << std::endl;
            return false;
        }

        std::ifstream cfgInput(cfgFile.c_str());
        std::string cfgString;
        std::string line;

        if (!cfgInput.is_open())
        {
            std::cerr << "Could not open file: " << cfgFile << std::endl;
            return false;
        }

        while (getline(cfgInput, line))
        {
            izenelib::util::Trim(line);
            if (line.empty() || line[0] == '#')
            {
                // ignore empty line and comment line
                continue;
            }

            if (!cfgString.empty())
            {
                cfgString.push_back('\n');
            }
            cfgString.append(line);
        }

        // check configuration properties
        properties props('\n', '=');
        props.loadKvString(cfgString);

        parseCfg(props);
    }
    catch (const std::exception& e)
    {
        std::cerr << e.what() << std::endl;
        return false;
    }

    return true;
}

void LogServerCfg::parseCfg(properties& props)
{
    parseServerCfg(props);
    parseStorageCfg(props);
    parseDataStreams(props);
}

void LogServerCfg::parseServerCfg(properties& props)
{
    if (!props.getValue("host", host_))
    {
        host_ = "localhost";
    }

    // rpc server
    if (!props.getValue("rpc.port", rpcPort_))
    {
        throw std::runtime_error("Log Server Configuration missing property: rpc.port");
    }

    if (!props.getValue("rpc.thread_num", rpcThreadNum_))
    {
        rpcThreadNum_ = DEFAULT_THREAD_NUM;
    }
    else
    {
        rpcThreadNum_ = std::min(MAX_THREAD_NUM, rpcThreadNum_);
    }

    if (!props.getValue("rpc.request_queue_size", rpcRequestQueueSize_))
    {
        rpcRequestQueueSize_ = DEFAULT_RPC_REQUEST_QUEUE_SIZE;
    }

    // driver server
    if (!props.getValue("driver.port", driverPort_))
    {
        throw std::runtime_error("Log Server Configuration missing property: driver.port");
    }

    if (!props.getValue("driver.thread_num", driverThreadNum_))
    {
        driverThreadNum_ = DEFAULT_THREAD_NUM;
    }
    else
    {
        driverThreadNum_ = std::min(MAX_THREAD_NUM, driverThreadNum_);
    }

    std::string collections;
    if (props.getValue("driver.collections", collections))
    {
        parseDriverCollections(collections);
    }
}

void LogServerCfg::parseStorageCfg(properties& props)
{
    // base
    if (!props.getValue("storage.base_dir", base_dir_))
    {
        base_dir_ = DEFAULT_STORAGE_BASE_DIR;
    }
    if (!bfs::exists(base_dir_))
    {
        bfs::create_directories(base_dir_);
    }

    if (!props.getValue("storage.flush_check_interval", flush_check_interval_))
    {
        flush_check_interval_ = DEFAULT_FLASH_CHECK_INTERVAL;
    }

}

void LogServerCfg::parseDataStreams(properties& props)
{
    uint32_t count;
    if(props.getValue("datastream.count", count))
    {
        std::string s, c;
        uint32_t d;
        for(uint32_t i=0;i<count;i++)
        {
            std::string is=boost::lexical_cast<std::string>(i);
            if(!props.getValue("datastream."+is+".service", s))
            {
                throw std::runtime_error("Configuration missing datatream service");
            }

            if(!props.getValue("datastream."+is+".collection", c))
            {
                throw std::runtime_error("Configuration missing datatream collection");
            }

            if(props.getValue("datastream."+is+".cronjob.start_time", d))
            {
                if(start_time_.find(s) == start_time_.end())
                {
                    CollCfgT m; m[c]=d;
                    start_time_[s]=m;
                }
                else
                    start_time_[s][c]=d;
                LOG(INFO)<<"datastream [ " <<s<<", "<<c<<" ].cronjob.start_time="<<d<<endl;
            }

            if(props.getValue("datastream."+is+".cronjob.slide_length", d))
            {
                if(slide_length_.find(s) == slide_length_.end())
                {
                    CollCfgT m;m[c]=d;
                    slide_length_[s]=m;
                }
                else
                    slide_length_[s][c]=d;
                LOG(INFO)<<"datastream [ " <<s<<", "<<c<<" ].cronjob.slide_length="<<d<<endl;
            }

            if(props.getValue("datastream."+is+".sketch.width", d))
            {
                if(sketch_width_.find(s) == sketch_width_.end())
                {
                    CollCfgT m; m[c]=d;
                    sketch_width_[s]=m;
                }
                else
                    sketch_width_[s][c]=d;
                LOG(INFO)<<"datastream [ " <<s<<", "<<c<<" ].sketch_width="<<d<<endl;
            }

            if(props.getValue("datastream."+is+".sketch.max_value", d))
            {
                if(sketch_max_value_.find(s) == sketch_max_value_.end())
                {
                    CollCfgT m; m[c]=d;
                    sketch_max_value_[s]=m;
                }
                else
                    sketch_max_value_[s][c]=d;
                LOG(INFO)<<"datastream [ " <<s<<", "<<c<<" ].sketch_max_value="<<d<<endl;
            }

            if(props.getValue("datastream."+is+".topk.max_count", d))
            {
                if(topk_max_count_.find(s) == topk_max_count_.end())
                {
                    CollCfgT m; m[c]=d;
                    topk_max_count_[s]=m;
                }
                else
                    topk_max_count_[s][c]=d;
                LOG(INFO)<<"datastream [ " <<s<<", "<<c<<" ].topk_max_count="<<d<<endl;
            }

        }
    }

    std::string im;
    if(props.getValue("userquery.import.from.database", im))
    {
        if(im.compare("y")==0 || im.compare("Y")==0)
            import_from_database_=true;
    }
}

void LogServerCfg::parseDriverCollections(const std::string& collections)
{
    boost::char_separator<char> sep(", ");
    boost::tokenizer<boost::char_separator<char> > tokens(collections, sep);

    boost::tokenizer<boost::char_separator<char> >::iterator it;
    for(it = tokens.begin(); it != tokens.end(); ++it)
    {
        driverCollections_.insert(*it);
    }
}

}
