/**
 * @file LogServerCfg.h
 * @author Zhongxia Li
 * @date Dec 20, 2011
 * @brief Log server configuration
 */
#ifndef LOG_SERVER_CFG_H_
#define LOG_SERVER_CFG_H_

#include <string>
#include <set>

#include <util/singleton.h>
#include <util/kv2string.h>
#include <boost/unordered_map.hpp>
#include <glog/logging.h>

namespace sf1r
{

typedef izenelib::util::kv2string properties;

class LogServerCfg
{
public:
    LogServerCfg();

    ~LogServerCfg();

    static LogServerCfg* get()
    {
        return izenelib::util::Singleton<LogServerCfg>::get();
    }

    bool parse(const std::string& cfgFile);

    inline const std::string& getLocalHost() const
    {
        return host_;
    }

    inline unsigned int getRpcServerPort() const
    {
        return rpcPort_;
    }

    inline unsigned int getRpcThreadNum() const
    {
        return rpcThreadNum_;
    }

    inline unsigned int getRpcRequestQueueSize() const
    {
        return rpcRequestQueueSize_;
    }

    inline unsigned int getDriverServerPort() const
    {
        return driverPort_;
    }

    inline unsigned int getDriverThreadNum() const
    {
        return driverThreadNum_;
    }

    const std::set<std::string>& getDriverCollections() const
    {
        return driverCollections_;
    }

    inline const std::string& getStorageBaseDir() const
    {
        return base_dir_;
    }

    inline unsigned int getFlushCheckInterval() const
    {
        return flush_check_interval_;
    }

    inline bool getStartTime(const std::string& s, const std::string& c, uint32_t& res)
    {
        if(start_time_.find(s) == start_time_.end())
            return false;
        if(start_time_[s].find(c) == start_time_[s].end())
            return false;
        res = start_time_[s][c];
        return true;
    }
    inline bool getSlideLength(const std::string& s, const std::string& c, uint32_t& res)
    {
        if(slide_length_.find(s) == slide_length_.end())
            return false;
        if(slide_length_[s].find(c) == slide_length_[s].end())
            return false;
        res = slide_length_[s][c];
        return true;
    }
    inline bool getSketchWidth(const std::string& s, const std::string& c, uint32_t& res)
    {
        if(sketch_width_.find(s) == sketch_width_.end())
            return false;
        if(sketch_width_[s].find(c) == sketch_width_[s].end())
            return false;
        res = sketch_width_[s][c];
        return true;
    }
    inline bool getSketchMaxValue(const std::string& s, const std::string& c, uint32_t& res)
    {
        if(sketch_max_value_.find(s) == sketch_max_value_.end())
            return false;
        if(sketch_max_value_[s].find(c) == sketch_max_value_[s].end())
            return false;
        res = sketch_max_value_[s][c];
        return true;
    }
    inline bool getTopKMaxCount(const std::string& s, const std::string& c, uint32_t& res)
    {
        if(topk_max_count_.find(s) == topk_max_count_.end())
            return false;
        if(topk_max_count_[s].find(c) == topk_max_count_[s].end())
            return false;
        res = topk_max_count_[s][c];
        return true;
    }
    inline bool getImport()
    {
        return import_from_database_;
    }
private:
    typedef boost::unordered_map<std::string, uint32_t> CollCfgT;
    typedef boost::unordered_map<std::string, CollCfgT> ServCfgT;

    bool parseCfgFile_(const std::string& cfgFile);

    void parseCfg(properties& props);

    void parseServerCfg(properties& props);

    void parseStorageCfg(properties& props);

    void parseDataStreams(properties& props);

    void parseDriverCollections(const std::string& collections);

    std::string cfgFile_;

    std::string host_;

    unsigned int rpcPort_;
    unsigned int rpcThreadNum_;
    unsigned int rpcRequestQueueSize_;

    unsigned int driverPort_;
    unsigned int driverThreadNum_;
    std::set<std::string> driverCollections_;

    std::string base_dir_;
    unsigned int flush_check_interval_;

    ServCfgT start_time_;
    ServCfgT slide_length_;
    ServCfgT sketch_width_;
    ServCfgT sketch_max_value_;
    ServCfgT topk_max_count_;

    bool import_from_database_;
};

}

#endif /* LOGSERVERCFG_H_ */
