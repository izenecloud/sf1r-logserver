#ifndef LOG_SERVER_STORAGE_H_
#define LOG_SERVER_STORAGE_H_

#include "LogServerCfg.h"
#include "LogAnalysisManager.h"
#include <util/singleton.h>
#include <boost/unordered_map.hpp>

using namespace sf1r::logserver;
namespace sf1r
{

#define USE_TC_HASH

class LogServerStorage
{
public:
    static LogServerStorage* get()
    {
        return izenelib::util::Singleton<LogServerStorage>::get();
    }

    bool init()
    {
        logAnalysisManager_ = new LogAnalysisManager(LogServerCfg::get()->getStorageBaseDir()+"/");
        return true;
    }

    void close()
    {
        try
        {
            if(logAnalysisManager_)
            {
                boost::unique_lock<boost::mutex> lock(logAnalysisManagerMutex_, boost::defer_lock);
                if(lock.try_lock())
                {
                    LOG(INFO) <<"delete logAnalysisManager_"<<endl;
                    logAnalysisManager_->close();
                    delete logAnalysisManager_;
                }
                else
                {
                    std::cout << "logAnalysisManager_ is still working..." << std::endl;
                    return;
                }
            }
        }
        catch (const std::exception& e)
        {
            std::cout << "LogServerStorage close: " << e.what() << std::endl;
        }
    }

    LogAnalysisManager* logAnalysisManager()
    {
        return logAnalysisManager_;
    }

    boost::mutex& logAnalysisManagerMutex()
    {
        return logAnalysisManagerMutex_;
    }

private:
    LogAnalysisManager* logAnalysisManager_;
    boost::mutex logAnalysisManagerMutex_;
};

}

#endif /* LOG_SERVER_STORAGE_H_ */
