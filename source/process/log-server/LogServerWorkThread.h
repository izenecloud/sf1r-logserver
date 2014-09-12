/**
 * @file LogServerWorkThread.h
 * @author Zhongxia Li
 * @date Jan 11, 2012
 * @brief
 */
#ifndef LOG_SERVER_WORK_THREAD_H_
#define LOG_SERVER_WORK_THREAD_H_

#include "LogServerRequestData.h"

#include <util/concurrent_queue.h>
#include <util/singleton.h>

#include <boost/thread.hpp>

namespace sf1r
{

class LogServerWorkThread
{
public:
    LogServerWorkThread();

    ~LogServerWorkThread();

    void stop();

    bool idle();

private:
    void run();

private:
    boost::posix_time::ptime lastProcessTime_;
    boost::posix_time::ptime lastCheckedTime_;
    long monitorInterval_; // in seconds

    boost::thread workThread_;
    boost::thread monitorThread_;
};

}

#endif /* LOG_SERVER_WORK_THREAD_H_ */
