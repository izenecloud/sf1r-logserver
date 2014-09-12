#include "LogServerWorkThread.h"
#include "LogServerStorage.h"
#include <ctime>

#include <glog/logging.h>

namespace sf1r
{

LogServerWorkThread::LogServerWorkThread()
    : monitorInterval_(LogServerCfg::get()->getFlushCheckInterval())
    , workThread_(&LogServerWorkThread::run, this)
{
}

LogServerWorkThread::~LogServerWorkThread()
{
    stop();
}

void LogServerWorkThread::stop()
{
    workThread_.interrupt();
    workThread_.join();

    monitorThread_.interrupt();
    monitorThread_.join();
}

void LogServerWorkThread::run()
{
}

}
