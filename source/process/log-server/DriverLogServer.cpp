#include "DriverLogServer.h"
#include "DriverLogServerController.h"
#include "LogDispatchHandler.h"
#include "LogServerCfg.h"
#include "LogServerStorage.h"

#include <util/driver/DriverConnectionFirewall.h>

using namespace izenelib::driver;

namespace sf1r
{

DriverLogServer::DriverLogServer(uint16_t port, uint32_t threadNum)
    : port_(port)
    , threadNum_(threadNum)
    , bStarted_(false)
{
}

DriverLogServer::~DriverLogServer()
{
    std::cout << "~DriverLogServer()" << std::endl;
    stop();
}

bool DriverLogServer::init()
{
    boost::asio::ip::tcp::endpoint endpoint(boost::asio::ip::tcp::v4(), port_);
    router_.reset(new ::izenelib::driver::Router);

    if (router_ && initRouter())
    {
        boost::shared_ptr<DriverConnectionFactory> factory(new DriverConnectionFactory(router_));
        factory->setFirewall(DriverConnectionFirewall());

        driverServer_.reset(new DriverServer(endpoint, factory, threadNum_));

        return driverServer_;
    }

    return false;
}

void DriverLogServer::start()
{
    if (!bStarted_)
    {
        bStarted_ = true;
        driverThread_.reset(new boost::thread(&DriverServer::run, driverServer_.get()));
    }
}

void DriverLogServer::join()
{
    driverThread_->join();
}

void DriverLogServer::stop()
{
    driverServer_->stop();
    driverThread_->interrupt();
    bStarted_ = false;
}

bool DriverLogServer::initRouter()
{
    typedef ::izenelib::driver::ActionHandler<DriverLogServerController> handler_t;

    boost::shared_ptr<LogDispatchHandler> logServerhandler(new LogDispatchHandler);
    logServerhandler->Init();
    DriverLogServerController logServerCtrl(logServerhandler);


    return true;
}

}
