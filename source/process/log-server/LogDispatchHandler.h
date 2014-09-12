#ifndef LOG_DISPATCH_HANDLER_H_
#define LOG_DISPATCH_HANDLER_H_

#include "LogServerStorage.h"

#include <util/driver/Controller.h>
#include <util/driver/writers/JsonWriter.h>
#include <net/sf1r/Sf1Driver.hpp>

using namespace izenelib::net::sf1r;
using namespace izenelib::driver;

namespace sf1r
{

/***********************************************
*@brief LogDispatchHandler is used to send specified requests
*  to SF1, to recover user behavior data from logs
************************************************/
class LogDispatchHandler
{
public:
    void setRequestContext(
        Request& request,
        Response& response)
    {
        request_ = &request;
        response_ = &response;
    }

    Request& request()
    {
        return *request_;
    }

    const Request& request() const
    {
        return *request_;
    }

    Response& response()
    {
        return *response_;
    }

    const Response& response() const
    {
        return *response_;
    }

public:
    void Init();

    void Flush();

    void InitSf1DriverClient_(const std::string& host, uint32_t port);

    void WriteFile_(const std::string& fileName, const std::string& line);

private:
    Request* request_;
    Response* response_;
    izenelib::driver::JsonWriter jsonWriter_;

    std::set<std::string> driverCollections_;
    std::string storageBaseDir_;

    boost::shared_ptr<Sf1Driver> sf1DriverClient_;
    std::string sf1Host_;
    uint32_t sf1Port_;
};

}

#endif

