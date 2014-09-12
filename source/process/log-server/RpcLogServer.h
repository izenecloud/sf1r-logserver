#ifndef RPC_LOG_SERVER_H_
#define RPC_LOG_SERVER_H_

#include "LogServerStorage.h"
#include "LogServerRequest.h"

#include <3rdparty/msgpack/rpc/server.h>
#include <boost/scoped_ptr.hpp>

namespace sf1r
{
class LogServerWorkThread;
class RpcLogServer : public msgpack::rpc::server::base
{
public:
    RpcLogServer(
        const std::string& host,
        uint16_t port,
        uint32_t threadNum);

    ~RpcLogServer();

    bool init();

    inline uint16_t getPort() const
    {
        return port_;
    }

    void start();

    void join();

    // start + join
    void run();

    void stop();

public:
    virtual void dispatch(msgpack::rpc::request req);

private:
    std::string host_;
    uint16_t port_;
    uint32_t threadNum_;

    boost::scoped_ptr<LogServerWorkThread> workerThread_;
};

}

#endif /* RPC_LOG_SERVER_H_ */
