#include "RpcLogServer.h"
#include "LogServerWorkThread.h"
#include <string>
#include <list>
#include "errno.h"

#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>


namespace sf1r
{

RpcLogServer::RpcLogServer(const std::string& host, uint16_t port, uint32_t threadNum)
    : host_(host)
    , port_(port)
    , threadNum_(threadNum)
{
}

RpcLogServer::~RpcLogServer()
{
    std::cout << "~RpcLogServer()" << std::endl;
    stop();
}

bool RpcLogServer::init()
{
    workerThread_.reset(new LogServerWorkThread());

    return true;
}

void RpcLogServer::start()
{
    instance.listen(host_, port_);
    instance.start(threadNum_);
}

void RpcLogServer::join()
{
    instance.join();
}

void RpcLogServer::run()
{
    start();
    join();
}

void RpcLogServer::stop()
{
    instance.end();
    instance.join();
    workerThread_->stop();
}

void RpcLogServer::dispatch(msgpack::rpc::request req)
{
    try
    {
        std::string method;
        req.method().convert(&method);
        
        if (method == LogServerRequest::method_names[LogServerRequest::METHOD_TEST])
        {
            msgpack::type::tuple<bool> params;
            req.params().convert(&params);

            req.result(true);
        }
        else if(method == LogServerRequest::method_names[LogServerRequest::METHOD_INSERT])
        {
            msgpack::type::tuple<InsertData> params;
            req.params().convert(&params);
            InsertData reqdata = params.get<0>();
            req.result(true);
            bool response;
            response = LogServerStorage::get()->logAnalysisManager()->insert(reqdata.service_,
                    reqdata.collection_,
                    reqdata.key_);
            if (!response)
            {
                LOG(ERROR)<<"METHOD_INSERT ERROR! service: "<<reqdata.service_<<" collection: "<<reqdata.collection_<<" key: " <<reqdata.key_<<endl;
            }
        }
        else if(method == LogServerRequest::method_names[LogServerRequest::METHOD_INSERT_WITH_VALUES])
        {
            msgpack::type::tuple<InsertWithValuesData> params;
            req.params().convert(&params);
            InsertWithValuesData reqdata = params.get<0>();
            req.result(true);
            bool response;
            response = LogServerStorage::get()->logAnalysisManager()->insert(reqdata.service_,
                    reqdata.collection_,
                    reqdata.key_,
                    reqdata.values_);
            if(!response)
            {
                LOG(ERROR)<<"METHOD_INSERT_WITH_VALUES service: "<<reqdata.service_<<" collection: "<<reqdata.collection_<<" key: " <<reqdata.key_<<endl;
            }
        }
        else if(method == LogServerRequest::method_names[LogServerRequest::METHOD_GET_CURRENT_TOPK])
        {
            msgpack::type::tuple<GetCurrentTopKData> params;
            req.params().convert(&params);
            GetCurrentTopKData reqdata = params.get<0>();
            std::list<std::pair<std::string, uint32_t> > response;
            LogServerStorage::get()->logAnalysisManager()->getTopK(reqdata.service_,
                    reqdata.collection_,
                    reqdata.limit_,
                    response);
            req.result(response);
        }
        else if(method == LogServerRequest::method_names[LogServerRequest::METHOD_GET_TOPK])
        {
            msgpack::type::tuple<GetTopKData> params;
            req.params().convert(&params);
            GetTopKData reqdata = params.get<0>();
            std::list<std::pair<std::string, uint32_t> > response;
            LogServerStorage::get()->logAnalysisManager()->getTopK(reqdata.service_,
                    reqdata.collection_,
                    reqdata.begin_time_,
                    reqdata.end_time_,
                    reqdata.limit_,
                    response);
            req.result(response);
        }
        else if(method == LogServerRequest::method_names[LogServerRequest::METHOD_GET_CURRENT_DVC])
        {
            msgpack::type::tuple<GetCurrentDVCData> params;
            req.params().convert(&params);
            GetCurrentDVCData reqdata = params.get<0>();
            uint64_t response=0;
            LogServerStorage::get()->logAnalysisManager()->getDVC(reqdata.service_,
                    reqdata.collection_,
                    response);
            req.result(response);
        }
        else if(method == LogServerRequest::method_names[LogServerRequest::METHOD_GET_DVC])
        {
            msgpack::type::tuple<GetDVCData> params;
            req.params().convert(&params);
            GetDVCData reqdata = params.get<0>();
            uint64_t response=0;
            LogServerStorage::get()->logAnalysisManager()->getDVC(reqdata.service_,
                    reqdata.collection_,
                    reqdata.begin_time_,
                    reqdata.end_time_,
                    response);
            req.result(response);
        }
        else if(method == LogServerRequest::method_names[LogServerRequest::METHOD_GET_VALUES])
        {
            msgpack::type::tuple<GetValueData> params;
            req.params().convert(&params);
            GetValueData reqdata = params.get<0>();
            std::list<std::map<std::string, std::string> > response;
            LogServerStorage::get()->logAnalysisManager()->getValue(reqdata.service_,
                    reqdata.collection_,
                    reqdata.time_,
                    reqdata.limit_,
                    response);
            req.result(response);
        }
        else if(method == LogServerRequest::method_names[LogServerRequest::METHOD_GET_VALUE_AND_COUNT])
        {
            msgpack::type::tuple<GetValueAndCountData> params;
            req.params().convert(&params);
            GetValueAndCountData reqdata = params.get<0>();
            std::list<std::map<std::string, std::string> > response;
            LogServerStorage::get()->logAnalysisManager()->getValueAndCount(reqdata.service_,
                    reqdata.collection_,
                    reqdata.begin_time_,
                    response);
            req.result(response);
        }
        else if(method == LogServerRequest::method_names[LogServerRequest::METHOD_GET_ALL_COLLECTION])
        {
            msgpack::type::tuple<GetAllCollectionData> params;
            req.params().convert(&params);
            GetAllCollectionData reqdata = params.get<0>();
            std::list<std::map<std::string, std::string> > response;
            LogServerStorage::get()->logAnalysisManager()->getAllCollectionData(reqdata.service_,
                    reqdata.begin_time_,
                    response);
            req.result(response);
        }
        else if(method == LogServerRequest::method_names[LogServerRequest::METHOD_INSERT_PROP_LABEL])
        {
            msgpack::type::tuple<InsertPropLabelData> params;
            req.params().convert(&params);
            InsertPropLabelData reqdata = params.get<0>();
            req.result(true);
            bool response;
            response = LogServerStorage::get()->logAnalysisManager()->insertPropertyLabel(reqdata.collection_,
                    reqdata.label_name_,
                    reqdata.hitnum_);
            if(!response)
            {
                LOG(ERROR)<<"insertPropertyLabel error!"<<endl;
            }
        }
        else if(method == LogServerRequest::method_names[LogServerRequest::METHOD_GET_PROP_LABEL])
        {
            msgpack::type::tuple<GetPropLabelData> params;
            req.params().convert(&params);
            GetPropLabelData reqdata = params.get<0>();
            std::list<std::map<std::string, std::string> > response;
            LogServerStorage::get()->logAnalysisManager()->getPropertyLabel(reqdata.collection_,
                    response);
            req.result(response);
        }
        else if(method == LogServerRequest::method_names[LogServerRequest::METHOD_DEL_PROP_LABEL])
        {
            msgpack::type::tuple<DelPropLabelData> params;
            req.params().convert(&params);
            DelPropLabelData reqdata = params.get<0>();
            req.result(true);
            bool response = LogServerStorage::get()->logAnalysisManager()->delPropertyLabel(reqdata.collection_);
            if(!response)
            {
                LOG(ERROR)<<"delPropertyLabel error!"<<endl;
            }
        }
        else
        {
            req.error(msgpack::rpc::NO_METHOD_ERROR);
        }
    }
    catch (const msgpack::type_error& e)
    {
        req.error(msgpack::rpc::ARGUMENT_ERROR);
    }
    catch (const std::exception& e)
    {
        req.error(std::string(e.what()));
    }
    catch (...)
    {
        LOG(INFO)<<"Rpc error"<<endl;
    }
}
}
