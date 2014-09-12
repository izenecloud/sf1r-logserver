#ifndef _LOG_SERVER_REQUEST_H_
#define _LOG_SERVER_REQUEST_H_

#include "LogServerRequestData.h"

namespace sf1r
{

class LogServerRequest
{
public:
    typedef std::string method_t;

    /// add method here
    enum METHOD
    {
        METHOD_TEST = 0,

        METHOD_INSERT,
        METHOD_INSERT_WITH_VALUES,
        METHOD_GET_CURRENT_TOPK,
        METHOD_GET_TOPK,
        METHOD_GET_CURRENT_DVC,
        METHOD_GET_DVC,
        METHOD_GET_VALUES,
        METHOD_GET_VALUE_AND_COUNT,
        METHOD_GET_ALL_COLLECTION,
        METHOD_INSERT_PROP_LABEL,
        METHOD_GET_PROP_LABEL,
        METHOD_DEL_PROP_LABEL,
        
        COUNT_OF_METHODS
    };

    static const method_t method_names[COUNT_OF_METHODS];

    METHOD method_;

public:
    LogServerRequest(const METHOD& method) : method_(method) {}
    virtual ~LogServerRequest() {}
};

template <typename RequestDataT>
class LogRequestRequestT : public LogServerRequest
{
public:
    LogRequestRequestT(METHOD method)
        : LogServerRequest(method)
    {
    }

    RequestDataT param_;

    MSGPACK_DEFINE(param_)
};

class InsertRequest: public LogRequestRequestT<InsertData>
{
public:
    InsertRequest()
        : LogRequestRequestT<InsertData>(METHOD_INSERT)
    {
    }
};

class InsertWithValuesDataRequest: public LogRequestRequestT<InsertWithValuesData>
{
public:
    InsertWithValuesDataRequest()
        : LogRequestRequestT<InsertWithValuesData>(METHOD_INSERT_WITH_VALUES)
    {
    }
};

class GetCurrentTopKRequest: public LogRequestRequestT<GetCurrentTopKData>
{
public:
    GetCurrentTopKRequest()
        : LogRequestRequestT<GetCurrentTopKData>(METHOD_GET_CURRENT_TOPK)
    {
    }
};

class GetTopKRequest: public LogRequestRequestT<GetTopKData>
{
public:
    GetTopKRequest()
        : LogRequestRequestT<GetTopKData>(METHOD_GET_TOPK)
    {
    }
};


class GetCurrentDVCRequest: public LogRequestRequestT<GetCurrentDVCData>
{
public:
    GetCurrentDVCRequest()
        : LogRequestRequestT<GetCurrentDVCData>(METHOD_GET_CURRENT_DVC)
    {
    }
};



class GetDVCRequest: public LogRequestRequestT<GetDVCData>
{
public:
    GetDVCRequest()
        : LogRequestRequestT<GetDVCData>(METHOD_GET_DVC)
    {
    }
};

class GetValueRequest: public LogRequestRequestT<GetValueData>
{
public:
    GetValueRequest()
        : LogRequestRequestT<GetValueData>(METHOD_GET_VALUES)
    {
    }
};

class GetValueAndCountRequest: public LogRequestRequestT<GetValueAndCountData>
{
public:
    GetValueAndCountRequest()
        : LogRequestRequestT<GetValueAndCountData>(METHOD_GET_VALUE_AND_COUNT)
    {
    }
};

class GetAllCollectionRequest: public LogRequestRequestT<GetAllCollectionData>
{
public:
    GetAllCollectionRequest()
        : LogRequestRequestT<GetAllCollectionData>(METHOD_GET_ALL_COLLECTION)
    {
    }
};
class InsertPropLabelRequest: public LogRequestRequestT<InsertPropLabelData>
{
public:
    InsertPropLabelRequest()
        : LogRequestRequestT<InsertPropLabelData>(METHOD_INSERT_PROP_LABEL)
    {
    }
};
class GetPropLabelRequest: public LogRequestRequestT<GetPropLabelData>
{
public:
    GetPropLabelRequest()
        : LogRequestRequestT<GetPropLabelData>(METHOD_GET_PROP_LABEL)
    {
    }
};
class DelPropLabelRequest: public LogRequestRequestT<DelPropLabelData>
{
public:
    DelPropLabelRequest()
        : LogRequestRequestT<DelPropLabelData>(METHOD_DEL_PROP_LABEL)
    {
    }
};
}

#endif /* _LOG_SERVER_REQUEST_H_ */
