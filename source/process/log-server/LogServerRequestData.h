#ifndef LOG_SERVER_REQUEST_DATA_H_
#define LOG_SERVER_REQUEST_DATA_H_

#include <3rdparty/msgpack/msgpack.hpp>

#include <string>
#include <vector>
#include <map>

namespace sf1r
{

inline std::ostream& operator<<(std::ostream& os, uint128_t uint128)
{
    os << hex << uint64_t(uint128>>64) << uint64_t(uint128) << dec;
    return os;
}

struct LogServerRequestData
{
};

struct InsertData: public LogServerRequestData
{
    std::string service_;
    std::string collection_;
    std::string key_;

    MSGPACK_DEFINE(service_, collection_, key_);
};

struct InsertWithValuesData: public LogServerRequestData
{
    std::string service_;
    std::string collection_;
    std::string key_;
    std::map<std::string, std::string> values_;

    MSGPACK_DEFINE(service_, collection_, key_, values_);
};

struct GetCurrentTopKData: public LogServerRequestData
{
    std::string service_;
    std::string collection_;
    uint32_t limit_;

    MSGPACK_DEFINE(service_, collection_, limit_);
};

struct GetTopKData: public LogServerRequestData
{
    std::string service_;
    std::string collection_;
    std::string begin_time_;
    std::string end_time_;
    uint32_t limit_;

    MSGPACK_DEFINE(service_, collection_, begin_time_, end_time_, limit_);
};

struct GetCurrentDVCData: public LogServerRequestData
{
    std::string service_;
    std::string collection_;

    MSGPACK_DEFINE(service_, collection_);
};

struct GetDVCData: public LogServerRequestData
{
    std::string service_;
    std::string collection_;
    std::string begin_time_;
    std::string end_time_;

    MSGPACK_DEFINE(service_, collection_, begin_time_, end_time_);
};

struct GetValueData: public LogServerRequestData
{
    std::string service_;
    std::string collection_;
    std::string time_;
    uint32_t limit_;

    MSGPACK_DEFINE(service_, collection_, time_, limit_);
};

struct GetValueAndCountData: public LogServerRequestData
{
    std::string service_;
    std::string collection_;
    std::string begin_time_;

    MSGPACK_DEFINE(service_, collection_, begin_time_);
};

struct GetAllCollectionData: public LogServerRequestData
{
    std::string service_;
    std::string begin_time_;

    MSGPACK_DEFINE(service_, begin_time_);
};

struct InsertPropLabelData: public LogServerRequestData
{
    std::string collection_;
    std::string label_name_;
    uint64_t hitnum_;

    MSGPACK_DEFINE(collection_, label_name_, hitnum_);
};

struct GetPropLabelData: public LogServerRequestData
{
    std::string collection_;

    MSGPACK_DEFINE(collection_);
};

struct DelPropLabelData: public LogServerRequestData
{
    std::string collection_;

    MSGPACK_DEFINE(collection_);
};

}

#endif /* LOG_SERVER_REQUEST_DATA_H_ */
