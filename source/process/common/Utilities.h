/*
 *  @file    Utilities.h
 *  @author  Kuilong Liu
 *  @date    2013.05.03
 */

#ifndef SF1R_LOGSERVER_COMMON_UTILITIES_H
#define SF1R_LOGSREVER_COMMON_UTILITIES_H

#include <string>
#include <util/hashFunction.h>

namespace sf1r { namespace logserver{
class Utilities
{
public:
    Utilities(){}
    ~Utilities(){}
    static uint128_t md5ToUint128(const std::string& str)
    {
        if(str.length() != 32)
            return izenelib::util::HashFunction<std::string>::generateHash128(str);

        unsigned long long high = 0, low = 0;
        if(sscanf(str.c_str(), "%016llx%016llx", &high, &low) == 2)
            return (uint128_t) high << 64 | (uint128_t) low;
        else
            return izenelib::util::HashFunction<std::string>::generateHash128(str);
    }

    static std::string uint128ToMD5(const uint128_t& val)
    {
        static char tmpstr[33];
        sprintf(tmpstr, "%016llx%016llx", (unsigned long long) (val >> 64), (unsigned long long) val);
        return std::string(reinterpret_cast<const char *> (tmpstr), 32);
    }
};
}//end of namespace logserver
}//end of namespace sf1r

#endif
