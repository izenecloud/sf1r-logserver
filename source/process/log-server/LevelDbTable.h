/*
*    @file  LevelDbTable.h
*    @author Kuilong Liu
*    @date 2013.04.02
*/

#ifndef _LEVELDB_TABLE_HPP_
#define _LEVELDB_TABLE_HPP_

#include <types.h>
#include <stdio.h>
#include <string>
#include <map>
#include <am/leveldb/Table.h>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>

using namespace izenelib::am;
using namespace izenelib::am::leveldb;
using namespace std;

namespace sf1r
{
/*
*    @class UserQueryDbTable
*/
    class UserQueryDbTable
    {
    public:
        typedef Table<std::string, std::string> UserQueryDbType;
        typedef Table<std::string, std::string>::cursor_type cur_type;
        UserQueryDbTable(const std::string& path)
            :path_(path)
            ,userQueryDbTable(NULL)
        {
            is_open_ = false;
            userQueryDbTable = new UserQueryDbType(path_);
        }

        ~UserQueryDbTable()
        {
            if(userQueryDbTable != NULL)
            {
                delete userQueryDbTable;
                userQueryDbTable = NULL;
            }
        }

        /*
         * @brief open db table
         */
        bool open()
        {
            //LOG(INFO) << "leveldb: " << path_ << endl;
            if(userQueryDbTable == NULL)
                userQueryDbTable = new UserQueryDbType(path_);
            if(is_open_) return true;
            if(userQueryDbTable->open())
            {
                is_open_ = true;
                return true;
            }
            return false;
        }

        void close()
        {
            if(userQueryDbTable != NULL)
            {
                if(is_open_)
                    userQueryDbTable->close();
                is_open_ = false;
                delete userQueryDbTable;
                userQueryDbTable = NULL;
            }
        }

        void clear()
        {
            userQueryDbTable->clear();
        }
        void set_path(const std::string path)
        {
            path_ = path;
        }

        /*
         * @brief insert a k-v into db
         * @param key      (std::string)
         * @param value    (std::string)
         *        value:  page_start:0;page_count:2;duration:000001;timestamp:20130201T153000;
         */
        bool add_item(const std::string& key, const std::string& value)
        {
            return userQueryDbTable->insert(key, value);
        }

        /*
         * @brief insert a k-v into db
         * @param key      (std::string)
         * @param mapvalue (std::map)
         * @      mapvalue is packed into a string value firstly
         */
        bool add_item(const std::string& key,
                const std::map<std::string, std::string>& mapvalue)
        {
            std::string value;
            if(!packValue_(mapvalue, value)) return false;
            //LOG(INFO) << "Value: " << value << endl;
            return userQueryDbTable->insert(key, value);
        }

        bool get_item(const std::string& key, std::string& value)
        {
            return userQueryDbTable->get(key,value);
        }

        bool get_item(const std::string& key,
                std::map<std::string, std::string>& mapvalue)
        {
            std::string value;
            if(!userQueryDbTable->get(key, value))
            {
                LOG(INFO) << "UserQueryDbTable: get false!  key:" << key << endl;
                return false;
            }
            if(!parseValue_(value, mapvalue))
                return false;
            return true;
        }
        bool get_item(const std::string& key,
                const std::string& attr,
                std::string & attrvalue)
        {
            std::string value;
            std::map<std::string, std::string> mapvalue;

            if(!userQueryDbTable->get(key, value))
            {
                LOG(INFO) << "UserQueryDbTable: get false!  key:" << key << endl;
                return false;
            }
            if(!parseValue_(value, mapvalue))
                return false;
            if(mapvalue.find(attr) == mapvalue.end())
            {
                LOG(INFO) << "UserQueryDbTable: get false [attr does not exist]! key:" << key <<" attr: " << attr << endl;
                return false;
            }
            attrvalue = mapvalue[attr];
            return true;
        }

        bool delete_item(const std::string& key)
        {
            return userQueryDbTable->del(key);
        }

        cur_type begin()
        {
            return userQueryDbTable->begin();
        }

        bool iterNext(cur_type& cursor)
        {
            return 

            userQueryDbTable->iterNext(cursor);
        }

        bool iterPrev(cur_type& cursor)
        {
            return userQueryDbTable->iterPrev(cursor);
        }

        bool fetch(cur_type& cursor, std::string& key, std::string& value)
        {
            return userQueryDbTable->fetch(cursor, key, value);
        }

        bool fetch(cur_type& cursor,
                std::string& key,
                std::map<std::string, std::string>& mapvalue)
        {
            std::string value;
            if(!fetch(cursor, key, value))
            {
                LOG(INFO) << "UserQueryDbTable: fetch error! "<< path_<< endl;
                return false;
            }
            if(!parseValue_(value, mapvalue))
                return false;
            return true;
        }

    private:
        bool packValue_(const std::map<std::string, std::string>& mapvalue,
                std::string& value)
        {
            value = "";
            std::map<std::string, std::string>::const_iterator iter;
            for(iter=mapvalue.begin();iter!=mapvalue.end();iter++)
            {
                /*
                if(iter->first.find(",---,")!=std::string::npos
                        || iter->first.find("/---/")!=std::string::npos
                        || iter->second.find(",---,")!=std::string::npos
                        || iter->second.find("/---/")!=std::string::npos)
                {
                    LOG(INFO) << "UserQueryDbTable: packValue error! attr: " << iter->first
                              << "value: "<<iter->second << endl;
                    return false;
                }
                */
                value += iter->first;
                value += "#";
                value += iter->second;
                value += "$";
            }
            value.erase(value.end()-1);
            return true;
        }
        bool parseValue_(const std::string& value,
                std::map<std::string, std::string>& mapvalue)
        {
            std::vector<string> strs;
            boost::split(strs, value, boost::is_any_of("$"));
            std::vector<string>::iterator iter;
            for(iter=strs.begin();iter!=strs.end();iter++)
            {
                std::vector<string> temp;
                boost::split(temp, *iter, boost::is_any_of("#"));
                if(temp.size() != 2)
                {
                    if(temp.size() == 1)
                    {
                        LOG(ERROR) << "UserQueryDbTable parseValue error! value: " << *iter << endl;
                        return false;
                    }
                    else
                    {
                        std::string property = iter->substr(0, iter->find_first_of('#'));
                        std::string value = iter->substr(iter->find_first_of('#')+1);
                        mapvalue[property]=value;
                    }
                }
                else
                {
                    mapvalue[temp[0]] = temp[1];
                }
            }
            return true;
        }

        std::string path_;
        UserQueryDbType *userQueryDbTable;
        bool is_open_;
    };

}
#endif
