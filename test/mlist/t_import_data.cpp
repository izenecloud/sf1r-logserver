#include <mysql/mysql.h>
#include <util/datastream/topk/fss/SpaceSaving.hpp>
#include <util/datastream/topk/cms/TopKCalculator.hpp>
#include <util/datastream/topk/cms/TopKItemEstimation.hpp>
#include <util/datastream/sketch/madoka/sketch.h>
#include <util/datastream/sketch/HyperLogLog.hpp>
#include <util/datastream/sketch/KMV.hpp>
#include <am/leveldb/Table.h>
#include <string>
#include <stdint.h>
#include <iostream>
#include <fstream>
#include <fstream>
#include <boost/lexical_cast.hpp>
#include <map>
#include <time.h>
#include <util/DynamicBloomFilter.h>
#include <log-server/LogAnalysisManager.h>
#include <log-server/StorageManager.cpp>
#include <log-server/LogServerCfg.cpp>

#define IMPORT_QUERY  0
#define IMPORT_LABEL  1

#define host            "10.10.99.138"
#define port             3306
#define user            "b5m"
#define psw             "iz3n3s0ft"
#define db              "SF1R"
#define query_table     "user_queries"
#define label_table     "property_labels"
#define path            "/home/lscm/codebase/sf1r-logserver/bin/log_server_storage/"
#define service         "user-query-analysis"

#define PRINT_MYSQL_ERROR(mysql, message)               \
    cout << "Error: " << mysql_errno(mysql)       \
               << " [" << mysql_error(mysql) << "] "    \
               << message;

using namespace std;
using namespace sf1r::logserver;



bool fetchRows(MYSQL* mysql, std::list< std::map<std::string, std::string> > & rows)
{
    bool success = true;
    int status = 0;
    while (status == 0)
    {
        MYSQL_RES* result = mysql_store_result(mysql);
        if (result)
        {
            uint32_t num_cols = mysql_num_fields(result);
            std::vector<std::string> col_nums(num_cols);
            for (uint32_t i=0;i<num_cols;i++)
            {
                MYSQL_FIELD* field = mysql_fetch_field_direct(result, i);
                col_nums[i] = field->name;
            }
            MYSQL_ROW row;
            while ((row = mysql_fetch_row(result)))
            {
                std::map<std::string, std::string> map;
                for (uint32_t i=0;i<num_cols;i++)
                {
                    map.insert(std::make_pair(col_nums[i], row[i]) );
                }
                rows.push_back(map);
            }
            mysql_free_result(result);
        }
        else
        {
            if (mysql_field_count(mysql))
            {
                PRINT_MYSQL_ERROR(mysql, "in mysql_field_count()");
                success = false;
                break;
            }
        }

        // more results? -1 = no, >0 = error, 0 = yes (keep looping)
        if ((status = mysql_next_result(mysql)) > 0)
        {
            PRINT_MYSQL_ERROR(mysql, "in mysql_next_result()");
            success = false;
            break;
        }
    }

    return success;
}



int main()
{
	MYSQL* mysql = mysql_init(NULL);
	if(!mysql)
	{
		cout << "mysql_init failed"<<endl;
		return 0;
	}

	mysql_options(mysql, MYSQL_SET_CHARSET_NAME, "utf8");
	my_bool reconnect = 1;
	if (mysql_options(mysql, MYSQL_OPT_RECONNECT, &reconnect))
    {
        PRINT_MYSQL_ERROR(mysql, "in mysql_options()");
        mysql_close(mysql);
        return 0;
    }

    if (mysql_options(mysql, MYSQL_INIT_COMMAND, "SET NAMES utf8"))
    {
        PRINT_MYSQL_ERROR(mysql, "in mysql_options()");
        mysql_close(mysql);
        return 0;
    }

    const int flags = CLIENT_MULTI_STATEMENTS;

    if (!mysql_real_connect(mysql, host,
                            user, psw,
                            db, port, NULL, flags))
    {
        PRINT_MYSQL_ERROR(mysql, "in mysql_real_connect()");
        mysql_close(mysql);
        return 0;
    }
    if(IMPORT_QUERY)
    {
        std::string sql="";
        sql += "select * from ";
        sql += query_table;
        sql += " where TimeStamp>='20130516T000000' and hit_docs_num > 0;";
        if ( mysql_real_query(mysql, sql.c_str(), sql.length()) >0 && mysql_errno(mysql) )
        {
            PRINT_MYSQL_ERROR(mysql, "in mysql_real_query(): " << sql);
            return 0;
        }

        std::list< std::map<std::string, std::string> > results;
        fetchRows(mysql, results);


        LogAnalysisManager lam(path);


        std::list<std::map<std::string, std::string> >::iterator it;
        for(it=results.begin(); it!=results.end();it++)
        {
 /*   	    std::map<std::string, std::string>::iterator iter;
    	    for(iter = it->begin(); iter!= it->end(); iter++)
    		    cout <<"["<<iter->first<<", "<<iter->second<<"]";
    	    cout<<endl;
*/
            lam.insert(service, (*it)["collection"], (*it)["TimeStamp"], (*it)["query"], *it);
        }
        lam.close();
    }
    if(IMPORT_LABEL)
    {
        std::string sql="";
        sql+= "select * from ";
        sql+= label_table;
        if(mysql_real_query(mysql, sql.c_str(), sql.length()) >0 && mysql_error(mysql) )
        {
            PRINT_MYSQL_ERROR(mysql, "in mysql_real_query(): " << sql);
            return 0;
        }

        std::list< std::map<std::string, std::string> > results;
        fetchRows(mysql, results);

        LogAnalysisManager lam(path);
        std::list< std::map<std::string, std::string> >::iterator it;
        for(it=results.begin(); it!=results.end();it++)
        {
            lam.insertPropertyLabel((*it)["collection"], (*it)["label_name"], boost::lexical_cast<uint64_t>((*it)["hit_docs_num"]));
        }
        lam.close();
    }

	return 0;
}
