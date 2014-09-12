#include <mysql/mysql.h>
#include <string>
#include <stdint.h>
#include <iostream>
#include <map>
#include <list>
#include <vector>

#define IMPORT_SCD  1

#define host            "10.10.99.103"
#define port             3306
#define user            "b5m"
#define psw             "unimas"
#define db              "b5m3"
#define table           "pdc_usershare"

#define PRINT_MYSQL_ERROR(mysql, message)               \
    cout << "Error: " << mysql_errno(mysql)       \
               << " [" << mysql_error(mysql) << "] "    \
               << message;

using namespace std;

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
    if(IMPORT_SCD)
    {
        std::string sql="";
        sql += "select id, content, img, shareSourceName from ";
        sql += table;
        sql += " where length(content) > 10 and img <> 'NULL'";
        sql += " limit 32000000, 8000000;";
        if ( mysql_real_query(mysql, sql.c_str(), sql.length()) >0 && mysql_errno(mysql) )
        {
            PRINT_MYSQL_ERROR(mysql, "in mysql_real_query(): " << sql);
            return 0;
        }

        std::list< std::map<std::string, std::string> > results;
        fetchRows(mysql, results);


        std::list<std::map<std::string, std::string> >::iterator it;
        for(it=results.begin(); it!=results.end();it++)
        {
    	    cout <<"<DOCID>"<<(*it)["id"]<<endl;
            cout <<"<Content>"<<(*it)["content"]<<endl;
            cout <<"<Img>"<<(*it)["img"]<<endl;
            cout <<"<ShareSourceName>"<<(*it)["shareSourceName"]<<endl;
        }
    }

	return 0;
}
