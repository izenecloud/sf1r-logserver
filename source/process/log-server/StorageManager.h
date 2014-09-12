/*
 *    @file  StorageManager.h
 *    @author Kuilong Liu
 *    @date  2013.04.23
 *
 *          Now the slide length of DVStorageManager is 24 flexiable.
 */

#ifndef _LEVELDB_MANAGER_HPP_
#define _LEVELDB_MANAGER_HPP_

#include <string>
#include <fstream>
#include <am/leveldb/Table.h>
#include <util/DynamicBloomFilter.h>
#include <util/datastream/sketch/HyperLogLog.hpp>
#include <util/datastream/sketch/madoka/sketch.h>
#include <boost/unordered_map.hpp>
#include <boost/filesystem.hpp>
#include <boost/signals2/mutex.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>

#include "LevelDbTable.h"
#include "PropertyCacheItem.h"

#define DBF_SIZE     1000000
#define DBF_FPP      0.0001

using namespace std;
using namespace boost::posix_time;
using namespace boost::gregorian;
using namespace boost;

namespace sf1r { namespace logserver{
/*
 *    @class DVStorageManager
 *    @      distinct values
 */
    class DVStorageManager
    {
    public:
        typedef std::string StrType;
        typedef std::map<StrType, StrType> ValType;
        typedef boost::unordered_map<std::string, UserQueryDbTable*> CollDbMapT;
        typedef boost::unordered_map<std::string, CollDbMapT> ServDbMapT;

        typedef izenelib::util::DynamicBloomFilter<StrType> DBFT;
        typedef boost::unordered_map<std::string, DBFT*> CollBFMapT;
        typedef boost::unordered_map<std::string, CollBFMapT> SerBFMapT;

        typedef boost::unordered_map<std::string, boost::mutex*> CollMutexMapT;
        typedef boost::unordered_map<std::string, CollMutexMapT> ServMutexMapT;

        DVStorageManager(string wd)
        {
            workdir_ = wd;
        }
        ~DVStorageManager()
        {
            LOG(INFO)<<"~DVStorageManager"<<endl;
            ServDbMapT::iterator it;
            CollDbMapT::iterator iter;

            SerBFMapT::iterator fit = fgps_.begin();
            ServMutexMapT::iterator mit = mgps_.begin();
            CollBFMapT::iterator fiter;
            CollMutexMapT::iterator miter;
            for(it = gps_.begin(); it!=gps_.end();it++, fit++, mit++)
            {
                fiter=(fit->second).begin();
                miter=(mit->second).begin();
                for(iter = (it->second).begin(); iter!=(it->second).end();iter++, fiter++, miter++)
                {
                    if(iter->second != NULL)
                    {
                        iter->second->close();
                        delete iter->second;
                        delete fiter->second;
                        delete miter->second;
                    }
                }
                (it->second).clear();
                (fit->second).clear();
                (mit->second).clear();
            }
            gps_.clear();
            fgps_.clear();
            mgps_.clear();
        }

        void reset(const std::string& s, const std::string c, const std::string& t)
        {
            std::string path = workdir_+"/"+s+"/"+c+"/";
            if(boost::filesystem::exists(path+"temp.db"))
            {
                if(boost::filesystem::exists(path+t+".db"))
                {
                    LOG(INFO)<<"LevelDb exists!"<<path<<t<<".db"<<endl;
                }
                else
                {
                    boost::filesystem::rename(path+"temp.db", path+t+".db");
                    delete gps_[s][c];
                    gps_[s][c]=NULL;
                    delete fgps_[s][c];
                    fgps_[s][c]=NULL;
                    delete mgps_[s][c];
                    mgps_[s][c]=NULL;
                }
            }
        }

        bool insert(const std::string& s, const std::string& c, const std::string& key,
                const std::map<std::string, std::string>& value);
        bool insert(const std::string& s, const std::string& c, const std::string& key,
                const std::string& value);
        bool get(const std::string& s, const std::string& c, const std::string& key,
                std::map<std::string, std::string>& value);
        bool get(const std::string& s, const std::string& c, const std::string& key,
                std::string& value);
        bool get(const std::string& s, const std::string& c, uint32_t limit,
                std::list<std::pair<std::string, std::string> >& values);
        bool get(const std::string& s, const std::string& c, uint32_t limit,
                std::list<std::map<std::string, std::string> >& values);
        bool get(const std::string& s, const std::string& c,
                std::list<std::map<std::string, std::string> >& values);
        bool get(const std::string& s, const std::string& c, const std::string& t,
                uint32_t limit,
                std::list<std::pair<std::string, std::string> >& values);
        bool get(const std::string& s, const std::string& c, const std::string& t,
                uint32_t limit,
                std::list<std::map<std::string, std::string> >& values);
        bool get(const std::string& s, const std::string& c, const std::string& t,
                std::list<std::map<std::string, std::string> >& values);
    private:
        bool getCheckLevelDb(const std::string& s, const std::string& c);
        bool insertCheckLevelDb(const std::string& s, const std::string& c);
        //leveldb+dv+service+collection+time.db
        string workdir_;
        ServDbMapT gps_;
        SerBFMapT fgps_;
        ServMutexMapT mgps_;
    };

/*
 *     @class  TopKStorageManager
 *     @       frequent items
 */
    class TopKStorageManager
    {
    public:
        typedef Table<std::string, uint32_t> TopKDbT;
        typedef TopKDbT::cursor_type CurType;
        typedef boost::unordered_map<std::string, TopKDbT*> CollDbMapT;
        typedef boost::unordered_map<std::string, CollDbMapT> ServDbMapT;
        TopKStorageManager(string wd)
        {
            workdir_ = wd;
        }
        ~TopKStorageManager()
        {
            LOG(INFO)<<"~TopKStorageManager"<<endl;
            ServDbMapT::iterator it;
            CollDbMapT::iterator iter;
            for(it=gps_.begin();it!=gps_.end();it++)
            {
                for(iter=(it->second).begin();iter!=(it->second).end();iter++)
                    delete iter->second;
            }
        }

        bool insert(const std::string& s, const std::string& c, const std::string& t,
                const std::list<std::pair<std::string, uint32_t> >& topk);

        bool get(const std::string& s, const std::string& c, const std::string& t,
                std::list<std::pair<std::string, uint32_t> >& topk);

    private:
        bool getCheckLevelDb(const std::string& s, const std::string& c);
        bool insertCheckLevelDb(const std::string& s, const std::string& c);

        //leveldb+topk+service+collection+time.db
        string workdir_;
        ServDbMapT gps_;
   };

/*
 *     @class HllStorageManager
 */
    class HllStorageManager
    {
    public:
        typedef HyperLL<uint128_t> HyperLLT;
        HllStorageManager(string wd)
            :workdir_(wd)
        {
        }
        ~HllStorageManager()
        {
            LOG(INFO)<<"~HllStorageManager"<<endl;
        }

        bool insert(const std::string& s, const std::string& c, const std::string& t,
                HyperLLT* hll)
        {
            std::string path = workdir_ + "/"+s+"/"+c+"/";
            boost::filesystem::create_directories(path);
            path += t + ".hll";
            if(boost::filesystem::exists(path))
            {
                LOG(INFO) <<"HLL File exists: "<<path<<endl;
                return true;
            }
            ofstream fout(path.c_str());
            hll->save(fout);
            return true;
        }

        bool get(const std::string& s, const std::string& c, const std::string& t,
                HyperLLT* hll)
        {
            std::string path = workdir_ + "/" +s+"/"+c+"/"+t+".hll";
            if(!boost::filesystem::exists(path))
                return false;
            ifstream fin(path.c_str());
            hll->load(fin);
            return true;
        }
    private:
        //hll+service+collection+time.hll
        string workdir_;
    };

/*
 *    @class CMSStorageManager
 *    store count min sketch for each day
 */
    class CMSStorageManager
    {
    public:
        CMSStorageManager(string wd)
            :workdir_(wd)
        {
        }
        ~CMSStorageManager()
        {
            LOG(INFO)<<"~CMSStorageManager"<<endl;
        }

        bool insert(const std::string& s, const std::string& c, const std::string& t,
                madoka::Sketch* sketch)
        {
            std::string path = workdir_ + "/"+s+"/"+c+"/";
            boost::filesystem::create_directories(path);
            path +=t + ".cms";
            if(boost::filesystem::exists(path))
            {
                LOG(INFO) << "CMS File exists: "<<path<<endl;
                return true;
            }
            sketch->save(path.c_str());
            return true;
        }

        bool get(const std::string& s, const std::string& c, const std::string& t,
                madoka::Sketch* sketch)
        {
            std::string path = workdir_ +"/"+s+"/"+c+"/"+t+".cms";
            if(!boost::filesystem::exists(path))
                return false;
            sketch->load(path.c_str());
            return true;
        }

        bool get(const std::string& s, const std::string& c, const std::string& t,
                madoka::Sketch& sketch)
        {
            std::string path = workdir_+"/"+s+"/"+c+"/"+t+".cms";
            if(!boost::filesystem::exists(path))
                return false;
            sketch.load(path.c_str());
            return true;
        }
    private:
        string workdir_;
    };

    /*
    *     @class PropertyLabelStorageManager
    */
    class PropertyLabelStorageManager
    {
    public:
        typedef Table<string, uint64_t> PropLabelDbT;
        typedef PropLabelDbT::cursor_type CurType;
        typedef boost::unordered_map<std::string, PropLabelDbT*> CollDbMapT;

        typedef boost::unordered_map<std::string, PropCacheItem> LabelHitMapT;
        typedef boost::unordered_map<std::string, LabelHitMapT> CollLabelMapT;

        typedef boost::unordered_map<std::string, boost::mutex*> CollMutexMapT;
        
        PropertyLabelStorageManager(string wd)
            :workdir_(wd)
        {
        }
        ~PropertyLabelStorageManager()
        {
            LOG(INFO)<<"~PropertyLabelStorageManager"<<endl;
            CollDbMapT::iterator it;
            CollMutexMapT::iterator mit = mgps_.begin();
            for(it=gps_.begin(); it!=gps_.end();it++, mit++)
            {
                delete mit->second;
                delete it->second;
            }
        }
        bool insert(const std::string& c, const std::string& label_name, uint64_t hitnum);
        bool get(const std::string& c, std::list<std::map<std::string, std::string> >& res);
        bool del(const std::string& c);
        bool load(CollLabelMapT& cache);
    private:
        bool insertCheckLevelDb(const std::string& c);
        bool getCheckLevelDb(const std::string& c);

        CollDbMapT gps_;
        CollMutexMapT mgps_;
        string workdir_;
    };
}//end of namespace logserver
}//end of namespace sf1r

#endif
