#include "StorageManager.h"
#include <dirent.h>

namespace sf1r{ namespace logserver{

    /*
     *     @class DVStorageManager
     */
    bool DVStorageManager::insert(const std::string& s, const std::string& c,
            const std::string& key,
            const std::map<std::string, std::string>& value)
    {
        if(!insertCheckLevelDb(s, c))
        {
            LOG(INFO) << "Open level db false!" << endl;
            return false;
        }
        mgps_[s][c]->lock();
        if(!gps_[s][c]->open())
        {
            LOG(INFO)<<"Open level db false"<<endl;
            mgps_[s][c]->unlock();
            return false;
        }
        if(fgps_[s][c]->Get(key))
        {
            mgps_[s][c]->unlock();
            return true;
        }
        fgps_[s][c]->Insert(key);
        gps_[s][c]->add_item(key, value);
//        gps_[s][c]->close();
        mgps_[s][c]->unlock();
        return true;
    }

    bool DVStorageManager::insert(const std::string& s, const std::string& c,
            const std::string& key,
            const std::string& value)
    {
        if(!insertCheckLevelDb(s,c))
        {
            LOG(INFO) << "Open level db false!" << endl;
            return false;
        }
        mgps_[s][c]->lock();
        if(!gps_[s][c]->open())
        {
            mgps_[s][c]->unlock();
            return false;
        }
        if(fgps_[s][c]->Get(key))
        {
            mgps_[s][c]->unlock();
            return true;
        }
        fgps_[s][c]->Insert(key);
        gps_[s][c]->add_item(key, value);
//        gps_[s][c]->close();
        mgps_[s][c]->unlock();
        return true;
    }
    bool DVStorageManager::get(const std::string& s, const std::string& c,
            const std::string& key,
            std::map<std::string, std::string>& value)
    {
        if(!getCheckLevelDb(s, c))
        {
            LOG(INFO) << "Open level db false!" << endl;
            return false;
        }
        mgps_[s][c]->lock();
        if(!(fgps_[s][c]->Get(key)))
        {
            mgps_[s][c]->unlock();
            return true;
        }
        if(!gps_[s][c]->open())
        {
            mgps_[s][c]->unlock();
            return false;
        }
        gps_[s][c]->get_item(key, value);
//        gps_[s][c]->close();
        mgps_[s][c]->unlock();
        return true;
    }
    bool DVStorageManager::get(const std::string& s, const std::string& c,
            const std::string& key,
            std::string& value)
    {
        if(!getCheckLevelDb(s, c))
        {
            LOG(INFO) << "Open level db false!" << endl;
            return false;
        }
        mgps_[s][c]->lock();
        if(!(fgps_[s][c]->Get(key)))
        {
            mgps_[s][c]->unlock();
            return true;
        }
        if(!gps_[s][c]->open())
        {
            mgps_[s][c]->unlock();
            return false;
        }
        gps_[s][c]->get_item(key, value);
//        gps_[s][c]->close();
        mgps_[s][c]->unlock();
        return true;
    }
    bool DVStorageManager::get(const std::string& s, const std::string& c,
            uint32_t limit,
            std::list<std::pair<std::string, std::string> >& values)
    {
        if(!getCheckLevelDb(s,c))
        {
            LOG(INFO) << "Open level db false!" <<endl;
            return false;
        }
        mgps_[s][c]->lock();
        if(!gps_[s][c]->open())
        {
            mgps_[s][c]->unlock();
            return false;
        }
        UserQueryDbTable::cur_type iter = gps_[s][c]->begin();
        uint32_t co = 0;
        do
        {
            std::string key;
            std::string value;
            if(gps_[s][c]->fetch(iter, key, value))
            {
                values.push_back(make_pair(key, value));
                co++;
            }
        }while(gps_[s][c]->iterNext(iter) && iter->Valid() && co < limit);
//        gps_[s][c]->close();
        mgps_[s][c]->unlock();
        return true;
    }
    bool DVStorageManager::get(const std::string& s, const std::string& c,
            uint32_t limit,
            std::list<std::map<std::string, std::string> >& values)
    {
        if(!getCheckLevelDb(s,c))
        {
            LOG(INFO) << "Open level db false!" <<endl;
            return false;
        }
        mgps_[s][c]->lock();
        if(!gps_[s][c]->open())
        {
            mgps_[s][c]->unlock();
            return false;
        }
        UserQueryDbTable::cur_type iter = gps_[s][c]->begin();
        uint32_t co = 0;
        do
        {
            std::string key;
            std::map<std::string, std::string> value;
            if(gps_[s][c]->fetch(iter, key, value))
            {
                values.push_back(value);
                co++;
            }
        }while(gps_[s][c]->iterNext(iter) && iter->Valid() && co < limit);
//        gps_[s][c]->close();
        mgps_[s][c]->unlock();
        return true;
    }
    bool DVStorageManager::get(const std::string& s, const std::string& c,
            std::list<std::map<std::string, std::string> >& values)
    {
        std::string path = workdir_+"/"+s+"/"+c+"/temp.db";
        UserQueryDbTable db(path);
        if(!db.open()) return false;

        UserQueryDbTable::cur_type iter = db.begin();
        if(!iter->Valid()) return false;
        do
        {
            std::string key;
            std::map<std::string, std::string> value;
            if(db.fetch(iter, key, value))
            {
                value["key"]=key;
                values.push_back(value);
            }
        }while(db.iterNext(iter) && iter->Valid());
//        db.close();
        return true;
    }
    bool DVStorageManager::get(const std::string& s, const std::string& c, const std::string& t,
            uint32_t limit,
            std::list<std::pair<std::string, std::string> >& values)
    {
        std::string path = workdir_+"/"+s+"/"+c +"/"+t+".db";
        UserQueryDbTable db(path);
        if(!db.open()) return false;

        UserQueryDbTable::cur_type iter = db.begin();
        if(!iter->Valid()) return false;
        uint32_t co = 0;
        do
        {
            std::string key;
            std::string value;
            if(db.fetch(iter, key, value))
            {
                values.push_back(make_pair(key, value));
                co++;
            }
        }while(db.iterNext(iter) && iter->Valid() && co < limit);
//        db.close();
        return true;
    }
    bool DVStorageManager::get(const std::string& s, const std::string& c, const std::string& t,
            std::list<std::map<std::string, std::string> >& values)
    {
        std::string path = workdir_+"/"+s+"/"+c+"/"+t+".db";
        UserQueryDbTable db(path);
        if(!db.open()) return false;

        UserQueryDbTable::cur_type iter = db.begin();
        if(!iter->Valid()) return false;
        do
        {
            std::string key;
            std::map<std::string, std::string> value;
            if(db.fetch(iter, key, value))
            {
                value["key"]=key;
                values.push_back(value);
            }
        }while(db.iterNext(iter) && iter->Valid());
//        db.close();
        return true;
    }
    bool DVStorageManager::get(const std::string& s, const std::string& c, const std::string& t,
            uint32_t limit,
            std::list<std::map<std::string, std::string> >& values)
    {
        std::string path = workdir_+"/"+s+"/"+c+"/"+t+".db";
        UserQueryDbTable db(path);
        if(!db.open()) return false;

        UserQueryDbTable::cur_type iter = db.begin();
        if(!iter->Valid()) return false;
        uint32_t co = 0;
        do
        {
            std::string key;
            std::map<std::string, std::string> value;
            if(db.fetch(iter, key, value))
            {
                values.push_back(value);
                co++;
            }
        }while(db.iterNext(iter) && iter->Valid() && co < limit);
//        db.close();
        return true;
    }
    bool DVStorageManager::getCheckLevelDb(const std::string& s, const std::string& c)
    {
        if(gps_.find(s) == gps_.end())
            return false;
        if(gps_[s].find(c) == gps_[s].end())
            return false;
        if(gps_[s][c]==NULL)
            return false;
        return true;
    }
    bool DVStorageManager::insertCheckLevelDb(const std::string& s, const std::string& c)
    {
        if(gps_.find(s) == gps_.end())
        {
            std::string path = workdir_+"/"+s+"/"+c+"/";
            boost::filesystem::create_directories(path);
            path += "temp.db";
            CollDbMapT cdt;
            cdt[c] = new UserQueryDbTable(path);
            gps_[s] = cdt;

            CollBFMapT cbf;
            cbf[c] = new DBFT(DBF_SIZE, DBF_FPP, DBF_SIZE);
            fgps_[s] = cbf;

            CollMutexMapT cmmt;
            cmmt[c] = new mutex();
            mgps_[s] = cmmt;
            return true;
        }

        if(gps_[s].find(c) == gps_[s].end() || gps_[s][c] == NULL)
        {
            std::string path = workdir_+"/"+s+"/"+c+"/";
            boost::filesystem::create_directories(path);
            path += "temp.db";
            gps_[s][c] = new UserQueryDbTable(path);
            fgps_[s][c] = new DBFT(DBF_SIZE, DBF_FPP, DBF_SIZE);
            mgps_[s][c] = new mutex();
            return true;
        }
        return true;
    }

    /*
     *     @class TopKStorageManager
     */
    bool TopKStorageManager::insert(const std::string& s, const std::string& c, const std::string& t,
            const std::list<std::pair<std::string, uint32_t> >& topk)
    {
        if(!insertCheckLevelDb(s, c))
        {
            LOG(INFO) << "Open level db false!" << endl;
            return false;
        }
        std::string path = workdir_+"/"+s+"/"+c+"/"+t+".db";
        if(!gps_[s][c]->open(path))
            return false;
        std::list<std::pair<std::string, uint32_t> >::const_iterator iter = topk.begin();
        for(;iter!=topk.end();iter++)
        {
            gps_[s][c]->insert(iter->first, iter->second);
        }
//        gps_[s][c]->close();
        return true;
    }

    bool TopKStorageManager::get(const std::string& s, const std::string& c, const std::string& t,
            std::list<std::pair<std::string, uint32_t> >& topk)
    {
        if(!getCheckLevelDb(s, c))
        {
            LOG(INFO) << "Open level db false!" <<endl;
            return false;
        }
        std::string path = workdir_+"/"+s+"/"+c+"/"+t+".db";
        if(!gps_[s][c]->open(path))
            return false;
        CurType iter = gps_[s][c]->begin();
        do
        {
            std::string elem;
            uint32_t count;
            gps_[s][c]->fetch(iter, elem, count);
            std::list<std::pair<std::string, uint32_t> >::iterator i;
            for(i=topk.begin();i!=topk.end();i++)
                if(count>=i->second)break;
            topk.insert(i, make_pair(elem, count));
        }while(gps_[s][c]->iterNext(iter) && iter->Valid());

//        gps_[s][c]->close();
        return true;
    }

    bool TopKStorageManager::getCheckLevelDb(const std::string& s, const std::string& c)
    {
        if(gps_.find(s) == gps_.end())
        {
            CollDbMapT cdm;
            cdm[c] = new TopKDbT();
            gps_[s] = cdm;
            return true;
        }
        if(gps_[s].find(c) == gps_[s].end())
        {
            gps_[s][c] = new TopKDbT();
            return true;
        }
        return true;
    }
    bool TopKStorageManager::insertCheckLevelDb(const std::string& s, const std::string& c)
    {
        if(gps_.find(s) == gps_.end())
        {
            std::string path = workdir_+"/"+s+"/"+c+"/";
            boost::filesystem::create_directories(path);
            CollDbMapT cdm;
            cdm[c] = new TopKDbT();
            gps_[s] = cdm;
            return true;
        }
        if(gps_[s].find(c) == gps_[c].end())
        {
            std::string path = workdir_+"/"+s+"/"+c+"/";
            boost::filesystem::create_directories(path);
            gps_[s][c] = new TopKDbT();
            return true;
        }
        return true;
    }

        /*
     *     @class PropertyLabelStorageManager
     */
    bool PropertyLabelStorageManager::insert(const std::string& c, const std::string& lable_name, uint64_t hitnum)
    {
        if(!insertCheckLevelDb(c))
        {
            LOG(INFO) << "Open level db false!" << endl;
            return false;
        }
        std::string path = workdir_+"/"+c+"/propertylabel.db";
        mgps_[c]->lock();
        if(!gps_[c]->open(path))
        {
            mgps_[c]->unlock();
            return false;
        }
        gps_[c]->update(lable_name, hitnum);
        mgps_[c]->unlock();
        return true;
    }

    bool PropertyLabelStorageManager::get(const std::string& c, std::list<std::map<std::string, std::string> >& res)
    {
        if(!getCheckLevelDb(c))
        {
            LOG(INFO) << "Open level db false!" <<endl;
            return false;
        }
        std::string path = workdir_+"/"+c+"/propertylabel.db";
        mgps_[c]->lock();
        if(!gps_[c]->open(path))
        {
            mgps_[c]->unlock();
            return false;
        }
        CurType iter = gps_[c]->begin();
        do
        {
            std::string label;
            uint64_t hitnum;
            gps_[c]->fetch(iter, label, hitnum);
            std::map<std::string, std::string> ma;
            ma["label_name"] = label;
            ma["hit_docs_num"] = boost::lexical_cast<std::string>(hitnum);
            res.push_back(ma);
        }while(gps_[c]->iterNext(iter) && iter->Valid());
        mgps_[c]->unlock();
        return true;
    }

    bool PropertyLabelStorageManager::del(const std::string& c)
    {
        std::string path = workdir_ + "/"+c+"/";
        boost::filesystem::remove_all(path);
        if(getCheckLevelDb(c))
        {
            if(gps_[c] != NULL)
                delete gps_[c];
            gps_.erase(c);
            if(mgps_[c]!= NULL)
                delete mgps_[c];
            mgps_.erase(c);
        }
        return true;
    }
    bool PropertyLabelStorageManager::load(CollLabelMapT& cache)
    {
        std::string dirname = workdir_;
        DIR* p_dir;
        struct dirent* p_dirent;
        p_dir = opendir(dirname.c_str());
        if(p_dir == NULL) return true;
        while((p_dirent = readdir(p_dir)))
        {
            std::string c_name(p_dirent->d_name);
            getCheckLevelDb(c_name);
        }
        CollDbMapT::iterator it;
        for(it=gps_.begin();it!=gps_.end();it++)
        {
            std::string path = workdir_+"/"+it->first+"/propertylabel.db";
            mgps_[it->first]->lock();
            if(!it->second->open(path))
            {
                mgps_[it->first]->unlock();
                continue;
            }
            LabelHitMapT lhm;
            CurType iter = it->second->begin();
            do
            {
                std::string label;
                uint64_t hitnum;
                it->second->fetch(iter, label, hitnum);
                PropCacheItem pci(hitnum);
                pci.modify = false;
                lhm[label]=pci;
            }while(it->second->iterNext(iter) && iter->Valid());
            cache[it->first] = lhm;
            mgps_[it->first]->unlock();
        }
        return true;
    }
    bool PropertyLabelStorageManager::getCheckLevelDb(const std::string& c)
    {
        if(gps_.find(c) == gps_.end())
        {
            gps_[c] = new PropLabelDbT();
            mgps_[c] = new boost::mutex();
            return true;
        }
        return true;
    }
    bool PropertyLabelStorageManager::insertCheckLevelDb(const std::string& c)
    {
        if(gps_.find(c) == gps_.end())
        {
            std::string path = workdir_+"/"+c+"/";
            boost::filesystem::create_directories(path);
            gps_[c] = new PropLabelDbT();
            mgps_[c] = new boost::mutex();
            return true;
        }
        return true;
    }
} //end of namespace logserver
} //end of namespace sf1r
