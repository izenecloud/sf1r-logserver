/*
 *  @file    LogAnalysisManager.h
 *  @author  Kuilong Liu
 *  @date    2013.04.27
 *
 *           the main class for log analysis
 */
#ifndef _LOG_ANALYSIS_MANAGER_H_
#define _LOG_ANALYSIS_MANAGER_H_

#include "SlideTimeManager.h"
#include "StorageManager.h"
#include "SubWindowManager.h"
#include "LogServerCfg.h"
#include "PropertyLabelCache.h"
#include <string>
#include <list>
#include <map>
#include <time.h>
#include <stdlib.h>
#include <common/Utilities.h>
#include <util/scheduler.h>
#include <util/datastream/topk/cms/TopKCalculator.hpp>
#include <util/datastream/sketch/HyperLogLog.hpp>
#include <boost/bind.hpp>
#include <boost/unordered_map.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/signals2/mutex.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>

using namespace boost::posix_time;
using namespace boost::gregorian;

#define START_TIME         24*60*60*1000
#define SLIDE_LENGTH       24*60*60*1000
#define SKETCH_WIDTH       1<<20
#define SKETCH_MAX_VALUE   1<<31
#define TOPK_MAXCOUNT      1024
#define MAX_PRIME          2147483647

#define th(k) k/2+k
#define SIZE 3

namespace sf1r { namespace logserver {
class CronJobManager
{
public:
    typedef HyperLL<uint128_t> HyperLLT;
    typedef TopKCalculator<std::string, uint32_t> TopKCalT;
    CronJobManager(std::string se, std::string co,
            SlideTimeManager* sm, DVStorageManager* dm, HllStorageManager* hs, CMSStorageManager* cm)
        :service_(se), collection_(co),
        stm_(sm), dldm_(dm), hsm_(hs), cmm_(cm)
    {
        uint32_t sw, smv, tm;
        if(!sf1r::LogServerCfg::get()->getStartTime(se, co, start_time_))
            start_time_=START_TIME;
        if(!sf1r::LogServerCfg::get()->getSlideLength(se, co, slide_length_))
            slide_length_=SLIDE_LENGTH;
        if(!sf1r::LogServerCfg::get()->getSketchWidth(se, co, sw))
            sw = SKETCH_WIDTH;
        if(!sf1r::LogServerCfg::get()->getSketchMaxValue(se, co, smv))
            smv = SKETCH_MAX_VALUE;
        if(!sf1r::LogServerCfg::get()->getTopKMaxCount(se, co, tm))
            tm = TOPK_MAXCOUNT;
        tc_=new TopKCalT(sw, smv, tm);
        hll_ = new HyperLLT(MAX_PRIME, 6);
        cur_count = 0;
    }
    ~CronJobManager()
    {
        delete tc_;
        delete hll_;
    }

    void init()
    {
        std::string jn = "LogServerCronJob";
        jn +="("+service_+","+collection_+")";
        ptime now(second_clock::local_time());
        uint64_t timepast = (now.time_of_day().ticks()) / time_duration::ticks_per_second();
        uint64_t start = start_time_ - (timepast*1000);
        if(start < 0) start = 0;

//        start = 0;
//        slide_length_=10*1000;
        LOG(INFO)<<"start time: "<<start_time_<<" slide_length_: "<<slide_length_<<endl;
        bool res = izenelib::util::Scheduler::addJob(jn, slide_length_, start,
                boost::bind(&CronJobManager::cronjob, this));
        if(res)
            LOG(INFO) << "Create cron job for: " << service_ <<", " << collection_ << endl;
        else
            LOG(ERROR) << "Failed in create cron job! " <<service_ <<", " << collection_ <<endl;
    }

    void cronjob()
    {
        ptime now(second_clock::local_time());
        LOG(INFO) << "cronjob " << now << "  "<<service_<<"  "<<collection_<<endl;
        if(cur_count > 0)
        {
            mutex_.lock();
            cur_time_=to_iso_string(now);
            SubWindow* sw = new SubWindow();
            sw->settime(cur_time_);
            sw->sethll(hll_);
            tc_->getTopK(sw->get());
            cmm_->insert(service_, collection_, cur_time_, tc_->getSketch());
            tc_->reset();
            stm_->insert(service_, collection_, sw);
            stm_->save();
            //now the slide length of leveldb is flexible
            dldm_->reset(service_, collection_, cur_time_);
            hsm_->insert(service_, collection_, cur_time_, hll_);

            hll_=new HyperLLT(MAX_PRIME, 6);
            mutex_.unlock();
            cur_count=0;
        }
    }

   bool insert(const std::string& elem)
    {
        mutex_.lock();
        cur_count++;
        hll_->updateSketch(Utilities::md5ToUint128(elem));
        bool res =  tc_->insert(elem);
        mutex_.unlock();
        return res;
    }

    bool insert(const std::string& elem, const std::map<std::string, std::string>& values)
    {
        mutex_.lock();
        cur_count++;
        hll_->updateSketch(Utilities::md5ToUint128(elem));
        tc_->insert(elem);
        bool res = dldm_->insert(service_, collection_, elem, values);
        mutex_.unlock();
        return res;
    }
    //for import datas from sql
    bool insert(const std::string& t, const std::string& elem,
            const std::map<std::string, std::string>& values)
    {
        if(cur_time_.length() < SIZE)
        {
            ptime pt = from_iso_string(t);
            ptime now(pt.date(), hours(0));
            cur_time_ = to_iso_string(now);
            days dd(1);
            now = now + dd;
            next_time_ = to_iso_string(now);
        }
        if(t > next_time_)
        {
            LOG(INFO) << "make slide for "<<cur_time_ << endl;
            SubWindow* sw = new SubWindow();
            sw->settime(cur_time_);
            sw->sethll(hll_);
            tc_->getTopK(sw->get());
            cmm_->insert(service_, collection_, cur_time_, tc_->getSketch());
            tc_->reset();
            stm_->insert(service_, collection_, sw);
            stm_->save();
            dldm_->reset(service_, collection_, cur_time_);
            hsm_->insert(service_, collection_, cur_time_, hll_);
            hll_=new HyperLLT(MAX_PRIME, 6);
            ptime pt = from_iso_string(t);
            ptime now(pt.date(), hours(0));
            cur_time_ = to_iso_string(now);
            days dd(1);
            now = now + dd;
            next_time_ = to_iso_string(now);
        }
        hll_->updateSketch(Utilities::md5ToUint128(elem));
        tc_->insert(elem);
        return dldm_->insert(service_, collection_, elem, values);
    }
    bool getTopK(uint32_t k, std::list<std::pair<std::string, uint32_t> >& topk)
    {
        mutex_.lock();
        bool res = tc_->getTopK(k, topk);
        mutex_.unlock();
        return res;
    }
    bool getTopK(const std::string& b, const std::string& e, uint32_t k,
            std::list<std::pair<std::string, uint32_t> >&topk)
    {
        //if today>=b and <=e
        ptime now(second_clock::local_time());
        std::string cur_t = to_iso_string(now);
        if(cur_t <= e && cur_t >= b)
        {
            mutex_.lock();
            tc_->getTopK(th(k), topk);
            mutex_.unlock();
            if(topk.size()>cur_count)
            {
                LOG(INFO)<<"some rubbish data in topk"<<endl;
                topk.clear();
            }
        }
        return stm_->get(service_, collection_, b, e, k, topk);
    }

    bool getDVC(uint64_t& res)
    {
        res = hll_->getCardinate();
        return true;
    }
    bool getDVC(const std::string& b, const std::string& e, uint64_t& res)
    {
        ptime now(second_clock::local_time());
        std::string cur_t = to_iso_string(now);
        HyperLLT hll(MAX_PRIME, 6);
        if(cur_t <= e && cur_t >=b)
            hll = *hll_;
        stm_->gethll(service_, collection_, b, e, hll);
        res = hll.getCardinate();
        return true;
    }
    madoka::Sketch* getSketch()
    {
        return tc_->getSketch();
    }
private:
    uint32_t start_time_;
    uint32_t slide_length_;
    std::string service_;
    std::string collection_;
    SlideTimeManager* stm_;
    DVStorageManager* dldm_;
    HllStorageManager* hsm_;
    CMSStorageManager* cmm_;

    TopKCalT* tc_;
    HyperLLT* hll_;
    std::string cur_time_;
    std::string next_time_;
    boost::mutex mutex_;
    uint32_t cur_count;
};

class LogAnalysisManager
{
public:
    typedef HyperLL<uint128_t> HyperLLT;
    typedef boost::unordered_map<std::string, CronJobManager*> CollCronMapT;
    typedef boost::unordered_map<std::string, CollCronMapT> SerCronMapT;

    LogAnalysisManager(std::string w)
        :wd_(w)
    {
        stm_=new SlideTimeManager(wd_);
        dldm_=new DVStorageManager(wd_+"leveldb/dv");
        hsm_=new HllStorageManager(wd_+"hll");
        cmm_=new CMSStorageManager(wd_+"cms");
        plsm_=new PropertyLabelStorageManager(wd_+"propertylabel");
        plc_=new PropertyLabelCache(plsm_);
        plc_->load();
        stm_->sethsm(hsm_);
    }
    ~LogAnalysisManager()
    {
        LOG(INFO)<<"~LogAnalysisManager"<<endl;

        SerCronMapT::iterator iter;
        CollCronMapT::iterator it;
        for(iter=gps_.begin();iter!=gps_.end();iter++)
            for(it=(iter->second).begin();it!=(iter->second).end();it++)
            {
                delete it->second;
            }  

        delete plc_;
        delete stm_;
        delete dldm_;
        delete hsm_;
        delete cmm_;
        delete plsm_;
    }

    void close()
    {
        LOG(INFO)<<"close"<<endl;
        SerCronMapT::iterator iter;
        CollCronMapT::iterator it;
        for(iter=gps_.begin();iter!=gps_.end();iter++)
            for(it=(iter->second).begin();it!=(iter->second).end();it++)
            {
                it->second->cronjob();
            }
        plc_->close();
    }
    bool insert(const std::string& s, const std::string& c, const std::string& elem)
    {
//        LOG(INFO)<<"<insert> service name: "<<s<<" collection name: " << c <<" elem: " << elem <<endl;
        if(!insertCheckCron(s,c))return false;
        return gps_[s][c]->insert(elem);
    }
    bool insert(const std::string& s, const std::string& c, const std::string& elem,
            const std::map<std::string, std::string>& values)
    {
//        LOG(INFO)<<"<insert> service name: "<<s<<" collection name: " << c <<" elem: " << elem <<endl;
        if(!insertCheckCron(s,c))return false;
        if(!gps_[s][c]->insert(elem, values))return false;
        return true;
    }
    //for import datas from sql
    bool insert(const std::string& s, const std::string& c, const std::string& t,
            const std::string& elem, const std::map<std::string, std::string>& values)
    {
//        LOG(INFO)<<"<insert> service name: "<<s<<" collection name: " << c << " time: " << t <<" elem: " << elem <<endl;
        if(!insertCheckCron(s,c))return false;
        if(!gps_[s][c]->insert(t, elem, values))return false;
        return true;
    }
    bool getTopK(const std::string& s, const std::string& c,
            uint32_t k,
            std::list<std::pair<std::string, uint32_t> >& res)
    {
        LOG(INFO)<<"<getTopK> service name: "<<s<<" collection name: " << c <<" k: "<<k<<endl;
        if(!getCheckCron(s,c)) return false;
        return gps_[s][c]->getTopK(k, res);
    }
    bool getTopK(const std::string& s, const std::string& c,
            const std::string& b, const std::string& e,
            uint32_t k,
            std::list<std::pair<std::string, uint32_t> >& res)
    {
        LOG(INFO)<<"<getTopK> service name: "<<s<<" collection name: " << c << " begin time: " << b <<" end time: " << e <<" k: "<<k<<endl;
        if(!getCheckCron(s,c)) 
            return stm_->get(s, c, b, e, k, res);
        return gps_[s][c]->getTopK(b, e, k, res);
    }
    bool getDVC(const std::string& s, const std::string& c, uint64_t& res)
    {
        LOG(INFO)<<"<getDVC> service name: "<<s<<" collection name: " << c <<endl;
        if(!getCheckCron(s,c)) return false;
        return gps_[s][c]->getDVC(res);
    }
    bool getDVC(const std::string& s, const std::string& c,
            const std::string& b, const std::string& e, uint64_t& res)
    {
        LOG(INFO)<<"<getDVC> service name: "<<s<<" collection name: " << c << " begin time: " << b <<" end time: " << e <<endl;
        if(!getCheckCron(s,c))
        {
            HyperLLT hll(MAX_PRIME, 6);
            stm_->gethll(s, c, b, e, hll);
            res = hll.getCardinate();
            return true;
        }
        return gps_[s][c]->getDVC(b, e, res);
    }

    bool getValue(const std::string& s, const std::string& c,
            const std::string& key, std::map<std::string, std::string>& values)
    {
        LOG(INFO)<<"<getValue> service name: "<<s<<" collection name: " << c << " key: " << key <<endl;
        return dldm_->get(s, c, key, values);
    }
    bool getValue(const std::string& s, const std::string& c,
            uint32_t limit, std::list<std::map<std::string, std::string> >& values)
    {
        LOG(INFO)<<"<getValue> service name: "<<s<<" collection name: " << c <<" limit: "<<limit<<endl;
        return dldm_->get(s,c, limit, values);
    }
    bool getValue(const std::string& s, const std::string& c, const std::string& t,
            uint32_t limit, std::list<std::map<std::string, std::string> >& values)
    {
        LOG(INFO)<<"<getValue> service name: "<<s<<" collection name: " << c << " time: " << t<<" limit: "<<limit<<endl;
        return dldm_->get(s, c, t, limit, values);
    }
    bool getValueAndCount(const std::string& s, const std::string& c,
            const std::string& b, std::list<std::map<std::string, std::string> >& values)
    {
        LOG(INFO)<<"<getValueAndCount> service name: "<<s<<" collection name: " << c << " begin_time: " << b<<endl;
        std::list<std::string> tl;
        stm_->getTime(s, c, tl);
        madoka::Sketch* sketch;
        sketch = new madoka::Sketch();
        if(getCheckCron(s,c))
            sketch->copy(*(gps_[s][c]->getSketch()));
        else
        {
            uint32_t sw;
            if(!sf1r::LogServerCfg::get()->getSketchWidth(s, c, sw))
                sw = SKETCH_WIDTH;
            sketch->create(sw);
        }
        std::list<std::string>::iterator it;
        for(it=tl.begin();it!=tl.end();it++)
        {
            if(*it >= b)
            {
                madoka::Sketch st;
                if(cmm_->get(s, c, *it, st))
                    sketch->merge(st);
            }
        }
        
        boost::unordered_map<std::string, std::map<std::string, std::string> > um;

        ptime now(second_clock::local_time());
        std::string cur_t = to_iso_string(now);
        if(cur_t >= b)
        {
            std::list<std::map<std::string, std::string> > mymap;
            dldm_->get(s,c,mymap);
            std::list<std::map<std::string, std::string> >::iterator iter;
            for(iter=mymap.begin();iter!=mymap.end();iter++)
            {
                std::string key = (*iter)["key"];
                if (um.find(key) == um.end())
                {
                    (*iter)["count"] = boost::lexical_cast<std::string>(sketch->get(key.c_str(), key.length()));
                    um[key] = *iter;
                }
            }
        }
        
        it=tl.end();
        while(it!=tl.begin())
        {
            it--;
            if(*it < b) continue;
            std::list<std::map<std::string, std::string> > mymap;
            dldm_->get(s, c, *it, mymap);
            std::list<std::map<std::string, std::string> >::iterator iter;
            for(iter=mymap.begin();iter!=mymap.end();iter++)
            {
                std::string key = (*iter)["key"];
                if (um.find(key) == um.end())
                {
                    (*iter)["count"] = boost::lexical_cast<std::string>(sketch->get(key.c_str(), key.length()));
                    um[key] = *iter;
                }
            }
        }
        
        boost::unordered_map<std::string, std::map<std::string, std::string> >::iterator ii;
        for(ii=um.begin();ii!=um.end();ii++)
        {
            values.push_back(ii->second);
        }
        LOG(INFO) <<"<getValueAndCount> OK"<<endl;
        delete sketch;
        return true;

    }
    bool getAllCollectionData(const std::string& s, const std::string& b, std::list<std::map<std::string, std::string> >& values)
    {
        LOG(INFO)<<"<getAllCollectionData> service name: "<<s<< " begin_time " << b <<endl;
        std::list<std::string> coll;
        stm_->getCollection(s, coll);
        std::list<std::string>::iterator it;
        for (it=coll.begin(); it!= coll.end(); it++)
        {
            getValueAndCount(s, *it, b, values);
        }
        LOG(INFO) <<"<getAllCollectionData> OK"<<endl;
        return true;
    }
    bool insertPropertyLabel(const std::string& c, const std::string& label_name, uint64_t hitnum)
    {
        LOG(INFO)<<"<insertPropertyLabel> collection name: "<<c<<" label_name: "<<label_name <<" hit_docs_num: "<<hitnum<<endl;
        return plc_->insert(c, label_name, hitnum);
    }
    bool getPropertyLabel(const std::string& c, std::list<std::map<std::string, std::string> >& res)
    {
        LOG(INFO)<<"<getPropertyLabel> collection name: "<<c<<endl;
        return plc_->get(c, res);
    }
    bool delPropertyLabel(const std::string& c)
    {
        LOG(INFO)<<"<delPropertyLabel> collection name: "<<c<<endl;
        return plc_->del(c);
    }
private:
    bool insertCheckCron(const std::string& s, const std::string& c)
    {
        if(gps_.find(s) == gps_.end())
        {
            CollCronMapT ccm;
            ccm[c] = new CronJobManager(s,c,stm_,dldm_, hsm_, cmm_);
            ccm[c]->init();
            gps_[s] = ccm;
            return true;
        }
        if(gps_[s].find(c) == gps_[s].end())
        {
            gps_[s][c] = new CronJobManager(s,c,stm_,dldm_, hsm_, cmm_);
            gps_[s][c]->init();
            return true;
        }
        return true;
    }
    bool getCheckCron(const std::string& s, const std::string& c)
    {
        if(gps_.find(s) == gps_.end())
            return false;
        if(gps_[s].find(c) == gps_[s].end())
            return false;
        return true;
    }
    std::string wd_;
    SlideTimeManager* stm_;
    DVStorageManager* dldm_;
    HllStorageManager* hsm_;
    CMSStorageManager* cmm_;
    PropertyLabelStorageManager* plsm_;
    PropertyLabelCache* plc_;

    SerCronMapT gps_;
};

} //end of namespace logserver
} //end of namespace sf1r
#endif
