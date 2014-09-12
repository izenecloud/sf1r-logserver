/*
 *  @file    SlideTimeManager.h
 *  @author  Kuilong Liu
 *  @date    2013.04.27
 *
 *           We need SlideTimeManager to maintenance a slide time list in memory
 *           It also works as an agent: when an user wants to get topk, SlideTimeManger
 *           ask SubWindowManger(cache) for subwindows, which are used to calculate topk,
 *           if some of the subwindows are not in cache, SlideTimeManager will access
 *           the StorageManager to get those subwindows, and then add those sunwindows
 *           into cache.
 */
#ifndef _SLIDE_TIME_MANAGER_HPP_
#define _SLIDE_TIME_MANAGER_HPP_

#include "StorageManager.h"
#include "SubWindowManager.h"
#include <list>
#include <vector>
#include <string>
#include <iterator>
#include <fstream>
#include <boost/unordered_map.hpp>
#include <util/datastream/sketch/HyperLogLog.hpp>

// 256 days data
#define CACHE_SIZE 256
#define MAX_PRIME  2147483647

namespace sf1r { namespace logserver {
class SlideTimeManager
{
public:
    typedef std::list<std::string> TimeListT;
    typedef boost::unordered_map<std::string, TimeListT > CollTimeMapT;
    typedef boost::unordered_map<std::string, CollTimeMapT > ServTimeMapT;

    typedef boost::unordered_map<std::string, SubWindowManager* > CollSubMapT;
    typedef boost::unordered_map<std::string, CollSubMapT > ServSubMapT;
    typedef HyperLL<uint128_t> HyperLLT;

    SlideTimeManager(std::string wd)
        :workdir_(wd)
    {
        tldm_ = new TopKStorageManager(wd+"leveldb/topk");
        load_();
    }
    ~SlideTimeManager()
    {
        save_();
        delete tldm_;
        ServSubMapT::iterator it;
        CollSubMapT::iterator iter;
        for(it = wgps_.begin();it!=wgps_.end();it++)
            for(iter = (it->second).begin();iter!=(it->second).end();iter++)
                delete iter->second;

    }

    bool insert(const std::string& s, const std::string& c, SubWindow* sw)
    {
        if(gps_.find(s) != gps_.end())
        {
            if(gps_[s].find(c) != gps_[s].end())
            {
                std::list<std::string>::iterator it;
                it=gps_[s][c].begin();
                while(it!=gps_[s][c].end())
                {
                    if(*it == sw->time())
                    {
                        LOG(INFO) <<"Time exist!"<<sw->time()<<endl;
                        return true;
                    }
                    it++;
                }
                gps_[s][c].push_back(sw->time());
                wgps_[s][c]->insert(sw);
            }
            else
            {
                TimeListT tl;
                tl.push_back(sw->time());
                gps_[s][c] = tl;

                SubWindowManager* swm = new SubWindowManager(CACHE_SIZE);
                swm->insert(sw);
                wgps_[s][c] = swm;
            }
        }
        else
        {
            TimeListT tl;
            tl.push_back(sw->time());
            CollTimeMapT cm;
            cm[c] = tl;
            gps_[s]=cm;

            SubWindowManager* swm = new SubWindowManager(CACHE_SIZE);
            swm->insert(sw);
            CollSubMapT csm;
            csm[c] = swm;
            wgps_[s] = csm;
        }
        tldm_->insert(s, c, sw->time(), sw->get());
        save_();
        return true;
    }

    bool get(const std::string& s, const std::string& c, const std::string& b, const std::string& e,
            uint32_t k, std::list<std::pair<std::string, uint32_t> >& topk)
    {
        if(gps_.find(s) == gps_.end())
            return false;
        if(gps_[s].find(c) == gps_[s].end())
            return false;

        TimeListT::iterator iter=gps_[s][c].begin();
        TimeListT tl;

        for(;iter!=gps_[s][c].end();iter++)
        {
            if(*iter >= b)
            {
                if(*iter > e) break;
                tl.push_back(*iter);
                if(!(wgps_[s][c]->check(*iter)))
                {
                    SubWindow* tsw = new SubWindow();
                    tsw->h_=new HyperLLT(MAX_PRIME, 6);
                    tsw->settime(*iter);
                    tldm_->get(s,c,*iter,tsw->get());
                    hsm_->get(s,c,*iter,tsw->h_);
                    wgps_[s][c]->insert(tsw);
                }
            }
        }

        return wgps_[s][c]->getTopK(tl, k, topk);
    }

    bool gethll(const std::string& s, const std::string& c, const std::string& b, const std::string& e,
            HyperLLT& hll)
    {
        if(gps_.find(s) == gps_.end())
            return false;
        if(gps_[s].find(c) == gps_[s].end())
            return false;

        LOG(INFO) <<s << "  " << c <<endl;
        TimeListT::iterator iter=gps_[s][c].begin();
        TimeListT tl;
        for(;iter!=gps_[s][c].end();iter++)
        {
            if(*iter >= b)
            {
                if(*iter > e) break;
                tl.push_back(*iter);
                if(!(wgps_[s][c]->check(*iter)))
                {
                    SubWindow* tsw = new SubWindow();
                    tsw->h_=new HyperLLT(MAX_PRIME, 6);
                    tsw->settime(*iter);
                    tldm_->get(s,c,*iter,tsw->get());
                    hsm_->get(s,c,*iter,tsw->h_);
                    wgps_[s][c]->insert(tsw);
                }
            }
        }
        return wgps_[s][c]->getHll(tl, hll);
    }
    bool getTime(const std::string& s, const std::string& c, std::list<std::string>& res)
    {
        if(gps_.find(s) == gps_.end())
            return false;
        if(gps_[s].find(c) == gps_[s].end())
            return false;
        res = gps_[s][c];
        return true;
    }
    bool getCollection(const std::string& s, std::list<std::string>& res)
    {
        if(gps_.find(s) == gps_.end())
            return false;
        CollTimeMapT::iterator it = gps_[s].begin();
        for(;it!=gps_[s].end();it++)
            res.push_back(it->first);
        return true;
    }
    void sethsm(HllStorageManager* h)
    {
        hsm_=h;
    }
    void save()
    {
        save_();
    }
private:
    void load_()
    {
        std::string path = workdir_+"timelist.dat";
        ifstream fin(path.c_str());
        std::string s;
        std::string c;
        std::string t;
        while(fin>>s)
        {
            fin >> c >> t;
            if(gps_.find(s) == gps_.end())
            {
                TimeListT tl;
                tl.push_back(t);
                CollTimeMapT cm;
                cm[c]=tl;
                gps_[s] = cm;

                CollSubMapT csm;
                csm[c]=new SubWindowManager(CACHE_SIZE);
                wgps_[s]=csm;
                continue;
            }
            if(gps_[s].find(c) == gps_[s].end())
            {
                TimeListT tl;
                tl.push_back(t);
                gps_[s][c]=tl;
                wgps_[s][c] = new SubWindowManager(CACHE_SIZE);
                continue;
            }
            gps_[s][c].push_back(t);
        }
    }
    void save_()
    {
        std::string path = workdir_+"/timelist.dat";
        ofstream fout(path.c_str());
        ServTimeMapT::iterator iter;
        CollTimeMapT::iterator it;
        TimeListT::iterator i;
        for(iter=gps_.begin();iter!=gps_.end();iter++)
        {
            for(it=(iter->second).begin();it!=(iter->second).end();it++)
            {
                for(i=(it->second).begin();i!=(it->second).end();i++)
                    fout<<iter->first<<"  "<<it->first<<"  "<<*i<<endl;
            }
        }
    }

    std::string workdir_;
    TopKStorageManager* tldm_;
    HllStorageManager* hsm_;
    ServTimeMapT gps_;
    ServSubMapT wgps_;
};

}//end of namespace logserver
}//end of namespace sf1r
#endif
