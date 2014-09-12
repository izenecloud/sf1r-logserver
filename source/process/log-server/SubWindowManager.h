/*
 *    @file    SubWindowManager.h
 *    @author  Kuilong Liu
 *    @date    2013.04.25
 *
 *             We need SubWindowManger to manage sub windows for
 *             sliding window algorithm, which is used to abtain
 *             topk frequent items dynamically
 */
#ifndef _SUB_WINDOW_MANAGER_H_
#define _SUB_WINDOW_MANAGER_H_

#include <list>
#include <string>
#include <stdint.h>
#include <boost/unordered_map.hpp>
#include <util/datastream/topk/cms/TopKItemEstimation.hpp>
#include <util/datastream/sketch/HyperLogLog.hpp>
#include <iostream>

#define th(k) k/2+k

namespace sf1r { namespace logserver {
class SubWindow
{
public:
    typedef std::list<std::pair<std::string, uint32_t> > TopKT;
    typedef TopKT::const_iterator ConIterT;
    typedef HyperLL<uint128_t> HyperLLT;
    SubWindow()
    {
    }
    ~SubWindow()
    {
        t_.clear();
        delete h_;
        h_=NULL;
    }

    TopKT& get()
    {
        return t_;
    }

    void get(ConIterT& begin, ConIterT& end)
    {
        begin = t_.begin();
        end = t_.end();
    }

    uint32_t width()
    {
        return width_;
    }

    void settime(const std::string& t)
    {
        time_=t;
    }
    void sethll(HyperLLT* h)
    {
        h_=h;
    }
    std::string time()
    {
        return time_;
    }
public:
    HyperLLT* h_;
private:
    uint32_t width_;
    std::string time_;

    TopKT t_;
};
class SubWindowManager
{
public:
    typedef std::list<std::pair<std::string, uint32_t> > TopKT;
    typedef TopKT::const_iterator ConIterT;
    typedef boost::unordered_map<std::string, SubWindow* > GpsT;
    typedef HyperLL<uint128_t> HyperLLT;
    SubWindowManager(uint32_t m)
        :cs_(0), M_(m)
    {
    }
    ~SubWindowManager()
    {
        GpsT::iterator it = gps_.begin();
        for(;it!=gps_.end();it++)
            delete it->second;
    }

    void resize(uint32_t m)
    {
        M_=m;
    }

    bool insert(SubWindow* sw)
    {
        if(gps_.find(sw->time()) != gps_.end())
            return true;
        if(cs_ < M_)
            cs_++;
        else
        {
            delete gps_[tl_.front()];
            gps_[tl_.front()]=NULL;
            gps_.erase(tl_.front());
            tl_.pop_front();
            tl_.push_back(sw->time());
        }
        gps_[sw->time()] = sw;
        return true;
    }

    bool get(const std::string& t, ConIterT& b, ConIterT& e)
    {
        if(gps_.find(t)!=gps_.end())
        {
            gps_[t]->get(b,e);
            return true;
        }
        return false;
    }

    bool check(const std::string& t)
    {
        if(gps_.find(t) == gps_.end())
            return false;
        return true;
    }
    //merge topk
    bool getTopK(const std::list<std::string>& tl,
            uint32_t k,
            TopKT& res)
    {
        boost::unordered_map<std::string, uint32_t> counter;
        std::list<std::string>::const_iterator it=tl.begin();
        izenelib::util::TopKEstimation<std::string, uint32_t> tke(k);

        ConIterT b, e;
        uint32_t tk = th(k);
        for(;it!=tl.end();it++)
        {
            if(gps_.find(*it)!=gps_.end())
            {
                gps_[*it]->get(b,e);
                uint32_t i=0;
                while(b!=e)
                {
                    counter[b->first]=counter[b->first]+b->second;
                    b++;i++;
                    if(i>=tk) break;
                }
            }
        }
        if(res.size()>0)
        {
            TopKT::iterator iter=res.begin();
            for(;iter!=res.end();iter++)
            {
                counter[iter->first] = counter[iter->first]+iter->second;
            }
            res.clear();
        }
        boost::unordered_map<std::string, uint32_t>::iterator cit;
        for(cit=counter.begin();cit!=counter.end();cit++)
        {
            TopKT::iterator tit;
            for(tit=res.begin();tit!=res.end();tit++)
                if(cit->second >= tit->second)break;
            res.insert(tit, make_pair(cit->first, cit->second));
        }
        res.resize(k);
        return true;
    }

    //union hll
    bool getHll(const std::list<std::string>& tl,
            HyperLLT& hll)
    {
        std::list<std::string>::const_iterator it = tl.begin();
        for(;it!=tl.end();it++)
        {
            if(gps_.find(*it) !=gps_.end())
            {
                hll.unionSketch(gps_[*it]->h_);
            }
        }
        return true;
    }
private:
    //current cache size
    uint32_t cs_;
    //max cache size
    uint32_t M_;
    //time list (in cache)
    std::list<std::string> tl_;
    GpsT gps_;
};

} //end of namespace logserver
} //end of namespace sf1r
#endif
