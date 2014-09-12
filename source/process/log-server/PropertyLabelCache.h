/*
 *  @file    PropertyLabelCache.h
 *  @author  Kuilong Liu
 *  @date    2013.05.17
 */
#ifndef _PROPERTY_LABEL_CACHE_HPP_
#define _PROPERTY_LABEL_CACHE_HPP_

#include "StorageManager.h"
#include "PropertyCacheItem.h"
#include <string>
#include <list>
#include <iostream>
#include <util/scheduler.h>
#include <boost/bind.hpp>
#include <boost/unordered_map.hpp>

using namespace std;

namespace sf1r { namespace logserver {

class PropertyLabelCache
{
public:
	typedef boost::unordered_map<std::string, PropCacheItem> LabelHitMapT;
	typedef boost::unordered_map<std::string, LabelHitMapT> CollLabelMapT;  
	PropertyLabelCache(PropertyLabelStorageManager* p)
		:plsm_(p)
	{
		std::string jn = "PropertyLabelCronJob";
        bool res = izenelib::util::Scheduler::addJob(jn, 
        	2*60*60*1000,
        	0,
            boost::bind(&PropertyLabelCache::cronjob, this));
        if(res)
            LOG(INFO) << "Create cron job for PropertyLabelCache."<< endl;
        else
            LOG(ERROR) << "Failed in create cron job! (PropertyLabelCache)"<<endl;
	}
	~PropertyLabelCache()
	{
		LOG(INFO)<<"~PropertyLabelCache"<<endl;
	}
	void close()
	{
		cronjob();
	}
	void load()
	{
		plsm_->load(cache_);
	}
	bool insert(const std::string& c, const std::string& label, uint64_t hitnum)
	{
		if(cache_.find(c) == cache_.end())
		{
			PropCacheItem pi(hitnum);
			LabelHitMapT lh;
			lh[label] = pi;
			cache_[c] = lh;
			return true;
		}
		if(cache_[c].find(label) == cache_[c].end())
		{
			PropCacheItem pi(hitnum);
			cache_[c][label]=pi;
			return true;
		}
		cache_[c][label].hit_docs_num += hitnum;
		cache_[c][label].modify = true;
		return true;
	}

	bool get(const std::string& c, std::list<std::map<std::string, std::string> >& res)
	{
		if(cache_.find(c) == cache_.end())
			return true;

		LabelHitMapT::iterator it;
		for(it=cache_[c].begin(); it!=cache_[c].end();it++)
		{
			std::map<std::string, std::string> ma;
			ma["label_name"] = it->first;
			ma["hit_docs_num"] = boost::lexical_cast<std::string>((it->second).hit_docs_num);
			res.push_back(ma);
		}
		return true;
	}
	bool del(const std::string& c)
	{
		if(cache_.find(c) == cache_.end())
			return true;
		cache_.erase(cache_.find(c));
		return plsm_->del(c);
	}
	void cronjob()
	{
		CollLabelMapT::iterator it;
		LabelHitMapT::iterator iter;
		for(it=cache_.begin();it!=cache_.end();it++)
		{
			std::string coll = it->first;
			for(iter=(it->second).begin();iter!=(it->second).end();iter++)
			{
				if((iter->second).modify)
				{
					std::string label = iter->first;
					uint64_t hitnum = (iter->second).hit_docs_num;
					plsm_->insert(coll, label, hitnum);
					(iter->second).modify = false;
				}
			}
		}
	}
private:
	CollLabelMapT cache_;
	PropertyLabelStorageManager* plsm_;
};

} // end of namespace logserver
} // end of namespace sf1r
#endif
