/*
 *  @file    PropertyCacheItem.h
 *  @author  Kuilong Liu
 *  @date    2013.05.17
 */
#ifndef _PROPERTY_CACHE_ITEM_HPP_
#define _PROPERTY_CACHE_ITEM_HPP_

#include <string>

namespace sf1r { namespace logserver {

class PropCacheItem
{
public:
	PropCacheItem()
		:hit_docs_num(0),
		modify(true)
	{
	}
	PropCacheItem(uint64_t h)
		:hit_docs_num(h),
		modify(true)
	{
	}
	PropCacheItem(uint64_t h, bool m)
		:hit_docs_num(h),
		modify(m)
	{
	}
	uint64_t hit_docs_num;
	bool modify;
};
}
}

#endif