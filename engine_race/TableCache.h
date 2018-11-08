// TableCache.h
#ifndef _ENGINE_RACE_TABLECACHE_H_
#define _ENGINE_RACE_TABLECACHE_H_

#include "util.h"
#include "BloomFilter.h"
#include "MemTable.h"
#include "config.h"
#include <list>
#include <mutex>

namespace polar_race {

// class FilterCache {
// public:

//     RetCode addFilter(int id, BloomFilter* filter);
//     BloomFilter* getFilter(int id);
// private:
//     std::list<std::pair<int, BloomFilter*> > _cache;
// };

// extern FilterCache filterCache;

// class IndexCache {
// public:
//     RetCode read(int table_id, const string &key, string &value);
//     RetCode cache(IndexBlock* block);
// private:
//     std::mutex _mtx;
    
// };

// class IndexBlock {
// public:
//     friend class IndexCache;
//     RetCode read(const string &key, string &value);
// private:
//     int _table_id;
// };
// #pragma pack(1)
// struct IndexCacheEntry {
//     unsigned short fid;
//     size_t offset;
// };

// class IndexCache {
// public:
    
// private:
//     std::map<long long int, IndexCacheEntry> _index;
// };

} // namespace polar_race

#endif //_ENGINE_RACE_TABLECACHE_H