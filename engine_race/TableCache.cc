// TableCache.cc

#include "TableCache.h"

namespace polar_race {

FilterCache filterCache;
// DataCache 
RetCode FilterCache::addFilter(int id, BloomFilter* filter) {
    if (_cache.size() >= MAX_FILTER_CACHE) {
        BloomFilter* tmp = _cache.back().second;
        _cache.pop_back();
        delete tmp;
    }
    _cache.push_front(std::pair<int, BloomFilter*>(id, filter));
    return RetCode::kSucc;
}

BloomFilter* FilterCache::getFilter(int id) {
    for (auto iter : _cache) {
        if (iter.first == id)
            return iter.second;
    }
    return nullptr;
}

RetCode IndexCache::read(int table_id, const string &key, string &value) {
    std::lock_guard<std::mutex> guard(_mtx);
    for (auto iter = _list.begin(); iter != _list.end(); iter++) {
        if (table_id != (*iter)->_table_id)
            continue;
        if ((*iter)->read(key, value) == RetCode::kSucc) {
            // find value
            IndexBlock* tmp = *iter;
            _list.erase(iter);
            _list.push_front(tmp);
            return RetCode::kSucc;
        }
    }
    return RetCode::kNotFound;
}

RetCode IndexCache::cache(IndexBlock* block) {
    std::lock_guard<std::mutex> guard(_mtx);
    
    if (_list.size() >= MAX_BLOCK_CACHE) {
        IndexBlock* old = _list.back();
        _list.pop_back();
        _list.push_back(block);
        delete old;
    }

    _list.push_back(block);

    return RetCode::kSucc;
}

} // namespace polar_race