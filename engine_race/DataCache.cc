#include "DataCache.h"
#include "DataFile.h"
namespace polar_race {

void DataCache::cache(const IndexEntry &_key) {
    dataFile[_key.num].readValue(_key.offset, value);
}

void DataCache::readCache(string* _value) {
    // wait for the cacher
    std::unique_lock<std::mutex> ulock(enable_mtx);
    while(!enable)
        enable_cv.wait(ulock);
    ulock.unlock();
    // read from cache
    _value->assign(value, 4096);
    
    on_reading.fetch_sub(1);
    on_reading_cv.notify_all();
}

void DataCache::read(const IndexEntry &_key, string* _value) {
    key_mtx.lock();
    if (sameKey(_key.key, key)) {
        // just read
        on_reading.fetch_add(1);
        key_mtx.unlock();
        
        readCache(_value);

    } else {
        // need to recache
        key_mtx.unlock();
        while(!sameKey(_key.key, key)) {
            std::mutex _mtx;
            std::unique_lock<std::mutex> ulock(_mtx);
            while(on_reading.fetch_add(0) > 0 && !sameKey(_key.key, key))
                on_reading_cv.wait(ulock);
            key_mtx.lock();
            if (on_reading.fetch_add(0) > 0 && !sameKey(_key.key, key)) {
                key_mtx.unlock();
                continue;
            } else {
                if (sameKey(_key.key, key)) {
                    on_reading.fetch_add(1);
                    key_mtx.unlock();
                    readCache(_value);
                    return;
                }
                else {
                    // cache
                    memcpy(key, _key.key, 8); // change key
                    enable = false;
                    on_reading.fetch_add(1);
                    key_mtx.unlock();
                    // new cache
                    cache(_key);
                    // finish cache
                    enable = true;
                    enable_cv.notify_all();
                    readCache(_value);
                    return;
                }
            }
        }
    }
}

void RaceVisitor::visit(const IndexEntry &key) {
    string value;
    dataCache[count % MAX_CACHE_SIZE].read(key, &value);
    _visitor->Visit(PolarString(key.key, 8), PolarString(value));
    count ++;
}

}// namespace polar_race