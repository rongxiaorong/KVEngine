#ifndef _ENGINE_RACE_DATA_CACHE_
#define _ENGINE_RACE_DATA_CACHE_

#include "../include/engine.h"
#include "util.h"
#include <atomic>
namespace polar_race {
class RaceVisitor {
public:
    RaceVisitor(){}
    RaceVisitor(Visitor &visitor):_visitor(&visitor){}
    static void visit(const IndexEntry &key, RaceVisitor &visitor) {
        visitor.visit(key);
    }
    void visit(const IndexEntry &key);
private:
    Visitor* _visitor = nullptr;
    unsigned long count = 0;
};

class DataCache {
public:
    DataCache(){
        on_reading.fetch_and(0);
    }
    void init() {
        value = (char*) malloc(4096);
    }
    void cache(const IndexEntry &_key);
    void read(const IndexEntry &_key, string* _value);
    void readCache(string* _value);
    static bool sameKey(const char* k1, const char* k2) {
        return PolarString(k1, 8).compare(PolarString(k2, 8)) == 0;
    }
private:
    char key[8];
    char* value;
    bool enable = true;
    std::mutex enable_mtx;
    std::condition_variable enable_cv;
    std::mutex key_mtx;

    std::atomic_int on_reading;
    std::condition_variable on_reading_cv;
};

extern DataCache dataCache[MAX_CACHE_SIZE];
} // namespace polar_race



#endif // _ENGINE_RACE_DATA_CHACHE_