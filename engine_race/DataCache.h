#ifndef _ENGINE_RACE_DATA_CACHE_
#define _ENGINE_RACE_DATA_CACHE_

#include "../include/engine.h"
#include "util.h"
#include <thread>
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
    void init(int n) {
        // value = (char*) malloc(4096);
        value = nullptr;
        id = n;
    }
    void cache(const IndexEntry &_key);
    void read(const IndexEntry &_key, char* _value);
    void readCache(char* _value);
    static bool sameKey(const char* k1, const char* k2) {
        return memcmp(k1, k2, 8) == 0;
    }
private:
    int id;
    char key[8];
    char* value = nullptr;
    bool enable = true;
    std::mutex enable_mtx;
    std::condition_variable enable_cv;
    std::mutex key_mtx;
 
    std::atomic_int on_reading;
    std::condition_variable on_reading_cv;
};

extern DataCache dataCache[MAX_CACHE_SIZE];

class DataCache2 {
public:
    void read(const IndexEntry &_key, int slot, char* _value);
    void cache();
    void cache(int n);
    void run();
    bool isRun = false;
private:

    std::thread* cacher = nullptr;
    typedef char ValueBuffer[4096];
    typedef char KeyBuffer[8];
    KeyBuffer* kbuf = nullptr;
    ValueBuffer* vbuf = nullptr;
    std::mutex *mtx = nullptr;
    std::condition_variable *empty_cv = nullptr;
    std::condition_variable *full_cv = nullptr;
    int* readers = nullptr;
    bool sameKey(const char* k1, const char* k2) {
        return memcmp(k1, k2 ,8) == 0;
    }
};

extern DataCache2 dataCache2;

void runDataCache2(int n);

} // namespace polar_race



#endif // _ENGINE_RACE_DATA_CHACHE_