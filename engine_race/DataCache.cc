#include "DataCache.h"
#include "DataFile.h"
#include "IndexFile.h"
namespace polar_race {

void DataCache::cache(const IndexEntry &_key) {
    if (value == nullptr)
        value = (char*) malloc(4096);
    static std::atomic_int count(0);
    if (count.fetch_add(1) < 20000) {
        INFO("Cache %d %d %02x %02x %02x %02x %02x %02x %02x %02x", count.fetch_add(0) - 1, id,
        _key.key[0] & 0xff, _key.key[1] & 0xff, _key.key[2] & 0xff, _key.key[3] & 0xff,
        _key.key[4] & 0xff, _key.key[5] & 0xff, _key.key[6] & 0xff, _key.key[7] & 0xff
        );
    }
    dataFile[_key.num].readValue(_key.offset, value);
}

void DataCache::readCache(char* _value) {
    // wait for the cacher
    std::unique_lock<std::mutex> ulock(enable_mtx);
    while(!enable)
        enable_cv.wait(ulock);
    ulock.unlock();
    // read from cache
    // _value->assign(value, 4096);
    memcpy(_value, value, 4096);
    on_reading.fetch_sub(1);
    on_reading_cv.notify_all();
}

void DataCache::read(const IndexEntry &_key, char* _value) {
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
                    enable_mtx.lock();
                    enable = false;
                    enable_mtx.unlock();
                    on_reading.fetch_add(1);
                    key_mtx.unlock();
                    // new cache
                    cache(_key);
                    // finish cache
                    enable_mtx.lock();
                    enable = true;
                    enable_cv.notify_all();
                    enable_mtx.unlock();
                    
                    readCache(_value);
                    return;
                }
            }
        }
    }
}

void RaceVisitor::visit(const IndexEntry &key) {
    // string value;
    // dataCache[count % MAX_CACHE_SIZE].read(key, &value);
    // _visitor->Visit(PolarString(key.key, 8), PolarString(value));
    // count ++;
}

extern IndexReader indexReader;
void DataCache2::cache() {
    int count = 0;
    IndexEntry* first = indexReader.first();
    static std::atomic_int global_count(0);
    while (true) {
        // cache loop
        // if (count < 1000)
        //     INFO("Caching %d",count);
        count = global_count.fetch_add(1) % INDEX_BUFFER_SIZE;
        int slot = count % MAX_CACHE_SIZE;
        
        // wait for readers finish reading
        std::unique_lock<std::mutex> ulock(mtx[slot]);
        while(readers[slot] > 0)
            empty_cv[slot].wait(ulock);
        // all readers finish reading
        // cache new entry

        IndexEntry* new_key = first + count;
        
        dataFile[new_key->num].readValue(new_key->offset, vbuf[slot]);
        // change key
        memcpy(kbuf[slot], new_key->key, 8);
        // wake readers
        readers[slot] = TEST_THREADS;
        full_cv[slot].notify_all();
        // count ++;
    }
}


void DataCache2::cache(int n) {
    int count = 0;
    int part = MAX_CACHE_SIZE / CACHE_THREADS;
    IndexEntry* first = indexReader.first();

    for (int round = 0; round < INDEX_BUFFER_SIZE / MAX_CACHE_SIZE; round ++) {
        count = round * MAX_CACHE_SIZE + n * part;
        for (int i = 0; i < part; i ++) {
            int slot = count % MAX_CACHE_SIZE;
        
            // wait for readers finish reading
            std::unique_lock<std::mutex> ulock(mtx[slot]);
            while(readers[slot] > 0)
                empty_cv[slot].wait(ulock);
            // all readers finish reading
            IndexEntry* new_key = first + count;
            
            dataFile[new_key->num].readValue(new_key->offset, vbuf[slot]);
            // change key
            memcpy(kbuf[slot], new_key->key, 8);
            // wake readers
            readers[slot] = TEST_THREADS;
            full_cv[slot].notify_all();
            count ++;
        }
    }
}

void DataCache2::read(const IndexEntry &_key, int slot, char* _value) {
    std::unique_lock<std::mutex> ulock(mtx[slot]);
    while(readers[slot] <= 0 || !sameKey(_key.key, kbuf[slot]))
        full_cv[slot].wait(ulock);
    ulock.unlock();
    
    // read cache
    memcpy(_value, vbuf[slot], 4096);
    
    ulock.lock();
    readers[slot] --;
    if (readers[slot] == 0)
        empty_cv[slot].notify_all();
    ulock.unlock();
}

void DataCache2::run() {
    static std::mutex begin_mtx;
    std::lock_guard<std::mutex> guard(begin_mtx);
    if (isRun)
        return;

    kbuf = (KeyBuffer*) malloc(MAX_CACHE_SIZE * sizeof(KeyBuffer));
    
    void* buf = nullptr;
    posix_memalign(&buf, 512, 4096 * MAX_CACHE_SIZE);
    vbuf = (ValueBuffer*) buf;//malloc(MAX_CACHE_SIZE * sizeof(ValueBuffer));
    readers = new int[MAX_CACHE_SIZE]();
    mtx = new std::mutex[MAX_CACHE_SIZE]();
    empty_cv = new std::condition_variable[MAX_CACHE_SIZE]();
    full_cv = new std::condition_variable[MAX_CACHE_SIZE]();

    // cacher = new std::thread(runDataCache2);
    // cacher->detach();
    std::thread* cachers[CACHE_THREADS];
    for(int i = 0; i < CACHE_THREADS; i++) {
        cachers[i] = new std::thread(runDataCache2, i);
        cachers[i]->detach();
    }
    // std::thread watcher(Watcher);
    // watcher.detach();
    isRun = true;
}

// void runDataCache2() {
//     INFO("Start cache");
//     dataCache2.cache();
// }

void runDataCache2(int n) {
    while(1) {
        // INFO("Start cache %d", n);
        // if (n == 0) {
        //     INFO("New round");
        // }
        // dataCache2.cache(n);
        dataCache2.cache();
    }
}
}// namespace polar_race