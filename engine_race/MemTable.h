// MemTable.h
#ifndef _ENGINE_RACE_MEMTABLE_H_
#define _ENGINE_RACE_MEMTABLE_H_

#include "util.h"
#include "config.h"
#include "../include/engine.h"
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <list>
#include <map>
#include <string>
#include <thread>
#include <vector>

namespace polar_race {
using std::string;

extern int TABLE_COUNT;

extern std::list<std::thread*> thread_list;


class MemTable {
public:

    static MemTable* getMemtable();

    MemTable(){
        // _filter = new BloomFilter(FILTER_SIZE, MEMTABLE_MAX_SIZE/4096);
        _id = TABLE_COUNT;
        INFO("Creating MemTable%d", _id);
        TABLE_COUNT++;
        size.fetch_and(0);
        _on_reading = 0;
        _on_writing = 0;
        _mutable = this;
        _immut = false;
        
    }
    ~MemTable() {
        std::unique_lock<std::mutex> ulock(_on_reading_mtx);
        while (_on_reading != 0)
            _on_reading_cv.wait(ulock);
        for (auto iter : index) 
            delete iter.second;
    }

    // set a mutex for write a new entry 
    // update entries can be free
    RetCode write(const PolarString& key, const PolarString& value);
    
    bool contains(const PolarString& key);

    RetCode read(const PolarString& key, string* value);
    
    int id(){return _id;}

    void setID(int new_id){_id = new_id;}

    void unset_auto_write(){_auto_write = false;}
    
    long get_size(){return size.fetch_add(0);}
private:
    static std::mutex table_mtx;
    static std::mutex immut_mtx;
    
    static MemTable* _mutable;
    static MemTable* immut;
    
    int _id;

    bool _auto_write = true;

    std::map<string, string*> index;
    // BloomFilter* _filter;

    std::atomic_long size;

    int _on_writing;
    std::mutex _on_writing_mtx;
    std::condition_variable _on_writing_cv;

    int _on_reading;
    std::mutex _on_reading_mtx;
    std::condition_variable _on_reading_cv;

    std::mutex _write_mtx;
    RetCode _write(const PolarString& key, const PolarString& value);
    
    RetCode _update(const PolarString& key, const PolarString& value);
    
    bool _immut = false;
    
    void setImmutable();
    friend class TableWriter;
    friend void writeImmutTable(MemTable* table);
};

class ImmutTableList {
public:
    ImmutTableList(){}
    MemTable* get(int n);
    void set(int n, MemTable* table);
    void remove(int n);
private:
    std::mutex _mtx;
    std::vector<MemTable*> _list = std::vector<MemTable*>(500);
};

extern ImmutTableList immutTableList;

} // namespace polar_race
#endif //_ENGINE_RACE_MEMTABLE_H_