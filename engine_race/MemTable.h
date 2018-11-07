// MemTable.h
#ifndef _ENGINE_RACE_MEMTABLE_H_
#define _ENGINE_RACE_MEMTABLE_H_

#include "util.h"
#include "config.h"
#include "../include/engine.h"
#include "BloomFilter.h"
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <list>
#include <map>
#include <string>

namespace polar_race {
using std::string;

int TABLE_COUNT = 0;

class MemTable {
public:

    static MemTable* getMemtable();
    static MemTable* getImmut();

    MemTable(){
        _filter = new BloomFilter(FILTER_SIZE, MEMTABLE_MAX_SIZE/4096);
        _id = TABLE_COUNT;
        TABLE_COUNT++;
    }

    // set a mutex for write a new entry 
    // update entries can be free
    RetCode write(const PolarString& key, const PolarString& value);
    
    bool contains(const PolarString& key);

    RetCode read(const PolarString& key, string* value);
    
    int id(){return _id;}

private:
    static std::mutex table_mtx;
    static std::mutex immut_mtx;
    
    static MemTable* _mutable;
    static MemTable* immut;
    
    int _id;

    std::map<string, string*> index;
    BloomFilter* _filter;

    std::atomic_long size;

    std::atomic_int _on_writing;
    std::condition_variable _on_writing_cv;


    std::mutex _write_mtx;
    RetCode _write(const PolarString& key, const PolarString& value);
    
    RetCode _update(const PolarString& key, const PolarString& value);
    
    bool _immut = false;
    void setImmutable();
    
    friend class TableWriter;
    friend RetCode writeImmutTable(MemTable* table);
};

} // namespace polar_race
#endif //_ENGINE_RACE_MEMTABLE_H_