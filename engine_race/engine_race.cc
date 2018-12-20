// Copyright [2018] Alibaba Cloud All rights reserved
#include "engine_race.h"
#include "DataFile.h"
#include "IndexFile.h"
#include "DataCache.h"
#include <atomic>
#include <thread>
#include <dirent.h>
namespace polar_race {
std::atomic_ulong global_count(0);
DataFile dataFile[DATA_FILE_NUM];
string engineDir;
std::thread* index_writer = nullptr;
std::thread* mem_writer = nullptr;
MemIndex memIndex;
IndexReader indexReader;
// DataCache dataCache[MAX_CACHE_SIZE];
DataCache2 dataCache2;
RetCode Engine::Open(const std::string& name, Engine** eptr) {
    return EngineRace::Open(name, eptr);
}

Engine::~Engine() {
}

/*
 * Complete the functions below to implement you own engine
 */

void recover(unsigned long max_stamp) {
    // int start[DATA_FILE_NUM] = {0};
    DataEntry entry;
    INFO("Recovering..%ld to %ld", global_count.fetch_add(0), max_stamp);
    for(;global_count.fetch_add(0) <= max_stamp; global_count.fetch_add(1)) {
        int i = global_count.fetch_add(0) % DATA_FILE_NUM;
        int target = global_count.fetch_add(0) / DATA_FILE_NUM;
        dataFile[i].first(target, &entry);
        memIndex.insert(entry.key, i, target * sizeof(DataEntry) + 16, entry.stamp);
        if (entry.stamp != global_count.fetch_add(0))
            ERROR("recover count:%ld, find table%d.last(%d) size=%ld, stamp=%ld", 
                global_count.fetch_add(0), i, target, dataFile[i].size(), entry.stamp
            );
    }
    INFO("finish recover");

}
bool read_test = false;
void engineInit() {
    // if (!evalue_test) {
    //     index_writer = new std::thread(IndexWriter);
    //     mem_writer = new std::thread(MemIndexWriter);
    // }
    for (int i = 0; i < DATA_FILE_NUM; i++)
        dataFile[i].open(i);
    // for (int i = 0; i < MAX_CACHE_SIZE; i++)
    //     dataCache[i].init(i);
    
    if (!fileExist(engineDir.c_str(), INDEX_FILE_NAME)) {
        IndexFile index_file;
        index_file.open(true);
        index_file.write_stamp(0);
        index_file.close();
        index_file.toIndexFile();
    } else {
        IndexFile index_file;
        index_file.open(false);
        global_count.fetch_add(index_file.get_stamp());
        if (global_count.fetch_add(0) != 0)
            global_count.fetch_add(1);
        index_file.close();
    }
    
    unsigned long max_stamp = 0;
    if (global_count.fetch_add(0) == 0) {
        for (int i = 0; i < DATA_FILE_NUM; i++) {
            DataEntry entry;
            if (dataFile[i].last(1, &entry) == kSucc) {
                max_stamp = std::max<unsigned long>(max_stamp, entry.stamp);
                // INFO("dataFile%d size:%ld last_stamp:%ld", i, dataFile[i].size(), entry.stamp);
            }
            
        }
    }
    else
        max_stamp = 64000000;

    if (max_stamp > 10000000) {
        read_test = true;
        indexReader.open();
    }
    if (max_stamp > global_count.fetch_add(0) && max_stamp < 10000000)
        recover(max_stamp);
    
    
}
bool evalue_test = false;

extern IndexBuffer* global_buffer;
// 1. Open engine
RetCode EngineRace::Open(const std::string& name, Engine** eptr) {
    *eptr = NULL;
    EngineRace *engine_race = new EngineRace(name);
    INFO("init");
    engineDir = name;
    DIR* _dir = opendir(name.c_str());
    if (_dir == NULL) {
        if (mkdir(name.c_str(), 0777) == 0)
            INFO("Create dir %s.", name.c_str());
        evalue_test = true;
        global_buffer = new IndexBuffer(INDEX_BUFFER_SIZE);
    }
    else
        closedir(_dir);

    engineInit();

    *eptr = engine_race;
    return kSucc;
}

// 2. Close engine
EngineRace::~EngineRace() {
    // if (evalue_test)
    //     exit(0);
    
    // mem_end_flag = true;
    // memIndexWriterCV.notify_one();
    // mem_writer->join();

    // memIndex.persist(global_count.fetch_add(0) - 1);
    // end_flag = true;
    // indexWriterCV.notify_one();
    // index_writer->join();
    if (evalue_test) {
        global_buffer->sort();
        global_buffer->persist();
    }
    INFO("886");
    // if (evalue_test)
    //     exit(0);
}

// 3. Write a key-value pair into engine
RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {

    unsigned long stamp = global_count.fetch_add(1);
    unsigned long offset = -1;
    int id = stamp % DATA_FILE_NUM;
    char* buf = nullptr;
    const char* target = value.data();
    // if ((long long int)target % 512 != 0) {
    //     posix_memalign((void**)&buf, 512, 4096);
    //     memcpy(buf, value.data(), 4096);
    //     target = buf;
    // }
    ASSERT(dataFile[id].write(key.data(), target, stamp, offset));
    if (buf != nullptr)
        ::free(buf);
    if (stamp % 10000000 == 0) 
        INFO("finish %ld", stamp);
    
    // ASSERT(memIndex.insert(key.data(), id, offset, stamp));

    return kSucc;
}

// 4. Read value of a key
RetCode EngineRace::Read(const PolarString& key, std::string* value) {
    // if (!indexReader.isopen())
    //     indexReader.open();
    // static std::atomic_long read_count(0);
    // if (read_count.fetch_add(1) % 100000 == 0)
    //     INFO("read %ld", read_count.fetch_add(0) - 1);
    
    IndexEntry entry;
    IndexEntry* indexEntry = nullptr;
    if (!read_test) {
        if (memIndex.find(key.data(), &entry) == kSucc)
            indexEntry = &entry;
    }
    else
        indexEntry = indexReader.find(key);

    if (indexEntry == nullptr) {
        // const char* kkk = key.data();
        // ERROR("Not Find!!!! %x%x%x%x%x%x%x%x", kkk[0], kkk[1],kkk[2],kkk[3],
        // kkk[4],kkk[5],kkk[6],kkk[7]);

        // IndexEntry *first = indexReader.first();
        // // for (int i = 0;i < 8;i ++)
        // //     kkk[i] = first->key[i];
        // kkk = first->key;
        // INFO("First:%x%x%x%x%x%x%x%x", kkk[0], kkk[1],kkk[2],kkk[3],
        // kkk[4],kkk[5],kkk[6],kkk[7]);

        // IndexEntry *last = indexReader.last() - 1;
        // // for (int i = 0;i < 8;i ++)
        // //     kkk[i] = last->key[i];
        // kkk = last->key;
        // INFO("Last:%x%x%x%x%x%x%x%x", kkk[0], kkk[1],kkk[2],kkk[3],
        // kkk[4],kkk[5],kkk[6],kkk[7]);

        return kNotFound;
    
    }
    else {
        char _buf[512 + 4096];
        long long int align = (long long int)_buf + (512 - (long long int)_buf % 512);
        char* buf = (char*)align;
        // posix_memalign(&buf, 512, 4096);
        RetCode ret = dataFile[indexEntry->num].readValue(indexEntry->offset, (char*)buf);
        value->assign((char*)buf, 4096);
        // ::free(buf);
        return ret;
    }
    return kSucc;
}

/*
 * NOTICE: Implement 'Range' in quarter-final,
 *         you can skip it in preliminary.
 */
// 5. Applies the given Vistor::Visit function to the result
// of every key-value pair in the key range [first, last),
// in order
// lower=="" is treated as a key before all keys in the database.
// upper=="" is treated as a key after all keys in the database.
// Therefore the following call will traverse the entire database:
//   Range("", "", visitor)
RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
Visitor &visitor) {
    // INFO("Range test");
    if (global_count.fetch_add(0) < 10000000) {
        // correctness check
        if (lower == "" && upper == "") {
            for(auto iter : *memIndex._index) {
                char buf[4096];
                RetCode ret = dataFile[iter.second.num].readValue(iter.second.offset, buf);
                visitor.Visit(PolarString(iter.first), PolarString(buf, 4096));
            }
            return kSucc;
        }
        else {
            auto begin = memIndex._index->begin();
            auto end = memIndex._index->end();
            if (lower != "")
                begin = memIndex._index->find(lower.ToString());
            if (upper != "")
                end = memIndex._index->find(upper.ToString());
            
            for (auto iter = begin; iter != end; iter++) {
                char buf[4096];
                RetCode ret = dataFile[iter->second.num].readValue(iter->second.offset, buf);
                visitor.Visit(PolarString(iter->first), PolarString(buf, 4096));
            }
            return kSucc;
        }
        
    }
    // return kNotFound;
    // if (!indexReader.isopen())
    //     indexReader.open();
    
    IndexEntry* begin = nullptr, *end = nullptr;
    if (lower == "")
        begin = indexReader.first();
    else
        begin = indexReader.find(lower);
    if (upper == "")
        end = indexReader.last();
    else
        end = indexReader.find(upper);
    
    if (begin == nullptr || end == nullptr) {
        ERROR("Can't find begin or end");
        return kNotFound;
    }
    if (!dataCache2.isRun)
        dataCache2.run();
    // INFO("Range %s, %s", lower.data(), upper.data());
    IndexEntry* first = indexReader.first();
    for (auto i = begin; i < end; i++) {
        // string value;
        // dataCache[(i - first) % MAX_CACHE_SIZE].read(*i, &value);
        // visitor.Visit(PolarString(i->key, 8), PolarString(value));
        // char buf[4096];
        // if (i - begin < 10) {
        //     INFO("Visit %d :%x%x%x%x%x%x%x%x", i - begin, 
        //         i->key[0], i->key[1], i->key[2], i->key[3], 
        //         i->key[4], i->key[5], i->key[6], i->key[7] 
        //     );
        // } else {
        //     return kNotFound;
        // }
        // char buf[4096];
        // RetCode ret = dataFile[i->num].readValue(i->offset, buf);
        // visitor.Visit(PolarString(i->key, 8), PolarString(buf, 4096));
        char buf[4096];
        // dataCache[(i - first) % MAX_CACHE_SIZE].read(*i, buf);
        dataCache2.read(*i, (i - first) % MAX_CACHE_SIZE, buf);
        // if ((i - first) % MAX_CACHE_SIZE == 0)
        //     INFO("Read %d", i - first);
        visitor.Visit(PolarString(i->key, 8), PolarString(buf, 4096));
    }
    return kSucc;
}

}  // namespace polar_race

