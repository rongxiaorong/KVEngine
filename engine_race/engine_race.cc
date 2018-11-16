// Copyright [2018] Alibaba Cloud All rights reserved
#include "engine_race.h"
#include "DataFile.h"
#include "IndexFile.h"
#include <atomic>
#include <thread>
namespace polar_race {
std::atomic_ulong global_count(0);
DataFile dataFile[DATA_FILE_NUM];
string engineDir;
std::thread* index_writer = nullptr;
MemIndex memIndex;
IndexReader indexReader;
RetCode Engine::Open(const std::string& name, Engine** eptr) {
    return EngineRace::Open(name, eptr);
}

Engine::~Engine() {
}

/*
 * Complete the functions below to implement you own engine
 */

void recover(unsigned long max_stamp) {
    int start[DATA_FILE_NUM] = {0};
    DataEntry entry;
    for (int i = 0; i < DATA_FILE_NUM; i++) {
        for (int j = 1; ;j++) {
            if (dataFile[i].last(1, &entry) == kSucc) {
                if (entry.stamp > global_count.fetch_add(0)) 
                    start[i] = j;
                else 
                    break;
            } else
                break;
        }
    }
    while (global_count.fetch_add(0) <= max_stamp) {
        for (int i = 0; i < DATA_FILE_NUM; i++) {
            if (start[i] <= 0) continue;
            dataFile[i].last(start[i], &entry) ;
            if (entry.stamp == global_count.fetch_add(0)) {
                memIndex.insert(entry.key, i, dataFile[i].size() - start[i] * sizeof(DataEntry), entry.stamp);
                global_count.fetch_add(1);
                start[i] --;
            }
        }
    }
}

void engineInit() {

    index_writer = new std::thread(IndexWriter);

    for (int i = 0; i < DATA_FILE_NUM; i++)
        dataFile[i].open(i);

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
        index_file.close();
    }

    unsigned long max_stamp = 0;
    for (int i = 0; i < DATA_FILE_NUM; i++) {
        DataEntry entry;
        if (dataFile[i].last(1, &entry) == kSucc) 
            max_stamp = std::max<unsigned long>(max_stamp, entry.stamp);
    }

    if (max_stamp > global_count.fetch_add(0))
        recover(max_stamp);
    
    
}

// 1. Open engine
RetCode EngineRace::Open(const std::string& name, Engine** eptr) {
    *eptr = NULL;
    EngineRace *engine_race = new EngineRace(name);

    engineDir = name;
    engineInit();

    *eptr = engine_race;
    return kSucc;
}

// 2. Close engine
EngineRace::~EngineRace() {
    memIndex.persist(global_count.fetch_add(0) - 1);
    indexWriterCV.notify_all();
    index_writer->join();
    INFO("886");
}

// 3. Write a key-value pair into engine
RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {

    unsigned long stamp = global_count.fetch_add(1);
    unsigned long offset = -1;
    int id = stamp % DATA_FILE_NUM;
    
    ASSERT(dataFile[id].write(key.data(), value.data(), stamp, offset));
    ASSERT(memIndex.insert(key.data(), id, offset, stamp));

    return kSucc;
}

// 4. Read value of a key
RetCode EngineRace::Read(const PolarString& key, std::string* value) {
    if (!indexReader.isopen())
        indexReader.open();

    IndexEntry entry;
    IndexEntry* indexEntry = nullptr;

    if (memIndex.find(key.data(), &entry) == kSucc)
        indexEntry = &entry;
    else
        indexEntry = indexReader.find(key);

    if (indexEntry == nullptr)
        return kNotFound;
    else {
        char buf[4096];
        RetCode ret = dataFile[indexEntry->num].readValue(indexEntry->offset, buf);
        value->assign(buf, 4096);
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
    return kSucc;
}

}  // namespace polar_race

