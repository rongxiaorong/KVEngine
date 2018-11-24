// Copyright [2018] Alibaba Cloud All rights reserved
#include "engine_race.h"
#include "DataFile.h"
#include "IndexFile.h"
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
AvlIndexFile avlIndexFile;
IndexReader indexReader;
DataCache dataCache[MAX_CACHE_SIZE];
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
        // memIndex.insert(entry.key, i, target * sizeof(DataEntry), entry.stamp);
        avlIndexFile.insert(entry.key, i, target * sizeof(DataEntry), entry.stamp);
        if (entry.stamp != global_count.fetch_add(0))
            ERROR("recover count:%ld, find table%d.last(%d) size=%ld, stamp=%ld", 
                global_count.fetch_add(0), i, target, dataFile[i].size(), entry.stamp
            );
    }
    INFO("finish recover");

}

void engineInit() {
    // if (!evalue_test) {
    //     index_writer = new std::thread(IndexWriter);
    mem_writer = new std::thread(MemIndexWriter);
    // }
    for (int i = 0; i < DATA_FILE_NUM; i++)
        dataFile[i].open(i);
    for (int i = 0; i < MAX_CACHE_SIZE; i++)
        dataCache[i].init();
    // if (!fileExist(engineDir.c_str(), INDEX_FILE_NAME)) {
    //     IndexFile index_file;
    //     index_file.open(true);
    //     index_file.write_stamp(0);
    //     index_file.close();
    //     index_file.toIndexFile();
    // } else {
    //     IndexFile index_file;
    //     index_file.open(false);
    //     global_count.fetch_add(index_file.get_stamp());
    //     if (global_count.fetch_add(0) != 0)
    //         global_count.fetch_add(1);
    //     index_file.close();
    // }

    avlIndexFile.open();
    global_count.fetch_add(avlIndexFile.index_stamp);
    if (global_count.fetch_add(0) != 0)
        global_count.fetch_add(1);

    unsigned long max_stamp = 0;
    for (int i = 0; i < DATA_FILE_NUM; i++) {
        DataEntry entry;
        if (dataFile[i].last(1, &entry) == kSucc) {
            max_stamp = std::max<unsigned long>(max_stamp, entry.stamp);
            INFO("dataFile%d size:%ld last_stamp:%ld", i, dataFile[i].size(), entry.stamp);
        }
        
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

    engineDir = name;
    DIR* _dir = opendir(name.c_str());
    if (_dir == NULL) {
        if (mkdir(name.c_str(), 0777) == 0)
            INFO("Create dir %s.", name.c_str());
        evalue_test = true;
        // global_buffer = new IndexBuffer(INDEX_BUFFER_SIZE);
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
    
    mem_end_flag = true;
    memIndexWriterCV.notify_one();
    mem_writer->join();

    avlIndexFile.save();

    // memIndex.persist(global_count.fetch_add(0) - 1);
    // end_flag = true;
    // indexWriterCV.notify_one();
    // index_writer->join();
    // if (evalue_test) {
    //     global_buffer->sort();
    //     global_buffer->persist();
    // }
    INFO("886");
    // if (evalue_test)
    //     exit(0);
}

// 3. Write a key-value pair into engine
RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {

    unsigned long stamp = global_count.fetch_add(1);
    unsigned long offset = -1;
    int id = stamp % DATA_FILE_NUM;
    
    ASSERT(dataFile[id].write(key.data(), value.data(), stamp, offset));
    
    if (stamp % 1000000 == 0) 
        INFO("finish %ld", stamp);
    
    // ASSERT(memIndex.insert(key.data(), id, offset, stamp));

    return kSucc;
}

// 4. Read value of a key
RetCode EngineRace::Read(const PolarString& key, std::string* value) {
    // if (!indexReader.isopen())
    //     indexReader.open();

    // IndexEntry entry;
    IndexEntry* indexEntry = nullptr;
    indexEntry = avlIndexFile.find(key);
    // if (memIndex.find(key.data(), &entry) == kSucc)
    //     indexEntry = &entry;
    // else
    //     indexEntry = indexReader.find(key);

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
    PolarString begin, end;
    RaceVisitor _visitor(visitor);
    if (lower == "" && upper == "") {
        avlIndexFile.foreach(RaceVisitor::visit, _visitor);
        return kSucc;
    }
    if (lower == "") 
        begin = PolarString(avlIndexFile.tree.minimum()->data.key, 8);
    else 
        begin = PolarString(avlIndexFile.tree.find(IndexEntry(lower.data(), 0, 0))->data.key, 8);
    
    if (upper == "")
        end = PolarString(avlIndexFile.tree.maximum()->data.key, 8);
    else 
        end = PolarString(avlIndexFile.tree.find(IndexEntry(upper.data(), 0, 0))->data.key, 8);
     
    avlIndexFile.foreach(begin, end, RaceVisitor::visit, _visitor);
     
    return kSucc;
}

}  // namespace polar_race

