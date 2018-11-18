#ifndef _ENGINE_RACE_INDEXFILE_H_
#define _ENGINE_RACE_INDEXFILE_H_

#include "config.h"
#include "util.h"
#include <map>
#include <mutex>
#include <queue>
namespace polar_race {


extern std::priority_queue<std::pair<unsigned long, std::map<string, IndexEntry>* > > indexQueue;
extern std::condition_variable indexWriterCV;
extern std::mutex indexWriterMtx;
extern bool end_flag;
extern string engineDir;

class IndexFile {
public:
    IndexFile(){}
    ~IndexFile(){close();}
    RetCode open(bool is_tmp);
    RetCode toIndexFile();
    RetCode close();
    RetCode remove();
    
    IndexEntry* next();
    RetCode write(const IndexEntry& _entry);
    RetCode write_stamp(const unsigned long stamp);
    
    unsigned long get_stamp() {
        return _stamp;
    }
private:
    string _fname;
    unsigned long _stamp = 0;
    FILE* _file = nullptr;
    IndexEntry _buf[10];
};

class IndexReader {
public:
    IndexReader() {}
    RetCode open();
    bool isopen() {return _isopen;}
    IndexEntry* find(const PolarString& key);
private:
    int _fd = -1;
    IndexEntry* _index = nullptr;
    int _count;
    bool _isopen = false;
    IndexEntry* binarySearch(const PolarString& key);
};

extern IndexReader indexReader;

class MemIndex {
public:
    MemIndex():_index(new std::map<string, IndexEntry>()){}
    ~MemIndex(){if (_index) delete _index;}
    RetCode find(const char* key, IndexEntry* entry);
    RetCode insert(const char* key, const int &num, const unsigned long offset, const unsigned long stamp);
    RetCode persist(const unsigned long stamp);
private:
    std::map<string, IndexEntry> * _index = nullptr;
    std::mutex _write_mtx;
};
extern MemIndex memIndex;
void IndexWriter();

void MergeIndex(const std::map<string, IndexEntry>* _mem_index, IndexFile& _index_file, IndexFile& _tmp_file);

extern std::mutex memIndexWriter_mtx;
extern std::condition_variable memIndexWriterCV;
extern bool mem_end_flag;
void MemIndexWriter();

void MemIndexInsert(const char* key, const int &num, const unsigned long offset, const unsigned long stamp);

extern BlockedQueue<std::pair<unsigned long, IndexEntry> > blockQueue;

} // namespace polar_race



#endif