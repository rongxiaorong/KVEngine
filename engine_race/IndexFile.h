#ifndef _ENGINE_RACE_INDEXFILE_H_
#define _ENGINE_RACE_INDEXFILE_H_

#include "config.h"
#include "util.h"
#include "AvlTree.h"
#include <map>
#include <mutex>
#include <queue>
#include <algorithm>
namespace polar_race {


extern std::priority_queue<std::pair<unsigned long, std::map<string, IndexEntry>* > > indexQueue;
extern std::condition_variable indexWriterCV;
extern std::mutex indexWriterMtx;
extern bool end_flag;
extern string engineDir;

class AvlIndexFile {
public:
    unsigned long size = 0;
    unsigned long index_stamp = 0;
    AvlTree<IndexEntry> tree;
    AvlIndexFile(){
        
    }
    ~AvlIndexFile(){}
    RetCode open();
    RetCode save();
    IndexEntry* find(const PolarString& key);
    RetCode insert(const char* key, const int &num, const unsigned long offset, const unsigned long stamp);
    void foreach(void (*func)(const IndexEntry &key, RaceVisitor &visitor), RaceVisitor &visitor);
    void foreach(const PolarString &beign, const PolarString &end, void (*func)(const IndexEntry &key, RaceVisitor &visitor), RaceVisitor &visitor);
private:

};

extern AvlIndexFile avlIndexFile;

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
class IndexBuffer {
public:
    IndexBuffer(int size) {
        _size = size;
        _buf = (IndexEntry*) malloc(_size * sizeof(IndexEntry));
    }
    ~IndexBuffer() {
        free(_buf);
    }
    void append(const IndexEntry& entry) {
        std::lock_guard<std::mutex> guard(_mtx);
        _buf[_whead++] = entry;
    }
    void write(const IndexEntry& entry, unsigned int offset) {
        _buf[offset] = entry;
    }
    IndexEntry* next() {
        if(_rhead < _whead)
            return &(_buf[_rhead++]);
        else
            return nullptr;
    }
    void copy(const IndexEntry* src);
    RetCode persist() {
        string index_name = engineDir + "/" + INDEX_FILE_NAME;
        if (fileExist(index_name.c_str())) 
            ::remove(index_name.c_str());
        int fd = ::open(index_name.c_str(), O_RDWR | O_APPEND | O_CREAT, 0777);
        if (fd <= 0)
            return kIOError;
        unsigned long size = _size;
        ::write(fd, &size, sizeof(size));
        ::write(fd, _buf, sizeof(IndexEntry) * _size);
        ::close(fd);
        return kSucc;
    }
    static bool cmpIndexEntry(const IndexEntry& x, const IndexEntry& y) {
        return PolarString(x.key, 8).compare(PolarString(y.key, 8)) < 0;
    }
    
    void sort() {
        std::sort(_buf, _buf + _size, cmpIndexEntry);
    }
    int size() {return _whead;}
private:
    int _size = 0;
    int _whead = 0;
    int _rhead = 0;
    std::mutex _mtx;
    IndexEntry* _buf;
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
void MergeIndex(const std::map<string, IndexEntry>* _mem_index, IndexBuffer *_index_buf, IndexBuffer * _tmp_buf);

extern std::mutex memIndexWriter_mtx;
extern std::condition_variable memIndexWriterCV;
extern bool mem_end_flag;
void MemIndexWriter();

void MemIndexInsert(const char* key, const int &num, const unsigned long offset, const unsigned long stamp);

extern BlockedQueue<std::pair<unsigned long, IndexEntry> > blockQueue;

} // namespace polar_race



#endif