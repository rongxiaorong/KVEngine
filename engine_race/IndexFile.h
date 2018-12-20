#ifndef _ENGINE_RACE_INDEXFILE_H_
#define _ENGINE_RACE_INDEXFILE_H_

#include "config.h"
#include "util.h"
#include <map>
#include <mutex>
#include <queue>
#include <algorithm>
#include <thread>
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
        // _buf[_whead++] = entry;
        memcpy(_buf + _whead, &entry, sizeof(IndexEntry));
        _whead ++;

        if (_whead % 8000000 == 0) {
            std::thread* sort_thread = new std::thread(_sort, _whead - 8000000, _whead);
            thread_list.push(sort_thread);
            INFO("sort thread %d", _whead);
        }

    }
    void write(const IndexEntry& entry, unsigned int offset) {
        // _buf[offset] = entry;
        memcpy(_buf + offset, &entry, sizeof(IndexEntry));
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
        INFO("persist size %ld", _size);
        return kSucc;
    }
    static bool cmpIndexEntry(const IndexEntry& x, const IndexEntry& y) {
        return memcmp(x.key, y.key, 8) < 0;
    }
    static int cmpIndexEntry2(const void* x,const void* y) {
        return string(((IndexEntry*)x)->key, 8).compare(string(((IndexEntry*)y)->key, 8));
    }
    void sort() {
        // std::sort(_buf, _buf + _size, cmpIndexEntry);
        // ::qsort(_buf, _size, sizeof(IndexEntry), cmpIndexEntry2);
        int count = 0;
        std::thread* eight[4];
        INFO("merge 8");
        while(thread_list.size() != 0) {
            std::thread* t1 = thread_list.front();
            thread_list.pop();
            std::thread* t2 = thread_list.front();
            thread_list.pop();

            t1->join();
            t2->join();

            // count += 1;
            eight[count] = new std::thread(_merge, count * 16000000, count * 16000000 + 8000000, (count + 1) * 16000000);
            count++;
        }
        std::thread* sixteen[2];
        eight[0]->join();
        eight[1]->join();
        INFO("merge 16 1");
        sixteen[0] = new std::thread(_merge, 0, 16000000, 32000000);
        eight[2]->join();
        eight[3]->join();
        INFO("merge 16 2");
        sixteen[1] = new std::thread(_merge, 32000000, 48000000, 64000000);
        
        sixteen[0]->join();
        sixteen[1]->join();
        INFO("merge 32");
        merge(0, 32000000, 64000000);
        INFO("finish merge");
    }
    void sort(int begin, int end) {
        std::sort(_buf + begin, _buf + end, cmpIndexEntry);
    }
    void merge(int begin, int mid, int end) {
        std::inplace_merge(_buf + begin, _buf + mid, _buf + end);
    }
    static void _merge(int begin, int mid, int end);
    static void _sort(int begin, int end); 

    int size() {return _whead;}
private:
    int _size = 0;
    int _whead = 0;
    int _rhead = 0;
    std::mutex _mtx;
    IndexEntry* _buf;
    std::queue<std::thread*> thread_list;
};

class IndexReader {
public:
    IndexReader() {}
    RetCode open();
    bool isopen() {return _isopen;}
    IndexEntry* find(const PolarString& key);
    IndexEntry* first() {
        return _index;
    }
    IndexEntry* last() {
        return _index + _count;
    }
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
    std::map<string, IndexEntry> * _index = nullptr;   
private:
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