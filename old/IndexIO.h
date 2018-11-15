#ifndef _ENGINE_RACE_INDEX_IO_H_
#define _ENGINE_RACE_INDEX_IO_H_
#include <queue>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <stdlib.h>
#include <sys/mman.h>
#include "util.h"
#include "config.h"
namespace polar_race {

// struct IndexEntry;
 
class HashTable {
public:
    struct HashNode {
        HashNode(IndexEntry* entry):_entry(-1),_next(-1){}
        int _entry;
        int _next;
    };
    HashTable(int size):_hash_size(size){
        _table = (int*)malloc(sizeof(int) * size);
        _nodes = (HashNode*)malloc(sizeof(HashNode) * size);
        for (int i = 0; i < size; i++) {
            _table[i] = -1;
            _nodes[i]._entry = -1;
            _nodes[i]._next = -1;
        }
    }
    ~HashTable(){if(_table) delete _table;}
    RetCode insert(const IndexEntry * ent, int offset);
    int find(const char* key, const IndexEntry* idx);
private:
    int _hash_size = 1;
    int* _table = nullptr;
    int count = 0;
    HashNode* _nodes = nullptr;
    unsigned int hash(const char * k) {
        unsigned long long int * key = (unsigned long long int*)k;
        return (unsigned int)((*key) % 64000031)%HASHTABLE_SIZE;
    }
};

class IndexReader {
public:
    IndexReader(){}
    RetCode open() {
        std::stringstream sstream;
        sstream << __engine_dir << "/" << INDEX_FILE_NAME;
        string new_name;
        sstream >> new_name;

        if (access(new_name.c_str(), F_OK) == -1)
            return kNotFound;
        // get size
        struct stat statbuf;
        stat(new_name.c_str(), &statbuf);
        size_t fsize = statbuf.st_size;
        INFO("Open Index size:%ld", fsize);
        _fd = ::open(new_name.c_str(), O_RDONLY);
        if (_fd <= 0)
            return kIOError;
        _index = (IndexEntry*) mmap(NULL, fsize, PROT_READ, MAP_SHARED, _fd, 0);
        if (_index == NULL) {
            ::close(_fd);
            _fd = -1;
            return kIOError;
        }
        _size = fsize / 16;
        is_open = true;
        initHashTable();
        return kSucc;
    }
    IndexEntry* find(const PolarString& key);
    bool is_open = false;
private:

    int _fd = -1;
    IndexEntry* _index = nullptr;
    int _size = 0;

    HashTable* _hash_table = nullptr;
    IndexEntry* binarySearch(const PolarString& key);
    RetCode initHashTable();
    friend class HashTable;
    
};
extern IndexReader *indexReader;
class IndexFile {
public:
    int size;
    IndexFile(string name):_fname(name){}
    IndexFile(string name, int _size){_fname = name; size = _size; _file = fopen(_fname.c_str(), "rb+");}
    IndexEntry* getNextEntry() {
        if (_begin >= _end) {
            _begin = 0;
            _end = fread(buf, 16, BUFFER_SIZE, _file);
        }
        if (_end == 0)
            return nullptr;
        else
            return &(buf[_begin++]);
    }
    string getName() {return _fname;}
    static string MergeIndex(IndexFile &l, IndexFile &r);
    void remove() {
        fclose(_file);
        ::remove(_fname.c_str());
    }
    friend bool operator < (const IndexFile &a, const IndexFile &b) {
        return a.size > b.size;
    }
private:
    string _fname;
    FILE* _file = nullptr;
    const static int BUFFER_SIZE = 1024;
    IndexEntry buf[BUFFER_SIZE];
    int _begin = 0, _end = 0;
    
};
extern std::priority_queue<IndexFile> index_queue;
extern std::condition_variable index_merger_cv;
extern std::mutex index_merger_mtx;
extern bool end_signal;
extern std::atomic_int IndexCount;

void IndexMerger();

} // namespace polar_race


#endif //_ENGINE_RACE_INDEX_IO_H_