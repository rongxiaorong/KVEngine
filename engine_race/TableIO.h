// TableIO.h
#ifndef _ENGINE_RACE_TABLEIO_H_
#define _ENGINE_RACE_TABLEIO_H_

#include <atomic>
#include "util.h"
#include "../include/engine.h"
#include "MemTable.h"
#include "config.h"
#include <map>
#include <hash_map>
namespace polar_race {

RetCode writeImmutTable(MemTable* table);

struct IndexEntry{
    char k[8];
    size_t p;
};

class TableWriter {
public:
    TableWriter(MemTable* table);
    ~TableWriter();
    RetCode open();
    RetCode write();
    RetCode flush();
private:

    MemTable* _table;
    WritableFile* _file;
    IndexEntry* _index;
    RetCode _write_data();
    RetCode _write_index();
    RetCode _write_footer();
    RetCode _write_bloomfilter();
    //
    // DataBlock [size_t keysize][...key][size_t valuesize][...value]
    // IndexBlock [size_t k1][size_t k2]...
    // FilterBlock [bits...]
    // FooterBlock [size_t index_size][size_t filter_size][magic.size MagicString]
    // 
};

// reader for sstable
// shared reader
// keep open
class TableReader {
public:
    // check cache before this
    TableReader():_file(nullptr){_using = 0;}
    
    RetCode open(int id);

    RetCode checkFilter(const string& key, bool& find);
    
    RetCode read(const string& key, string* value);

private:
    
    std::atomic_int _using;
    int _id;
    std::map<string, size_t>* _index_cache = nullptr;
    size_t _fsize;
    size_t _filter_head;
    size_t _filter_size;
    size_t _index_head;
    size_t _index_size;
    RandomAccessFile* _file;
    
    std::mutex mtx;

    RetCode readIndex(const string& key, string* value);
    RetCode cacheIndex();
    RetCode readValue(size_t offset, string* value);
    size_t binarySearch(const string& key, const IndexEntry* _index_array, int len);
};

extern std::map<int, TableReader*> SSTableMap;


} // namespace polar_race
#endif // _ENGINE_RACE_TABLEIO_H_