// TableIO.h
#ifndef _ENGINE_RACE_TABLEIO_H_
#define _ENGINE_RACE_TABLEIO_H_

#include <atomic>
#include "util.h"
#include "../include/engine.h"
#include "MemTable.h"
#include "config.h"
#include <map>

namespace polar_race {

RetCode writeImmutTable(MemTable* table);

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
    size_t* _index;
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

    RetCode checkFilter(const string& key);
    
    RetCode read(const string& key, string& value);

private:
    // RetCode 
    std::atomic_int _using;
    int _id;
    RandomAccessFile* _file;
    
};

extern std::map<int, TableReader*> SSTableMap;


} // namespace polar_race
#endif // _ENGINE_RACE_TABLEIO_H_