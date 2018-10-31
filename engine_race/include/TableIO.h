// TableIO.h
#ifndef _ENGINE_RACE_TABLEIO_H_
#define _ENGINE_RACE_TABLEIO_H_

#include "util.h"
#include "../../include/engine.h"
#include "MemTable.h"
#include "config.h"
namespace polar_race {

class TableWriter {
public:
    TableWriter(MemTable* table);
    
    RetCode open();
    RetCode write();
    
private:
    MemTable* _table;
    WritableFile* _file;
    size_t* index;
    RetCode _write_data();
    RetCode _write_index();
    RetCode _write_magic();
    RetCode _write_bloomfilter();
    //
    // DataBlock [size_t keysize][...key][size_t valuesize][...value]
    // IndexBlock [size_t k1][size_t k2]...
    // FooterBlock [size_t size][magic.size MagicString]
    //
};

} // namespace polar_race
#endif //#ifndef _ENGINE_RACE_TABLEIO_H_