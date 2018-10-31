// TableIO.h

#include <sstream>
#include "util.h"
#include "../../include/engine.h"
#include "MemTable.h"
#include "config.h"
#include "TableIO.h"

namespace polar_race {

TableWriter::TableWriter(MemTable* table) {
    _table = table;
    _file = nullptr;
}

RetCode TableWriter::open() {
    int id = _table->_id;

    // get table name
    std::stringstream sstream;
    sstream << SSTABLE_FILE_NAME << id;
    std::string table_name; 
    sstream >> table_name;

    // create sstable
    WritableFile* sstable = new WritableFile();
    ASSERT(sstable->open(table_name.c_str()));
    _file = sstable;
    return RetCode::kSucc;
}

RetCode TableWriter::write() {
    if (_file == nullptr) {
        ERROR("TableWriter::write() _file = nullptr");
        return RetCode::kNotFound;
    }
    ASSERT(_write_data());
    ASSERT(_write_index());
    ASSERT(_write_magic());
    
    ASSERT(_file->flush());
    ASSERT(_file->close());

    return RetCode::kSucc;
}

RetCode TableWriter::_write_data() {
    index = new size_t[_table->index.size()];
    int i = 0;
    auto iter = _table->index.begin();
    while (iter != _table->index.end()) {
        index[i] = _file->size();
        size_t key_size = iter->first.size();
        size_t value_size = iter->second->size();
        ASSERT(_file->append((char*)&key_size, sizeof(key_size)));
        ASSERT(_file->append(iter->first.c_str(), iter->first.size()));
        ASSERT(_file->append((char*)&value_size, sizeof(value_size)));
        ASSERT(_file->append(iter->second->c_str(), iter->second->size()));
        i++;
        iter++;
    }
    return RetCode::kSucc;
}

RetCode TableWriter::_write_index() {
    size_t size = _table->index.size();
    ASSERT(_file->append((char *)index, sizeof(size_t) * _table->index.size()));
    ASSERT(_file->append((char *)&size, sizeof(size_t)));
    return RetCode::kSucc;

}

RetCode TableWriter::_write_magic() {
    string magic(MAGIC_STRING);
    ASSERT(_file->append(magic.c_str(), magic.size()));
    return RetCode::kSucc;
}

} // namespace polar_race