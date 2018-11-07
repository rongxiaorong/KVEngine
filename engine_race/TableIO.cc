// TableIO.h

#include <sstream>
#include "util.h"
#include "../../include/engine.h"
#include "MemTable.h"
#include "config.h"
#include "TableIO.h"

namespace polar_race {

RetCode writeImmutTable(MemTable* table) {
    // wait for all writers of the table
    while(table->_on_writing);
    TableWriter tableWriter(table);
    ASSERT(tableWriter.open());
    ASSERT(tableWriter.write());
}

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
    ASSERT(_write_bloomfilter());
    ASSERT(_write_footer());
    
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
    ASSERT(_file->append((char *)index, sizeof(size_t) * _table->index.size()));
    return RetCode::kSucc;

}

RetCode TableWriter::_write_bloomfilter() {
    ASSERT(_file->append((char* )_table->_filter.data(), _table->_filter.size()));
    return RetCode::kSucc;
}

RetCode TableWriter::_write_footer() {
    size_t index_size = _table->index.size();
    size_t filter_size = _table->_filter.size();
    ASSERT(_file->append((char*)&index_size, sizeof(size_t)));
    ASSERT(_filt->append((char*)&filter_size, sizeof(size_t)));
    string magic(MAGIC_STRING);
    ASSERT(_file->append(magic.c_str(), magic.size()));
    return RetCode::kSucc;
}

RetCode TableReader::open(int table_id) {
    // open the sstable with _id
    
    // get sstable name
    std::stringstream sstream;
    sstream << SSTABLE_FILE_NAME << table_id;
    std::string table_name; 
    sstream >> table_name;

    ASSERT(_file->open(table_name));

    _id = table_id;
    return RetCode::kSucc;
}

// RetCode

RetCode TableReader::read(const std::string &key, std::string &value) {
    if (_file == nullptr) {
        ERROR("TableRader::read() read unopen file.");
        return RetCode::kNotFound;
    }

}

} // namespace polar_race