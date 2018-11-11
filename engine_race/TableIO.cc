// TableIO.h

#include <sstream>
#include <vector>
#include <sys/stat.h>
#include "util.h"
#include "../include/engine.h"
#include "MemTable.h"
#include "config.h"
#include "TableIO.h"
#include "DataLog.h"

namespace polar_race {

std::vector<TableReader*> SSTableMap(512);
 

void writeImmutTable(MemTable* table) { 
    // wait for all writers of the table
    std::unique_lock<std::mutex> ulock(table->_on_writing_mtx);
    // mtx.lock();
    while (table->_on_writing != 0) {
        INFO("Fuck waiting %d:%d", table->id(), table->_on_writing);
        table->_on_writing_cv.wait(ulock);
    }
    INFO("Writing table %d size %ld", table->id(), table->size.load());
    // persist the table
    TableWriter* tableWriter = new TableWriter(table);
    tableWriter->open();
    tableWriter->write();
    delete tableWriter; 
    INFO("Finish writing table %d", table->_id);
   
    // // cache the filter 
    // filterCache.addFilter(table->id, table->_filter);
 
    // open TableReader
    TableReader* tableReader = new TableReader();
    tableReader->open(table->_id);
    SSTableMap[table->_id] = tableReader;
    
    // delete Logfile
    DataLog::deleteLog(table->_id);

    // free MemTable
    // if (table->immut == table) {
    //     table->immut = nullptr;
    // }
    immutTableList.remove(table->_id);
    delete table;
    

    return;
}

TableWriter::TableWriter(MemTable* table) {
    _table = table;
    _file = nullptr;
    _index = nullptr;
}

TableWriter::~TableWriter() {
    if (_index)
        delete _index;
    if (_file)
        delete _file;
}

RetCode TableWriter::open() {
    int id = _table->_id;

    // get table name
    std::stringstream sstream;
    sstream <<  __engine_dir << "/" << SSTABLE_FILE_NAME << id;
    std::string table_name; 
    sstream >> table_name;

    // remove the table before create
    if(remove(table_name.c_str()) == 0) 
        INFO("remove SSTable%d", id);
    else
        INFO("can't remove SSTable%d", id);
    

    // create sstable
    WritableFile* sstable = new WritableFile();
    ASSERT(sstable->open(table_name.c_str()));
    _file = sstable;
    return RetCode::kSucc;
}

RetCode TableWriter::flush() {
    if (_file == nullptr) {
        ERROR("TableWriter::flush() _file = nullptr");
        return RetCode::kNotFound;
    }
    return _file->flush();
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
    _index = new IndexEntry[_table->index.size()];
    int i = 0;
    auto iter = _table->index.begin();
    while (iter != _table->index.end()) {
        _index[i].p = _file->size();
        memcpy(_index[i].k, iter->first.c_str(), 8);

        // size_t key_size = iter->first.size();
        size_t value_size = iter->second.size();
        // ASSERT(_file->append((char*)&key_size, sizeof(key_size)));
        // ASSERT(_file->append(iter->first.c_str(), iter->first.size()));
        ASSERT(_file->append((char*)&value_size, sizeof(value_size)));
        ASSERT(_file->append(iter->second.data(), iter->second.size()));
        i++;
        iter++;
    }
    return RetCode::kSucc;
}

RetCode TableWriter::_write_index() {
    ASSERT(_file->append((char *)_index, sizeof(IndexEntry) * _table->index.size()));
    return RetCode::kSucc;

}

RetCode TableWriter::_write_bloomfilter() {
    // ASSERT(_file->append((char* )_table->_filter->data(), _table->_filter->size()));
    return RetCode::kSucc;
}

RetCode TableWriter::_write_footer() {
    size_t index_size = _table->index.size() * sizeof(IndexEntry);
    size_t filter_size = 0;
    ASSERT(_file->append((char*)&index_size, sizeof(size_t)));
    ASSERT(_file->append((char*)&filter_size, sizeof(size_t)));
    string magic(MAGIC_STRING);
    ASSERT(_file->append(magic.c_str(), magic.size()));
    return RetCode::kSucc;
}

RetCode TableReader::open(int table_id) {
    // open the sstable with _id
    
    // get sstable name
    std::stringstream sstream;
    sstream <<  __engine_dir << "/" << SSTABLE_FILE_NAME << table_id;
    std::string table_name; 
    sstream >> table_name;

    // open file
    _file = new RandomAccessFile();
    ASSERT(_file->open(table_name));
    _id = table_id;
    
    // get file size
    struct stat statbuf;
    stat(table_name.c_str(), &statbuf);
    _fsize = statbuf.st_size;

    // get index info & filter info
    size_t buf[2];
    ASSERT(_file->read(buf, _fsize - strlen(MAGIC_STRING) - sizeof(size_t) * 2, sizeof(size_t) * 2));
    _index_size = buf[0];
    _filter_size = buf[1];

    _filter_head = _fsize - strlen(MAGIC_STRING) - sizeof(size_t) * 2 - _filter_size;
    _index_head = _filter_head - _index_size;
    
    return RetCode::kSucc;
}

// RetCode TableReader::checkFilter(const string& key, bool& find) {
//     BloomFilter filter(FILTER_SIZE, MEMTABLE_MAX_SIZE/4096);

//     ASSERT(_file->read(filter.data(), _filter_head, _filter_size));
//     if (filter.get(key))
//         find = true;
//     else
//         find = false;

//     return RetCode::kSucc;
// }

RetCode TableReader::cacheIndex() {
    std::lock_guard<std::mutex> guard(mtx);
    if (_index_cache) 
        return RetCode::kSucc;
    _index_cache = new std::map<string, size_t>();
    std::vector<char> buf(_index_size);
    ASSERT(_file->read(buf.data(), _index_head, _index_size));
    IndexEntry* tmp = (IndexEntry*)buf.data();
    int count = _index_size / (sizeof(IndexEntry));
    for (int i = 0; i < count; i++)
        _index_cache->insert(std::pair<string, size_t>(string(tmp[i].k, 8), tmp[i].p));
        // (*_index_cache)[string(tmp[i].k)] = tmp[i].p;
    return RetCode::kSucc;
}

RetCode TableReader::readValue(size_t offset, string* value) {
    // size_t value_offset = _index_cache->at(key);
    char buf[8192];
    if (_file->read(buf, offset, 8192) != RetCode::kSucc) 
        return RetCode::kNotFound;
    size_t* value_size = (size_t *)buf;
    value->assign(buf + sizeof(size_t), *value_size);        
    return RetCode::kSucc;
}

RetCode TableReader::read(const std::string &key, std::string *value) {
    std::lock_guard<std::mutex> guard(read_mtx);
    if (_file == nullptr) {
        ERROR("TableRader::read() read unopen file.");
        return RetCode::kIOError;
    }
    if (_index_cache) {
        // read cache
        if (_index_cache->find(key) == _index_cache->end())
            return RetCode::kNotFound;
        // find key, read value
        return readValue(_index_cache->at(key), value);
    }
    else 
        return readIndex(key, value);
}

RetCode TableReader::readIndex(const string& key, string* value) {
    static int _cache_count = 0;
    static std::mutex _cache_count_mtx;
    _cache_count_mtx.lock();
    if (_cache_count < MAX_INDEX_CACHE) {
        // still can cache the index
        _cache_count ++;
        _cache_count_mtx.unlock();
        ASSERT(cacheIndex());
        if (_index_cache->find(key) != _index_cache->end())
            return readValue(_index_cache->at(key), value);  
        else
            return kNotFound;
    } else {
        // can't cache, read the index and release then
        _cache_count_mtx.unlock();
        std::vector<char> buf(_index_size);
        ASSERT(_file->read(buf.data(), _index_head, _index_size));
        IndexEntry* tmp = (IndexEntry*)buf.data();
        int count = _index_size / (sizeof(IndexEntry));
        size_t offset = binarySearch(key, tmp, count);
        // buf.clear()
        if (offset == 1)
            return RetCode::kNotFound;
        else
            return readValue(offset, value);
    }
}

size_t TableReader::binarySearch(const string& key, const IndexEntry* _index_array, int len) {
    int left = 0, right = len - 1;
    PolarString _k(key);
    while (left <= right) {
        int mid = left + ((right - left) >> 1);
        int cmp = PolarString(_index_array[mid].k, 8).compare(_k);
        if (cmp > 0) // mid > _k
            right = mid - 1; 
        else if (cmp < 0) // mid < _k
            left = mid + 1;
        else 
            return _index_array[mid].p;
    }
    return 1;
}

RetCode TableReader::testMagic() {
    char buf[20];
    int magic_len = strlen(MAGIC_STRING);
    ASSERT(_file->read(buf, _fsize - magic_len, magic_len));
    if (PolarString(buf, magic_len).compare(PolarString(MAGIC_STRING, magic_len)) != 0)
        return kNotFound;
    return kSucc;
}

} // namespace polar_race
