#include "IndexFile.h"
#include <chrono>
#include <sys/mman.h>

namespace polar_race {

std::priority_queue<std::pair<unsigned long, std::map<string, IndexEntry>* > > indexQueue;
std::condition_variable indexWriterCV;
std::mutex indexWriterMtx;
bool end_flag;
void IndexWriter() {
    using namespace std::chrono;
    INFO("IndexWriter start");
    while(true) {
        std::unique_lock<std::mutex> ulock(indexWriterMtx);
        while (indexQueue.size() <= 0 && !end_flag) {
            indexWriterCV.wait_until(ulock, time_point<system_clock, milliseconds >(milliseconds(500)));
            // INFO("Writer Waking... %ld",indexQueue.size());
        }

        while (indexQueue.size() > 0) {
            IndexFile index_file, tmp_file;
            if (index_file.open(false) != kSucc)
                ERROR("Can't open index file");
            if (tmp_file.open(true) != kSucc)
                ERROR("Can't open tmp index file");
            auto queue_entry = indexQueue.top();
            auto mem_index = queue_entry.second;
            indexQueue.pop();
            tmp_file.write_stamp(queue_entry.first);
            INFO("Merging Index %ld", queue_entry.first);
            MergeIndex(mem_index, index_file, tmp_file);
            
            index_file.close();
            tmp_file.close();
            index_file.remove();
            tmp_file.toIndexFile();
            delete mem_index;
        }

        if (end_flag) {
            INFO("IndexWriter exit.");
            break;
        }
    }
}

void MergeIndex(const std::map<string, IndexEntry>* _mem_index, IndexFile& _index_file, IndexFile& _tmp_file) {
    IndexEntry *fe = _index_file.next();
    auto me = _mem_index->begin();
    while (fe != nullptr && me != _mem_index->end()) {
        PolarString fk(fe->key, 8), mk(me->second.key, 8);
        int cmp = fk.compare(mk);
        if (cmp == 0) {
            // equal
            _tmp_file.write(me->second);
            fe = _index_file.next();
            me ++;
        } 
        else if (cmp < 0) {
            // f < m
            _tmp_file.write(*fe);
            fe = _index_file.next();
        } 
        else if (cmp > 0) {
            // f > m
            _tmp_file.write(me->second);
            me ++;
        }
    }
    while (fe != nullptr) {
        _tmp_file.write(*fe);
        fe = _index_file.next();
    }
    while (me != _mem_index->end()) {
        _tmp_file.write(me->second);
        me ++;
    }
}

RetCode MemIndex::find(const char* key, IndexEntry* entry) {    
    auto iter = _index->find(string(key, 8));
    if (iter == _index->end())
        return kNotFound;
    memcpy(entry, &(iter->second), sizeof(IndexEntry));
    return kSucc;
}

RetCode MemIndex::insert(const char* key, const int &num, const unsigned long offset, const unsigned long stamp) {
    // insert index
    IndexEntry entry(key, num, offset);
    string _key(entry.key, 8);
    std::lock_guard<std::mutex> guard(_write_mtx);
    auto res = _index->insert(std::pair<string, IndexEntry>(_key, entry));
    if (res.second == false)
        _index->at(_key) = entry;
    // make new index
    if (_index->size() >= MAX_INDEX_SIZE) {
        persist(stamp);
        _index = new std::map<string, IndexEntry>();
    }
    return kSucc;
}

RetCode MemIndex::persist(const unsigned long stamp) {
    std::lock_guard<std::mutex> guard(indexWriterMtx);
    indexQueue.push(std::pair<unsigned long, std::map<string, IndexEntry>*>(stamp, _index));
    indexWriterCV.notify_all();
    return kSucc;
}

RetCode IndexFile::open(bool is_tmp) {
    if (is_tmp) {
        // create a tmp file
        string tmp_name = engineDir + "/" + INDEX_TMP_FILE_NAME;
        if (fileExist(tmp_name.c_str()))
            ::remove(tmp_name.c_str());
        _file = fopen(tmp_name.c_str(), "a+");
        if (_file == nullptr)
            return kIOError;
        _fname = tmp_name;  
    } else {
        string index_name = engineDir + "/" + INDEX_FILE_NAME;
        if (!fileExist(index_name.c_str()))
            return kIOError;
        _file = fopen(index_name.c_str(), "rb+");
        if (_file == nullptr)
            return kIOError;
        _fname = index_name;
        fread(&_stamp, sizeof(_stamp), 1, _file);
    }
    return kSucc;
}

RetCode IndexFile::close() {
    if (_file) {
        fclose(_file);
        _file = nullptr;
    }
    return kSucc;
}

RetCode IndexFile::remove() {
    ::remove(_fname.c_str());
    return kSucc;
}

RetCode IndexFile::toIndexFile() {
    string indexfilename = engineDir + "/" + INDEX_FILE_NAME;
    ::rename(_fname.c_str(), indexfilename.c_str());
    return kSucc;
}

RetCode IndexFile::write(const IndexEntry& _entry) {
    if (fwrite(&_entry, sizeof(IndexEntry), 1, _file) == sizeof(IndexEntry))
        return kSucc;
    else 
        return kIOError;
}

RetCode IndexFile::write_stamp(const unsigned long stamp) {
    if (fwrite(&stamp, sizeof(stamp), 1, _file) == sizeof(stamp))
        return kSucc;
    else 
        return kIOError;
}

IndexEntry* IndexFile::next() {
    size_t ret = fread(_buf, 1, sizeof(IndexEntry), _file);
    if (ret != sizeof(IndexEntry))
        return nullptr;
    else
        return _buf;
}

RetCode IndexReader::open() {
    static std::mutex reader_mtx;
    std::lock_guard<std::mutex> guard(reader_mtx);
    if (_isopen)
        return kSucc;
    string indexfilename = engineDir + "/" + INDEX_FILE_NAME;
    int fd = ::open(indexfilename.c_str(), O_RDONLY);
    if (fd <= 0)
        return kIOError;
    _fd = fd;
    size_t size = fileSize(indexfilename.c_str());
    _count = size / sizeof(IndexEntry);

    char* tmp = (char*) mmap(NULL, _count * sizeof(IndexEntry), PROT_READ, MAP_SHARED, _fd, 0);
    if (tmp == NULL) 
        return kIOError;
    _index = (IndexEntry*) (tmp + 8);
    _isopen = true;
    return kSucc; 
}

IndexEntry* IndexReader::binarySearch(const PolarString& key) {
    int left = 0, right = _count- 1;
    PolarString _k(key);
    while (left <= right) {
        int mid = left + ((right - left) >> 1);
        int cmp = PolarString(_index[mid].key, 8).compare(_k);
        if (cmp > 0) // mid > _k
            right = mid - 1; 
        else if (cmp < 0) // mid < _k
            left = mid + 1;
        else 
            return &(_index[mid]);
    }
    return nullptr;
}

IndexEntry* IndexReader::find(const PolarString& key) {
    if (!_isopen)
        return nullptr;
    else {
        // if(_hash_table == nullptr)  
        return binarySearch(key);
        // else {
        //     int idx =  _hash_table->find(key.data(), _index);
        //     if (idx != -1)
        //         return &(_index[idx]);
        //     else
        //         return nullptr;
        // }
    }       
}

BlockedQueue<std::pair<unsigned long, IndexEntry> > blockQueue;
void MemIndexInsert(const char* key, const int &num, const unsigned long offset, const unsigned long stamp) {
    blockQueue.push(std::pair<unsigned long, IndexEntry>(stamp, IndexEntry(key, num, offset)));
}

std::condition_variable memIndexWriterCV;
void MemIndexWriter() {
    using namespace std::chrono;
    std::mutex _mtx;
    while(1) {
        std::unique_lock<std::mutex> ulock(_mtx);
        while (blockQueue.size() <= 0 && !end_flag) {
            memIndexWriterCV.wait_until(ulock, time_point<system_clock, milliseconds >(milliseconds(500)));
            // INFO("Writer Waking... %ld",indexQueue.size());
        }
        while(blockQueue.size() > 0) {
            std::pair<unsigned long, IndexEntry> entry = blockQueue.top_pop();
            memIndex.insert(entry.second.key, entry.second.num, entry.second.offset, entry.first);
        }

    }
}

} // namespace polar_race