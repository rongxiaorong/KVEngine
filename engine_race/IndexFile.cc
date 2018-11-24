#include "IndexFile.h"
#include <chrono>
#include <sys/mman.h>

namespace polar_race {

std::priority_queue<std::pair<unsigned long, std::map<string, IndexEntry>* > > indexQueue;
std::condition_variable indexWriterCV;
std::mutex indexWriterMtx;
bool end_flag = false;
void IndexWriter() {
    using namespace std::chrono;
    INFO("IndexWriter start");
    IndexBuffer* indexBuffer = nullptr;
    while(true) {
        std::unique_lock<std::mutex> ulock(indexWriterMtx);
        while (indexQueue.size() <= 0 && !end_flag) {
            indexWriterCV.wait_until(ulock, time_point<system_clock, milliseconds >(milliseconds(500)));
            // INFO("Writer Waking... %ld",indexQueue.size());
        }

        while (indexQueue.size() > 0) {
            
            auto queue_entry = indexQueue.top();
            auto mem_index = queue_entry.second;
            if (mem_index->size() <= 0) {
                indexQueue.pop();
                delete mem_index;
                continue;
            }
            indexQueue.pop();
            INFO("merge index %d", queue_entry.first);
            if (indexBuffer == nullptr) {
                indexBuffer = new IndexBuffer(mem_index->size());
                for(auto iter : *mem_index) 
                    indexBuffer->append(iter.second);
            } else {
                IndexBuffer* tmpBuffer = new IndexBuffer(indexBuffer->size() + mem_index->size());
                MergeIndex(mem_index, indexBuffer, tmpBuffer);
                delete indexBuffer;
                indexBuffer = tmpBuffer;
            }

            delete mem_index;
        }

        if (end_flag) {
            INFO("Persisting index");
            if (indexBuffer)
                if(indexBuffer->persist() != kSucc)
                    ERROR("fail to persist");
            INFO("IndexWriter exit.");
            break;
        }
    }
}
void MergeIndex(const std::map<string, IndexEntry>* _mem_index, IndexBuffer *_index_buf, IndexBuffer * _tmp_buf) {
    IndexEntry* fe = _index_buf->next();
    auto me = _mem_index->begin();
    while (fe != nullptr && me != _mem_index->end()) {
        PolarString fk(fe->key, 8), mk(me->second.key, 8);
        int cmp = fk.compare(mk);
        if (cmp == 0) {
            // equal
            _tmp_buf->append(me->second);
            fe = _index_buf->next();
            me ++;
        } 
        else if (cmp < 0) {
            // f < m
            _tmp_buf->append(*fe);
            fe = _index_buf->next();
        } 
        else if (cmp > 0) {
            // f > m
            _tmp_buf->append(me->second);
            me ++;
        }
    }
    while (fe != nullptr) {
        _tmp_buf->append(*fe);
        fe = _index_buf->next();
    }
    while (me != _mem_index->end()) {
        _tmp_buf->append(me->second);
        me ++;
    }
} 
// void MergeIndex(const std::map<string, IndexEntry>* _mem_index, IndexFile& _index_file, IndexFile& _tmp_file) {
//     IndexEntry *fe = _index_file.next();
//     auto me = _mem_index->begin();
//     while (fe != nullptr && me != _mem_index->end()) {
//         PolarString fk(fe->key, 8), mk(me->second.key, 8);
//         int cmp = fk.compare(mk);
//         if (cmp == 0) {
//             // equal
//             _tmp_file.write(me->second);
//             fe = _index_file.next();
//             me ++;
//         } 
//         else if (cmp < 0) {
//             // f < m
//             _tmp_file.write(*fe);
//             fe = _index_file.next();
//         } 
//         else if (cmp > 0) {
//             // f > m
//             _tmp_file.write(me->second);
//             me ++;
//         }
//     }
//     while (fe != nullptr) {
//         _tmp_file.write(*fe);
//         fe = _index_file.next();
//     }
//     while (me != _mem_index->end()) {
//         _tmp_file.write(me->second);
//         me ++;
//     }
// }

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
    INFO("Open indexReader %d", _count);
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
        return binarySearch(key);
    }       
}

BlockedQueue<std::pair<unsigned long, IndexEntry> > blockQueue;
extern bool evalue_test;
IndexBuffer* global_buffer = nullptr;
void MemIndexInsert(const char* key, const int &num, const unsigned long offset, const unsigned long stamp) {
    // if (evalue_test) 
    //     global_buffer->write(IndexEntry(key, num, offset), stamp);
    // else
    blockQueue.push(std::pair<unsigned long, IndexEntry>(stamp, IndexEntry(key, num, offset)));
    //     memIndex.insert(key, num, offset,stamp);

}

std::condition_variable memIndexWriterCV;
bool mem_end_flag = false;
void MemIndexWriter() {
    using namespace std::chrono;
    std::mutex _mtx;
    while(1) {
        std::unique_lock<std::mutex> ulock(_mtx);
        while (blockQueue.size() <= 0 && !mem_end_flag) {
            memIndexWriterCV.wait_until(ulock, time_point<system_clock, milliseconds >(milliseconds(500)));
            // INFO("Writer Waking... %ld",indexQueue.size());
        }
        while(blockQueue.size() > 0) {
            std::pair<unsigned long, IndexEntry> entry = blockQueue.top_pop();
            // memIndex.insert(entry.second.key, entry.second.num, entry.second.offset, entry.first);
            avlIndexFile.insert(entry.second.key, entry.second.num, entry.second.offset, entry.first);
        }
        if (mem_end_flag) {
            INFO("MemIndex Writer exit.");
            break;
        }
    }
}
template<>
AvlNode<IndexEntry>* NodePointer<IndexEntry>::head = nullptr;
RetCode AvlIndexFile::open() {
    tree.init(INDEX_ENTRIES);
    if (fileExist(engineDir.c_str(), INDEX_FILE_NAME)) {
        // open and recover index file here
        string file_name = engineDir + "/" + INDEX_FILE_NAME;
        int fd = ::open(file_name.c_str(), O_RDONLY);
        if (fd <= 0) {
            INFO("Can't recover index AvlIndexFile");
            return kNotFound;
        }
        unsigned long buf[3];
        ::read(fd, &buf, sizeof(unsigned long) * 3);
        index_stamp = buf[0];
        size = buf[1];
        tree.setRoot(buf[2]);
        ::read(fd, tree.data(), sizeof(AvlNode<IndexEntry>) * size);
        ::close(fd);
        return kSucc;
    } else {
        INFO("Can't open index avlIndexFile");
        index_stamp = 0;
        size = 0;

        return kSucc;
    }
}

RetCode AvlIndexFile::save() {
    string file_name = engineDir + "/" + INDEX_FILE_NAME;
    if (fileExist(engineDir.c_str(), INDEX_FILE_NAME)) {
        // remove file
        ::remove(file_name.c_str());
    }
    int fd = ::open(file_name.c_str(), O_WRONLY|O_CREAT|O_APPEND, 0777);
    unsigned long buf[3];
    buf[0] = index_stamp;
    buf[1] = size;
    buf[2] = tree.getRoot();
    ::write(fd, buf, sizeof(unsigned long) * 3);
    ::write(fd, tree.data(), sizeof(AvlNode<IndexEntry>) * size);
    INFO("save index file stamp:%ld size:%ld", index_stamp, size);
    return kSucc;
}

RetCode AvlIndexFile::insert(const char* key, const int &num, const unsigned long offset, const unsigned long stamp) {
    IndexEntry entry(key, num, offset);
    tree.insert(entry);
    if (stamp > index_stamp)
        index_stamp = stamp;
    size = tree.size();
    return kSucc;
}

IndexEntry* AvlIndexFile::find(const PolarString& key) {
    IndexEntry entry(key.data(), 0, 0);
    auto result = tree.find(entry);
    if (result == nullptr)
        return nullptr;
    else
        return &result->data;
}

void AvlIndexFile::foreach(void (*func)(const IndexEntry &key, RaceVisitor &visitor), RaceVisitor &visitor) {
    tree.foreach(func, visitor);
}
  
void AvlIndexFile::foreach(const PolarString &begin, const PolarString &end, void (*func)(const IndexEntry &key, RaceVisitor &visitor), RaceVisitor &visitor) {
    IndexEntry _begin(begin.data(), 0, 0), _end(end.data(), 0, 0);
    tree.foreach(_begin, _end, func, visitor);
}



} // namespace polar_race