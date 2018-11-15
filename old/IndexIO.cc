#include "util.h"
#include "config.h"
#include "IndexIO.h"
#include "TableIO.h"
#include <queue>
#include <sstream>
namespace polar_race {
std::atomic_int IndexCount;
string IndexFile::MergeIndex(IndexFile &l, IndexFile &r) {
    WritableFile wf;
    // create name
    std::stringstream sstream;
    sstream << __engine_dir << "/" << INDEX_FILE_NAME << IndexCount.fetch_add(1);
    string new_index_file_name; 
    sstream >> new_index_file_name;
 
    wf.open(new_index_file_name.c_str());
    IndexEntry *le = l.getNextEntry(), *re = r.getNextEntry();
    while (le != nullptr && re != nullptr) {
        PolarString lk(le->k, 8), rk(re->k, 8);
        int cmp = lk.compare(rk);
        if (cmp == 0) {
            // equal
            if (le->n > re->n)
                wf.append((char*)le, sizeof(IndexEntry));
            else
                wf.append((char*)re, sizeof(IndexEntry));
            le = l.getNextEntry();
            re = r.getNextEntry();
        }
        else if (cmp < 0) {
            // l < r
            wf.append((char*)le, sizeof(IndexEntry));
            le = l.getNextEntry();
        }
        else if (cmp > 0) {
            // l > r
            wf.append((char*)re, sizeof(IndexEntry));
            re = r.getNextEntry();
        }
    }
    while (le != nullptr) {
        wf.append((char*)le, sizeof(IndexEntry));
        le = l.getNextEntry();
    }
    while (re != nullptr) {
        wf.append((char*)re, sizeof(IndexEntry));
        re = r.getNextEntry();
    }
    wf.close();
    return new_index_file_name;
}

std::priority_queue<IndexFile> index_queue;
std::condition_variable index_merger_cv;
std::mutex index_merger_mtx;
bool end_signal = false;

void IndexMerger() {
    std::unique_lock<std::mutex> ulock(index_merger_mtx);
    while(1) {
        INFO("I'm coming!");
        while (index_queue.size() <= 1 && !end_signal)
            index_merger_cv.wait(ulock);
        index_merger_mtx.unlock();
        index_merger_mtx.lock();
        while (index_queue.size() >= 2) {
            IndexFile l = index_queue.top();
            index_queue.pop();
            IndexFile r = index_queue.top();
            index_queue.pop();
            index_merger_mtx.unlock();

            
            string new_file_name = IndexFile::MergeIndex(l, r);
            INFO("Merge %s:%d + %s:%d to %s", l.getName().c_str(), l.size, r.getName().c_str(), r.size, new_file_name.c_str());
            index_merger_mtx.lock();
            IndexFile new_file(new_file_name, l.size + r.size);
            l.remove();
            r.remove();
            index_queue.push(new_file);
        }
        index_merger_mtx.unlock();
        if (end_signal) {
            if (!index_queue.empty()) {
                IndexFile last = index_queue.top();
                string last_name = last.getName();
                std::stringstream sstream;
                sstream << __engine_dir << "/" << INDEX_FILE_NAME;
                string new_name;
                sstream >> new_name;
                rename(last_name.c_str(), new_name.c_str());
                INFO("rename %s to %s", last_name.c_str(), new_name.c_str());
            }
            break;
        }
    }
}

IndexEntry* IndexReader::find(const PolarString& key) {
    if (!is_open)
        return nullptr;
    else {
        if(_hash_table == nullptr)  
            return binarySearch(key);
        else {
            int idx =  _hash_table->find(key.data(), _index);
            if (idx != -1)
                return &(_index[idx]);
            else
                return nullptr;
        }
    }   
}

IndexEntry* IndexReader::binarySearch(const PolarString& key) {
    int left = 0, right = _size - 1;
    PolarString _k(key);
    while (left <= right) {
        int mid = left + ((right - left) >> 1);
        int cmp = PolarString(_index[mid].k, 8).compare(_k);
        if (cmp > 0) // mid > _k
            right = mid - 1; 
        else if (cmp < 0) // mid < _k
            left = mid + 1;
        else 
            return &(_index[mid]);
    }
    return nullptr;
}

RetCode IndexReader::initHashTable() {
    _hash_table = new HashTable(HASHTABLE_SIZE);
    for (int i = 0; i < _size; i++) 
       ASSERT(_hash_table->insert(&(_index[i]), i));
    return kSucc;
}

RetCode HashTable::insert(const IndexEntry* ent, int offset) {
    unsigned int h = hash(ent->k);
    
    _nodes[count]._entry = offset;
    
    if (_table[h] == -1) {
        _table[h] = count;
        count ++;
        return kSucc;
    }

    int p = _table[h];
    while (_nodes[p]._next != -1)
        p = _nodes[p]._next;
    _nodes[p]._next = count;
    count ++;
    return kSucc;
}

int HashTable::find(const char* key, const IndexEntry* idx) {
    int h = hash(key);
    if (_table[h] == -1)
        return -1;
    int p = _table[h];
    while (p != -1) { 
        const IndexEntry* ent = &(idx[_nodes[p]._entry]);
        if (PolarString(ent->k, 8).compare(PolarString(key, 8)) == 0)
            return _nodes[p]._entry;
        p = _nodes[p]._next;
    }
    return -1;
}

} // namespace polar_race