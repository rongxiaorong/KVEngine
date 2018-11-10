#include "MemTable.h"
#include "TableIO.h"
#include "DataLog.h"
#include <mutex>
#include <thread>


namespace polar_race {
using std::string;
std::list<std::thread*> thread_list;
ImmutTableList immutTableList;

std::mutex MemTable::table_mtx;
std::mutex MemTable::immut_mtx;
MemTable* MemTable::_mutable = nullptr;
MemTable* MemTable::immut = nullptr;

MemTable* MemTable::getMemtable() {
    std::lock_guard<std::mutex> guard(table_mtx);
    // if (_mutable == nullptr) {
    //     // set new memtable
    //     _mutable = new MemTable();
    // }
    return _mutable;
}

// MemTable* MemTable::getImmut() {
//     return immut;
// } 

void MemTable::setImmutable() {
    std::lock_guard<std::mutex> guard(table_mtx);

    // make sure each memtable call this function only once
    if (this->_immut) return;
    this->_immut = true; 

    INFO("MemTable%d size%ld", _id, size.fetch_add(0));
    immutTableList.set(this->_id, this);
    // call immut collect thread
    std::thread* immut_writer = new std::thread(writeImmutTable, this);
    thread_list.push_back(immut_writer);
    
    DataLog::createLog(_mutable->_id + 1);
    INFO("setImmutable create Memtable%d", _mutable->_id + 1);
    _mutable = new MemTable();
}

RetCode MemTable::_write(const PolarString& key, const PolarString& value) {
    std::lock_guard<std::mutex> guard(_write_mtx);
    DEBUG("write into table%d on_writing:%d",_id, _on_writing);
    string _key = key.ToString();
    if (index.find(_key) != index.end()) {
        ERROR("MemTable::_write() Existed key.");
        return _update(key, value);
    }
    else {
        string* new_v = new string(value.data(), value.size());
        index.insert(std::pair<string, string*>(_key, new_v));
        size.fetch_add(key.size());
        size.fetch_add(value.size());
        // _filter->set(_key);  
        if (size.fetch_add(0) > MEMTABLE_MAX_SIZE && _auto_write) 
            setImmutable();

    }
    return RetCode::kSucc;
}

RetCode MemTable::_update(const PolarString& key, const PolarString& value) {
    INFO("update into table%d  on_writing:%d", _id, _on_writing);
    string _key = key.ToString();
    if (index.find(_key) == index.end()) {
        ERROR("MemTable::_update() Unexisted key.");
        return RetCode::kNotFound;
    }
    else {
        // string* new_v = new string(value.data(), value.size());
        // string* old_v = index.find(_key)->second;
        // size.fetch_sub(old_v->size());
        // delete old_v;
        index[_key]->assign(value.data(), value.size());
        // size.fetch_add(value.size());
    }
    return RetCode::kSucc;
}

bool MemTable::contains(const PolarString& key) {
    return index.find(key.ToString()) != index.end();
}

RetCode MemTable::write(const PolarString& key, const PolarString& value) {
    _on_writing_mtx.lock();
    _on_writing ++;
    _on_writing_mtx.unlock();
    RetCode ret;

    if (contains(key)) 
        ret = _update(key, value);
    else 
        ret = _write(key, value);
    
    _on_writing_mtx.lock();
    _on_writing --;
    _on_writing_cv.notify_all();
    // INFO("Table%d _on_writing=%d", _id, _on_writing);
    _on_writing_mtx.unlock();
    
    return ret;    
}   

RetCode MemTable::read(const PolarString& key, string* value) {
    _on_reading_mtx.lock();
    _on_reading ++;
    _on_reading_mtx.unlock();

    RetCode ret;
    if (contains(key)) {
        value->assign(*index[key.ToString()]);
        ret = RetCode::kSucc;
    }
    else 
        ret = RetCode::kNotFound;
    
    _on_reading_mtx.lock();
    _on_reading --;
    _on_reading_cv.notify_all();
    _on_reading_mtx.unlock();

    return ret;
}

MemTable* ImmutTableList::get(int n) {
    std::lock_guard<std::mutex> guard(_mtx);
    return _list[n];
}

void ImmutTableList::set(int n, MemTable* table) {
    _list[n] = table;
}

void ImmutTableList::remove(int n) {
    std::lock_guard<std::mutex> guard(_mtx);
    INFO("remove immutTable %d", n);
    _list[n] = nullptr;
}


} // namespace polar_race