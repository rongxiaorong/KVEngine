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
    
    DEBUG("write into table%d on_writing:%d",_id, _on_writing);
    string _key = key.ToString();
    if (index.find(_key) != index.end()) {
        ERROR("MemTable::_write() Existed key.");
        return _update(key, value);
    }
    else {
        // string* new_v = new string(value.data(), value.size());
        char* new_v = (char*)memManager.allocate();
        memcpy(new_v, value.data(), value.size());
        index.insert(std::pair<string, PolarString>(_key, PolarString(new_v, value.size())));
        size.fetch_add(key.size());
        size.fetch_add(value.size());
        // _filter->set(_key);  
        if (size.fetch_add(0) > MEMTABLE_MAX_SIZE && _auto_write) 
            setImmutable();

    }
    return RetCode::kSucc;
}

RetCode MemTable::_update(const PolarString& key, const PolarString& value) {
    // INFO("update into table%d  on_writing:%d", _id, _on_writing);
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
        memManager.free(index[_key].data());
        char* new_v = (char*)memManager.allocate();
        memcpy(new_v, value.data(), value.size());
        index[_key] = PolarString(new_v, value.size());
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
    RetCode ret = kSucc;
    char* new_v = (char*)memManager.allocate();
    memcpy(new_v, value.data(), value.size());
    string _key(key.data(), 8);
    // std::lock_guard<std::mutex> guard(_write_mtx);
    _write_mtx.lock();
    auto insert_res = index.insert(std::pair<string, PolarString>(_key, PolarString(new_v, value.size())));
    if (insert_res.second == true) {
        // insert successfully
        size.fetch_add(key.size() + value.size());
        if (size.fetch_add(0) > MEMTABLE_MAX_SIZE && _auto_write) 
            setImmutable();
    } else {
        // update
        index[_key] = PolarString(new_v, value.size());
    }
    _write_mtx.unlock();
    // if (contains(key)) 
    //     ret = _update(key, value);
    // else 
    //     ret = _write(key, value);
    
    _on_writing_mtx.lock();
    _on_writing --;
    _on_writing_cv.notify_all();
    // INFO("Table%d _on_writing=%d", _id, _on_writing);
    _on_writing_mtx.unlock();
    
    return ret;    
}   

RetCode MemTable::read(const PolarString& key, string* value) {
    // _on_reading_mtx.lock();
    // _on_reading ++;
    // _on_reading_mtx.unlock();

    RetCode ret;
    if (contains(key)) {
        PolarString v = index[key.ToString()];
        value->assign(v.data(), v.size());
        ret = RetCode::kSucc;
    }
    else 
        ret = RetCode::kNotFound;
    
    // _on_reading_mtx.lock();
    // _on_reading --;
    // _on_reading_cv.notify_all();
    // _on_reading_mtx.unlock();

    return ret;
}

MemTable* ImmutTableList::get(int n) {
    std::lock_guard<std::mutex> guard(_mtx);
    return _list[n];
}

void ImmutTableList::set(int n, MemTable* table) {
    count.fetch_add(1);
    _list[n] = table;
    if (count.fetch_add(0) > 1)
        INFO("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! count > 1, %d",count.fetch_add(0));
}

void ImmutTableList::remove(int n) {
    std::lock_guard<std::mutex> guard(_mtx);
    count.fetch_sub(1);
    INFO("remove immutTable %d", n);
    _list[n] = nullptr;
    // _write_for_all.notify_all();
}


} // namespace polar_race