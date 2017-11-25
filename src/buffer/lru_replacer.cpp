/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {
  exthash = new ExtendibleHash<T, typename std::list<T>::iterator> (100);
  readers = 0;
}

template <typename T> LRUReplacer<T>::~LRUReplacer() {
  lru.clear();
  delete exthash;
}


/*
 * Readers Locking and Unlocking helper functions
 */
template<typename T> void LRUReplacer<T>::rd_lock() {
  rd_mutex.lock();
  readers++;
  if(readers == 1) 
    list_mutex.lock();
  rd_mutex.unlock();
}


template<typename T> void LRUReplacer<T>::rd_unlock() {
  rd_mutex.lock();
  readers--;
  if(readers == 0)
    list_mutex.unlock();
  rd_mutex.unlock();
}


/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
  typename std::list<T>::iterator lst_it;

  list_mutex.lock();  

    if(exthash->Find(value, lst_it))
      lru.erase(lst_it);
    
    lru.push_back(value); 
    exthash->Insert(value, --lru.end());
    
  list_mutex.unlock();
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
  bool result = false;

  rd_lock();
    if(!lru.empty()) {
      value = lru.front();
      lru.pop_front();
      exthash->Remove(value);
      result = true;
    }
  rd_unlock();
  return result;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
  bool result = false;
  typename std::list<T>::iterator lst_it;

  list_mutex.lock();

    if(exthash->Find(value, lst_it)){
      lru.erase(lst_it);
      exthash->Remove(value);
      result = true;
    }
  
  list_mutex.unlock();

  return result;
}

template <typename T> size_t LRUReplacer<T>::Size() { 
  uint64_t node_count  = 0;
  rd_lock();
    node_count = lru.size();
  rd_unlock();
 
  return node_count;
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
