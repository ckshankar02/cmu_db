/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 */

#pragma once

#include "buffer/replacer.h"
#include "hash/extendible_hash.h"

#include <list>

namespace cmudb {

template <typename T> class LRUReplacer : public Replacer<T> {
public:
  // do not change public interface
  LRUReplacer();

  ~LRUReplacer();

  void Insert(const T &value);

  bool Victim(T &value);

  bool Erase(const T &value);

  size_t Size();

  void rd_lock();

  void rd_unlock();

private:
  // add your member variables here
  std::list<T> lru; //Maintains the LRU
   
  //HashTable to map value to node address.
  ExtendibleHash<T, typename std::list<T>::iterator> *exthash;

  std::mutex list_mutex;
  std::mutex rd_mutex;
  uint64_t readers;
};

} // namespace cmudb
