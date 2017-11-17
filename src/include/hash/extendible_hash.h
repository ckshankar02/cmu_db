/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>

#include "hash/hash_table.h"

#define GLOBAL_DEPTH 1
#define  LOCAL_DEPTH 1


namespace cmudb {

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
public:
  // constructor
  ExtendibleHash(size_t size);
  // helper function to generate hash addressing
  size_t HashKey(const K &key);
  // helper function to get global & local depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // lookup and modifier
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key, const V &value) override;

  //Newly added helper functions
  size_t GetBucketID(const K &key);
  void DumpAll();
  void DumpDir();  

private:
  // add your own member variables here

	int global_depth; //Maintains the global_depth
	int bucket_size;  //Maintains the local bucket size
	
	struct bucket {
		unsigned int local_index;
		int local_depth; // local depth of the each bucket
		std::vector<std::pair<K, V>> kv_pairs; //Key Value pairs
	};
	
	//Vector of buckets
	std::vector<bucket *> buckets;

	//Hash directory - maintains the index of the vector it is pointing to
	std::vector<int> hash_dir;
};
} // namespace cmudb
