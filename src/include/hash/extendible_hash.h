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

#include <mutex>

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
	bool GetKeyValue(const uint64_t bkt_id, 
										const uint64_t kv_entry,
										K &key, V &value);
	uint64_t GetNumEntriesInBkt(const uint64_t bkt_id);

  void DumpAll();
  void DumpDir();  

private:
  // add your own member variables here

	uint64_t global_depth; //Maintains the global_depth
	uint64_t bucket_size;  //Maintains the local bucket size
	
	struct bucket {
		mutable std::mutex bkt_mutex; //Bucket lock
		uint64_t logical_idx; //Logical index used for reverse lookup
		uint64_t local_depth; //Local depth of the each bucket
		std::vector<std::pair<K, V>> kv_pairs; //Key Value pairs
	};
	
	//Vector to hold all the buckets
	std::vector<bucket *> buckets;

	//Hash directory - maintains the bucket's indexes
	std::vector<uint64_t> hash_dir;

	//Director lock and readers/writers counters
	mutable std::mutex dir_mutex;
	mutable std::mutex rd_mutex;
	uint64_t readers;
};
} // namespace cmudb
