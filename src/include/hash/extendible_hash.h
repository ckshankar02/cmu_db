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


#define GDEPTH 1
#define LDEPTH 1

namespace cmudb {
	template <typename K, typename V>
		class ExtendibleHash : public HashTable<K, V> {
			public:
				// constructor
				ExtendibleHash(size_t size);
				// helper function to generate hash addressing
				size_t HashKey(const K &key);
				void DumpHash();
				// helper function to get global & local depth
				int GetGlobalDepth() const;
				int GetLocalDepth(int bucket_id) const;
				int GetNumBuckets() const;
				int GetBucketID(size_t hash_result) const;
				// lookup and modifier
				bool FindByBucketID(int bkt_id, const K &key, const V &value);
				void RearrangeHashDir(int bkt_id);
				bool Find(const K &key, V &value) override;
				bool Remove(const K &key) override;
				void Insert(const K &key, const V &value) override;

			private:
				// add your own member variables here
				int global_depth;
				int num_bkts;
				size_t bkt_size;
				
				struct key_value {
					K k;
					V v;
				};

				struct buckets{
					int local_depth;
					std::vector<struct key_value> bucket;
				};

				std::vector<struct buckets> local_bkts;	
				std::vector<int> hash_dir;
				
		};
} // namespace cmudb
