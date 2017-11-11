#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

	/*
	 * constructor
	 * array_size: fixed array size for each bucket
	 */
	template <typename K, typename V>
		ExtendibleHash<K, V>::ExtendibleHash(size_t size) {
			int i;
			buckets *bkt;

			bkt_size = size;
			global_depth = GDEPTH;

			bkt = new buckets[global_depth];
			
			for(i=0;i<global_depth;i++){
				bkt[i].local_depth = LDEPTH;
				bkt[i].bucket.reserve(bkt_size);
				hash_dir.push_back(&bkt[i]);
			}

			num_bkts = GDEPTH;
		}

	/*
	 * helper function to calculate the hashing address of input key
	 */
	template <typename K, typename V>
		size_t ExtendibleHash<K, V>::HashKey(const K &key) {
			return 0;
		}

	/*
	 * helper function to return global depth of hash table
	 * NOTE: you must implement this function in order to pass test
	 */
	template <typename K, typename V>
		int ExtendibleHash<K, V>::GetGlobalDepth() const {
			return global_depth;
		}

	/*
	 * helper function to return local depth of one specific bucket
	 * NOTE: you must implement this function in order to pass test
	 */
	template <typename K, typename V>
		int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
			if(bucket_id < 0 || bucket_id > hash_dir.size()) return 0;
			else return hash_dir[bucket_id]->local_depth;
				
		}

	/*
	 * helper function to return current number of bucket in hash table
	 */
	template <typename K, typename V>
		int ExtendibleHash<K, V>::GetNumBuckets() const {
			return num_bkts;
		}


	/*
	 * lookup function to find value associate with input key
	 */
	template <typename K, typename V>
		bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
			size_t gbld_mask = (1<<global_depth)-1;	
			int bkt_id = HashKey(key)&gbld_mask;
			typename std::vector<V>::iterator bkt_it;
	
			for(bkt_it  = hash_dir[bkt_id]->bucket.begin(); 
			    bkt_it != hash_dir[bkt_id]->bucket.end(); ++bkt_it) {
				if(*bkt_it == value) return true;
			}

			return false;
		}

	/*
	 * delete <key,value> entry in hash table
	 * Shrink & Combination is not required for this project
	 */
	template <typename K, typename V>
		bool ExtendibleHash<K, V>::Remove(const K &key) {
			return false;
		}

	/*
	 * insert <key,value> entry in hash table
	 * Split & Redistribute bucket when there is overflow and if necessary increase
	 * global depth
	 */
	template <typename K, typename V>
		void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
	
	
	}

	template class ExtendibleHash<page_id_t, Page *>;
	template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
	// test purpose
	template class ExtendibleHash<int, std::string>;
	template class ExtendibleHash<int, std::list<int>::iterator>;
	template class ExtendibleHash<int, int>;
} // namespace cmudb
