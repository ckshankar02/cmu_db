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

			bkt_size = size;
			global_depth = GDEPTH;
			num_bkts = GDEPTH;
			
			for(i=0;i<global_depth;i++){
				buckets bkt;
				bkt.local_depth = LDEPTH;
				local_bkts.push_back(bkt);
				hash_dir.push_back(i);
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
			if(bucket_id < 0 || bucket_id > bkt_size) return 0;
			else return local_bkts[hash_dir[bucket_id]].local_depth;
		}

	/*
	 * helper function to return current number of bucket in hash table
	 */
	template <typename K, typename V>
		int ExtendibleHash<K, V>::GetNumBuckets() const {
			return num_bkts;
		}

	template <typename K, typename V>
		int ExtendibleHash<K,V>::GetBucketID(size_t hash_result) const {
			size_t gdepth_mask = (1<<global_depth) - 1;
			return hash_dir[hash_result & gdepth_mask];
		}

	
	template <typename K, typename V>
		bool ExtendibleHash<K,V>::FindByBucketID(int bkt_id, const V &value) const {
			typename std::vector<V>::const_iterator bkt_it;
	
			for(bkt_it  = local_bkts[bkt_id].bucket.begin(); 
			    bkt_it != local_bkts[bkt_id].bucket.end(); ++bkt_it) {
				if(*bkt_it == value) return true;
			}

			return false;	
		}


	/*
	 * lookup function to find value associate with input key
	 */
	template <typename K, typename V>
		bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
			int bkt_id = GetBucketID(HashKey(key));
			return FindByBucketID(bkt_id, value);
		}

	/*
	 * delete <key,value> entry in hash table
	 * Shrink & Combination is not required for this project
	 */
	template <typename K, typename V>
		bool ExtendibleHash<K, V>::Remove(const K &key) {
			int bkt_id = GetBucketID(HashKey(key));
			local_bkts[bkt_id].bucket.clear();
			return true;
			//return false;
		}


	template <typename K, typename V>
		void ExtendibleHash<K,V>::RearrangeHashDir(int bkt_id) {
			int hash_dir_idx = 0;
			int depth_diff = global_depth - local_bkts[bkt_id].local_depth;
			local_bkts[bkt_id].local_depth++;
			for(int i=0;i<depth_diff;i++) {
				hash_dir_idx  = bkt_id |(i<<local_bkts[bkt_id].local_depth);
				hash_dir[hash_dir_idx] = bkt_id;
			}	
		}

	/*
	 * insert <key,value> entry in hash table
	 * Split & Redistribute bucket when there is overflow and if necessary increase
	 * global depth
	 */
	template <typename K, typename V>
		void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
		size_t hash_result = HashKey(key);
		int bkt_id = GetBucketID(hash_result);
		int new_idx = 0;
	

		if(FindByBucketID(bkt_id, value)) return;
			
		if(local_bkts[bkt_id].bucket.size() == bkt_size) {
			if(global_depth <= local_bkts[bkt_id].local_depth) {
				//CKS: Increase the hash directory size
				std::vector<int>::iterator iter;
				size_t old_size = hash_dir.size();
				hash_dir.resize(old_size);
				global_depth++;

				for(iter = hash_dir.begin(); iter != hash_dir.begin()+old_size; ++iter) {
					*(iter+old_size) = *iter;
				}
			}	
			//CKS: Split the buckets					
			new_idx = local_bkts.size();	
			local_bkts.resize(new_idx+1);				

			//CKS: Rearrange the hash directory to bucket links
			RearrangeHashDir(bkt_id); //old bucket
			RearrangeHashDir(new_idx); //new bucket 
		} 
	}

	template class ExtendibleHash<page_id_t, Page *>;
	template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
	// test purpose
	template class ExtendibleHash<int, std::string>;
	template class ExtendibleHash<int, std::list<int>::iterator>;
	template class ExtendibleHash<int, int>;
} // namespace cmudb
