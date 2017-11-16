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
	int i = 0;	
	bucket_size = size; // size of each local bucket.
	global_depth = GLOBAL_DEPTH;
	
	//Initializing the hash directory.
	for(i=0;i<(1<<global_depth);i++){
		bucket* bkt = new bucket;
		bucket->local_depth = LOCAL_DEPTH;
		
		//adding new buckets to the vector 
		buckets.push_back(bkt); 
		//New hash entry - points to newly added bucket. 
		hash_dir.push_back(buckets.size()-1);	
	}
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
	return std::hash<K> {} (key);
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
	return buckets[bucket_id]->local_depth;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
	return buckets.size();
}

/* 
 * helper function to fetch hash and lookup hash directory
 */
template <typename K, typename V>
size_t ExtendibleHash<K,V>::GetBucketID(const K &key) {
	size_t hash_result = HashKey(key);
	size_t gbl_mask = (1<<global_depth) - 1;

	size_t hash_idx = hash_result&gbl_mask;

	return hash_dir[hash_idx];	
}



/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
	size_t bkt_id = GetBucketID(key);
	std::vector<bucket *>::iterator iter; 
	
	for(iter = buckets[bkt_id].begin(); iter != buckets[bkt_id].end(); iter++) {
		if(iter->kv.first == key) {
			value = iter->kv.second;
			return true;
		}
	}

	return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
	size_t bkt_id = GetBucketID(key);
	std::vector<bucket *>::iterator iter;

	for(iter = buckets[bkt_id].begin(); iter != buckets[bkt_id].end(); iter++) {
		if(iter->kv.first == key) {
			iter = buckets.erase(iter);
			return true;
		}
	}

	return false;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
	size_t bkt_id = GetBucketID(key);
	std::vector<bucket *>::iterator iter;
	int new_bkt_idx = -1;


	for(iter = buckets[bkt_id].begin(); iter != buckets[bkt_id].end(); iter++) {
		if(iter->kv.first == key) {
			*(iter->kv.second) = value;
			return;
		}
	}

	//check if bucket spliting or hash directory expansion is required.
	if(buckets[bkt_id].size() == bucket_size) {
		//Hash directory expansion	
		if(global_depth == buckets[bkt_id]->local_depth) {
			int i = 0;			
			int prev_size = 0;

			//Double the number of hash directory entries.
			global_depth++;
			prev_size = hash_dir.size();
			hash_dir.resize(1<<global_depth);		
		
			//Initially make the new entries still point to old buckets.						
			for(i=0;i<prev_size;i++) {
				hash_dir[i+prev_size] = hash_dir[i];
			}	
		}
		
		//Create a new bucket 
		bucket* bkt = new bucket;
		new_bkt_idx = buckets.size();
		buckets.push_back(bkt);


		//Increase the local depth for both old and new buckets	
		buckets[bkt_id]->local_depth++;
		bucket->local_depth = buckets[bkt_id]->local_depth;

		//Rearrange Hash directory pointers to point to new bucket	
		for(i=0;i<(global_depth - 				
	}
 	
	//make a key value pair
	
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
