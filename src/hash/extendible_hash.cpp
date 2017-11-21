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
	uint64_t i = 0;	
	readers = 0;
		
	dir_mutex.lock();
	bucket_size = size; // size of each local bucket.
	global_depth = GLOBAL_DEPTH;
	
	//Initializing the hash directory.
	for(i=0;i<(1<<global_depth);i++){
		bucket* bkt = new bucket;
		bkt->local_depth = LOCAL_DEPTH;
		bkt->logical_idx = i;

		//Push the new bucket into the vector
		buckets.push_back(bkt);			

		//New hash entry - holds logical index to the new bucket. 
		hash_dir.push_back(i);	
	}
	dir_mutex.unlock();
}


template <typename K, typename V>
void ExtendibleHash<K, V>::DumpAll(){
	std::cout<<"Global Depth:"<<global_depth<<std::endl;
	std::cout<<"HashDir Size:"<<hash_dir.size()<<std::endl;
	std::cout<<"Num of Bkts :"<<buckets.size()<<std::endl;
	
	for(int i=0;i<buckets.size();i++) {
		std::cout<<"Bucket "<<i<<std::endl;
		std::cout<<"Num of Pairs :"<<buckets[i]->kv_pairs.size()<<std::endl;
		std::cout<<"Local depth  :"<<buckets[i]->local_depth<<std::endl;
	}
	std::cout<<std::endl;
}


template <typename K, typename V>
void ExtendibleHash<K, V>::DumpDir(){
	uint64_t i = 0;
	for(i=0;i<hash_dir.size();i++) {
		std::cout<<"hash_dir["<<i<<"] = "<<hash_dir[i]<<std::endl;
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
	dir_mutex.lock();
	uint64_t gd = global_depth;
	dir_mutex.unlock();
	return gd;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
	dir_mutex.lock();
	buckets[bucket_id]->bkt_mutex.lock();
	uint64_t ld = buckets[bucket_id]->local_depth;
	buckets[bucket_id]->bkt_mutex.unlock();
	dir_mutex.unlock();
	return ld;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
	dir_mutex.lock();
	uint64_t bkt_size = buckets.size();
	dir_mutex.unlock();
	return bkt_size;
}

/* 
 * helper function to fetch hash and lookup hash directory
 */
template <typename K, typename V>
size_t ExtendibleHash<K,V>::GetBucketID(const K &key){
	uint64_t hash_result = HashKey(key);
	uint64_t gbl_mask = (1<<global_depth) - 1;

	uint64_t hash_idx = hash_result&gbl_mask;

	return hash_dir[hash_idx];	
}



/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
	rd_mutex.lock();
	readers++;
	if(readers == 1) 
		dir_mutex.lock();
	rd_mutex.unlock();
	
	bool result = false;
	uint64_t bkt_id = GetBucketID(key);
	typename std::vector<std::pair<K,V>>::iterator iter;

	for(iter = buckets[bkt_id]->kv_pairs.begin(); iter != buckets[bkt_id]->kv_pairs.end(); iter++) {	
		if(iter->first == key) {
			value = iter->second;
			result = true;
			break;
		}
	}


	rd_mutex.lock();
	readers--;
	if(readers == 0) 
		dir_mutex.unlock();
	rd_mutex.unlock();

	return result;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
	dir_mutex.lock();
	bool result = false;
	uint64_t bkt_id = GetBucketID(key);
	typename std::vector<std::pair<K,V>>::iterator iter;

	for(iter = buckets[bkt_id]->kv_pairs.begin(); iter != buckets[bkt_id]->kv_pairs.end(); iter++) {
		if(iter->first == key) {
			iter = buckets[bkt_id]->kv_pairs.erase(iter);
			result = true;
			break;
		}
	}
	dir_mutex.unlock();

	return result;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
	dir_mutex.lock();	
	uint64_t bkt_id = GetBucketID(key);
	typename std::vector<std::pair<K,V>>::iterator iter;

	uint64_t new_bkt_idx = 0;

	//check if the key is already present, if so, update value and return. 
	for(iter = buckets[bkt_id]->kv_pairs.begin(); iter != buckets[bkt_id]->kv_pairs.end(); iter++) {
		if(iter->first == key) {
			iter->second = value;
			dir_mutex.lock();
			return;
		}
	}

	//check if bucket spliting or hash directory expansion is required.
	if(buckets[bkt_id]->kv_pairs.size() == bucket_size) {
		uint64_t depth_diff = 0;
		uint64_t gbl_mask = 0;
		uint64_t i = 0;

		//Hash directory expansion	
		if(global_depth == buckets[bkt_id]->local_depth) {
			uint64_t prev_size = hash_dir.size();

			//Double the number of hash directory entries.
			global_depth++;
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
		bkt->logical_idx = buckets[bkt_id]->logical_idx | (1<<buckets[bkt_id]->local_depth);
		buckets[bkt_id]->local_depth++;
		bkt->local_depth = buckets[bkt_id]->local_depth;

		//Rearrange Hash directory pointers to point to new bucket	
		depth_diff = global_depth - buckets[bkt_id]->local_depth;
		gbl_mask = (1<<global_depth)-1;		

		for(i=0;i<(1<<depth_diff);i++) { 			
			uint64_t lg_idx = buckets[new_bkt_idx]->logical_idx;
			uint64_t hd_idx = lg_idx | (i << buckets[new_bkt_idx]->local_depth);
			
			hash_dir[hd_idx & gbl_mask] = new_bkt_idx;
		}
	
		iter = buckets[bkt_id]->kv_pairs.begin();
		while(iter != buckets[bkt_id]->kv_pairs.end()) {
			//Recompute the bucket index for each key in the old bucket
			new_bkt_idx = GetBucketID(iter->first);

			//If the newly computed index is different, then redistribute.
			if(new_bkt_idx != bkt_id) {
				std::pair<K,V> new_kv_pair;
				new_kv_pair = std::make_pair (iter->first, iter->second);
				buckets[new_bkt_idx]->kv_pairs.push_back(new_kv_pair);
			        iter = buckets[bkt_id]->kv_pairs.erase(iter);	
			} 
			else		
				++iter;
		}
	}
	
	//Get the new bucket id after spliting(Almost alway the new bucket!!)
	//This is the actual key that needs to be inserted.
	new_bkt_idx = GetBucketID(key);
	
	if(buckets[new_bkt_idx]->kv_pairs.size() >= bucket_size) {
		dir_mutex.unlock();
		Insert(key, value);
		dir_mutex.lock();
	}
	else {
		//make a key value pair	
		std::pair<K,V> kv_pair;
		kv_pair = std::make_pair (key, value);
		buckets[new_bkt_idx]->kv_pairs.push_back(kv_pair);
	}
	dir_mutex.unlock();
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
