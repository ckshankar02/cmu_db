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
		bkt->local_depth = LOCAL_DEPTH;
		bkt->local_index = i;
			
		//adding new buckets to the vector 
		buckets.push_back(bkt); 
		//New hash entry - points to newly added bucket. 
		hash_dir.push_back(buckets.size()-1);	
	}
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
	for(int i=0;i<hash_dir.size();i++) {
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
size_t ExtendibleHash<K,V>::GetBucketID(const K &key){
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
	typename std::vector<std::pair<K,V>>::iterator iter;

	for(iter = buckets[bkt_id]->kv_pairs.begin(); iter != buckets[bkt_id]->kv_pairs.end(); iter++) {	
		if(iter->first == key) {
			value = iter->second;
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
	typename std::vector<std::pair<K,V>>::iterator iter;

	for(iter = buckets[bkt_id]->kv_pairs.begin(); iter != buckets[bkt_id]->kv_pairs.end(); iter++) {
		if(iter->first == key) {
			iter = buckets[bkt_id]->kv_pairs.erase(iter);
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
	typename std::vector<std::pair<K,V>>::iterator iter;

	int new_bkt_idx = -1;

	//check if the key is already present, if so, update value and return. 
	for(iter = buckets[bkt_id]->kv_pairs.begin(); iter != buckets[bkt_id]->kv_pairs.end(); iter++) {
		if(iter->first == key) {
			iter->second = value;
			return;
		}
	}

	//check if bucket spliting or hash directory expansion is required.
	if(buckets[bkt_id]->kv_pairs.size() == bucket_size) {
		std::cout<<"Attempting Split\n";
		int depth_diff = 0;
		unsigned int gbl_mask = 0;
		//int old_local_depth = 0;		

		//Hash directory expansion	
		if(global_depth <= buckets[bkt_id]->local_depth) {
			std::cout<<"Hash Resize\n";
			int prev_size = 0;

			//Double the number of hash directory entries.
			global_depth++;
			prev_size = hash_dir.size();
			hash_dir.resize(1<<global_depth);		
		
			//Initially make the new entries still point to old buckets.						
			for(int i=0;i<prev_size;i++) {
				hash_dir[i+prev_size] = hash_dir[i];
			}
		}
		
		//Create a new bucket 
		bucket* bkt = new bucket;
		new_bkt_idx = buckets.size();
		buckets.push_back(bkt);


		//Increase the local depth for both old and new buckets
		//old_local_depth = buckets[bkt_id]->local_depth;	
		bkt->local_index = buckets[bkt_id]->local_index | (1<<buckets[bkt_id]->local_depth);
		buckets[bkt_id]->local_depth++;
		bkt->local_depth = buckets[bkt_id]->local_depth;

		//Rearrange Hash directory pointers to point to new bucket	
		depth_diff = global_depth - buckets[bkt_id]->local_depth;
		gbl_mask = (1<<global_depth)-1;		

		for(int i=0;i<(1<<depth_diff);i++) {
			//rearranging old bucket pointers
			unsigned int lcl_idx = 0;
			unsigned int dir_idx = 0;
			
			lcl_idx = buckets[bkt_id]->local_index;
			dir_idx = (lcl_idx | (i<<buckets[bkt_id]->local_depth)) & gbl_mask;
			hash_dir[dir_idx] = bkt_id;

			//rearranging new bucket pointers
			lcl_idx = buckets[new_bkt_idx]->local_index;
			dir_idx = (lcl_idx | (i<<buckets[new_bkt_idx]->local_depth)) & gbl_mask;
			hash_dir[dir_idx] = new_bkt_idx;	
		}
	
		DumpDir();

		std::cout<<"Trying to Redistribute "<<bkt_id<<std::endl;
		while(iter != buckets[bkt_id]->kv_pairs.end()) {
			//Recompute the bucket index for each key in the old bucket
			new_bkt_idx = GetBucketID(iter->first);

			//If the newly computed index is different, then redistribute.
			if(new_bkt_idx != bkt_id) {
				std::pair<K,V> new_kv_pair;
				new_kv_pair = std::make_pair (iter->first, iter->second);
				buckets[new_bkt_idx]->kv_pairs.push_back(new_kv_pair);
			        iter = buckets[bkt_id]->kv_pairs.erase(iter);	
				std::cout<<"Redistribute"<<std::endl;
			} 
			else		
				++iter;
		}
	
		//Re-distribute key value pairs from old bucket
		/*for(iter = buckets[bkt_id]->kv_pairs.begin(); 
			iter != buckets[bkt_id]->kv_pairs.end(); iter++) {

			//Recompute the bucket index for each key in the old bucket
			new_bkt_idx = GetBucketID(iter->first);

			//If the newly computed index is different, then redistribute.
			if(new_bkt_idx != bkt_id) {
				std::pair<K,V> new_kv_pair;
				new_kv_pair = std::make_pair (iter->first, iter->second);
				buckets[new_bkt_idx]->kv_pairs.push_back(new_kv_pair);
			        //iter = buckets[bkt_id]->kv_pairs.erase(iter);	
				std::cout<<"SCANJEE:Redistributed\n";
				DumpAll();
			} else {
				std::cout<<"Retained:"<<bkt_id<<","<<new_bkt_idx<<std::endl;		
			}
		}*/	
	}

	
	//Get the new bucket id after spliting(Almost alway the new bucket!!)
	//This is the actual key that needs to be inserted.
	new_bkt_idx = GetBucketID(key);
	
	if(buckets[new_bkt_idx]->kv_pairs.size() >= bucket_size) {
		Insert(key, value);
	}
	else {
		//make a key value pair	
		std::pair<K,V> kv_pair;
		kv_pair = std::make_pair (key, value);
		buckets[new_bkt_idx]->kv_pairs.push_back(kv_pair);
	}
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
