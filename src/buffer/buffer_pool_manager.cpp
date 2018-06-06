#include "buffer/buffer_pool_manager.h"

namespace cmudb {

/*
 * BufferPoolManager Constructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                                 const std::string &db_file)
    : pool_size_(pool_size), disk_manager_{db_file} {
  // a consecutive memory space for buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHash<page_id_t, Page *>(100);
  replacer_ = new LRUReplacer<Page *>;
  free_list_ = new std::list<Page *>;

  // put all the pages into free list
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_->push_back(&pages_[i]);
  }
}

/*
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::~BufferPoolManager() {
  FlushAllPages();
  delete[] pages_;
  delete page_table_;
  delete replacer_;
  delete free_list_;
}

/**
 * 1. search hash table.
 *  1.1 if exist, pin the page and return immediately
 *  1.2 if no exist, find a replacement entry from either free list or lru
 *      replacer. (NOTE: always find from free list first)
 * 2. If the entry chosen for replacement is dirty, write it back to disk.
 * 3. Delete the entry for the old page from the hash table and insert an entry
 * for the new page.
 * 4. Update page metadata, read page content from disk file and return page
 * pointer
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) { 
  Page *tmp_page = NULL;

  latch_.lock();
  //Page not found
  if(!page_table_->Find(page_id,tmp_page)) {
    //Free pages are not available
    if(free_list_->empty()) {
      //Fetch a victim page
      if(!replacer_->Victim(tmp_page)) {
        latch_.unlock();
        return nullptr; //all pages are pinned
      }

      //Flush and Cleanse the page
      if(!AddToFreeList(tmp_page)) {
        latch_.unlock();
        return nullptr;
      }
    }  

    //Always fetch from free_list.
    tmp_page = free_list_->front();
    free_list_->pop_front();
    //Update page_id for the page object
    tmp_page->page_id_ = page_id;
    disk_manager_.ReadPage(tmp_page->page_id_,
                        tmp_page->data_);
  }
    
  //Set page metadata
  tmp_page->pin_count_++;
  replacer_->Erase(tmp_page);
  //Insert page into the page table
  page_table_->Insert(page_id, tmp_page);
  latch_.unlock();
  return tmp_page;
}


/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to replacer
 * if pin_count<=0 before this call, return false.
 * is_dirty: set the dirty flag of this page
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  Page *tmp_page = NULL;

  latch_.lock();
  if(page_table_->Find(page_id, tmp_page)){
    //Pin count is already '0'
    if(!tmp_page->pin_count_) {
      latch_.unlock();
      return false;
    }
    
    //Unpin once
    tmp_page->pin_count_--;
    
    if(tmp_page->pin_count_ == 0) {
      replacer_->Insert(tmp_page); //Inserting into LRU replacer
      tmp_page->is_dirty_ = is_dirty; 
      latch_.unlock();
      return true;
    }
  } 

  latch_.unlock();
  return false;
}

/*
 * Used to flush a particular page of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) { 
  Page *tmp_page = NULL;

  latch_.lock();
  const bool found = page_table_->Find(page_id, tmp_page);

  //Page Found 
  if(found && tmp_page->page_id_ != INVALID_PAGE_ID && 
     tmp_page->is_dirty_) {
    disk_manager_.WritePage(page_id, tmp_page->data_);
    tmp_page->is_dirty_ = false;
    latch_.unlock();
    return true;
  } 

  latch_.unlock();
  //Page not in hash_table
  return false; 
}

/*
 * Used to flush all dirty pages in the buffer pool manager
 */
void BufferPoolManager::FlushAllPages() {
  latch_.lock();
  for(uint64_t i=0;i<pool_size_;i++) {
    if(pages_[i].page_id_ != INVALID_PAGE_ID && 
       pages_[i].is_dirty_) 
    { 
        disk_manager_.WritePage(pages_[i].page_id_,
                                pages_[i].data_);
        pages_[i].is_dirty_ = false;
    }
  }
  latch_.unlock();
}

/**
 * User should call this method for deleting a page. This routine will call disk
 * manager to deallocate the page.
 * First, if page is found within page table, buffer pool manager should be
 * reponsible for removing this entry out of page table, reseting page metadata
 * and adding back to free list. Second, call disk manager's DeallocatePage()
 * method to delete from disk file.
 * If the page is found within page table, but pin_count != 0, return false
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) { 
  Page *tmp_page = NULL;

  latch_.lock();  
  //Checking the page table for the page_id 
  if(page_table_->Find(page_id, tmp_page)) {
    //Page table entry found. 
    //Adding the page to the freelist.
    //     1) Check for pin count - return false if > 0
    //     2) remove from LRU replacer
    //     3) remove from page table
    if(!AddToFreeList(tmp_page)) {
      latch_.unlock();
      return false;
    }

    //Deallocate the page from disk
    disk_manager_.DeallocatePage(page_id);
    latch_.unlock();
    return true; 
  }
  
  latch_.unlock();
  return false; 
}

/**
 * User should call this method if needs to create a new page. This routine
 * will call disk manager to allocate a page.
 * Buffer pool manager should be responsible to choose a victim page either from
 * free list or lru replacer(NOTE: always choose from free list first), update
 * new page's metadata, zero out memory and add corresponding entry into page
 * table.
 * return nullptr is all the pages in pool are pinned
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) { 
  Page *new_page = NULL;

  latch_.lock();

  /*Checking if the Free list is empty*/
  if(free_list_->empty()) {
    //Free list is empty
    //Trying to fetch a empty, unpinned page from the LRU replacer
    //Note: all the pages in LRU replacer are unpinned. Only 
    //unpinned pages are inserted into the LRU replacer.
    if(!replacer_->Victim(new_page)){
      //Unable to find a unpinned victim page
      latch_.unlock();
      return nullptr;
    }

    //Found a Victim page from either free list or LRU replacer
    //Flush and Cleanse the page
    if(!AddToFreeList(new_page)) {
      //Unable to add the page to Free list - may be pinned (unlikely).
      latch_.unlock();
      return nullptr;
    }
  } 

  //Always fetch from free_list. 
  new_page = free_list_->front();
  free_list_->pop_front();
  
  page_id = disk_manager_.AllocatePage();
  
  //Set page metadata
  new_page->page_id_ = page_id;
  new_page->pin_count_++;

  disk_manager_.ReadPage(new_page->page_id_,
                        new_page->data_);

  //Insert page into the page table
  page_table_->Insert(page_id, new_page);

  latch_.unlock();
  return new_page;
}

//Resets a given page object
void BufferPoolManager::CleanPage(Page *tmp_page) {
  tmp_page->ResetMemory();
  tmp_page->page_id_   = INVALID_PAGE_ID;
  tmp_page->is_dirty_  = false;
  tmp_page->pin_count_ = 0;
}

bool BufferPoolManager::AddToFreeList(Page *tmp_page) {
    if(tmp_page->pin_count_) return false;

    if(tmp_page->is_dirty_) 
      disk_manager_.WritePage(tmp_page->page_id_, 
                                      tmp_page->data_);

    //Adding the page back to free list
    //Removing the selected page from the LRU replacer
    replacer_->Erase(tmp_page);
    //Removing the corresponding entry from the page table
    page_table_->Remove(tmp_page->page_id_);  
    CleanPage(tmp_page);  //Helper function to reset page
    free_list_->push_back(tmp_page);
    return true;    
}

} // namespace cmudb
