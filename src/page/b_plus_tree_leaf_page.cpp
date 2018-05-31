/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id) 
{
  int max_size = 0;
  this->SetPageType(LEAF_PAGE);
  this->SetSize(0);
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);

  max_size = (PAGE_SIZE-this->GetHeaderSize())/sizeof(MappingType);
  this->SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return this->next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) 
{
  this->next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
    const KeyType &key, const KeyComparator &comparator) const 
{
  int sidx = 0;
  int eidx = this->GetSize()-1;
  int mid = 0;
  int8_t cmp_result = 0;

  while(sidx < eidx) {
    mid = (sidx+eidx)/2;
    cmp_result = comparator(this->array[mid].first, key);
    if(cmp_result == 0)
      return mid;
    else if(cmp_result < 0)
      sidx = mid+1;
    else
      eidx = mid;
  }

  if(sidx == eidx &&
      (comparator(this->array[sidx].first,key)>=0))
    return sidx;
    
  return INVALID_INDEX;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                       const ValueType &value,
                                       const KeyComparator &comparator) 
{
    int key_index = this->KeyIndex(key, comparator);
    if(key_index == INVALID_INDEX)
      key_index = this->GetSize();
    
    /*Assuming that there is enough space in the leaf page*/
    int idx = this->GetSize()-1;
    while(idx >= key_index) 
    {
        this->array[idx+1] = this->array[idx];
        idx--; 
    }

    this->array[idx+1].first = key;
    this->array[idx+1].second = value;
    this->IncreaseSize(1);

    return this->GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
    BPlusTreeLeafPage *recipient,
    __attribute__((unused)) BufferPoolManager *buffer_pool_manager) 
{
    int move_size = this->GetSize()>>1;

    recipient->CopyHalfFrom(this->array, move_size);
    
    this->DecreaseSize(move_size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size) 
{
   int start_idx = this->GetSize()-1-size;
   for(int i=0; i<size; i++)
   {
      this->array[i] = items[start_idx];
      items[start_idx++] = 0;
   }
   this->IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                        const KeyComparator &comparator) const {
  int sidx = 0;
  int eidx = this->GetSize()-1;
  int mid = 0;
  int8_t cmp_result = 0;

  while(sidx <= eidx)
  {
      mid = (sidx+eidx)>>1;
      cmp_result = comparator(this->array[mid].first, key);

      if(cmp_result == 0) //Key Match Case 
      {
        value = this->array[mid].second;
        return true;
      }
      else if(cmp_result < 0)
      {
        sidx = mid+1;
      }
      else 
      {
        eidx = mid-1;
      }
  }

  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immdiately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
    const KeyType &key, const KeyComparator &comparator)     
{
    int key_index = this->KeyIndex(key, comparator);
  
    //Key not found. Return immediately.
    if(comparator(this->array[key_index].first, key) != 0)
        return this->GetSize();

    for(int i=key_index;i<this->GetSize()-1;i++)
        this->array[i] = this->array[i+1];

    this->DecreaseSize(1); 
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                           int, BufferPoolManager *) 
{
    recipient->CopyAllFrom(this->array, this->GetSize());
    recipient->SetNextPageId(this->next_page_id_);
    this->SetSize(0);
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size)
{
    int start_idx = this->GetSize();

    for(int i=0;i<size;i++) 
        this->array[start_idx++] = items[i]; 

    this->IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */ 
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeLeafPage *recipient,
    BufferPoolManager *buffer_pool_manager) 
{
    BPlusTreeInternalPage *parent = 
              buffer_pool_manager->FetchPage(this->GetParentPageId());   

    int parent_index = parent->KeyIndex(this->array[0].first);

    recipient->CopyLastFrom(this->array[0]);
     
    for(int i=0;i<this->GetSize()-1;i++)
    {
      this->array[i] = this->array[i+1];
    }
      
    this->DecreaseSize(1);
    this->parent[parent_index].first = this->array[0].first;

    buffer_pool_manager->UnpinPage(parent->GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) 
{
    int idx = this->GetSize();
    this->array[idx] = item;
    this->IncreaseSize(1);
}


/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeLeafPage *recipient, int parentIndex,
    BufferPoolManager *buffer_pool_manager) 
{    
    int idx = this->GetSize()-1;
    recipient->CopyFirstFrom(this->array[idx], parentIndex,
                                        buffer_pool_manager);    
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
    const MappingType &item, int parentIndex,
    BufferPoolManager *buffer_pool_manager) 
{
    BPlusTreeInternalPage *parent = 
        buffer_pool_manager->FetchPage(this->GetParentPageId());
    
    parent->array[parentIndex].first = item.first;

    for(int i=this->GetSize()-1;i>=0;i--)
    {
        this->array[i+1] = this->array[i];
    }

    this->array[0] = item.second;

    buffer_pool_manager->UnpinPage(this->GetParentPageId(), true);
}


/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream stream;
  if (verbose) {
    stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
           << "]<" << GetSize() << "> ";
  }
  int entry = 0;
  int end = GetSize();
  bool first = true;

  while (entry < end) {
    if (first) {
      first = false;
    } else {
      stream << " ";
    }
    stream << std::dec << array[entry].first;
    if (verbose) {
      stream << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID,
                                       GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID,
                                       GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID,
                                       GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID,
                                       GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID,
                                       GenericComparator<64>>;
} // namespace cmudb
