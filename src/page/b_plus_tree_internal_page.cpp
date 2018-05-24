/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) 
{
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
}


/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  KeyType key;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) 
{
  this->array[index].first = key;  
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const 
{
  for(int i=0;i<this->GetSize();i++)
  {
    if(this->array[i].second == value)
      return i;
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const 
{ 
  this->array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const {
  uint32_t sidx = 1, eidx = this->GetSize() - 1;
  uint32_t mid = 0;
  int8_t cmp_result;

  if(comparator(this->array[eidx].first,key) <= 0)
  {
    return this->array[eidx].second;
  }

  while(sidx <= eidx){
    if(sidx == eidx) 
    {
      cmp_result = comparator(this->array[sidx].first, key);
      if(cmp_result >= 0)
        return this->array[sidx+1].second;
      else return this->array[sidx-1].second;
    }

    mid = (sidx+eidx)/2;
    cmp_result = comparator(this->array[mid].first, key);

    if(cmp_result == 0) 
      return this->array[mid].second;
    else if(cmp_result > 0) 
      sidx = mid;
    else
      eidx = mid-1;
  }

  return INVALID_PAGE_ID;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) 
{
    
}
  
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) 
{
  return 0;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) 
{
    recipient->CopyHalfFrom(this->array, 
                            this->GetSize()/2, buffer_pool_manager);
    this->SetSize(this->GetSize()/2);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) 
{
    int start_idx = size+1;
 
    BPlusInternalPage *parent = 
                    buffer_pool_manager->FetchPage(this->GetParentPageId());
    int index_in_parent = parent->ValueIndex(this->GetPageId());

    for(int i=1;i<size;i++) 
    {
        this->array[i] = items[start_idx++];
    }  
    
    this->array[0].second = items[size].second;
    parent->array[index_in_parent].first = items[size].first;

    this->IncreaseSize(size-1);

    buffer_pool_manager->UnpinPage(this->GetParentPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) 
{

}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() 
{
  return INVALID_PAGE_ID;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
            BPlusTreeInternalPage *recipient, int index_in_parent,
                              BufferPoolManager *buffer_pool_manager) 
{
    BPlusTreeInternalPage *parent = 
                      buffer_pool_manager->FetchPage(this->GetParentPageId());

    this->array[0].first = parent->array[index_in_parent].first;

    recipient->CopyAllFrom(this->array, this->GetSize(), buffer_pool_manager);

    for(int i=index_in_parent;i<parent->GetSize();i++)
      parent->array[i] = parent->array[i+1];

    parent->SetSize(--parent->GetSize());

    buffer_pool_manager->UnpinPage(this->GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) 
{
    int start_idx = this->GetSize(); 
    
    for(int i=0;i<size;i++) 
    {
      this->array[start_idx++] = items[i];     
    }  
    this->IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) 
{
  this->array[0].first = this->array[1].first;
  receipent->CopyLastFrom(this->array[1]);

  for(int i=2;i<this->GetSize();i++)
  {
      array[i-1] = array[i];
  }
  this->SetSize(this->GetSize()-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager) 
{
    BPlusTreeInternalPage *parent = 
                  buffer_pool_manager->FetchPage(this->parent_page_id_);
    int idx = parent->ValueIndex(pair.first);
    this->array[this->GetSize()].first = parent->array[idx].first;
    this->array[this->GetSize()].second = pair.second;
    parent->array[idx].first = pair.first;
    this->IncreaseSize(1);
    buffer_pool_manager->UnpinPage(this->GetParentPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager) 
{
    int i=0;
    for(i=recipient->GetSize(); i >= 0; i--) 
    {
       recipient->array[i] = recipient->array[i-1];
    }
    recipient->IncreaseSize(1);
    recipient->CopyFirstFrom(this->array[this->GetSize()-1], 
                              parent_index, buffer_pool_manager);    
    this->SetSize(this->GetSize()-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager) 
{
    BPlusTreeInternalPage *parent = 
                    buffer_pool_manager->FetchPage(this->GetParentPageId());

    this->array[1].first = parent->array[parent_index].first;
    this->array[0].second = pair.second;

    parent->array[parent_index].first = pair.first;
    buffer_pool_manager->UnpinPage(this->GetParentPageId(), true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < GetSize(); i++) 
  {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node =
        reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream os;
  if (verbose) {
    os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
       << "]<" << GetSize() << "> ";
  }

  int entry = verbose ? 0 : 1;
  int end = GetSize();
  bool first = true;
  while (entry < end) {
    if (first) {
      first = false;
    } else {
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose) {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                           GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                           GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                           GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                           GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                           GenericComparator<64>>;
} // namespace cmudb
