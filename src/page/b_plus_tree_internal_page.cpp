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
  int max_size = 0;
  this->SetPageType(IndexPageType::INTERNAL_PAGE);
  this->SetSize(0);
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);

  max_size = (PAGE_SIZE - this->GetHeaderSize())/sizeof(MappingType);
  this->SetMaxSize(max_size);
}


/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  return this->array[index].first;
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
  return INVALID_INDEX;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const 
{ 
  return this->array[index].second;
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


  /* If the last element in 'array' is less than input 'key'
   * then return the last element */
  if(comparator(this->array[eidx].first,key) <= 0)
  {
      return this->array[eidx].second;
  }

  while(sidx <= eidx)
  {
      if(sidx == eidx) 
      {
          cmp_result = comparator(this->array[sidx].first, key);
          if(cmp_result > 0)
              return this->array[sidx-1].second;
          else 
              return this->array[sidx].second;
      }

      mid = (sidx+eidx)/2;
      cmp_result = comparator(this->array[mid].first, key);

      if(cmp_result == 0) 
          return this->array[mid].second;
      else if(cmp_result <  0) 
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
    this->array[0].second = old_value;
    this->array[1].first  = new_key;
    this->array[1].second = new_value;
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
  int idx = this->ValueIndex(old_value);
  
  if(idx == INVALID_INDEX) 
      return this->GetSize();

  for(int i=this->GetSize()-1; i>idx+1; i--)
    this->array[i] = this->array[i+1];
  
  this->array[idx+1].first = new_key;
  this->array[idx+1].second = new_value;
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) 
{
    int move_size = this->GetSize()>>1;

    recipient->CopyHalfFrom(this->array, move_size, buffer_pool_manager);
    this->SetSize(this->GetSize()-move_size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) 
{
    int start_idx = size+1;
 
    for(int i=0;i<size;i++) 
    {
        this->array[i] = items[start_idx++];
    }  
    
    this->IncreaseSize(size-1);
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
    for(int i=index;i<this->GetSize();i++)
        this->array[i] = this->array[i+1];

    this->DecreaseSize(1);
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
        (BPlusTreeInternalPage *)buffer_pool_manager->FetchPage
                                                    (this->GetParentPageId());

    this->array[0].first = parent->array[index_in_parent].first;
    recipient->CopyAllFrom(this->array, this->GetSize(), buffer_pool_manager);

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
    int move_size = recipient->GetMinSize() - recipient->GetSize();
    
    B_PLUS_TREE_INTERNAL_PG_PGID *parent = 
        (B_PLUS_TREE_INTERNAL_PG_PGID *)buffer_pool_manager->FetchPage
                                               (this->GetParentPageId());

    int index_in_parent = parent->ValueIndex(this->GetPageId());
    
    this->array[0].first = parent->array[index_in_parent].first;

    for(int i=0;i<move_size;i++)
        recipient->CopyLastFrom(this->array[i], buffer_pool_manager);
  
    for(int i=0;i<move_size;i++)
        this->array[i] = this->array[i+move_size];
    this->DecreaseSize(move_size);

    parent->array[index_in_parent].first = this->array[0].first;
    buffer_pool_manager->UnpinPage(parent->GetPageId(), true);

  /*this->array[0].first = this->array[1].first;
  receipent->CopyLastFrom(this->array[0]);

  for(int i=1;i<this->GetSize();i++)
  {
      array[i-1] = array[i];
  }
  this->SetSize(this->GetSize()-1);*/
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager) 
{
    /*BPlusTreeInternalPage *parent = 
                  buffer_pool_manager->FetchPage(this->parent_page_id_);
    int idx = parent->ValueIndex(this->GetPageId());

    this->array[this->GetSize()].first = parent->array[idx].first;
    this->array[this->GetSize()].second = pair.second;
    parent->array[idx].first = pair.first;

    this->IncreaseSize(1);

    buffer_pool_manager->UnpinPage(this->GetParentPageId(), true);*/

    this->array[this->GetSize()] = pair;
    this->IncreaseSize(1);
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
    int move_size = recipient->GetMinSize() - recipient->GetSize();
    
    B_PLUS_TREE_INTERNAL_PG_PGID *parent =
        (B_PLUS_TREE_INTERNAL_PG_PGID *)buffer_pool_manager->FetchPage
                                               (this->GetParentPageId());

    int index_in_parent = parent->ValueIndex(recipient->GetPageId());

    recipient->array[0].first = parent->array[index_in_parent].first;
    for(int i=recipient->GetSize()-1;i>=0; i--)
        recipient->array[i+move_size] = recipient->array[i]; 
    
    for(int i=this->GetSize()-move_size,j=0; i<this->GetSize(); i++,j++)
        recipient->CopyFirstFrom(this->array[i], j, buffer_pool_manager);
    this->DecreaseSize(move_size);

    parent->array[index_in_parent].first = recipient->array[0].first;

    buffer_pool_manager->UnpinPage(parent->GetPageId(), true);

   /* int i=0;
    for(i=recipient->GetSize(); i > 1; i--) 
    {
       recipient->array[i] = recipient->array[i-1];
    }
    recipient->IncreaseSize(1);
    recipient->CopyFirstFrom(this->array[this->GetSize()-1], 
                              parent_index, buffer_pool_manager);    
    this->SetSize(this->GetSize()-1);*/
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int insert_index,
    BufferPoolManager *buffer_pool_manager) 
{
    this->array[insert_index] = pair;
    /*BPlusTreeInternalPage *parent = 
                    buffer_pool_manager->FetchPage(this->GetParentPageId());

    this->array[1].first = parent->array[parent_index].first;
    this->array[0].second = pair.second;

    parent->array[parent_index].first = pair.first;
    buffer_pool_manager->UnpinPage(this->GetParentPageId(), true);*/
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
