/**
 * b_plus_tree_page.cpp
 */
#include "page/b_plus_tree_page.h"

namespace cmudb {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
bool BPlusTreePage::IsLeafPage() const 
{
  if(this->GetPageType() == IndexPagetype::LEAF_PAGE)
      return true;

  return false;
}

bool BPlusTreePage::IsRootPage() const 
{
  if(this->GetParentId() == NO_PARENT) 
    return true;
 
  return false;
}


IndexPageType BPlusTree::GetPageType() const
{
  return this->page_type_;
}


void BPlusTreePage::SetPageType(IndexPageType page_type) 
{
  this->page_type_ = page_type;
}

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
int BPlusTreePage::GetSize() const 
{ 
  return this->size_; 
}

void BPlusTreePage::SetSize(int size) 
{
  this->size_ = size;
}

void BPlusTreePage::IncreaseSize(int amount) 
{
  this->size_ += amount;
}

void BPlusTreePage::DecreaseSize(int amount) 
{
  this->size_ -= amount;
}
/*
 * Helper methods to get/set max size (capacity) of the page
 */
int BPlusTreePage::GetMaxSize() const 
{ 
  return this->max_size_; 
}

void BPlusTreePage::SetMaxSize(int size) 
{
  this->max_size_ = size;
}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
int BPlusTreePage::GetMinSize() const 
{
  if(this->IsRootPage()) 
       return 2;

  return max_size_/2; 
}

/*
 * Helper methods to get/set parent page id
 */
page_id_t BPlusTreePage::GetParentPageId() const 
{ 
  return this->parent_page_id_; 
}

void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) 
{
  this->parent_page_id_ = parent_page_id;
}

/*
 * Helper methods to get/set self page id
 */
page_id_t BPlusTreePage::GetPageId() const 
{
  return this->page_id_;   
}

void BPlusTreePage::SetPageId(page_id_t page_id) 
{
  this->page_id_ = page_id;
}

int BPlusTreePage::GetHeaderSize() const
{
  if(this->page_type_ == LEAF_PAGE) 
      return 24; //24 bytes - Check out header format
  else 
      return 20; //20 bytes
}

} // namespace cmudb
