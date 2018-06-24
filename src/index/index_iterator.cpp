/**
 * index_iterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"

namespace cmudb {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() 
{
    this->buffer_pool_manager->UnpinPage(start_page->GetPageId(), false);
    if(current_page != end_page)
        this->buffer_pool_manager->UnpinPage(current_page->GetPageId(), false);
    this->buffer_pool_manager->UnpinPage(end_page->GetPageId(), false);

    delete this;
}

INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::Init(BufferPoolManager *buffer_pool_manager,
                        B_PLUS_TREE_LEAF_PAGE_TYPE *start_leaf_page) 
{
    B_PLUS_TREE_LEAF_PAGE_TYPE *tmp_page;
    this->buffer_pool_manager = buffer_pool_manager;
    this->start_page = start_leaf_page;
    this->current_page = start_leaf_page;
  

    tmp_page = this->start_page;


    page_id_t next_page_id = tmp_page->GetNextPageId();
    while(next_page_id != -1)
    {
        if(tmp_page != start_page) 
            buffer_pool_manager->UnpinPage(tmp_page->GetPageId(), false);
        tmp_page = (B_PLUS_TREE_LEAF_PAGE_TYPE *)buffer_pool_manager->FetchPage(
                                                                  next_page_id);
        this->end_page = tmp_page;
        next_page_id = this->end_page->GetNextPageId();
    }

    this->current_index = 0;
    this->last_index = this->current_page->GetSize()-1;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() 
{
    if(!current_page) 
        return true;

    if(current_page != end_page) 
        return false;

    if(current_index <= last_index)
        return false;

    return true;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType INDEXITERATOR_TYPE::operator*() 
{
   return current_page->GetItem(current_index);
}

INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::operator++() 
{
    if(current_index <= last_index) 
    {
        current_index++;
    }
    else
    {
        if(current_page != end_page)
        {
            page_id_t next_page_id = current_page->GetNextPageId();
            buffer_pool_manager->UnpinPage(current_page->GetPageId(), false);
            current_page =
                (B_PLUS_TREE_LEAF_PAGE_TYPE *)buffer_pool_manager->FetchPage
                                                                (next_page_id);
            current_index = 0;
            last_index = current_page->GetSize()-1;
        }
    }
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;
} // namespace cmudb
