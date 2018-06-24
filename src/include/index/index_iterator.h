/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

#define INDEXITERATOR_TYPE                                                     \
  IndexIterator<KeyType, ValueType, KeyComparator>

#define INDEXITERATOR_TYPE_LEAF                                               \
  IndexIterator<KeyType, RID, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  ~IndexIterator();
  void Init(BufferPoolManager *buffer_pool_manager, 
                        B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page);

  bool isEnd();

  const MappingType operator*();

  void operator++();

private:
  // add your own private member variables here
  B_PLUS_TREE_LEAF_PAGE_TYPE *start_page;
  B_PLUS_TREE_LEAF_PAGE_TYPE *current_page;
  B_PLUS_TREE_LEAF_PAGE_TYPE *end_page;

  BufferPoolManager *buffer_pool_manager;

  int current_index;
  int last_index;
};

} // namespace cmudb
