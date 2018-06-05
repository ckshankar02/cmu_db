/*
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name, 
                                BufferPoolManager *buffer_pool_manager,
                                const KeyComparator &comparator,
                                page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const 
{ 
  if(this->root_page_id_ == INVALID_PAGE_ID) return true;
  return false; 
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) 
{
    bool res = false;
    BPlusTreePage *page_ptr = 
              this->buffer_pool_manager_->FetchPage(this->root_page_id_);

    ValueType pg_id = 0;

    while(!page_ptr->IsLeafPage())
    {
        pg_id = 
          ((BPlusTreeInternalPage *)page_ptr)->Lookup(key, this->comparator);
        this->buffer_pool_manager_->Unpin(page_ptr->GetPageId());
        page_ptr = this->buffer_pool_manager_->FetchPage(pg_id);
    }

    if(((BPlusTreeLeafPage *)page_ptr)->Lookup(key, pg_id, this->comparator))
    {
        result.push_back(pg_id);
        res = true;
    }

    this->buffer_pool_manager_->Unpin(page_ptr->GetPageId());
    return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction)
{
    /* Current tree is empty */
    if(this->IsEmpty())
    {
      this->StartNewTree(key, value);
      return true;
    }
    
    return this->InsertIntoLeaf(key, value, transaction); 
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) 
{
    page_id_t root_page_id;

    BPlusTreeLeafPage *root_page = 
      (BPlusTreeLeafPage*)this->buffer_pool_manager_->NewPage(root_page_id);

    if(root_page == nullptr) 
        throw "Out of memory!";

    this->root_page_id_ = root_page_id;

    this->UpdateRootPageId(true);

    root_page->array[0].first = key;
    root_page->array[0].second = value;

    this->buffer_pool_manager_->Unpin(this->root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) 
{
    BPlusTreeLeafPage *leaf_pg = nullptr, sib_leaf_pg = nullptr;
    BPlusTreePage *tree_pg = 
              this->buffer_pool_manager_->FetchPage(this->root_page_id_);

    KeyType nw_key = 0;
    ValueType val = 0;

    while(!tree_pg->IsLeafPage())
    {
        val = ((BPlusTreeInternalPage *)tree_pg)->Lookup(key, this->comparator);
        this->buffer_pool_manager_->UnpinPage(tree_pg->GetPageId());
        tree_pg = this->buffer_pool_manager_->FetchPage(page_id_ptr);
    }

    leaf_pg = (BPlusTreeLeafPage *)tree_pg;
    
    /* Key already exists. Trying to insert duplicate key*/
    if(!leaf_pg->Lookup(key, val, this->comparator))
    {
        this->buffer_pool_manager_->UnpinPage(leaf_pg->GetPageId(),false);
        return false;
    }

    /* Page/Node is not full */
    if(leaf_pg->GetSize() < leaf_pg->GetMaxSize())
        leaf_pg->Insert(key, value, this->comparator);
    else
    {
        /* Node is max'ed out, need to split */
        sib_leaf_pg = this->Split(leaf_pg);

        nw_key = sib_leaf_pg->array[0].first;

        if(this->comparator(nw_key, key) < 0)
            sib_leaf_pg->Insert(key, value, this->comparator);
        else
            leaf_pg->Insert(key, value, this->comparator);

        this->InsertIntoParent(leaf_pg, nw_key, sib_leaf_pg, transaction);  

        this->buffer_pool_manager_->UnpinPage(sib_leaf_pg->GetPageId(), true);
    }

    this->buffer_pool_manager_->UnpinPage(leaf_pg->GetPageId(), true);
    return true; 
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node) 
{
    page_id_t pg_id;
    page_id_t parent_pg_id = node->GetParentPageId();

    BPlusTreePage *bt_pg = 
      (BPlusTreePage *) this->buffer_pool_manager_->NewPage(pg_id);

    if(bt_pg == nullptr) 
        throw "Out of memory!";


    if(node->GetPageType == LEAF_PAGE)
    {
      BPlusTreeLeafPage *leaf_pg = ((BPlusTreeLeafPage *)bt_pg);
      leaf_pg->Init(pg_id, parent_pg_id);
      node->MoveHalfTo(leaf_pg, this->buffer_pool_manager_);
    }
    else
    {
      BPlusTreeInternalPage *int_pg = ((BPlusTreeInternalPage *)bt_pg);
      int_pg->Init(pg_id, parent_pg_id);
      node->MoveHalfTo(int_pg, this->buffer_pool_manager_);
    }

    this->buffer_pool_manager_->UnpinPage(bt_pg->GetPageId(), true);
    return bt_pg; 
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) 
{
    int idx_in_parent = 0;
    KeyType nw_key = 0;
    page_id_t parent_pg_id = old_node->GetParentPageId();
    BPlusTreeInternalPage *sib_pg = nullptr;
  
    BPlusTreeInternalPage *parent_pg = 
       this->buffer_pool_manager_->FetchPage(parent_pg_id);

    idx_in_parent = parent_pg->ValueIndex(old_node->GetPageId());

    /* Enough space left in parent */
    if(parent_pg->GetSize() < parent_pg->GetMaxSize())
    {
        parent_pg->InsertNodeAfter(old_node->GetPageId(),
                                   new_node->array[0].first, 
                                   new_node->GetPageId());
    }
    else /* Not enough space left. Split required */
    {
        ValueType val = 0;
        sib_pg = this->Split(parent_pg);
        nw_key = sib_pg->array[0].first;

        if(this->comparator(nw_key, key) < 0)
        {
            val = sib_pg->Lookup(key, this->comparator);
            sib_pg->InsertNodeAfter(val, key, new_node->GetPageId());
        }
        else
        {
            val = parent_pg->Lookup(key, this->comparator);
            parent_pg->InsertNodeAfter(val, key, new_node->GetPageId());
        }
        `
        if(parent->IsRootPage())
        {
            page_id_t root_pgid = 0;
            BufferPoolManager *bpm = this->buffer_pool_manager_;

            BPlusTreeInternalPage *new_root_pg = 
                (BPlusTreeInternalPage*)bpm->NewPage(root_pgid);
        
            new_root_pg->Init(root_pgid, NO_PARENT);

            this->root_page_id_ = root_pgid;

            this->UpdateRootPageId(false);

            parent_pg->SetParentPageId(root_pgid);
            sib_pg->SetParentPageId(root_pgid);

            new_root_pg->PopulateNewRoot(parent_pg, nw_key, sib_pg);
            this->buffer_pool_manager_(root_pgid, true);
        }
        else 
        {
            this->InsertIntoParent(parent_pg, nw_key, sib_pg, transaction);
        }

        this->buffer_pool_manager_(sib_pg->GetPageId(), true);
    }

    this->buffer_pool_manager_(parent_pg_id, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) 
{
    if(this->IsEmpty())
        return;
    
    BPlusTreeLeafPage *leaf_pg = this->FindLeafPage(key, false);

    leaf_pg->RemoveAndDeleteRecord(key, this->comparator);

    if(leaf_pg->IsRootPage())
    {
        if(leaf_pg->this->GetSize() <= 0)
        {
            this->buffer_pool_manager_->UnpinPage(leaf_pg->GetPageId());
            this->buffer_pool_manager_->DeletePage(leaf_pg->GetPageId());
        }
        return;
    }


    if(leaf_pg->GetSize() < leaf_pg->GetMinSize())
        this->CoalesceOrRedistribute(leaf_page, transaction);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction)
{
    if(node->IsRootPage())
    {
        return AdjustRoot(node);
    }      

    BPlusTreeInternalPage *parent = 
      (BPlusTreeInternalPage *)this->buffer_pool_manager_->FetchPage
                                                    (node->GetParentPageId());
    
    /* Check posibility of Coalescing */
    int parent_index = parent->ValueIndex(node->GetPageId());

    int sib_index = this->CheckMergeSibbling(parent_index, parent);

    if(sib_index >= 0)
    {
       BPlusTreePage *sib_pg = 
         this->buffer_pool_manager_->FetchPage(parent->array[sib_index].second);


       if(sib_index < parent_index)
          this->Coalesce(sib_pg, node, parent, parent_index, transaction);
       else 
          this->Coalesce(node, sib_pg, parent, sib_index, transaction);
        
          
       this->buffer_pool_manager_->UnpinPage(sib_pg, true);
       return true;
    }
    else 
    {
        this->Redistribute();
        return false;
    }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N *&neighbor_node, N *&node, 
              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
                                            int index, Transaction *transaction)
{
    /*
     *   Neighbor node - Recepient 
     *   Node          - Donor
     *   parent        - parent of donor & recepient
     *   index         - index of donor in parent node
     *   transaction   - not used
     */
    node->MoveAllTo(neighbor_node, index, this->buffer_pool_manager_);

    this->buffer_pool_manager_->UnpinPage(node->GetPageId(),true);
    this->buffer_pool_manager_->DeletePage(node->GetPageId());

    for(int i=index;i<parent->GetSize();i++)
        parent->array[i] = parent->array[i+1];

    parent->DecreaseSize(1);

    if(parent->GetSize() < parent->GetMinSize())
    {
        return this->CoalesceOrRedistribute(parent, transaction);
    }

    return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) 
{
    int parent_pg_id = node->GetParentPageId();
    BPlusTreeInternalPage *parent = (BPlusTreeInternalPage *)
      this->buffer_pool_manager_->FetchPage(parent_pg_id);

    int move_size = node->GetMinSize()-node->GetSize();
    
    if(index)
       neighbor_node->MoveFirstNTo(node, move_size, this->buffer_pool_manager_);
    else 
       node->MoveLastNTo(neighbor_node, move_size, this->buffer_pool_manager_);

    this->buffer_pool_manager_->UnpinPage(parent_pg_id);
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) 
{
    if(old_root_node->IsLeafPage()) 
        return true;

    this->root_page_id = old_root_node->array[0].second;
    this->buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(),false);
    this->buffer_pool_manager_->DeletePage(node->GetPageId());
    this->UpdateRootPageId(false);
    return false;
}

INDEX_TEMPLATE_ARGUMENTS
int CheckMergeSibbling(int parent_index, BPlusTreeInternalPage *parent)
{
    int left_child_size  = 0;
    int right_child_size = 0;

    int left_index  = 
              (parent_index==0)?-1:(parent_index-1);
    int right__index = 
              (parent_index==parent->GetSize()-1)?-1:(parent_index+1);

    if(left_index >= 0)
    {
       BPlusTreePage *bt_pg = 
        this->buffer_pool_manager_->FetchPage(parent->array[left_index].second);
      
       left_child_size = bt_pg->GetSize();
       this->buffer_pool_manager_->UnpingPage(bt_pg->GetPageId());
    }

    if(right_index >= 0)
    { 
       BPlusTreePage *bt_pg = 
       this->buffer_pool_manager_->FetchPage(parent->array[right_index].second);
      
       right_child_size = bt_pg->GetSize();
       this->buffer_pool_manager_->UnpingPage(bt_pg->GetPageId());
    }

    if(left_child_size < right_child_size)
    {
        if(left_child_size != 0) return left_index;
        else return right_index;
    }
    else if(left_child_size > right_child_size)
    {
        if(right_child_size != 0) return right_index;
        else return left_index;
    }
    else
       return INVALID_INDEX;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  return INDEXITERATOR_TYPE();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         bool leftMost) 
{   
    BPlusTreePage *page_ptr = 
              this->buffer_pool_manager_->FetchPage(this->root_page_id_);

    while(!page_ptr->IsLeafPage())
    {
        BPlusTreeInternalPage *int_pg_ptr = (BPlusTreeInternalPage *)page_ptr;

        if(leftMost)
            pg_id = int_pg_ptr->array[0].second;
        else
            pg_id = int_pg_ptr->Lookup(key, this->comparator);

        this->buffer_pool_manager_->Unpin(page_ptr->GetPageId());
        page_ptr = this->buffer_pool_manager_->FetchPage(pg_id);
    }
    
    return page_ptr;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) { return "Empty tree"; }

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}

/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
