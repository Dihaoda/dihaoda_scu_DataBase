/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace scudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                                BufferPoolManager *buffer_pool_manager,
                                const KeyComparator &comparator,
                                page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

INDEX_TEMPLATE_ARGUMENTS
thread_local bool BPLUSTREE_TYPE::root_is_locked = false;

/*
 * 确定当前b+树是否为空的Helper函数
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * 返回与输入键关联的唯一值
 * 此方法用于点查询
 * @return：true表示密钥存在
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) {
    auto* leaf = FindLeafPage(key, false, Operation::READONLY, transaction);
    bool ret = false;
    if (leaf != nullptr){
        ValueType value;
        if (leaf->Lookup(key, value, comparator_)){
            result.push_back(value);
            ret = true;
        }

        UnlockUnpinPages(Operation::READONLY, transaction);

        if (transaction == nullptr){
            auto page_id = leaf->GetPageId();
            buffer_pool_manager_->FetchPage(page_id)->RUnlatch();
            buffer_pool_manager_->UnpinPage(page_id, false);

            buffer_pool_manager_->UnpinPage(page_id, false);
        }
    }
    return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * 将常量键值对插入b+树
 * 如果当前树为空，则启动新树，更新根页id并插入
 * 否则插入叶页。
 * @return：由于我们只支持唯一密钥，如果用户尝试插入重复的
 * 键返回false，否则返回true。
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (IsEmpty()){
      StartNewTree(key, value);
      return true;
    }
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 *将常量键值对插入空树
 *用户需要首先从缓冲池管理器请求新页面（注意：抛出
 *如果返回值为nullptr，则出现“内存不足”异常），然后更新b+
 *树的根页id，并将条目直接插入叶页。
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
    auto* page = buffer_pool_manager_->NewPage(root_page_id_);
    if (page == nullptr){
        throw Exception(EXCEPTION_TYPE_INDEX,
            "all page are pinned while StartNewTree");
    }
    auto root =
        reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType,
        KeyComparator>*>(page->GetData());
    UpdateRootPageId(true);
    root->Init(root_page_id_, INVALID_PAGE_ID);
    root->Insert(key, value, comparator_);

    buffer_pool_manager_->UnpinPage(root->GetPageId(), true);
}

/*
 *将常量键和值对插入叶页
 *用户需要首先找到右页作为插入目标，然后查看
 *通过叶页查看插入键是否存在。如果存在，返回
 *immdiately，否则插入条目。如果需要，记得处理拆分。
 *@return：由于我们只支持唯一密钥，如果用户尝试插入重复的
 *键返回false，否则返回true。
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) {
    auto* leaf = FindLeafPage(key, false, Operation::INSERT, transaction);
    if (leaf == nullptr){
        return false;
    }
    ValueType v;
    if (leaf->Lookup(key, v, comparator_)){
        UnlockUnpinPages(Operation::INSERT, transaction);
        return false;
    }

    if (leaf->GetSize() < leaf->GetMaxSize()){
        leaf->Insert(key, value, comparator_);
    }
    else{
        auto* leaf2 = Split<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>(leaf);
        if (comparator_(key, leaf2->KeyAt(0)) < 0){
            leaf->Insert(key, value, comparator_);
        }
        else{
            leaf2->Insert(key, value, comparator_);
        }

        if (comparator_(leaf->KeyAt(0), leaf2->KeyAt(0)) < 0){
            leaf2->SetNextPageId(leaf->GetNextPageId());
            leaf->SetNextPageId(leaf2->GetPageId());
        }
        else{
            leaf2->SetNextPageId(leaf->GetPageId());
        }

        InsertIntoParent(leaf, leaf2->KeyAt(0), leaf2, transaction);
    }

    UnlockUnpinPages(Operation::INSERT, transaction);
    return true;
}

/*
 *拆分输入页面并返回新创建的页面。
 *使用模板N表示内部页面或叶页面。
 *用户需要首先从缓冲池管理器请求新页面（注意：抛出
 *如果返回值为nullptr，则出现“内存不足”异常），然后移动一半
 *从输入页到新创建的页的键和值对
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node) { 
    page_id_t page_id;
    auto* page = buffer_pool_manager_->NewPage(page_id);
    if (page == nullptr){
        throw Exception(EXCEPTION_TYPE_INDEX,
            "all page are pinned while Split");
    }
    auto new_node = reinterpret_cast<N*>(page->GetData());
    new_node->Init(page_id);
    node->MoveHalfTo(new_node, buffer_pool_manager_);
    return new_node;
}

/*
 *拆分后将键值对插入内部页面
 *@param old_node从split（）方法输入页面
 *@param键
 *@param new_node从split（）方法返回页面
 *用户需要首先找到old_node的父页，父节点必须是
 *调整为考虑new_node的信息。记住要处理拆分
 *如果需要，递归地。
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) {
    if (old_node->IsRootPage()){
        auto* page = buffer_pool_manager_->NewPage(root_page_id_);
        if (page == nullptr){
            throw Exception(EXCEPTION_TYPE_INDEX,
                "all page are pinned while InsertIntoParent");
        }
        assert(page->GetPinCount() == 1);
        auto root =
            reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
            KeyComparator>*>(page->GetData());
        root->Init(root_page_id_);
        root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

        old_node->SetParentPageId(root_page_id_);
        new_node->SetParentPageId(root_page_id_);

        UpdateRootPageId(false);
        buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(root->GetPageId(), true);
    }
    else{
        auto* page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
        if (page == nullptr){
            throw Exception(EXCEPTION_TYPE_INDEX,
                "all page are pinned while InsertIntoParent");
        }
        auto internal =
            reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
            KeyComparator>*>(page->GetData());

        if (internal->GetSize() < internal->GetMaxSize()){
            internal->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
            new_node->SetParentPageId(internal->GetPageId());
            buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
        }
        else{
            page_id_t page_id;
            auto* page = buffer_pool_manager_->NewPage(page_id);
            if (page == nullptr){
                throw Exception(EXCEPTION_TYPE_INDEX,
                    "all page are pinned while InsertIntoParent");
            }
            assert(page->GetPinCount() == 1);

            auto* copy =
                reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
                KeyComparator>*>(page->GetData());
            copy->Init(page_id);
            copy->SetSize(internal->GetSize());
            for (int i = 1, j = 0; i <= internal->GetSize(); ++i, ++j){
                if (internal->ValueAt(i - 1) == old_node->GetPageId()){
                    copy->SetKeyAt(j, key);
                    copy->SetValueAt(j, new_node->GetPageId());
                    ++j;
                }
                if (i < internal->GetSize()){
                    copy->SetKeyAt(j, internal->KeyAt(i));
                    copy->SetValueAt(j, internal->ValueAt(i));
                }
            }

            assert(copy->GetSize() == copy->GetMaxSize());
            auto internal2 =
                Split<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>(copy);

            internal->SetSize(copy->GetSize() + 1);
            for (int i = 0; i < copy->GetSize(); ++i){
                internal->SetKeyAt(i + 1, copy->KeyAt(i));
                internal->SetValueAt(i + 1, copy->ValueAt(i));
            }

            if (comparator_(key, internal2->KeyAt(0)) < 0){
                new_node->SetParentPageId(internal->GetPageId());
            }
            else if (comparator_(key, internal2->KeyAt(0)) == 0){
                new_node->SetParentPageId(internal2->GetPageId());
            }
            else{
                new_node->SetParentPageId(internal2->GetPageId());
                old_node->SetParentPageId(internal2->GetPageId());
            }

            buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
            buffer_pool_manager_->UnpinPage(copy->GetPageId(), false);
            buffer_pool_manager_->DeletePage(copy->GetPageId());

            InsertIntoParent(internal, internal2->KeyAt(0), internal2);
        }
        buffer_pool_manager_->UnpinPage(internal->GetPageId(), true);
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 *删除与输入键关联的键值对
 *如果当前树为空，则立即返回。
 *如果没有，用户需要首先找到右叶页作为删除目标，然后
 *从叶页中删除条目。记住在以下情况下处理重新分发或合并
 *必要时。
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
    if (IsEmpty())
        return;

    auto* leaf = FindLeafPage(key, false, Operation::DELETE, transaction);
    if (leaf != nullptr){
        int size_before_deletion = leaf->GetSize();
        if (leaf->RemoveAndDeleteRecord(key, comparator_) != size_before_deletion){
            if (CoalesceOrRedistribute(leaf, transaction)){
                transaction->AddIntoDeletedPageSet(leaf->GetPageId());
            }
        }
        UnlockUnpinPages(Operation::DELETE, transaction);
    }
}

/*
 *用户需要首先找到输入页面的同级。如果兄弟姐妹的大小+输入
 *页面大小>页面最大大小，然后重新分配。否则，请合并。
 *使用模板N表示内部页面或叶页面。
 *@return：true表示应该删除目标叶页，false表示没有
 *发生删除
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
    if (node->IsRootPage()){
        return AdjustRoot(node);
    }
    if (node->IsLeafPage()){
        if (node->GetSize() >= node->GetMinSize()){
            return false;
        }
    }
    else{
        if (node->GetSize() > node->GetMinSize()){
            return false;
        }
    }

    auto* page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    if (page == nullptr){
        throw Exception(EXCEPTION_TYPE_INDEX,
            "all page are pinned while CoalesceOrRedistribute");
    }
    auto parent =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
        KeyComparator>*>(page->GetData());
    int value_index = parent->ValueIndex(node->GetPageId());

    assert(value_index != parent->GetSize());

    int sibling_page_id;
    if (value_index == 0){
        sibling_page_id = parent->ValueAt(value_index + 1);
    }
    else{
        sibling_page_id = parent->ValueAt(value_index - 1);
    }

    page = buffer_pool_manager_->FetchPage(sibling_page_id);
    if (page == nullptr){
        throw Exception(EXCEPTION_TYPE_INDEX,
            "all page are pinned while CoalesceOrRedistribute");
    }
    
    page->WLatch();
    transaction->AddIntoPageSet(page);
    auto sibling = reinterpret_cast<N*>(page->GetData());
    bool redistribute = false;

    if (sibling->GetSize() + node->GetSize() > node->GetMaxSize()){
        redistribute = true;
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    }
    if (redistribute) {
        if (value_index == 0){
            Redistribute<N>(sibling, node, 1);
        }
        return false;
    }

    bool ret;
    if (value_index == 0) {
        Coalesce<N>(node, sibling, parent, 1, transaction);
        transaction->AddIntoDeletedPageSet(sibling_page_id);
        ret = false;
    }
    else {
        Coalesce<N>(sibling, node, parent, value_index, transaction);
        ret = true;
    }
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return ret;
}

/*
 *将所有键和值对从一个页面移动到其同级页面，并通知
 *缓冲池管理器删除此页面。父页必须调整为
 *考虑删除信息。记得处理联合或
 *如果需要，递归地重新分发。
 *使用模板N表示内部页面或叶页面。
 *@param neighbor_node输入“node”的同级页面
 *@param节点输入，来自方法coalizeOrRedistribute（）
 *@param输入“node”的父级页面
 *@return true表示应删除父节点，false表示不删除
 *发生
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Coalesce(
    N *&neighbor_node, N *&node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int index, Transaction *transaction) {
    
    node->MoveAllTo(neighbor_node, index, buffer_pool_manager_);
    parent->Remove(index);
    if (CoalesceOrRedistribute(parent, transaction)){
        transaction->AddIntoDeletedPageSet(parent->GetPageId());
    }
}

/*
 *将键和值对从一个页面重新分配到其同级页面。If索引==
 *0，将同级页面的第一个键和值对移动到输入“节点”的末尾，
 *否则，将同级页的最后一个键和值对移动到输入的开头
 *“节点”。
 *使用模板N表示内部页面或叶页面。
 *@param neighbor_node输入“node”的同级页面
 *@param节点输入，来自方法coalizeOrRedistribute（）
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
    if (index == 0){
        neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
    }
    else{
        auto* page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
        if (page == nullptr){
            throw Exception(EXCEPTION_TYPE_INDEX,
                "all page are pinned while Redistribute");
        }
        auto parent =
            reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
            KeyComparator>*>(page->GetData());
        int idx = parent->ValueIndex(node->GetPageId());
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);

        neighbor_node->MoveLastToFrontOf(node, idx, buffer_pool_manager_);
    }
}
/*
 *必要时更新根页
 *注意：根页面的大小可以小于最小大小，此方法仅
 *在联合OrRedistribute（）方法中调用
 *情况1：删除根页面中的最后一个元素，但根页面仍然
 *有最后一个孩子
 *案例2：删除整个b+树中的最后一个元素时
 *@return：true表示应该删除根页面，false表示不删除
 *发生
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
    if (old_root_node->IsLeafPage()){
        if (old_root_node->GetSize() == 0){
            root_page_id_ = INVALID_PAGE_ID;
            UpdateRootPageId(false);
            return true;
        }
        return false;
    }

    if (old_root_node->GetSize() == 1){
        auto root =
            reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
            KeyComparator>*>(old_root_node);
        root_page_id_ = root->ValueAt(0);
        UpdateRootPageId(false);

        auto* page = buffer_pool_manager_->FetchPage(root_page_id_);
        if (page == nullptr){
            throw Exception(EXCEPTION_TYPE_INDEX,
                "all page are pinned while AdjustRoot");
        }
        auto new_root =
            reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
            KeyComparator>*>(page->GetData());
        new_root->SetParentPageId(INVALID_PAGE_ID);
        buffer_pool_manager_->UnpinPage(root_page_id_, true);
        return true;
    }
    return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 *输入参数为空，请先找到最左边的页，然后构造
 *索引迭代器
 *@return：索引迭代器
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
    KeyType key{};
    return IndexIterator<KeyType, ValueType, KeyComparator>(
        FindLeafPage(key, true), 0, buffer_pool_manager_);
}

/*
 *输入参数为低键，查找包含输入键的叶页
 *首先，然后构造索引迭代器
 *@return：索引迭代器
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
    auto* leaf = FindLeafPage(key, false);
    int index = 0;
    if (leaf != nullptr)
        index = leaf->KeyIndex(key, comparator_);

    return IndexIterator<KeyType, ValueType, KeyComparator>(
        leaf, index, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockUnpinPages(Operation op, Transaction* transaction){
    if (transaction == nullptr)
        return;

    for (auto* page : *transaction->GetPageSet()){
        if (op == Operation::READONLY){
            page->RUnlatch();
            buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
        }
        else{
            page->WUnlatch();
            buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        }
    }
    transaction->GetPageSet()->clear();

    for (auto page_id : *transaction->GetDeletedPageSet()){
        buffer_pool_manager_->DeletePage(page_id);
    }
    transaction->GetDeletedPageSet()->clear();

    if (root_is_locked) {
        root_is_locked = false;
        unlockRoot();
    }
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::isSafe(N* node, Operation op){
    if (op == Operation::INSERT){
        return node->GetSize() < node->GetMaxSize();
    }
    else if (op == Operation::DELETE){
        return node->GetSize() > node->GetMinSize() + 1;
    }
    return true;
}
 
 /*
 *查找包含特定键的叶页，如果leftMost标志==true，则查找
 *最左边的页
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE* BPLUSTREE_TYPE::
 FindLeafPage(const KeyType &key, bool leftMost, Operation op, Transaction *transaction){
    if (op != Operation::READONLY){
        lockRoot();
        root_is_locked = true;
    }

    if (IsEmpty()){
        return nullptr;
    }

    auto* parent = buffer_pool_manager_->FetchPage(root_page_id_);
    if (parent == nullptr){
        throw Exception(EXCEPTION_TYPE_INDEX,
            "all page are pinned while FindLeafPage");
    }

    if (op == Operation::READONLY){
        parent->RLatch();
    }
    else{
        parent->WLatch();
    }
    if (transaction != nullptr){
        transaction->AddIntoPageSet(parent);
    }

    auto* node = reinterpret_cast<BPlusTreePage*>(parent->GetData());
    while (!node->IsLeafPage()) {
        auto internal =
            reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
            KeyComparator>*>(node);
        page_id_t parent_page_id = node->GetPageId(), child_page_id;
        if (leftMost){
            child_page_id = internal->ValueAt(0);
        }
        else {
            child_page_id = internal->Lookup(key, comparator_);
        }

        auto* child = buffer_pool_manager_->FetchPage(child_page_id);
        if (child == nullptr){
            throw Exception(EXCEPTION_TYPE_INDEX,
                "all page are pinned while FindLeafPage");
        }

        if (op == Operation::READONLY){
            child->RLatch();
            UnlockUnpinPages(op, transaction);
        }
        else{
            child->WLatch();
        }
        node = reinterpret_cast<BPlusTreePage*>(child->GetData());
        assert(node->GetParentPageId() == parent_page_id);

        if (op != Operation::READONLY && isSafe(node, op)){
            UnlockUnpinPages(op, transaction);
        }
        if (transaction != nullptr){
            transaction->AddIntoPageSet(child);
        }
        else{
            parent->RUnlatch();
            buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
            parent = child;
        }
    }
    return reinterpret_cast<BPlusTreeLeafPage<KeyType,
        ValueType, KeyComparator>*>(node);
}

/*
 *在头页中更新/插入根页id（其中page_id=0，header_page为
 *在include/page/header_page.h下定义）
 *每次更改根页面id时调用此方法。
 *@parameter:insert_record defalt值为false。当设置为真时，
 *将记录<index_name，root_page_id>插入页眉页，而不是
 *更新它。
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
 *此方法仅用于调试
 *按等级打印出整个b+树结构
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) {
    if (IsEmpty())
        return "Empty tree";

    std::queue<BPlusTreePage*> todo, tmp;
    std::stringstream tree;
    auto node = reinterpret_cast<BPlusTreePage*>(
        buffer_pool_manager_->FetchPage(root_page_id_));
    if (node == nullptr){
        throw Exception(EXCEPTION_TYPE_INDEX,
            "all page are pinned while printing");
    }
    todo.push(node);
    bool first = true;
    while (!todo.empty()){
        node = todo.front();
        if (first){
            first = false;
            tree << "| ";
        }
        // leaf page, print all key-value pairs
        if (node->IsLeafPage()){
            auto page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*>(node);
            tree << page->ToString(verbose) << "| ";
        }
        else{
            auto page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(node);
            tree << page->ToString(verbose) << "| ";
            page->QueueUpChildren(&tmp, buffer_pool_manager_);
        }
        todo.pop();
        if (todo.empty() && !tmp.empty()){
            todo.swap(tmp);
            tree << '\n';
            first = true;
        }
        // unpin node when we are done
        buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    }
    return tree.str();
}

/*
 *此方法仅用于测试
 *从文件中读取数据并逐个插入
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
 *此方法仅用于测试
 *从文件中读取数据并逐个删除
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

}