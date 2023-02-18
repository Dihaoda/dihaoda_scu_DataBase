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
 * ȷ����ǰb+���Ƿ�Ϊ�յ�Helper����
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * �����������������Ψһֵ
 * �˷������ڵ��ѯ
 * @return��true��ʾ��Կ����
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
 * ��������ֵ�Բ���b+��
 * �����ǰ��Ϊ�գ����������������¸�ҳid������
 * �������Ҷҳ��
 * @return����������ֻ֧��Ψһ��Կ������û����Բ����ظ���
 * ������false�����򷵻�true��
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
 *��������ֵ�Բ������
 *�û���Ҫ���ȴӻ���ع�����������ҳ�棨ע�⣺�׳�
 *�������ֵΪnullptr������֡��ڴ治�㡱�쳣����Ȼ�����b+
 *���ĸ�ҳid��������Ŀֱ�Ӳ���Ҷҳ��
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
 *����������ֵ�Բ���Ҷҳ
 *�û���Ҫ�����ҵ���ҳ��Ϊ����Ŀ�꣬Ȼ��鿴
 *ͨ��Ҷҳ�鿴������Ƿ���ڡ�������ڣ�����
 *immdiately�����������Ŀ�������Ҫ���ǵô����֡�
 *@return����������ֻ֧��Ψһ��Կ������û����Բ����ظ���
 *������false�����򷵻�true��
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
 *�������ҳ�沢�����´�����ҳ�档
 *ʹ��ģ��N��ʾ�ڲ�ҳ���Ҷҳ�档
 *�û���Ҫ���ȴӻ���ع�����������ҳ�棨ע�⣺�׳�
 *�������ֵΪnullptr������֡��ڴ治�㡱�쳣����Ȼ���ƶ�һ��
 *������ҳ���´�����ҳ�ļ���ֵ��
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
 *��ֺ󽫼�ֵ�Բ����ڲ�ҳ��
 *@param old_node��split������������ҳ��
 *@param��
 *@param new_node��split������������ҳ��
 *�û���Ҫ�����ҵ�old_node�ĸ�ҳ�����ڵ������
 *����Ϊ����new_node����Ϣ����סҪ������
 *�����Ҫ���ݹ�ء�
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
 *ɾ��������������ļ�ֵ��
 *�����ǰ��Ϊ�գ����������ء�
 *���û�У��û���Ҫ�����ҵ���Ҷҳ��Ϊɾ��Ŀ�꣬Ȼ��
 *��Ҷҳ��ɾ����Ŀ����ס����������´������·ַ���ϲ�
 *��Ҫʱ��
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
 *�û���Ҫ�����ҵ�����ҳ���ͬ��������ֵܽ��õĴ�С+����
 *ҳ���С>ҳ������С��Ȼ�����·��䡣������ϲ���
 *ʹ��ģ��N��ʾ�ڲ�ҳ���Ҷҳ�档
 *@return��true��ʾӦ��ɾ��Ŀ��Ҷҳ��false��ʾû��
 *����ɾ��
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
 *�����м���ֵ�Դ�һ��ҳ���ƶ�����ͬ��ҳ�棬��֪ͨ
 *����ع�����ɾ����ҳ�档��ҳ�������Ϊ
 *����ɾ����Ϣ���ǵô������ϻ�
 *�����Ҫ���ݹ�����·ַ���
 *ʹ��ģ��N��ʾ�ڲ�ҳ���Ҷҳ�档
 *@param neighbor_node���롰node����ͬ��ҳ��
 *@param�ڵ����룬���Է���coalizeOrRedistribute����
 *@param���롰node���ĸ���ҳ��
 *@return true��ʾӦɾ�����ڵ㣬false��ʾ��ɾ��
 *����
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
 *������ֵ�Դ�һ��ҳ�����·��䵽��ͬ��ҳ�档If����==
 *0����ͬ��ҳ��ĵ�һ������ֵ���ƶ������롰�ڵ㡱��ĩβ��
 *���򣬽�ͬ��ҳ�����һ������ֵ���ƶ�������Ŀ�ͷ
 *���ڵ㡱��
 *ʹ��ģ��N��ʾ�ڲ�ҳ���Ҷҳ�档
 *@param neighbor_node���롰node����ͬ��ҳ��
 *@param�ڵ����룬���Է���coalizeOrRedistribute����
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
 *��Ҫʱ���¸�ҳ
 *ע�⣺��ҳ��Ĵ�С����С����С��С���˷�����
 *������OrRedistribute���������е���
 *���1��ɾ����ҳ���е����һ��Ԫ�أ�����ҳ����Ȼ
 *�����һ������
 *����2��ɾ������b+���е����һ��Ԫ��ʱ
 *@return��true��ʾӦ��ɾ����ҳ�棬false��ʾ��ɾ��
 *����
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
 *�������Ϊ�գ������ҵ�����ߵ�ҳ��Ȼ����
 *����������
 *@return������������
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
    KeyType key{};
    return IndexIterator<KeyType, ValueType, KeyComparator>(
        FindLeafPage(key, true), 0, buffer_pool_manager_);
}

/*
 *�������Ϊ�ͼ������Ұ����������Ҷҳ
 *���ȣ�Ȼ��������������
 *@return������������
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
 *���Ұ����ض�����Ҷҳ�����leftMost��־==true�������
 *����ߵ�ҳ
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
 *��ͷҳ�и���/�����ҳid������page_id=0��header_pageΪ
 *��include/page/header_page.h�¶��壩
 *ÿ�θ��ĸ�ҳ��idʱ���ô˷�����
 *@parameter:insert_record defaltֵΪfalse��������Ϊ��ʱ��
 *����¼<index_name��root_page_id>����ҳüҳ��������
 *��������
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
 *�˷��������ڵ���
 *���ȼ���ӡ������b+���ṹ
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
 *�˷��������ڲ���
 *���ļ��ж�ȡ���ݲ��������
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
 *�˷��������ڲ���
 *���ļ��ж�ȡ���ݲ����ɾ��
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