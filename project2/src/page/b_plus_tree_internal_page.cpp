/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace scudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 *创建新的内部页面后的Init方法
 *包括设置页面类型、设置当前大小、设置页面id、设置父id和设置
 *最大页面大小
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) {
    
    SetPageType(IndexPageType::INTERNAL_PAGE);      // 设置页面类型
    SetSize(1);     // 初始化大小
    SetPageId(page_id);
    SetParentPageId(parent_id);
    // 计算最大大小
    int size = (PAGE_SIZE - sizeof(BPlusTreeInternalPage)) / (sizeof(KeyType) + sizeof(ValueType));
    SetMaxSize(size);
}
/*
 *获取/设置与输入“index”（又名
 *阵列偏移）
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
    // 替换为您自己的代码
    assert(0 <= index && index < GetSize());
    return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
    assert(0 <= index && index < GetSize());
    array[index].first = key;
}

/*
 *用于查找和返回数组索引（或偏移量）的Helper方法，使其值
 *等于输入“value”
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
    for (int i = 0; i < GetSize(); i++) {
        if (array[i].second == value)
            return i;
    }
    return GetSize();
}

/*
 *获取与输入“index”（也称为数组）关联的值的Helper方法
 *偏移）
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
    assert(0 <= index && index < GetSize());
    return array[index].second;
}
// 在页面数组中设置值
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType& value){
    assert(0 <= index && index < GetSize());
    array[index].second = value;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 *查找并返回指向子页的子指针（page_id）
 *包含输入“key”的
 *从第二个键开始搜索（第一个键应始终无效）
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const {
    assert(GetSize() > 1);      // 检查节点的值
    // 如果小于最小值，则返回null
    if (comparator(key, array[1].first) < 0){
        return array[0].second;
    }
    // 如果大于最大值，则返回相应的最大ptr
    else if (comparator(key, array[GetSize() - 1].first) >= 0){
        return array[GetSize() - 1].second;
    }

    int low = 1;
    int high = GetSize() - 1;
    int mid;
    while (low < high && low + 1 != high){
        mid = low + (high - low) / 2;
        if (comparator(key, array[mid].first) < 0){
            high = mid;
        }
        else if (comparator(key, array[mid].first) > 0){
            low = mid;
        }
        else{
            return array[mid].second;
        }
    }
    return array[low].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 *用old_value+new_key和new_value填充新根页
 *当插入导致从叶页一直溢出到根时
 *页面，您应该创建一个新的根页面并填充其元素。
 *注意：此方法仅在InsertIntoParent（）（b_plus_tree.cpp）中调用
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot
(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value) {
    assert(GetSize() == 1);     //???????????????????????
    array[0].second = old_value;
    array[1] = { new_key, new_value };
    IncreaseSize(1);
}
/*
 *将new_key和new_value对插入到具有其值的对之后==
 *旧值（_V）
 *@return：插入后的新大小
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
    for (int i = GetSize(); i > 0; i--){
        if (array[i - 1].second == old_value){
            array[i] = { new_key, new_value };
            IncreaseSize(1);
            break;
        }
        array[i] = array[i - 1];
    }
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * 从此页面中删除一半密钥和值对到“收件人”页面
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
    
    auto half = (GetSize() + 1) / 2;
    recipient->CopyHalfFrom(array + GetSize() - half, half, buffer_pool_manager);

    // 更新子节点id
    for (auto index = GetSize() - half; index < GetSize(); ++index){
        auto* page = buffer_pool_manager->FetchPage(ValueAt(index));
        //if (page == nullptr){
        //    throw Exception(EXCEPTION_TYPE_INDEX,
        //        "all page are pinned while CopyLastFrom");
        //}
        auto child = reinterpret_cast<BPlusTreePage*>(page->GetData());
        child->SetParentPageId(recipient->GetPageId());
        assert(child->GetParentPageId() == recipient->GetPageId());
        buffer_pool_manager->UnpinPage(child->GetPageId(), true);
    }
    IncreaseSize(-1 * half);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
    assert(size > 0 && GetSize() == 1 && !IsLeafPage());
    for (int i = 0; i < size; i++) {
        array[i] = *items++;
    }
    IncreaseSize(size - 1);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 *根据输入索引（又名
 *阵列偏移）
 *注意：删除后连续存储密钥和值对
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
    assert(index >= 0 && index < GetSize());
    for (int i = index; i < GetSize() - 1; i++) {
        array[i] = array[i + 1];
    }
    IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
    IncreaseSize(-1);
    assert(GetSize() == 1);
    return ValueAt(0);
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 *将此页面中的所有键值对删除到“收件人”页面，然后
 *更新父页中的relavent键和值对。
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager) {
    auto* page = buffer_pool_manager->FetchPage(GetParentPageId());
    if (page == nullptr){
        throw Exception(EXCEPTION_TYPE_INDEX,
            "all page are pinned while MoveAllTo");
    }
    auto* parent = reinterpret_cast<BPlusTreeInternalPage*>(page->GetData());

    SetKeyAt(0, parent->KeyAt(index_in_parent));
    assert(parent->ValueAt(index_in_parent) == GetPageId());
    buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
    recipient->CopyAllFrom(array, GetSize(), buffer_pool_manager);

    for (auto index = 0; index < GetSize(); ++index){
        auto* page = buffer_pool_manager->FetchPage(ValueAt(index));
        if (page == nullptr){
            throw Exception(EXCEPTION_TYPE_INDEX,
                "all page are pinned while CopyLastFrom");
        }
        auto child = reinterpret_cast<BPlusTreePage*>(page->GetData());
        child->SetParentPageId(recipient->GetPageId());
        assert(child->GetParentPageId() == recipient->GetPageId());
        buffer_pool_manager->UnpinPage(child->GetPageId(), true);
    }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
    assert(GetSize() + size <= GetMaxSize());
    int start = GetSize();
    for (int i = 0; i < size; ++i){
        array[start + i] = *items++;
    }
    IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 *从该页面中删除“收件人”尾部的第一个关键字和值对
 *页面，然后更新其父页中的relavent键和值对。
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
    assert(GetSize() > 1);
    MappingType pair{ KeyAt(1), ValueAt(0) };
    page_id_t child_page_id = ValueAt(0);
    SetValueAt(0, ValueAt(1));
    Remove(1);

    recipient->CopyLastFrom(pair, buffer_pool_manager);

    auto* page = buffer_pool_manager->FetchPage(child_page_id);
    if (page == nullptr){
        throw Exception(EXCEPTION_TYPE_INDEX,
            "all page are pinned while CopyLastFrom");
    }
    auto child = reinterpret_cast<BPlusTreePage*>(page->GetData());
    child->SetParentPageId(recipient->GetPageId());
    assert(child->GetParentPageId() == recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
    
    assert(GetSize() + 1 <= GetMaxSize());
    auto* page = buffer_pool_manager->FetchPage(GetParentPageId());
    if (page == nullptr){
        throw Exception(EXCEPTION_TYPE_INDEX,
            "all page are pinned while CopyLastFrom");
    }
    auto parent = reinterpret_cast<BPlusTreeInternalPage*>(page->GetData());

    auto index = parent->ValueIndex(GetPageId());
    auto key = parent->KeyAt(index + 1);

    array[GetSize()] = { key, pair.second };
    IncreaseSize(1);
    parent->SetKeyAt(index + 1, pair.first);
    buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
}

/*
 *删除此页面中最后一个键值对至“收件人”的标题
 *页面，然后更新其父页中的relavent键和值对。
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
    
    assert(GetSize() > 1);
    IncreaseSize(-1);
    MappingType pair = array[GetSize()];
    page_id_t child_page_id = pair.second;

    recipient->CopyFirstFrom(pair, parent_index, buffer_pool_manager);

    auto* page = buffer_pool_manager->FetchPage(child_page_id);
    if (page == nullptr){
        throw Exception(EXCEPTION_TYPE_INDEX,
            "all page are pinned while CopyLastFrom");
    }
    auto child = reinterpret_cast<BPlusTreePage*>(page->GetData());
    child->SetParentPageId(recipient->GetPageId());
    assert(child->GetParentPageId() == recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager) {

    assert(GetSize() + 1 < GetMaxSize());
    auto* page = buffer_pool_manager->FetchPage(GetParentPageId());
    if (page == nullptr){
        throw Exception(EXCEPTION_TYPE_INDEX,
            "all page are pinned while CopyFirstFrom");
    }
    auto parent = reinterpret_cast<BPlusTreeInternalPage*>(page->GetData());
    auto key = parent->KeyAt(parent_index);
    parent->SetKeyAt(parent_index, pair.first);
    InsertNodeAfter(array[0].second, key, array[0].second);
    array[0].second = pair.second;

    buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < GetSize(); i++) {
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

// 内部节点的值类型应为页id_t
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
}