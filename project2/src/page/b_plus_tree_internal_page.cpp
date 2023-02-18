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
 *�����µ��ڲ�ҳ����Init����
 *��������ҳ�����͡����õ�ǰ��С������ҳ��id�����ø�id������
 *���ҳ���С
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) {
    
    SetPageType(IndexPageType::INTERNAL_PAGE);      // ����ҳ������
    SetSize(1);     // ��ʼ����С
    SetPageId(page_id);
    SetParentPageId(parent_id);
    // ��������С
    int size = (PAGE_SIZE - sizeof(BPlusTreeInternalPage)) / (sizeof(KeyType) + sizeof(ValueType));
    SetMaxSize(size);
}
/*
 *��ȡ/���������롰index��������
 *����ƫ�ƣ�
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
    // �滻Ϊ���Լ��Ĵ���
    assert(0 <= index && index < GetSize());
    return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
    assert(0 <= index && index < GetSize());
    array[index].first = key;
}

/*
 *���ڲ��Һͷ���������������ƫ��������Helper������ʹ��ֵ
 *�������롰value��
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
 *��ȡ�����롰index����Ҳ��Ϊ���飩������ֵ��Helper����
 *ƫ�ƣ�
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
    assert(0 <= index && index < GetSize());
    return array[index].second;
}
// ��ҳ������������ֵ
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType& value){
    assert(0 <= index && index < GetSize());
    array[index].second = value;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 *���Ҳ�����ָ����ҳ����ָ�루page_id��
 *�������롰key����
 *�ӵڶ�������ʼ��������һ����Ӧʼ����Ч��
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const {
    assert(GetSize() > 1);      // ���ڵ��ֵ
    // ���С����Сֵ���򷵻�null
    if (comparator(key, array[1].first) < 0){
        return array[0].second;
    }
    // ����������ֵ���򷵻���Ӧ�����ptr
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
 *��old_value+new_key��new_value����¸�ҳ
 *�����뵼�´�Ҷҳһֱ�������ʱ
 *ҳ�棬��Ӧ�ô���һ���µĸ�ҳ�沢�����Ԫ�ء�
 *ע�⣺�˷�������InsertIntoParent������b_plus_tree.cpp���е���
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
 *��new_key��new_value�Բ��뵽������ֵ�Ķ�֮��==
 *��ֵ��_V��
 *@return���������´�С
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
 * �Ӵ�ҳ����ɾ��һ����Կ��ֵ�Ե����ռ��ˡ�ҳ��
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
    
    auto half = (GetSize() + 1) / 2;
    recipient->CopyHalfFrom(array + GetSize() - half, half, buffer_pool_manager);

    // �����ӽڵ�id
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
 *������������������
 *����ƫ�ƣ�
 *ע�⣺ɾ���������洢��Կ��ֵ��
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
 *����ҳ���е����м�ֵ��ɾ�������ռ��ˡ�ҳ�棬Ȼ��
 *���¸�ҳ�е�relavent����ֵ�ԡ�
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
 *�Ӹ�ҳ����ɾ�����ռ��ˡ�β���ĵ�һ���ؼ��ֺ�ֵ��
 *ҳ�棬Ȼ������丸ҳ�е�relavent����ֵ�ԡ�
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
 *ɾ����ҳ�������һ����ֵ�������ռ��ˡ��ı���
 *ҳ�棬Ȼ������丸ҳ�е�relavent����ֵ�ԡ�
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

// �ڲ��ڵ��ֵ����ӦΪҳid_t
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