/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>
#include <include/page/b_plus_tree_internal_page.h>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"

namespace scudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 *������Ҷҳ���Init����
 *��������ҳ�����ͣ�����ǰ��С����Ϊ�㣬����ҳ��id/��id������
 *��һҳid����������С
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  assert(sizeof(BPlusTreeLeafPage) == 28);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize((PAGE_SIZE - sizeof(BPlusTreeLeafPage))/sizeof(MappingType) - 1); //minus 1 for insert first then split
}

/**
 * ����/��ȡ��һҳid�İ������򷽷�
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {next_page_id_ = next_page_id;}

/**
 *Helper�������ҵ�һ������i��ʹarray[i].first>=��
 *ע�⣺�˷���������������������ʱʹ��
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
    const KeyType &key, const KeyComparator &comparator) const {
  assert(GetSize() >= 0);
  int st = 0, ed = GetSize() - 1;
  while (st <= ed) { //find the last key in array <= input
    int mid = (ed - st) / 2 + st;
    if (comparator(array[mid].first,key) >= 0) ed = mid - 1;
    else st = mid + 1;
  }
  return ed + 1;
}

/*
 *���ڲ��Ҳ����������롰index��������
 *����ƫ�ƣ�
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  assert(index >= 0 && index < GetSize());
  return array[index].first;
}

/*
 *���ڲ��Ҳ���������������ļ���ֵ�Ե�Helper����
 *����������Ҳ��Ϊ����ƫ������
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  assert(index >= 0 && index < GetSize());
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 *����ֵ�Բ��뵽���������Ҷҳ�У�&V��
 *�����@����ҳ���С
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                       const ValueType &value,
                                       const KeyComparator &comparator) {
  int idx = KeyIndex(key,comparator); //first larger than key
  assert(idx >= 0);
  IncreaseSize(1);
  int curSize = GetSize();
  for (int i = curSize - 1; i > idx; i--) {
    array[i].first = array[i - 1].first;
    array[i].second = array[i - 1].second;
  }
  array[idx].first = key;
  array[idx].second = value;
  return curSize;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * �Ӵ�ҳ����ɾ��һ����Կ��ֵ�Ե����ռ��ˡ�ҳ��
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
    BPlusTreeLeafPage *recipient,
    __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
  assert(recipient != nullptr);
  int total = GetMaxSize() + 1;
  assert(GetSize() == total);
  //�������һ��
  int copyIdx = (total)/2;//7 is 4,5,6,7; 8 is 4,5,6,7,8
  for (int i = copyIdx; i < total; i++) {
    recipient->array[i - copyIdx].first = array[i].first;
    recipient->array[i - copyIdx].second = array[i].second;
  }
  //����ָ��
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(recipient->GetPageId());
  //���ϴ�С�����������ϴ�������һ����
  SetSize(copyIdx);
  recipient->SetSize(total - copyIdx);

}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size) {}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 *���ڸ����ļ���������Ƿ������Ҷҳ�С������
 *������Ӧֵ�洢�����롰value���в�����true��
 *�����Կ�����ڣ��򷵻�false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                        const KeyComparator &comparator) const {
  int idx = KeyIndex(key,comparator);
  if (idx < GetSize() && comparator(array[idx].first, key) == 0) {
    value = array[idx].second;
    return true;
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 *���Ȳ鿴Ҷҳ�������Ƿ����ɾ���������
 *���ڣ�ִ��ɾ���������������ء�
 *ע�⣺ɾ���������洢��Կ��ֵ��
 *ɾ����@����ҳ���С
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
    const KeyType &key, const KeyComparator &comparator) {
  int firIdxLargerEqualThanKey = KeyIndex(key,comparator);
  if (firIdxLargerEqualThanKey >= GetSize() || comparator(key,KeyAt(firIdxLargerEqualThanKey)) != 0) {
    return GetSize();
  }
  //quick deletion
  int tarIdx = firIdxLargerEqualThanKey;
  memmove(array + tarIdx, array + tarIdx + 1,
          static_cast<size_t>((GetSize() - tarIdx - 1)*sizeof(MappingType)));
  IncreaseSize(-1);
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 *����ҳ���е����м�ֵ��ɾ�������ռ��ˡ�ҳ�棬Ȼ��
 *������һҳid
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                           int, BufferPoolManager *) {
  assert(recipient != nullptr);

  //�������һ��
  int startIdx = recipient->GetSize();//7 is 4,5,6,7; 8 is 4,5,6,7,8
  for (int i = 0; i < GetSize(); i++) {
    recipient->array[startIdx + i].first = array[i].first;
    recipient->array[startIdx + i].second = array[i].second;
  }
  //����ָ��
  recipient->SetNextPageId(GetNextPageId());
  //���ϴ�С�����������ϴ�������һ����
  recipient->IncreaseSize(GetSize());
  SetSize(0);

}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) {}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 *����ҳ���еĵ�һ���ؼ��ֺ�ֵ��ɾ�������ռ��ˡ�ҳ�棬Ȼ��
 *���¸�ҳ�е�relavent����ֵ�ԡ�
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeLeafPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
  MappingType pair = GetItem(0);
  IncreaseSize(-1);
  memmove(array, array + 1, static_cast<size_t>(GetSize()*sizeof(MappingType)));
  recipient->CopyLastFrom(pair);
  //���¸�ҳ�е�relavent����ֵ�ԡ�
  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  B_PLUS_TREE_INTERNAL_PAGE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
  parent->SetKeyAt(parent->ValueIndex(GetPageId()), array[0].first);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  assert(GetSize() + 1 <= GetMaxSize());
  array[GetSize()] = item;
  IncreaseSize(1);
}
/*
 *����ҳ���е����һ����ֵ��ɾ�������ռ��ˡ�ҳ�棬Ȼ��
 *���¸�ҳ�е�relavent����ֵ�ԡ�
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeLeafPage *recipient, int parentIndex,
    BufferPoolManager *buffer_pool_manager) {
  MappingType pair = GetItem(GetSize() - 1);
  IncreaseSize(-1);
  recipient->CopyFirstFrom(pair, parentIndex, buffer_pool_manager);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
    const MappingType &item, int parentIndex,
    BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() + 1 < GetMaxSize());
  memmove(array + 1, array, GetSize()*sizeof(MappingType));
  IncreaseSize(1);
  array[0] = item;

  Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
  B_PLUS_TREE_INTERNAL_PAGE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
  parent->SetKeyAt(parentIndex, array[0].first);
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream stream;
  if (verbose) {
    stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
           << "]<" << GetSize() << "> ";
  }
  int entry = 0;
  int end = GetSize();
  bool first = true;

  while (entry < end) {
    if (first) {
      first = false;
    } else {
      stream << " ";
    }
    stream << std::dec << array[entry].first;
    if (verbose) {
      stream << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID,
                                       GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID,
                                       GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID,
                                       GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID,
                                       GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID,
                                       GenericComparator<64>>;
}