/**
 * b_plus_tree_internal_page.h
 *
 *在内部页面中存储n个索引键和n+1个子指针（page_id）。
 *指针PAGE_ID（i）指向其中所有键K满足以下条件的子树：
 *K（i）<=K<K（i+1）。
 *注意：由于键的数量不等于子指针的数量，
 *第一个密钥始终保持无效。也就是说，任何搜索/查找
 *应忽略第一个键。
 *
 *内部页面格式（按键按递增顺序存储）：
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */

#pragma once

#include <queue>

#include "page/b_plus_tree_page.h"

namespace scudb {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE                                         \
  BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>

#define B_PLUS_TREE_INTERNAL_PAGE  BPlusTreeInternalPage <KeyType, page_id_t, KeyComparator>
  
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
public:
  // 必须在“创建”新节点后调用initialize方法
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID);

  KeyType KeyAt(int index) const;
  void SetKeyAt(int index, const KeyType &key);
  int ValueIndex(const ValueType &value) const;
  ValueType ValueAt(int index) const;

  ValueType Lookup(const KeyType &key, const KeyComparator &comparator) const;
  void PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                       const ValueType &new_value);
  int InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                      const ValueType &new_value);
  void Remove(int index);
  ValueType RemoveAndReturnOnlyChild();

  void MoveHalfTo(BPlusTreeInternalPage *recipient,
                  BufferPoolManager *buffer_pool_manager);
  void MoveAllTo(BPlusTreeInternalPage *recipient, int index_in_parent,
                 BufferPoolManager *buffer_pool_manager);
  void MoveFirstToEndOf(BPlusTreeInternalPage *recipient,
                        BufferPoolManager *buffer_pool_manager);
  void MoveLastToFrontOf(BPlusTreeInternalPage *recipient,
                         int parent_index,
                         BufferPoolManager *buffer_pool_manager);
  // DEUBG and PRINT
  std::string ToString(bool verbose) const;
  void QueueUpChildren(std::queue<BPlusTreePage *> *queue,
                       BufferPoolManager *buffer_pool_manager);

private:
  void CopyHalfFrom(MappingType *items, int size,
                    BufferPoolManager *buffer_pool_manager);
  void CopyAllFrom(MappingType *items, int size,
                   BufferPoolManager *buffer_pool_manager);
  void CopyLastFrom(const MappingType &pair,
                    BufferPoolManager *buffer_pool_manager);
  void CopyFirstFrom(const MappingType &pair, int parent_index,
                     BufferPoolManager *buffer_pool_manager);
  MappingType array[0];
};
} // namespace scudb
