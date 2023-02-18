/**
 * b_plus_tree.h
 *
 *实现内部页面指向的简单b+树数据结构
 *搜索页和叶页包含实际数据。
 *（1）我们只支持唯一密钥
 *（2）支架插入和移除
 *（3）结构应动态收缩和增长
 *（4）实现范围扫描的索引迭代器
 */
#pragma once

#include <queue>
#include <vector>

#include "concurrency/transaction.h"
#include "index/index_iterator.h"
#include "page/b_plus_tree_internal_page.h"
#include "page/b_plus_tree_leaf_page.h"

namespace scudb {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>
// 提供交互式B+树API的主类。
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
public:
  explicit BPlusTree(const std::string &name,
                     BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator,
                     page_id_t root_page_id = INVALID_PAGE_ID);

  // 如果此B+树没有键和值，则返回true。
  bool IsEmpty() const;

  // 将键值对插入此B+树。
  bool Insert(const KeyType &key, const ValueType &value,
              Transaction *transaction = nullptr);

  // 从这个B+树中删除一个键及其值。
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // 返回与给定键关联的值
  bool GetValue(const KeyType &key, std::vector<ValueType> &result,
                Transaction *transaction = nullptr);

  // 索引迭代器
  INDEXITERATOR_TYPE Begin();
  INDEXITERATOR_TYPE Begin(const KeyType &key);

  // 使用简单的命令行将此B+树打印到stdout
  std::string ToString(bool verbose = false);

  // 从文件中读取数据并逐个插入
  void InsertFromFile(const std::string &file_name,
                      Transaction *transaction = nullptr);

  // 从文件中读取数据并逐个删除
  void RemoveFromFile(const std::string &file_name,
                      Transaction *transaction = nullptr);
  // 试验用暴露
  B_PLUS_TREE_LEAF_PAGE_TYPE *FindLeafPage(const KeyType &key,
                                           bool leftMost = false,
                                           OpType op = OpType::READ,
                                           Transaction *transaction = nullptr);
  // expose for test purpose
  bool Check(bool force = false);
  bool openCheck = true;
private:
  BPlusTreePage *FetchPage(page_id_t page_id);

  void StartNewTree(const KeyType &key, const ValueType &value);

  bool InsertIntoLeaf(const KeyType &key, const ValueType &value,
                      Transaction *transaction = nullptr);

  void InsertIntoParent(BPlusTreePage *old_node, const KeyType &key,
                        BPlusTreePage *new_node,
                        Transaction *transaction = nullptr);

  template <typename N> N *Split(N *node, Transaction *transaction);

  template <typename N>
  bool CoalesceOrRedistribute(N *node, Transaction *transaction = nullptr);

  template <typename N>
  bool FindLeftSibling(N *node, N * &sibling, Transaction *transaction);

  template <typename N>
  bool Coalesce(
          N *&neighbor_node, N *&node,
          BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
          int index, Transaction *transaction = nullptr);

  template <typename N> void Redistribute(N *neighbor_node, N *node, int index);

  bool AdjustRoot(BPlusTreePage *node);

  void UpdateRootPageId(int insert_record = false);

  BPlusTreePage *CrabingProtocalFetchPage(page_id_t page_id,OpType op, page_id_t previous, Transaction *transaction);

  void FreePagesInTransaction(bool exclusive,  Transaction *transaction, page_id_t cur = -1);

  inline void Lock(bool exclusive,Page * page) {
    if (exclusive) {
      page->WLatch();
    } else {
      page->RLatch();
    }
  }

  inline void Unlock(bool exclusive,Page * page) {
    if (exclusive) {
      page->WUnlatch();
    } else {
      page->RUnlatch();
    }
  }
  inline void Unlock(bool exclusive,page_id_t pageId) {
    auto page = buffer_pool_manager_->FetchPage(pageId);
    Unlock(exclusive,page);
    buffer_pool_manager_->UnpinPage(pageId,exclusive);
  }
  inline void LockRootPageId(bool exclusive) {
    if (exclusive) {
      mutex_.WLock();
    } else {
      mutex_.RLock();
    }
    rootLockedCnt++;
  }

  inline void TryUnlockRootPageId(bool exclusive) {
    if (rootLockedCnt > 0) {
      if (exclusive) {
        mutex_.WUnlock();
      } else {
        mutex_.RUnlock();
      }
      rootLockedCnt--;
    }
  }


  int isBalanced(page_id_t pid);
  bool isPageCorr(page_id_t pid,pair<KeyType,KeyType> &out);
  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  RWMutex mutex_;
  static thread_local int rootLockedCnt;

};
} // namespace scudb