/**
 * b_plus_tree.h
 *
 *ʵ���ڲ�ҳ��ָ��ļ�b+�����ݽṹ
 *����ҳ��Ҷҳ����ʵ�����ݡ�
 *��1������ֻ֧��Ψһ��Կ
 *��2��֧�ܲ�����Ƴ�
 *��3���ṹӦ��̬����������
 *��4��ʵ�ַ�Χɨ�������������
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
// �ṩ����ʽB+��API�����ࡣ
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
public:
  explicit BPlusTree(const std::string &name,
                     BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator,
                     page_id_t root_page_id = INVALID_PAGE_ID);

  // �����B+��û�м���ֵ���򷵻�true��
  bool IsEmpty() const;

  // ����ֵ�Բ����B+����
  bool Insert(const KeyType &key, const ValueType &value,
              Transaction *transaction = nullptr);

  // �����B+����ɾ��һ��������ֵ��
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // �����������������ֵ
  bool GetValue(const KeyType &key, std::vector<ValueType> &result,
                Transaction *transaction = nullptr);

  // ����������
  INDEXITERATOR_TYPE Begin();
  INDEXITERATOR_TYPE Begin(const KeyType &key);

  // ʹ�ü򵥵������н���B+����ӡ��stdout
  std::string ToString(bool verbose = false);

  // ���ļ��ж�ȡ���ݲ��������
  void InsertFromFile(const std::string &file_name,
                      Transaction *transaction = nullptr);

  // ���ļ��ж�ȡ���ݲ����ɾ��
  void RemoveFromFile(const std::string &file_name,
                      Transaction *transaction = nullptr);
  // �����ñ�¶
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