/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"

namespace scudb {

#define INDEXITERATOR_TYPE                                                     \
  IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // 您可以根据成员变量定义自己的构造函数
  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, int index, BufferPoolManager *bufferPoolManager);
  ~IndexIterator();

  bool isEnd(){
    return (leaf_ == nullptr);
  }

  const MappingType &operator*() {
    return leaf_->GetItem(index_);
  }

  IndexIterator &operator++() {
    index_++;
    if (index_ >= leaf_->GetSize()) {
      page_id_t next = leaf_->GetNextPageId();
      UnlockAndUnPin();
      if (next == INVALID_PAGE_ID) {
        leaf_ = nullptr;
      } else {
        Page *page = bufferPoolManager_->FetchPage(next);
        page->RLatch();
        leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
        index_ = 0;
      }
    }
    return *this;
  }

private:
  // 在此处添加您自己的私有成员变量
  void UnlockAndUnPin() {
    bufferPoolManager_->FetchPage(leaf_->GetPageId())->RUnlatch();
    bufferPoolManager_->UnpinPage(leaf_->GetPageId(), false);
    bufferPoolManager_->UnpinPage(leaf_->GetPageId(), false);
  }
  int index_;
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_;
  BufferPoolManager *bufferPoolManager_;
};

} // namespace scudb
