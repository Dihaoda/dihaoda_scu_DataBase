/**
 * b_plus_tree_page.h
 *
 *内部页和叶页都从该页继承。
 *
 *它实际上是每个B+树页面的标题部分
 *包含叶页和内部页共享的信息。
 *
 * Header format (size in byte, 20 bytes in total):
 * ----------------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 * ----------------------------------------------------------------------------
 * | ParentPageId (4) | PageId(4) |
 * ----------------------------------------------------------------------------
 */

#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>


#include "buffer/buffer_pool_manager.h"
#include "index/generic_key.h"

namespace scudb {

#define MappingType std::pair<KeyType, ValueType>

#define INDEX_TEMPLATE_ARGUMENTS                                               \
  template <typename KeyType, typename ValueType, typename KeyComparator>

// 定义页面类型枚举
enum class IndexPageType { INVALID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE };
enum class OpType { READ = 0, INSERT, DELETE };
// 抽象类。
class BPlusTreePage {
public:
  bool IsLeafPage() const;
  bool IsRootPage() const;
  void SetPageType(IndexPageType page_type);

  int GetSize() const;
  void SetSize(int size);
  void IncreaseSize(int amount);

  int GetMaxSize() const;
  void SetMaxSize(int max_size);
  int GetMinSize() const;

  page_id_t GetParentPageId() const;
  void SetParentPageId(page_id_t parent_page_id);

  page_id_t GetPageId() const;
  void SetPageId(page_id_t page_id);

  void SetLSN(lsn_t lsn = INVALID_LSN);

  bool IsSafe(OpType op);
private:
  // 成员变量，内部页和叶页共享的属性
  IndexPageType page_type_;
  lsn_t lsn_;
  int size_;
  int max_size_;
  page_id_t parent_page_id_;
  page_id_t page_id_;

};
} // namespace scudb
