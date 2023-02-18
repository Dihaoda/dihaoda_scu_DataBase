/**
 * page.h
 *
 *主内存中实际数据页的包装器，还包含簿记
 *缓冲池管理器使用的信息，如pin_count/dirty_flag/page_id。
 *将页面用作数据库系统中的基本单元
 */

#pragma once

#include <cstring>
#include <iostream>

#include "common/config.h"
#include "common/rwmutex.h"

namespace scudb {

class Page {
  friend class BufferPoolManager;

public:
  Page() { ResetMemory(); }
  ~Page(){};
  // 获取实际数据页内容
  inline char *GetData() { return data_; }
  // 获取页面id
  inline page_id_t GetPageId() { return page_id_; }
  // 获取页面pin计数
  inline int GetPinCount() { return pin_count_; }
  // 用于锁定/解锁页面内容的方法
  inline void WUnlatch() { rwlatch_.WUnlock(); }
  inline void WLatch() { rwlatch_.WLock(); }
  inline void RUnlatch() { rwlatch_.RUnlock(); }
  inline void RLatch() { rwlatch_.RLock(); }

  inline lsn_t GetLSN() { return *reinterpret_cast<lsn_t *>(GetData() + 4); }
  inline void SetLSN(lsn_t lsn) { memcpy(GetData() + 4, &lsn, 4); }

private:
  // 缓冲池管理器使用的方法
  inline void ResetMemory() { memset(data_, 0, PAGE_SIZE); }
  // members
  char data_[PAGE_SIZE]; // actual data
  page_id_t page_id_ = INVALID_PAGE_ID;
  int pin_count_ = 0;
  bool is_dirty_ = false;
  RWMutex rwlatch_;
};

} // namespace scudb
