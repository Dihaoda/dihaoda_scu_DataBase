/**
 * page.h
 *
 *���ڴ���ʵ������ҳ�İ�װ��������������
 *����ع�����ʹ�õ���Ϣ����pin_count/dirty_flag/page_id��
 *��ҳ���������ݿ�ϵͳ�еĻ�����Ԫ
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
  // ��ȡʵ������ҳ����
  inline char *GetData() { return data_; }
  // ��ȡҳ��id
  inline page_id_t GetPageId() { return page_id_; }
  // ��ȡҳ��pin����
  inline int GetPinCount() { return pin_count_; }
  // ��������/����ҳ�����ݵķ���
  inline void WUnlatch() { rwlatch_.WUnlock(); }
  inline void WLatch() { rwlatch_.WLock(); }
  inline void RUnlatch() { rwlatch_.RUnlock(); }
  inline void RLatch() { rwlatch_.RLock(); }

  inline lsn_t GetLSN() { return *reinterpret_cast<lsn_t *>(GetData() + 4); }
  inline void SetLSN(lsn_t lsn) { memcpy(GetData() + 4, &lsn, 4); }

private:
  // ����ع�����ʹ�õķ���
  inline void ResetMemory() { memset(data_, 0, PAGE_SIZE); }
  // members
  char data_[PAGE_SIZE]; // actual data
  page_id_t page_id_ = INVALID_PAGE_ID;
  int pin_count_ = 0;
  bool is_dirty_ = false;
  RWMutex rwlatch_;
};

} // namespace scudb
