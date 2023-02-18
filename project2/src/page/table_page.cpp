/**
 * header_page.cpp
 */

#include <cassert>

#include "page/table_page.h"

namespace scudb {
/**
 * Header related
 */
void TablePage::Init(page_id_t page_id, size_t page_size,
                     page_id_t prev_page_id, LogManager *log_manager,
                     Transaction *txn) {
  memcpy(GetData(), &page_id, 4); // set page_id
  if (ENABLE_LOGGING) {
    // TODO: 在此处添加日志记录逻辑
  }
  SetPrevPageId(prev_page_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetFreeSpacePointer(page_size);
  SetTupleCount(0);
}

page_id_t TablePage::GetPageId() {
  return *reinterpret_cast<page_id_t *>(GetData());
}

page_id_t TablePage::GetPrevPageId() {
  return *reinterpret_cast<page_id_t *>(GetData() + 8);
}

page_id_t TablePage::GetNextPageId() {
  return *reinterpret_cast<page_id_t *>(GetData() + 12);
}

void TablePage::SetPrevPageId(page_id_t prev_page_id) {
  memcpy(GetData() + 8, &prev_page_id, 4);
}

void TablePage::SetNextPageId(page_id_t next_page_id) {
  memcpy(GetData() + 12, &next_page_id, 4);
}

/**
 * Tuple related
 */
bool TablePage::InsertTuple(const Tuple &tuple, RID &rid, Transaction *txn,
                            LockManager *lock_manager,
                            LogManager *log_manager) {
  assert(tuple.size_ > 0);
  if (GetFreeSpaceSize() < tuple.size_) {
    return false; // 空间不足
  }

  // 首先尝试重新使用空闲插槽
  int i;
  for (i = 0; i < GetTupleCount(); ++i) {
    rid.Set(GetPageId(), i);
    if (GetTupleSize(i) == 0) { // empty slot
      if (ENABLE_LOGGING) {
        assert(txn->GetSharedLockSet()->find(rid) ==
                   txn->GetSharedLockSet()->end() &&
               txn->GetExclusiveLockSet()->find(rid) ==
                   txn->GetExclusiveLockSet()->end());
      }
      break;
    }
  }

  // 没有剩余的空闲插槽
  if (i == GetTupleCount() && GetFreeSpaceSize() < tuple.size_ + 8) {
    return false; // 空间不足
  }

  SetFreeSpacePointer(GetFreeSpacePointer() -
                      tuple.size_); // 首先更新可用空间指针
  memcpy(GetData() + GetFreeSpacePointer(), tuple.data_, tuple.size_);
  SetTupleOffset(i, GetFreeSpacePointer());
  SetTupleSize(i, tuple.size_);
  if (i == GetTupleCount()) {
    rid.Set(GetPageId(), i);
    SetTupleCount(GetTupleCount() + 1);
  }
  // 设置rid后写入日志
  if (ENABLE_LOGGING) {
    // 获取独占锁
    assert(lock_manager->LockExclusive(txn, rid.Get()));
    // TODO:在此处添加日志记录逻辑
  }
  // LOG_DEBUG("Tuple inserted");
  return true;
}

/*
 *MarkDelete方法不会真正从表页中删除元组
 *相反，它通过将元组大小元数据更改为
 *否定，这样其他事务都不能重用此槽
 *
 */
bool TablePage::MarkDelete(const RID &rid, Transaction *txn,
                           LockManager *lock_manager, LogManager *log_manager) {
  int slot_num = rid.GetSlotNum();
  if (slot_num >= GetTupleCount()) {
    if (ENABLE_LOGGING) {
      txn->SetState(TransactionState::ABORTED);
    }
    return false;
  }

  int32_t tuple_size = GetTupleSize(slot_num);
  if (tuple_size < 0) {
    if (ENABLE_LOGGING) {
      txn->SetState(TransactionState::ABORTED);
    }
    return false;
  }

  if (ENABLE_LOGGING) {
    // 获取独占锁
    // 如果具有共享锁
    if (txn->GetSharedLockSet()->find(rid) != txn->GetSharedLockSet()->end()) {
      if (!lock_manager->LockUpgrade(txn, rid))
        return false;
    } else if (txn->GetExclusiveLockSet()->find(rid) ==
                   txn->GetExclusiveLockSet()->end() &&
               !lock_manager->LockExclusive(txn, rid)) { // 无共享锁
      return false;
    }
    // TODO:在此处添加日志记录逻辑
  }

  // 将元组大小设置为负值
  if (tuple_size > 0)
    SetTupleSize(slot_num, -tuple_size);
  return true;
}

bool TablePage::UpdateTuple(const Tuple &new_tuple, Tuple &old_tuple,
                            const RID &rid, Transaction *txn,
                            LockManager *lock_manager,
                            LogManager *log_manager) {
  int slot_num = rid.GetSlotNum();
  if (slot_num >= GetTupleCount()) {
    if (ENABLE_LOGGING) {
      txn->SetState(TransactionState::ABORTED);
    }
    return false;
  }
  int32_t tuple_size = GetTupleSize(slot_num); // old tuple size
  if (tuple_size <= 0) {
    if (ENABLE_LOGGING) {
      txn->SetState(TransactionState::ABORTED);
    }
    return false;
  }
  if (GetFreeSpaceSize() < new_tuple.size_ - tuple_size) {
    // 应删除/插入，因为空间不足
    return false;
  }

  // 复制旧值
  int32_t tuple_offset =
      GetTupleOffset(slot_num); // 旧元组的元组偏移量
  old_tuple.size_ = tuple_size;
  if (old_tuple.allocated_)
    delete[] old_tuple.data_;
  old_tuple.data_ = new char[old_tuple.size_];
  memcpy(old_tuple.data_, GetData() + tuple_offset, old_tuple.size_);
  old_tuple.rid_ = rid;
  old_tuple.allocated_ = true;

  if (ENABLE_LOGGING) {
    // 获取独占锁
    // 如果具有共享锁
    if (txn->GetSharedLockSet()->find(rid) != txn->GetSharedLockSet()->end()) {
      if (!lock_manager->LockUpgrade(txn, rid))
        return false;
    } else if (txn->GetExclusiveLockSet()->find(rid) ==
                   txn->GetExclusiveLockSet()->end() &&
               !lock_manager->LockExclusive(txn, rid)) { // no shared lock
      return false;
    }
    // TODO:在此处添加日志记录逻辑
  }

  // 更新
  int32_t free_space_pointer =
      GetFreeSpacePointer(); // 指向可用空间的旧指针
  assert(tuple_offset >= free_space_pointer);
  memmove(GetData() + free_space_pointer + tuple_size - new_tuple.size_,
          GetData() + free_space_pointer, tuple_offset - free_space_pointer);
  SetFreeSpacePointer(free_space_pointer + tuple_size - new_tuple.size_);
  memcpy(GetData() + tuple_offset + tuple_size - new_tuple.size_,
         new_tuple.data_,
         new_tuple.size_);                 // 复制新元组
  SetTupleSize(slot_num, new_tuple.size_); // 更新插槽中的元组大小
  for (int i = 0; i < GetTupleCount();
       ++i) { // 更新元组偏移量（包括更新的元组偏移量）
    int32_t tuple_offset_i = GetTupleOffset(i);
    if (GetTupleSize(i) > 0 && tuple_offset_i < tuple_offset + tuple_size) {
      SetTupleOffset(i, tuple_offset_i + tuple_size - new_tuple.size_);
    }
  }
  return true;
}

/*
 *ApplyDelete函数真正从表页中删除元组，并使槽
 *可再次使用。
 *当事务提交或撤消插入时调用此函数
 */
void TablePage::ApplyDelete(const RID &rid, Transaction *txn,
                            LogManager *log_manager) {
  int slot_num = rid.GetSlotNum();
  assert(slot_num < GetTupleCount());
  // 已删除元组的元组偏移量
  int32_t tuple_offset = GetTupleOffset(slot_num);
  int32_t tuple_size = GetTupleSize(slot_num);
  if (tuple_size < 0) { // 提交删除
    tuple_size = -tuple_size;
  } // else: 回滚插入操作

  // 复制删除值，用于撤消
  Tuple delete_tuple;
  delete_tuple.size_ = tuple_size;
  delete_tuple.data_ = new char[delete_tuple.size_];
  memcpy(delete_tuple.data_, GetData() + tuple_offset, delete_tuple.size_);
  delete_tuple.rid_ = rid;
  delete_tuple.allocated_ = true;

  if (ENABLE_LOGGING) {
    // 必须已获取独占锁
    assert(txn->GetExclusiveLockSet()->find(rid) !=
           txn->GetExclusiveLockSet()->end());
    // TODO:在此处添加日志记录逻辑
  }

  int32_t free_space_pointer =
      GetFreeSpacePointer(); // 指向可用空间的旧指针
  assert(tuple_offset >= free_space_pointer);
  memmove(GetData() + free_space_pointer + tuple_size,
          GetData() + free_space_pointer, tuple_offset - free_space_pointer);
  SetFreeSpacePointer(free_space_pointer + tuple_size);
  SetTupleSize(slot_num, 0);
  SetTupleOffset(slot_num, 0); // 无效偏移量
  for (int i = 0; i < GetTupleCount(); ++i) {
    int32_t tuple_offset_i = GetTupleOffset(i);
    if (GetTupleSize(i) != 0 && tuple_offset_i < tuple_offset) {
      SetTupleOffset(i, tuple_offset_i + tuple_size);
    }
  }
}

/*
 *RollbackDelete是与MarkDelete函数相关的补充函数。
 *它将元组大小从负值翻转为正值，从而使元组变为
 *再次可见。
 *中止事务时调用此函数
 */
void TablePage::RollbackDelete(const RID &rid, Transaction *txn,
                               LogManager *log_manager) {
  if (ENABLE_LOGGING) {
    // 必须已获取独占锁
    assert(txn->GetExclusiveLockSet()->find(rid) !=
           txn->GetExclusiveLockSet()->end());

    // TODO:在此处添加日志记录逻辑
  }

  int slot_num = rid.GetSlotNum();
  assert(slot_num < GetTupleCount());
  int32_t tuple_size = GetTupleSize(slot_num);

  // 将元组大小设置为正值
  if (tuple_size < 0)
    SetTupleSize(slot_num, -tuple_size);
}

bool TablePage::GetTuple(const RID &rid, Tuple &tuple, Transaction *txn,
                         LockManager *lock_manager) {
  int slot_num = rid.GetSlotNum();
  if (slot_num >= GetTupleCount()) {
    if (ENABLE_LOGGING)
      txn->SetState(TransactionState::ABORTED);
    return false;
  }
  int32_t tuple_size = GetTupleSize(slot_num);
  if (tuple_size <= 0) {
    if (ENABLE_LOGGING)
      txn->SetState(TransactionState::ABORTED);
    return false;
  }

  if (ENABLE_LOGGING) {
    // 获取共享锁
    if (txn->GetExclusiveLockSet()->find(rid) ==
            txn->GetExclusiveLockSet()->end() &&
        txn->GetSharedLockSet()->find(rid) == txn->GetSharedLockSet()->end() &&
        !lock_manager->LockShared(txn, rid)) {
      return false;
    }
  }

  int32_t tuple_offset = GetTupleOffset(slot_num);
  tuple.size_ = tuple_size;
  if (tuple.allocated_)
    delete[] tuple.data_;
  tuple.data_ = new char[tuple.size_];
  memcpy(tuple.data_, GetData() + tuple_offset, tuple.size_);
  tuple.rid_ = rid;
  tuple.allocated_ = true;
  return true;
}

/**
 * Tuple iterator
 */
bool TablePage::GetFirstTupleRid(RID &first_rid) {
  for (int i = 0; i < GetTupleCount(); ++i) {
    if (GetTupleSize(i) > 0) { // valid tuple
      first_rid.Set(GetPageId(), i);
      return true;
    }
  }
  // 当前页面中没有元组
  first_rid.Set(INVALID_PAGE_ID, -1);
  return false;
}

bool TablePage::GetNextTupleRid(const RID &cur_rid, RID &next_rid) {
  assert(cur_rid.GetPageId() == GetPageId());
  for (auto i = cur_rid.GetSlotNum() + 1; i < GetTupleCount(); ++i) {
    if (GetTupleSize(i) > 0) { // valid tuple
      next_rid.Set(GetPageId(), i);
      return true;
    }
  }
  return false; // 最后一个元组的结尾
}

/**
 * helper functions
 */

// tuple slots
int32_t TablePage::GetTupleOffset(int slot_num) {
  return *reinterpret_cast<int32_t *>(GetData() + 24 + 8 * slot_num);
}

int32_t TablePage::GetTupleSize(int slot_num) {
  return *reinterpret_cast<int32_t *>(GetData() + 28 + 8 * slot_num);
}

void TablePage::SetTupleOffset(int slot_num, int32_t offset) {
  memcpy(GetData() + 24 + 8 * slot_num, &offset, 4);
}

void TablePage::SetTupleSize(int slot_num, int32_t offset) {
  memcpy(GetData() + 28 + 8 * slot_num, &offset, 4);
}

// free space
int32_t TablePage::GetFreeSpacePointer() {
  return *reinterpret_cast<int32_t *>(GetData() + 16);
}

void TablePage::SetFreeSpacePointer(int32_t free_space_pointer) {
  memcpy(GetData() + 16, &free_space_pointer, 4);
}

// tuple count
int32_t TablePage::GetTupleCount() {
  return *reinterpret_cast<int32_t *>(GetData() + 20);
}

void TablePage::SetTupleCount(int32_t tuple_count) {
  memcpy(GetData() + 20, &tuple_count, 4);
}

// 用于自由空间计算
int32_t TablePage::GetFreeSpaceSize() {
  return GetFreeSpacePointer() - 24 - GetTupleCount() * 8;
}
}
