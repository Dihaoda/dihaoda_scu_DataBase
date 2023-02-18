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
    // TODO: �ڴ˴������־��¼�߼�
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
    return false; // �ռ䲻��
  }

  // ���ȳ�������ʹ�ÿ��в��
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

  // û��ʣ��Ŀ��в��
  if (i == GetTupleCount() && GetFreeSpaceSize() < tuple.size_ + 8) {
    return false; // �ռ䲻��
  }

  SetFreeSpacePointer(GetFreeSpacePointer() -
                      tuple.size_); // ���ȸ��¿��ÿռ�ָ��
  memcpy(GetData() + GetFreeSpacePointer(), tuple.data_, tuple.size_);
  SetTupleOffset(i, GetFreeSpacePointer());
  SetTupleSize(i, tuple.size_);
  if (i == GetTupleCount()) {
    rid.Set(GetPageId(), i);
    SetTupleCount(GetTupleCount() + 1);
  }
  // ����rid��д����־
  if (ENABLE_LOGGING) {
    // ��ȡ��ռ��
    assert(lock_manager->LockExclusive(txn, rid.Get()));
    // TODO:�ڴ˴������־��¼�߼�
  }
  // LOG_DEBUG("Tuple inserted");
  return true;
}

/*
 *MarkDelete�������������ӱ�ҳ��ɾ��Ԫ��
 *�෴����ͨ����Ԫ���СԪ���ݸ���Ϊ
 *�񶨣������������񶼲������ô˲�
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
    // ��ȡ��ռ��
    // ������й�����
    if (txn->GetSharedLockSet()->find(rid) != txn->GetSharedLockSet()->end()) {
      if (!lock_manager->LockUpgrade(txn, rid))
        return false;
    } else if (txn->GetExclusiveLockSet()->find(rid) ==
                   txn->GetExclusiveLockSet()->end() &&
               !lock_manager->LockExclusive(txn, rid)) { // �޹�����
      return false;
    }
    // TODO:�ڴ˴������־��¼�߼�
  }

  // ��Ԫ���С����Ϊ��ֵ
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
    // Ӧɾ��/���룬��Ϊ�ռ䲻��
    return false;
  }

  // ���ƾ�ֵ
  int32_t tuple_offset =
      GetTupleOffset(slot_num); // ��Ԫ���Ԫ��ƫ����
  old_tuple.size_ = tuple_size;
  if (old_tuple.allocated_)
    delete[] old_tuple.data_;
  old_tuple.data_ = new char[old_tuple.size_];
  memcpy(old_tuple.data_, GetData() + tuple_offset, old_tuple.size_);
  old_tuple.rid_ = rid;
  old_tuple.allocated_ = true;

  if (ENABLE_LOGGING) {
    // ��ȡ��ռ��
    // ������й�����
    if (txn->GetSharedLockSet()->find(rid) != txn->GetSharedLockSet()->end()) {
      if (!lock_manager->LockUpgrade(txn, rid))
        return false;
    } else if (txn->GetExclusiveLockSet()->find(rid) ==
                   txn->GetExclusiveLockSet()->end() &&
               !lock_manager->LockExclusive(txn, rid)) { // no shared lock
      return false;
    }
    // TODO:�ڴ˴������־��¼�߼�
  }

  // ����
  int32_t free_space_pointer =
      GetFreeSpacePointer(); // ָ����ÿռ�ľ�ָ��
  assert(tuple_offset >= free_space_pointer);
  memmove(GetData() + free_space_pointer + tuple_size - new_tuple.size_,
          GetData() + free_space_pointer, tuple_offset - free_space_pointer);
  SetFreeSpacePointer(free_space_pointer + tuple_size - new_tuple.size_);
  memcpy(GetData() + tuple_offset + tuple_size - new_tuple.size_,
         new_tuple.data_,
         new_tuple.size_);                 // ������Ԫ��
  SetTupleSize(slot_num, new_tuple.size_); // ���²���е�Ԫ���С
  for (int i = 0; i < GetTupleCount();
       ++i) { // ����Ԫ��ƫ�������������µ�Ԫ��ƫ������
    int32_t tuple_offset_i = GetTupleOffset(i);
    if (GetTupleSize(i) > 0 && tuple_offset_i < tuple_offset + tuple_size) {
      SetTupleOffset(i, tuple_offset_i + tuple_size - new_tuple.size_);
    }
  }
  return true;
}

/*
 *ApplyDelete���������ӱ�ҳ��ɾ��Ԫ�飬��ʹ��
 *���ٴ�ʹ�á�
 *�������ύ��������ʱ���ô˺���
 */
void TablePage::ApplyDelete(const RID &rid, Transaction *txn,
                            LogManager *log_manager) {
  int slot_num = rid.GetSlotNum();
  assert(slot_num < GetTupleCount());
  // ��ɾ��Ԫ���Ԫ��ƫ����
  int32_t tuple_offset = GetTupleOffset(slot_num);
  int32_t tuple_size = GetTupleSize(slot_num);
  if (tuple_size < 0) { // �ύɾ��
    tuple_size = -tuple_size;
  } // else: �ع��������

  // ����ɾ��ֵ�����ڳ���
  Tuple delete_tuple;
  delete_tuple.size_ = tuple_size;
  delete_tuple.data_ = new char[delete_tuple.size_];
  memcpy(delete_tuple.data_, GetData() + tuple_offset, delete_tuple.size_);
  delete_tuple.rid_ = rid;
  delete_tuple.allocated_ = true;

  if (ENABLE_LOGGING) {
    // �����ѻ�ȡ��ռ��
    assert(txn->GetExclusiveLockSet()->find(rid) !=
           txn->GetExclusiveLockSet()->end());
    // TODO:�ڴ˴������־��¼�߼�
  }

  int32_t free_space_pointer =
      GetFreeSpacePointer(); // ָ����ÿռ�ľ�ָ��
  assert(tuple_offset >= free_space_pointer);
  memmove(GetData() + free_space_pointer + tuple_size,
          GetData() + free_space_pointer, tuple_offset - free_space_pointer);
  SetFreeSpacePointer(free_space_pointer + tuple_size);
  SetTupleSize(slot_num, 0);
  SetTupleOffset(slot_num, 0); // ��Чƫ����
  for (int i = 0; i < GetTupleCount(); ++i) {
    int32_t tuple_offset_i = GetTupleOffset(i);
    if (GetTupleSize(i) != 0 && tuple_offset_i < tuple_offset) {
      SetTupleOffset(i, tuple_offset_i + tuple_size);
    }
  }
}

/*
 *RollbackDelete����MarkDelete������صĲ��亯����
 *����Ԫ���С�Ӹ�ֵ��תΪ��ֵ���Ӷ�ʹԪ���Ϊ
 *�ٴοɼ���
 *��ֹ����ʱ���ô˺���
 */
void TablePage::RollbackDelete(const RID &rid, Transaction *txn,
                               LogManager *log_manager) {
  if (ENABLE_LOGGING) {
    // �����ѻ�ȡ��ռ��
    assert(txn->GetExclusiveLockSet()->find(rid) !=
           txn->GetExclusiveLockSet()->end());

    // TODO:�ڴ˴������־��¼�߼�
  }

  int slot_num = rid.GetSlotNum();
  assert(slot_num < GetTupleCount());
  int32_t tuple_size = GetTupleSize(slot_num);

  // ��Ԫ���С����Ϊ��ֵ
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
    // ��ȡ������
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
  // ��ǰҳ����û��Ԫ��
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
  return false; // ���һ��Ԫ��Ľ�β
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

// �������ɿռ����
int32_t TablePage::GetFreeSpaceSize() {
  return GetFreeSpacePointer() - 24 - GetTupleCount() * 8;
}
}
