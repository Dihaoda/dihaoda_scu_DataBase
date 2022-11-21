//邸浩达2019141410141
#include "buffer/buffer_pool_manager.h"

namespace scudb {
//BufferPoolManager结构器
//当log_manager是nullptr的时候, logging被禁用
BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager) : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  //缓冲池的连续内存空间
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHash<page_id_t, Page *>(BUCKET_SIZE);
  replacer_ = new LRUReplacer<Page *>;
  free_list_ = new std::list<Page *>;
  //将所有页面放入空闲列表
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_->push_back(&pages_[i]);
  }
}
//BufferPoolManager析构器
BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
  delete free_list_;
}
/**
1. 搜索哈希表
1.1 如果存在，请锁定页面并立即返回
1.2 如果不存在，请从自由列表或lru中查找替换条目更换器。（注意：总是先从自由列表中查找）
2. 如果选择替换的条目是脏的，请将其写回磁盘。
3. 从哈希表中删除旧页面的条目，并插入新页面的条目。
4. 更新页面元数据，从磁盘文件读取页面内容并返回页面指针
*/
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  lock_guard<mutex> lck(latch_);
  Page *tar = nullptr;
  if (page_table_->Find(page_id,tar)) {
    tar->pin_count_++;
    replacer_->Erase(tar);
    return tar;
  }
  tar = GetVictimPage();
  if (tar == nullptr) return tar;
  if (tar->is_dirty_) {
    disk_manager_->WritePage(tar->GetPageId(),tar->data_);
  }
  page_table_->Remove(tar->GetPageId());
  page_table_->Insert(page_id,tar);
  disk_manager_->ReadPage(page_id,tar->data_);
  tar->pin_count_ = 1;
  tar->is_dirty_ = false;
  tar->page_id_= page_id;
  return tar;
}
/*
1. 搜索哈希表
1.1 如果存在，请锁定页面并立即返回
1.2 如果不存在，请从自由列表或lru中查找替换条目更换器。（注意：总是先从自由列表中查找）
2. 如果选择替换的条目是脏的，请将其写回磁盘。
3. 从哈希表中删除旧页面的条目，并插入新页面的条目。
4. 更新页面元数据，从磁盘文件读取页面内容并返回页面指针
*/
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  lock_guard<mutex> lck(latch_);
  Page *tar = nullptr;
  page_table_->Find(page_id,tar);
  if (tar == nullptr) {
    return false;
  }
  tar->is_dirty_ = is_dirty;
  if (tar->GetPinCount() <= 0) {
    return false;
  }
  ;
  if (--tar->pin_count_ == 0) {
    replacer_->Insert(tar);
  }
  return true;
}
/*
用于将缓冲池的特定页面刷新到磁盘。应调用磁盘管理器的write_page方法
如果在页表中找不到页，则返回false
注意：确保page_id != 无效的页面ID
*/
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  lock_guard<mutex> lck(latch_);
  Page *tar = nullptr;
  page_table_->Find(page_id,tar);
  if (tar == nullptr || tar->page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  if (tar->is_dirty_) {
    disk_manager_->WritePage(page_id,tar->GetData());
    tar->is_dirty_ = false;
  }
  return true;
}

/*
用户应调用此方法删除页面。此例程将调用磁盘管理器以释放页面。首先，如果在页面中找到页面表，缓冲池管理器应负责删除此条目
重新设置页面元数据并添加回自由列表。第二调用磁盘管理器的DeallocatePage()方法从磁盘文件中删除。如果页面在页面表中找到，但pin_count != 0，返回false
*/
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  lock_guard<mutex> lck(latch_);
  Page *tar = nullptr;
  page_table_->Find(page_id,tar);
  if (tar != nullptr) {
    if (tar->GetPinCount() > 0) {
      return false;
    }
    replacer_->Erase(tar);
    page_table_->Remove(page_id);
    tar->is_dirty_= false;
    tar->ResetMemory();
    free_list_->push_back(tar);
  }
  disk_manager_->DeallocatePage(page_id);
  return true;
}
/*
如果需要创建新页面，用户应调用此方法。此例程调用磁盘管理器来分配页面。
缓冲池管理器应负责选择受害页面从空闲列表或lru-replacer（注意：始终先从空闲列表中选择），
更新新页面的元数据，清空内存并添加相应的条目到页表中。如果池中的所有页面都被固定，则返回nullptr
*/
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  lock_guard<mutex> lck(latch_);
  Page *tar = nullptr;
  tar = GetVictimPage();
  if (tar == nullptr) return tar;
  page_id = disk_manager_->AllocatePage();
  if (tar->is_dirty_) {
    disk_manager_->WritePage(tar->GetPageId(),tar->data_);
  }
  page_table_->Remove(tar->GetPageId());
  page_table_->Insert(page_id,tar);
  tar->page_id_ = page_id;
  tar->ResetMemory();
  tar->is_dirty_ = false;
  tar->pin_count_ = 1;
  return tar;
}
Page *BufferPoolManager::GetVictimPage() {
  Page *tar = nullptr;
  if (free_list_->empty()) {
    if (replacer_->Size() == 0) {
      return nullptr;
    }
    replacer_->Victim(tar);
  } else {
    tar = free_list_->front();
    free_list_->pop_front();
    assert(tar->GetPageId() == INVALID_PAGE_ID);
  }
  assert(tar->GetPinCount() == 0);
  return tar;
}
} // 命名空间scudb
