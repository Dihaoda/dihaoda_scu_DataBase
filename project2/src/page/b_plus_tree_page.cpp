/**
 * b_plus_tree_page.cpp
 */
#include "page/b_plus_tree_page.h"

namespace scudb {

/*
 *获取/设置页面类型的帮助程序方法
 *页面类型枚举类在b_plus_tree_Page.h中定义
 */
bool BPlusTreePage::IsLeafPage() const {
	return page_type_ == IndexPageType::LEAF_PAGE;
}
bool BPlusTreePage::IsRootPage() const {
	return parent_page_id_ == INVALID_PAGE_ID;
}
void BPlusTreePage::SetPageType(IndexPageType page_type) {
	page_type_ = page_type;
}

/*
 *获取/设置大小的帮助程序方法（其中存储的键/值对的数量
 *第页）
 */
int BPlusTreePage::GetSize() const { 
	return size_;
}
void BPlusTreePage::SetSize(int size) { 
	size_ = size; 
}
void BPlusTreePage::IncreaseSize(int amount) { 
	size_ += amount; 
}

/*
 * 获取/设置页面最大大小（容量）的帮助程序方法
 */
int BPlusTreePage::GetMaxSize() const { 
	return max_size_;
}
void BPlusTreePage::SetMaxSize(int size) {
	max_size_ = size;
}

/*
 *获取最小页面大小的Helper方法
 *通常，最小页面大小==最大页面大小/2
 */
int BPlusTreePage::GetMinSize() const {
	return max_size_ / 2;
}

/*
 * 获取/设置父页id的帮助程序方法
 */
page_id_t BPlusTreePage::GetParentPageId() const { 
	return parent_page_id_;
}
void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) {
	parent_page_id_ = parent_page_id;
}

/*
 * 获取/设置自身页面id的帮助程序方法
 */
page_id_t BPlusTreePage::GetPageId() const { 
	return page_id_;
}
void BPlusTreePage::SetPageId(page_id_t page_id) {
	page_id_ = page_id;
}

/*
 * 设置lsn的帮助程序方法
 */
void BPlusTreePage::SetLSN(lsn_t lsn) { lsn_ = lsn; }

}