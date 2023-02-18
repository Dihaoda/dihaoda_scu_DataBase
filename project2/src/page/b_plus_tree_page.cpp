/**
 * b_plus_tree_page.cpp
 */
#include "page/b_plus_tree_page.h"

namespace scudb {

/*
 *��ȡ/����ҳ�����͵İ������򷽷�
 *ҳ������ö������b_plus_tree_Page.h�ж���
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
 *��ȡ/���ô�С�İ������򷽷������д洢�ļ�/ֵ�Ե�����
 *��ҳ��
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
 * ��ȡ/����ҳ������С���������İ������򷽷�
 */
int BPlusTreePage::GetMaxSize() const { 
	return max_size_;
}
void BPlusTreePage::SetMaxSize(int size) {
	max_size_ = size;
}

/*
 *��ȡ��Сҳ���С��Helper����
 *ͨ������Сҳ���С==���ҳ���С/2
 */
int BPlusTreePage::GetMinSize() const {
	return max_size_ / 2;
}

/*
 * ��ȡ/���ø�ҳid�İ������򷽷�
 */
page_id_t BPlusTreePage::GetParentPageId() const { 
	return parent_page_id_;
}
void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) {
	parent_page_id_ = parent_page_id;
}

/*
 * ��ȡ/��������ҳ��id�İ������򷽷�
 */
page_id_t BPlusTreePage::GetPageId() const { 
	return page_id_;
}
void BPlusTreePage::SetPageId(page_id_t page_id) {
	page_id_ = page_id;
}

/*
 * ����lsn�İ������򷽷�
 */
void BPlusTreePage::SetLSN(lsn_t lsn) { lsn_ = lsn; }

}