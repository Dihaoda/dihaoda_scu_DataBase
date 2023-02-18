/**
 * header_page.h
 *
 *���ݿ�ʹ�õ�һҳ��page_id=0����Ϊͷҳ���洢Ԫ���ݣ���
 *�ڱ����У����ǽ������йر�/�������ƣ�����С��
 *32�ֽڣ������Ӧ��root_id
 *
 * Format (size in byte):
 *  -----------------------------------------------------------------
 * | RecordCount (4) | Entry_1 name (32) | Entry_1 root_id (4) | ... |
 *  -----------------------------------------------------------------
 */

#pragma once

#include "page/page.h"

#include <cstring>

namespace scudb {

class HeaderPage : public Page {
public:
  void Init() { SetRecordCount(0); }
  /**
   * ���¼���
   */
  bool InsertRecord(const std::string &name, const page_id_t root_id);
  bool DeleteRecord(const std::string &name);
  bool UpdateRecord(const std::string &name, const page_id_t root_id);

  // return root_id if success
  bool GetRootId(const std::string &name, page_id_t &root_id);
  int GetRecordCount();

private:
  /**
   * helper functions
   */
  int FindRecord(const std::string &name);

  void SetRecordCount(int record_count);
};
} // namespace scudb
