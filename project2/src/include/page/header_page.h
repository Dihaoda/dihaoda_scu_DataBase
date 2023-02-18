/**
 * header_page.h
 *
 *数据库使用第一页（page_id=0）作为头页来存储元数据，在
 *在本例中，我们将包含有关表/索引名称（长度小于
 *32字节）及其对应的root_id
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
   * 与记录相关
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
