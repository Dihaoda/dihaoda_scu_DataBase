/**
 * index.h
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "catalog/schema.h"
#include "table/tuple.h"
#include "type/value.h"

namespace scudb {

/**
 *类IndexMetadata-保存索引对象的元数据
 *
 *元数据对象维护
 *索引，因为外部调用方不知道
 *索引键，因此索引负责维护这样的
 *映射关系，并进行元组键和索引键之间的转换
 */
class Transaction;
class IndexMetadata {
  IndexMetadata() = delete;

public:
  IndexMetadata(std::string index_name, std::string table_name,
                const Schema *tuple_schema, const std::vector<int> &key_attrs)
      : name_(index_name), table_name_(table_name), key_attrs_(key_attrs) {
    key_schema_ = Schema::CopySchema(tuple_schema, key_attrs_);
  }

  ~IndexMetadata() { delete key_schema_; };

  inline const std::string &GetName() const { return name_; }

  inline const std::string &GetTableName() { return table_name_; }

  // 返回表示索引键的架构对象指针
  inline Schema *GetKeySchema() const { return key_schema_; }

  //返回索引键（不在元组键中）内的列数
  //注意，这必须在cpp源文件中定义
  //因为它使用了此处未知的catalog:：Schema的成员
  int GetIndexColumnCount() const { return (int)key_attrs_.size(); }

  //返回索引列与基表之间的映射关系
  //柱
  inline const std::vector<int> &GetKeyAttrs() const { return key_attrs_; }

  // Get a string representation for debugging
  const std::string ToString() const {
    std::stringstream os;

    os << "IndexMetadata["
       << "Name = " << name_ << ", "
       << "Type = B+Tree, "
       << "Table name = " << table_name_ << "] :: ";
    os << key_schema_->ToString();

    return os.str();
  }

private:
  std::string name_;
  std::string table_name_;
  // The mapping relation between key schema and tuple schema
  const std::vector<int> key_attrs_;
  // schema of the indexed key
  Schema *key_schema_;
};

/////////////////////////////////////////////////////////////////////
// Index class definition
/////////////////////////////////////////////////////////////////////

/**
 *class Index-不同类型派生索引的基类
 *
 *索引结构主要维护有关
 *基础表的模式和索引键之间的映射关系
 *并为外部世界提供了一种抽象的方式
 *与底层索引实现交互而不暴露
 *实际实现的接口。
 *
 *索引对象还处理谓词扫描，
 *删除、谓词插入、点查询和完整索引扫描。谓词扫描
 *仅支持连接，可能会优化，也可能不会优化，具体取决于
 *谓词内部表达式的类型。
 */
class Index {
public:
  Index(IndexMetadata *metadata) : metadata_(metadata) {}

  virtual ~Index() { delete metadata_; }

  // 返回与索引关联的元数据对象
  IndexMetadata *GetMetadata() const { return metadata_; }

  int GetIndexColumnCount() const { return metadata_->GetIndexColumnCount(); }

  const std::string &GetName() const { return metadata_->GetName(); }

  Schema *GetKeySchema() const { return metadata_->GetKeySchema(); }

  const std::vector<int> &GetKeyAttrs() const {
    return metadata_->GetKeyAttrs();
  }

  // 获取用于调试的字符串表示形式
  const std::string ToString() const {
    std::stringstream os;

    os << "INDEX: (" << GetName() << ")";
    os << metadata_->ToString();
    return os.str();
  }

  ///////////////////////////////////////////////////////////////////
  // Point Modification
  ///////////////////////////////////////////////////////////////////
  // 设计用于二级索引。
  virtual void InsertEntry(const Tuple &key, RID rid,
                           Transaction *transaction = nullptr) = 0;

  // 删除链接到给定元组的索引项
  virtual void DeleteEntry(const Tuple &key,
                           Transaction *transaction = nullptr) = 0;

  virtual void ScanKey(const Tuple &key, std::vector<RID> &result,
                       Transaction *transaction = nullptr) = 0;

private:
  //===--------------------------------------------------------------------===//
  //  Data members
  //===--------------------------------------------------------------------===//
  IndexMetadata *metadata_;
};

} // namespace scudb
