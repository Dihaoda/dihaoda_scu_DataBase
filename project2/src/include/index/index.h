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
 *��IndexMetadata-�������������Ԫ����
 *
 *Ԫ���ݶ���ά��
 *��������Ϊ�ⲿ���÷���֪��
 *�������������������ά��������
 *ӳ���ϵ��������Ԫ�����������֮���ת��
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

  // ���ر�ʾ�������ļܹ�����ָ��
  inline Schema *GetKeySchema() const { return key_schema_; }

  //����������������Ԫ����У��ڵ�����
  //ע�⣬�������cppԴ�ļ��ж���
  //��Ϊ��ʹ���˴˴�δ֪��catalog:��Schema�ĳ�Ա
  int GetIndexColumnCount() const { return (int)key_attrs_.size(); }

  //���������������֮���ӳ���ϵ
  //��
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
 *class Index-��ͬ�������������Ļ���
 *
 *�����ṹ��Ҫά���й�
 *�������ģʽ��������֮���ӳ���ϵ
 *��Ϊ�ⲿ�����ṩ��һ�ֳ���ķ�ʽ
 *��ײ�����ʵ�ֽ���������¶
 *ʵ��ʵ�ֵĽӿڡ�
 *
 *�������󻹴���ν��ɨ�裬
 *ɾ����ν�ʲ��롢���ѯ����������ɨ�衣ν��ɨ��
 *��֧�����ӣ����ܻ��Ż���Ҳ���ܲ����Ż�������ȡ����
 *ν���ڲ����ʽ�����͡�
 */
class Index {
public:
  Index(IndexMetadata *metadata) : metadata_(metadata) {}

  virtual ~Index() { delete metadata_; }

  // ����������������Ԫ���ݶ���
  IndexMetadata *GetMetadata() const { return metadata_; }

  int GetIndexColumnCount() const { return metadata_->GetIndexColumnCount(); }

  const std::string &GetName() const { return metadata_->GetName(); }

  Schema *GetKeySchema() const { return metadata_->GetKeySchema(); }

  const std::vector<int> &GetKeyAttrs() const {
    return metadata_->GetKeyAttrs();
  }

  // ��ȡ���ڵ��Ե��ַ�����ʾ��ʽ
  const std::string ToString() const {
    std::stringstream os;

    os << "INDEX: (" << GetName() << ")";
    os << metadata_->ToString();
    return os.str();
  }

  ///////////////////////////////////////////////////////////////////
  // Point Modification
  ///////////////////////////////////////////////////////////////////
  // ������ڶ���������
  virtual void InsertEntry(const Tuple &key, RID rid,
                           Transaction *transaction = nullptr) = 0;

  // ɾ�����ӵ�����Ԫ���������
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
