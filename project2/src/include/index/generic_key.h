/**
 * generic_key.h
 *
 *用于索引不透明数据的键
 *
 *此键类型使用固定长度数组来保存用于索引的数据
 *目的，其实际大小已指定并实例化
 *使用模板参数。
 */
#pragma once

#include <cstring>

#include "table/tuple.h"
#include "type/value.h"

namespace scudb {
template <size_t KeySize> class GenericKey {
public:
  inline void SetFromKey(const Tuple &tuple) {
    // intialize to 0
    memset(data, 0, KeySize);
    memcpy(data, tuple.GetData(), tuple.GetLength());
  }

  // 注：仅用于测试目的
  inline void SetFromInteger(int64_t key) {
    memset(data, 0, KeySize);
    memcpy(data, &key, sizeof(int64_t));
  }

  inline Value ToValue(Schema *schema, int column_id) const {
    const char *data_ptr;
    const TypeId column_type = schema->GetType(column_id);
    const bool is_inlined = schema->IsInlined(column_id);
    if (is_inlined) {
      data_ptr = (data + schema->GetOffset(column_id));
    } else {
      int32_t offset = *reinterpret_cast<int32_t *>(
          const_cast<char *>(data + schema->GetOffset(column_id)));
      data_ptr = (data + offset);
    }
    return Value::DeserializeFrom(data_ptr, column_type);
  }

  //注：仅用于测试目的
  //从数据向量中将前8个字节解释为int64t
  inline int64_t ToString() const {
    return *reinterpret_cast<int64_t *>(const_cast<char *>(data));
  }

  //注：仅用于测试目的
  //从数据向量中将前8个字节解释为int64t
  friend std::ostream &operator<<(std::ostream &os, const GenericKey &key) {
    os << key.ToString();
    return os;
  }

  // 数据的实际位置延伸到末端。
  char data[KeySize];
};

/**
 * 如果lhs＜rhs，则函数对象返回true，用于树
 */
template <size_t KeySize> class GenericComparator {
public:
  inline int operator()(const GenericKey<KeySize> &lhs,
                        const GenericKey<KeySize> &rhs) const {
    int column_count = key_schema_->GetColumnCount();

    for (int i = 0; i < column_count; i++) {
      Value lhs_value = (lhs.ToValue(key_schema_, i));
      Value rhs_value = (rhs.ToValue(key_schema_, i));

      if (lhs_value.CompareLessThan(rhs_value) == CMP_TRUE)
        return -1;

      if (lhs_value.CompareGreaterThan(rhs_value) == CMP_TRUE)
        return 1;
    }
    // equals
    return 0;
  }

  GenericComparator(const GenericComparator &other) {
    this->key_schema_ = other.key_schema_;
  }

  // constructor
  GenericComparator(Schema *key_schema) : key_schema_(key_schema) {}

private:
  Schema *key_schema_;
};

} // namespace scudb
