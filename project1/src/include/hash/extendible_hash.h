/*
 * 邸浩达2019141410141
 * extendible_hash.h
 * 功能：缓冲池管理器必须维护一个页表，将PageId快速映射到其对应的存储器位置；或交替地报告PageId与任何当前缓冲的页面不匹配。
 */
#pragma once

#include <cstdlib>
#include <vector>
#include <string>
#include <map>
#include <memory>
#include <mutex>

#include "hash/hash_table.h"
using namespace std;
namespace scudb {
template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
  struct Bucket {
    Bucket(int depth) : localDepth(depth) {};
    int localDepth;
    map<K, V> kmap;
    mutex latch;
  };
public:
  // 结构器
  ExtendibleHash(size_t size);
  ExtendibleHash();
  // 用于生成哈希寻址的函数
  size_t HashKey(const K &key) const;
  // 获取全局和局部深度的函数
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // 查找和修改器
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key,const V &value) override;
  int getIdx(const K &key) const;
private:
  // 在此处添加您自己的成员变量
  int globalDepth;
  size_t bucketSize;
  int bucketNum;
  vector<shared_ptr<Bucket>> buckets;
  mutable mutex latch;
};
} // 命名空间scudb
