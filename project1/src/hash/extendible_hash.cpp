//邸浩达2019141410141
#include <list>
#include "hash/extendible_hash.h"
#include "page/page.h"
using namespace std;

namespace scudb {
//构造器
//array_size: 每个存储桶的固定数组大小
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) :  globalDepth(0),bucketSize(size),bucketNum(1) {
  buckets.push_back(make_shared<Bucket>(0));
}
template<typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash() {
  ExtendibleHash(64);
}
//用于计算输入键的哈希地址
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) const{
  return hash<K>{}(key);
}
//返回哈希表全局深度
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const{
  lock_guard<mutex> lock(latch);
  return globalDepth;
}
//返回一个特定桶的局部深度
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  if (buckets[bucket_id]) {
    lock_guard<mutex> lck(buckets[bucket_id]->latch);
    if (buckets[bucket_id]->kmap.size() == 0) return -1;
    return buckets[bucket_id]->localDepth;
  }
  return -1;
}
//用于返回哈希表中桶的当前数量
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const{
  lock_guard<mutex> lock(latch);
  return bucketNum;
}
//查找与输入键关联的值的函数
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {

  int idx = getIdx(key);
  lock_guard<mutex> lck(buckets[idx]->latch);
  if (buckets[idx]->kmap.find(key) != buckets[idx]->kmap.end()) {
    value = buckets[idx]->kmap[key];
    return true;
  }
  return false;
}
template <typename K, typename V>
int ExtendibleHash<K, V>::getIdx(const K &key) const{
  lock_guard<mutex> lck(latch);
  return HashKey(key) & ((1 << globalDepth) - 1);
}
//删除哈希表中的＜key，value＞条目
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  int idx = getIdx(key);
  lock_guard<mutex> lck(buckets[idx]->latch);
  shared_ptr<Bucket> cur = buckets[idx];
  if (cur->kmap.find(key) == cur->kmap.end()) {
    return false;
  }
  cur->kmap.erase(key);
  return true;
}
//在哈希表中插入＜key，value＞条目
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
  int idx = getIdx(key);
  shared_ptr<Bucket> cur = buckets[idx];
  while (true) {
    lock_guard<mutex> lck(cur->latch);
    if (cur->kmap.find(key) != cur->kmap.end() || cur->kmap.size() < bucketSize) {
      cur->kmap[key] = value;
      break;
    }
    int mask = (1 << (cur->localDepth));
    cur->localDepth++;

    {
      lock_guard<mutex> lck2(latch);
      if (cur->localDepth > globalDepth) {

        size_t length = buckets.size();
        for (size_t i = 0; i < length; i++) {
          buckets.push_back(buckets[i]);
        }
        globalDepth++;

      }
      bucketNum++;
      auto newBuc = make_shared<Bucket>(cur->localDepth);

      typename map<K, V>::iterator it;
      for (it = cur->kmap.begin(); it != cur->kmap.end(); ) {
        if (HashKey(it->first) & mask) {
          newBuc->kmap[it->first] = it->second;
          it = cur->kmap.erase(it);
        } else it++;
      }
      for (size_t i = 0; i < buckets.size(); i++) {
        if (buckets[i] == cur && (i & mask))
          buckets[i] = newBuc;
      }
    }
    idx = getIdx(key);
    cur = buckets[idx];
  }
}
template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // 命名空间 scudb
