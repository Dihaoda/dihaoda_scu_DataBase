//邸浩达2019141410141
//实现LRU
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace scudb {
template <typename T> LRUReplacer<T>::LRUReplacer() {
  head = make_shared<Node>();
  tail = make_shared<Node>();
  head->next = tail;
  tail->prev = head;
}
template <typename T> LRUReplacer<T>::~LRUReplacer() {}
//向LRU中插入value
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
  lock_guard<mutex> lck(latch);
  shared_ptr<Node> dhdcur;
  if (map.find(value) != map.end()) {
    dhdcur = map[value];
    shared_ptr<Node> dhdprev = dhdcur->dhdprev;
    shared_ptr<Node> dhdsucc = dhdcur->next;
    dhdprev->next = dhdsucc;
    dhdsucc->dhdprev = dhdprev;
  } else {
    dhdcur = make_shared<Node>(value);
  }
  shared_ptr<Node> dhdfir = head->next;
  dhdcur->next = dhdfir;
  dhdfir->prev = dhdcur;
  dhdcur->prev = head;
  head->next = dhdcur;
  map[value] = dhdcur;
  return;
}
//如果LRU为非空，则将LRU中的头成员弹出到参数value，并且返回true。如果LRU为空，则返回false
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
  lock_guard<mutex> lck(latch);
  if (map.empty()) {
    return false;
  }
  shared_ptr<Node> dhdlast = tail->prev;
  tail->prev = dhdlast->prev;
  dhdlast->prev->next = tail;
  value = dhdlast->val;
  map.erase(dhdlast->val);
  return true;
}
//从LRU中删除值。如果删除成功，则返回true，否则返回false
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
  lock_guard<mutex> lck(latch);
  if (map.find(value) != map.end()) {
    shared_ptr<Node> dhdcur = map[value];
    dhdcur->prev->next = dhdcur->next;
    dhdcur->next->prev = dhdcur->prev;
  }
  return map.erase(value);
}
template <typename T> size_t LRUReplacer<T>::Size() {
  lock_guard<mutex> lck(latch);
  return map.size();
}
template class LRUReplacer<Page *>;
template class LRUReplacer<int>;
} // 命名空间scudb
