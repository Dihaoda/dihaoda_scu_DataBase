/*
 * 邸浩达2019141410141
 * lru_replacer.h
 * 功能：缓冲池管理器必须维护要收集的LRU列表取消固定并准备交换的所有页面。最简单的方法实现LRU是一个FIFO队列，但请记住在页面从未固定变为固定，反之亦然。
 */
#pragma once

#include <memory>
#include <unordered_map>
#include <mutex>
#include "buffer/replacer.h"

using namespace std;
namespace scudb {

template <typename T> class LRUReplacer : public Replacer<T> {
  struct Node {
    Node() {};
    Node(T val) : val(val) {};
    T val;
    shared_ptr<Node> prev;
    shared_ptr<Node> next;
  };
public:
  LRUReplacer();

  ~LRUReplacer();

  void Insert(const T &value);

  bool Victim(T &value);

  bool Erase(const T &value);

  size_t Size();

private:
  shared_ptr<Node> head;
  shared_ptr<Node> tail;
  unordered_map<T,shared_ptr<Node>> map;
  mutable mutex latch;
};

} // 命名空间scudb
