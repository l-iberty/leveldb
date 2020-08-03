// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_SKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <atomic>
#include <cassert>
#include <cstdlib>

#include "util/arena.h"
#include "util/random.h"

// SkipList的核心操作就两个: FindGreaterOrEqual, Insert

// Why skiplist?
// skiplist 实现了近似于 AVL 的性能, 那为什么不直接用 AVL 之类的平衡树?
// 平衡树在操作过程中需要调整, 所以需要锁住整个树, 并发性差; skiplist 是
// 单链结构, 所以我们在代码中没有看到加锁操作, 简单的原子操作即可实现线程
// 安全. skiplist 的妙处在于通过单链结构实现树形结构的查找性能的同时, 通
// 过原子操作实现无锁并发

namespace leveldb {

class Arena;

template <typename Key, class Comparator>
class SkipList {
 private:
  struct Node;

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  explicit SkipList(Comparator cmp, Arena* arena);

  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  void Insert(const Key& key);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Key& key) const;

  // Iteration over the contents of a skip list
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const SkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

   private:
    const SkipList* list_;
    Node* node_;
    // Intentionally copyable
  };

 private:
  enum { kMaxHeight = 12 };

  inline int GetMaxHeight() const {
    return max_height_.load(std::memory_order_relaxed);
  }

  Node* NewNode(const Key& key, int height);
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return nullptr if there is no such node.
  //
  // If prev is non-null, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  Node* FindLessThan(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  Node* FindLast() const;

  // Immutable after construction
  Comparator const compare_;
  Arena* const arena_;  // Arena used for allocations of nodes

  // head_里面保存的若干个std::atomic<Node*>是指向每层链表第一个节点的指针, head_->key无意义.
  Node* const head_;

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  std::atomic<int> max_height_;  // Height of the entire list

  // Read/written only by Insert().
  Random rnd_;
};

// Implementation details follow
template <typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node {
  explicit Node(const Key& k) : key(k) {}

  Key const key;

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  Node* Next(int n) {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return next_[n].load(std::memory_order_acquire);
  }
  void SetNext(int n, Node* x) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    next_[n].store(x, std::memory_order_release);
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return next_[n].load(std::memory_order_relaxed);
  }
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_relaxed);
  }

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  std::atomic<Node*> next_[1];
  // next_的实际长度只有在调用NewNode创建Node时才能确定, 且等于该节点在SkipList中的高度(最底层节点的高度为1)
  // 假设节点x的高度为n(节点位于SkipList的第n层), 那么next_[n-1]指向节点x在当前层中的后继.
};

// NewNode(key, height) 创建一个SkipList::Node
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::NewNode(
    const Key& key, int height) {
  // struct Node定义了两个成员变量: Key key和std::atomic<Node*> next_[1], 其中的next_的实际
  // 长度为height, sizeof(Node)只包含了1个std::atomic<Node*>, 所以还需要分配(height - 1)个.
  char* const node_memory = arena_->AllocateAligned(
      sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));
  // 在内存地址node_memory上调用构造函数
  return new (node_memory) Node(key);
}

template <typename Key, class Comparator>
inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList* list) {
  list_ = list;
  node_ = nullptr;
}

template <typename Key, class Comparator>
inline bool SkipList<Key, Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
}

template <typename Key, class Comparator>
inline const Key& SkipList<Key, Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <typename Key, class Comparator>
int SkipList<Key, Comparator>::RandomHeight() {
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
  // null n is considered infinite
  return (n != nullptr) && (compare_(n->key, key) < 0);
}

// level_(kMaxHeight-1) [] --> null
//  ...
// level_4 [] --> null
// level_3 [] -->[7]------------------------------>[31]
// level_2 [] -->[7]------------------------------>[31]
// level_1 [] -->[7]--------->[15]---------------->[31]--------->[45]
// level_0 [] -->[7]-->[10]-->[15]-->[24]-->[29]-->[31]-->[34]-->[45]-->[56]
//
// - head_里保存了kMaxHeight个std::atomic<Node*>指针, 指向每层链表的第一个节点.
// - max_height_是SkipList的当前高度, 本例为4.
// - [7]、[15]、[31]和[45]每个节点在内存中只有一个, 上图只是SkipList的逻辑表示, 并不是说内存中有4个[7]、2个[15]...
//   以[7]为例, 该节点的next_成员实际长度为4, next_[0..3]分别指向了不同的节点, 例如next_[1]的含义是: 把节点[7]看成
//   逻辑上位于level_1的[7], 它的后继是[15], 这个[15]既可以看成level_1的[15], 也可以看成level_0的[15], 从内存角度
//   上讲, 二者都是一个节点. 下面给出上述SkipList内存图景的详细描述:
//   head_->next_保存了kMaxHeight个指针, 其中next_[0..3]指向同一个节点[7]. 节点[7]的next_保存了4个指针, 其中
//   next_[0]指向[10], next_[1]指向[15], next[2..3]指向同一个[31]. 节点[31]的next_保存了4个指针, 其中next_[2..3]
//   指向NULL, next_[0]指向[34], next_[1]指向[45], 这个[45]和[34]的next_[0]指向的[45]是同一个. 对于level_0
//   除[7]、[15]、[31]和[45]外的节点, next_的长度均为1, next_[0]指向各自的后继.
//
// 假设以key=[30]为参数调用FindGreaterOrEqual, 过程如下:
// 初始化x=head_, level=4-1=3
// loop 1: next=x->Next(3)=[7], key=[30]在next=[7]之后, 令x=next=[7]
// loop 2: next=x->Next(3)=[31], key=[30]不在next=[31]之后, 令prev[3]=x=[7], level=3-1=2
// loop 3: next=x->Next(2)=[31], key=[30]不在next=[31]之后, 令prev[2]=x=[7], level=2-1=1
// loop 4: next=x->Next(1)=[15], key=[30]在next=[15]之后, 令x=next=[15]
// loop 5: next=x->Next(1)=[31], key=[30]不在next=[31]之后, 令prev[1]=x=[15], level=1-1=0
// loop 6: next=x->Next(0)=[24], key=[30]在next=[24]之后, 令x=next=[24]
// loop 7: next=x->Next(0)=[29], key=[30]在next=[29]之后, 令x=next=[29]
// loop 8: next=x->Next(0)=[31], key=[30]不在next=[31]之后, 令prev[0]=x=[29], level=0, 返回next=[31]
// 结果:
// prev[0]=[29], prev[1]=[15], prev[2]=[7], prev[3]=[7]是[30]在每一层中的前驱; next=[31]是[30]的后继
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key,
                                              Node** prev) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1; // level指向最顶层
  while (true) {
    Node* next = x->Next(level); // next指向当前层的第一个节点, 首次插入节点时SkipList为空, 所以next为null
    if (KeyIsAfterNode(key, next)) { // if (next != null && key > next->key)
      // Keep searching in this list
      x = next;
    } else {
      if (prev != nullptr) prev[level] = x;
      if (level == 0) {
        return next;
      } else {
        // Switch to next list
        level--;
      }
    }
  }
}

// level_(kMaxHeight-1) [] --> null
//  ...
// level_4 [] --> null
// level_3 [] -->[7]------------------------------>[31]
// level_2 [] -->[7]------------------------------>[31]
// level_1 [] -->[7]--------->[15]---------------->[31]--------->[45]
// level_0 [] -->[7]-->[10]-->[15]-->[24]-->[29]-->[31]-->[34]-->[45]-->[56]
//
// 假设以key=[30]为参数调用FindGreaterOrEqual, 过程如下:
// 初始化x=head_, level=4-1=3
// loop 1: next=x->Next(3)=[7] != null, next=[7] < key=[30], so x=next=[7]
// loop 2: next=x->Next(3)=[31] != null, next=[31] >= key=[30], so level=3-1=2
// loop 3: next=x->Next(2)=[31] != null, next=[31] >= key=[30], so level=2-1=1
// loop 4: next=x->Next(1)=[15] != null, next=[15] < key[30], so x=next=[15]
// loop 5: next=x->Next(1)=[31] != null, next=[31] >= key=[30], so level=1-1=0
// loop 6: next=x->Next(0)=[24] != null, next=[24] < key=[30], so x=next=[24]
// loop 7: next=x->Next(0)=[29] != null, next=[29] < key=[30], so x=next=[29]
// loop 8: next=x->Next(0)=[31] != null, next=[31] >= key=[30], level == 0, so return x=[29]即为[30]的前驱.
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindLessThan(const Key& key) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_(x->key, key) < 0);
    Node* next = x->Next(level);
    if (next == nullptr || compare_(next->key, key) >= 0) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

// 如果SkipList为空就返回head_; 如果非空, 以该逻辑结构为例:
//
// level_(kMaxHeight-1) [] --> null
//  ...
// level_4 [] --> null
// level_3 [] -->[7]------------------------------>[31]
// level_2 [] -->[7]------------------------------>[31]
// level_1 [] -->[7]--------->[15]---------------->[31]--------->[45]
// level_0 [] -->[7]-->[10]-->[15]-->[24]-->[29]-->[31]-->[34]-->[45]-->[56]
//
// 初始化x=head_, level=4-1=3
// loop 1: next=x->Next(3)=[7] != null, so x=next=[7]
// loop 2: next=x->Next(3)=[31] != null, so x=next=[31]
// loop 3: next=x->Next(3)=null, so level=3-1=2
// loop 4: next=x->Next(2)=null, so level=2-1=1
// loop 5: next=x->Next(1)=[45] != null, so x=next=[45]
// loop 6: next=x->Next(1)=null, so level=1-1=0
// loop 7: next=x->Next(0)=[56] != null, so x=next=[56]
// loop 8: next=x->Next(0)=null, level == 0, return x=[56]
//
// FindLast在查找SkipList的末尾节点时没有从level_0开始直线遍历到末尾, 这么做的话T(n)=O(n), 而是采用逐层下降的方法,
// 充分利用SkipList的特性以提高效率, 使得T(n)=O(height)=O(log n)
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindLast() const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == nullptr) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template <typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0 /* any key will do */, kMaxHeight)),
      max_height_(1),
      rnd_(0xdeadbeef) {
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, nullptr);
  }
}

// level_(kMaxHeight-1) [] --> null
//  ...
// level_4 [] --> null
// level_3 [] -->[7]------------------------------>[31]
// level_2 [] -->[7]------------------------------>[31]
// level_1 [] -->[7]--------->[15]---------------->[31]--------->[45]
// level_0 [] -->[7]-->[10]-->[15]-->[24]-->[29]-->[31]-->[34]-->[45]-->[56]
//
// 插入节点[30], 如果节点的随机高度height=5, 那么生成的SkipList逻辑结构如下:
//
// level_(kMaxHeight-1) [] --> null
//  ...
// level_4 [] ------------------------------------>[30]
// level_3 [] -->[7]------------------------------>[30]-->[31]
// level_2 [] -->[7]------------------------------>[30]-->[31]
// level_1 [] -->[7]--------->[15]---------------->[30]-->[31]--------->[45]
// level_0 [] -->[7]-->[10]-->[15]-->[24]-->[29]-->[30]-->[31]-->[34]-->[45]-->[56]
//
// 注意到节点[30]在内存中只有一份, 相关分析参见FindGreaterOrEqual的注释
template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key& key) {
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  Node* prev[kMaxHeight]; // prev[]是key在SkipList每一层链表中的前驱
  Node* x = FindGreaterOrEqual(key, prev); // 查找key的后继

  // Our data structure does not allow duplicate insertion
  assert(x == nullptr || !Equal(key, x->key));

  int height = RandomHeight(); // 随机生成节点的高度, 即节点在SkipList中占几层
  if (height > GetMaxHeight()) {
    // prev[0..max_height_-1]由FindGreaterOrEqual赋值, 现在对剩下的prev[max_height_..height-1]赋值
    for (int i = GetMaxHeight(); i < height; i++) {
      prev[i] = head_;
    }
    // It is ok to mutate max_height_ without any synchronization
    // with concurrent readers.  A concurrent reader that observes
    // the new value of max_height_ will see either the old value of
    // new level pointers from head_ (nullptr), or a new value set in
    // the loop below.  In the former case the reader will
    // immediately drop to the next level since nullptr sorts after all
    // keys.  In the latter case the reader will use the new node.
    max_height_.store(height, std::memory_order_relaxed); // 更新SkipList的当前高度
  }

  x = NewNode(key, height);
  // 将x链入SkipList的每一层
  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    prev[i]->SetNext(i, x);
  }
}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, nullptr);
  if (x != nullptr && Equal(key, x->key)) {
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIPLIST_H_
