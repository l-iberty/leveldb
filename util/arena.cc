// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"

namespace leveldb {

static const int kBlockSize = 4096;

Arena::Arena()
    : alloc_ptr_(nullptr), alloc_bytes_remaining_(0), memory_usage_(0) {}

Arena::~Arena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
    delete[] blocks_[i];
  }
}

// 如果bytes大于(kBlockSize / 4), 就单独分配一个大小为bytes的内存块; 否则直接分配一个默认大小的
// 内存块, 然后更新alloc_ptr_和alloc_bytes_remaining_以指示这个新内存块的状态, 最后在新内存块
// 之上完成内存分配. 如果下一次分配内存时alloc_ptr_指向的内存块的剩余空间够用, 那么就会接着在这个
// 内存块上完成分配, 否则再次调用AllocateFallback分配新的内存块.
char* Arena::AllocateFallback(size_t bytes) {
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  // We waste the remaining space in the current block.
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;

  char* result = alloc_ptr_;
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes;
  return result;
}

// +--------+--------+--------+
// |        |        |        |
// +--------+--^-----+--------+
//          |  |     |
//          B  |     C
//             |
//          alloc_ptr_
//
// 格子的大小代表系统的内存对齐粒度align, 本次分配内存的起始地址为alloc_ptr_, 但需要先将其对齐到格子
// 的边界. 如上图所示, 如果alloc_ptr_未对齐, 那么current_mod等于(alloc_ptr_ - B), slop等于
// (C - alloc_ptr_). 为了在分配内存时对齐, 必须填充的字节数为slop, 所以总共需要needed = bytes + slop.
// 如果当前内存块的剩余空间够用, 就把地址(alloc_ptr_ + slop)——相当于上图的C——作为结果返回, 并更新
// alloc_ptr_和alloc_bytes_remaining_; 如果不够则直接调用AllocateFallback分配内存. AllocateFallback
// 最终会调用AllocateNewBlock, AllocateNewBlock通过new运算符申请内存, 而new分配的内存总是对齐的,
// 所以AllocateFallback返回的也一定是对齐的内存地址.
char* Arena::AllocateAligned(size_t bytes) {
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8; // 获取系统的内存对齐粒度
  static_assert((align & (align - 1)) == 0,
                "Pointer size should be a power of 2");
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1); // 相当于 " % align "
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
  size_t needed = bytes + slop;
  char* result;
  if (needed <= alloc_bytes_remaining_) {
    result = alloc_ptr_ + slop;
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returned aligned memory
    result = AllocateFallback(bytes);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
  return result;
}

// 分配指定大小的内存块, 将block的指针保存到blocks_
char* Arena::AllocateNewBlock(size_t block_bytes) {
  char* result = new char[block_bytes];
  blocks_.push_back(result);
  // 内存的使用包括: 分配block_bytes字节的内存块; 保存在blocks_中的内存块的指针
  memory_usage_.fetch_add(block_bytes + sizeof(char*),
                          std::memory_order_relaxed);
  return result;
}

}  // namespace leveldb
