// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <assert.h>

#include <algorithm>

#include "leveldb/comparator.h"
#include "leveldb/options.h"
#include "util/coding.h"

namespace leveldb {

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options), restarts_(), counter_(0), finished_(false) {
  assert(options->block_restart_interval >= 1);
  restarts_.push_back(0);  // First restart point is at offset 0
}

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);  // First restart point is at offset 0
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {
  return (buffer_.size() +                       // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +  // Restart array
          sizeof(uint32_t));                     // Restart array length
}

// Finish结束后, buffer_的结构如下:
//   +-> 0:  [key中公共前缀的长度][key中非公共前缀的长度][value长度][key中非公共前缀的值][value值] `.
//   |   1:  [key中公共前缀的长度][key中非公共前缀的长度][value长度][key中非公共前缀的值][value值]  | 第1组前缀压缩
//   |    ...                                                                                 |
//   |   15: [key中公共前缀的长度][key中非公共前缀的长度][value长度][key中非公共前缀的值][value值] /
// +-|-> 0:  [key中公共前缀的长度][key中非公共前缀的长度][value长度][key中非公共前缀的值][value值] `.
// | |   1:  [key中公共前缀的长度][key中非公共前缀的长度][value长度][key中非公共前缀的值][value值]  | 第2组前缀压缩
// | |     ...                                                                                 |
// | |   15: [key中公共前缀的长度][key中非公共前缀的长度][value长度][key中非公共前缀的值][value值] /
// | |     ...
// | +-- 0: restarts_[0] `.
// +---- 1: restarts_[1]  | 重启点数组(每组前缀压缩数据的偏移地址, 指向的KV数据未使用前缀压缩)
//         ...           /
//       restarts_.size 重启点的个数
Slice BlockBuilder::Finish() {
  // Append restart array
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]); // 重启点的位置
  }
  PutFixed32(&buffer_, restarts_.size()); // 重启点的个数
  finished_ = true;
  return Slice(buffer_);
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  Slice last_key_piece(last_key_);
  assert(!finished_);
  assert(counter_ <= options_->block_restart_interval);
  assert(buffer_.empty()  // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);
  size_t shared = 0;
  if (counter_ < options_->block_restart_interval) { // 每隔block_restart_interval(16)个KV数据就会重新开始前缀压缩
    // See how much sharing to do with previous string
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;
    } // shared = key和last_key_piece的公共前缀的长度
  } else {
    // Restart compression
    restarts_.push_back(buffer_.size()); // 记录重启点的位置(重启点相对与buffer_首地址的偏移)
    counter_ = 0;
  }
  const size_t non_shared = key.size() - shared;

  // Add "<shared><non_shared><value_size>" to buffer_
  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, non_shared);
  PutVarint32(&buffer_, value.size());

  // Add string delta to buffer_ followed by value
  buffer_.append(key.data() + shared, non_shared);
  buffer_.append(value.data(), value.size());

  // 最终的buffer_如下:
  // [key中公共前缀的长度][key中非公共前缀的长度][value长度][key中非公共前缀的值][value值]

  // Update state
  // last_key已经包含了与key的公共前缀, 只需把key的剩下部分append到last_key即可, 使内存拷贝最小化
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);
  assert(Slice(last_key_) == key);
  counter_++;
}

}  // namespace leveldb
