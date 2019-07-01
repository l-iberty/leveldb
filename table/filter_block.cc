// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

// 考虑这样一种调用顺序:
// 若干次AddKey() -> GenerateFilter() -> 若干次AddKey() -> GenerateFilter() -> ... -> Finish()
//
// 那么result_就会保存了若干个BloomFilter结构, filter_offsets_[]保存了这些结构的偏移地址, 最后调用
// Finish()时, 先将filter_offsets_[]的数据append到result_, 然后把filter_offsets_[]在result_中的
// 偏移地址append到result_, 最后附上解码参数kFilterBaseLg, 最终result_的数据结构如下:
//
// [filter_1] `.
// [filter_2]  | 若干BloomFilter结构
// ...         |
// [filter_n] /
// [filter_offsets_0] `.
// [filter_offsets_1]  | 每个BloomFilter结构的偏移地址
// ...                 |
// [filter_offsets_n] /
// [array_offset]  filter_offsets_[]在result_中的偏移地址
// [kFilterBaseLg] 编码参数
Slice FilterBlockBuilder::Finish() {
  if (!start_.empty()) {
    GenerateFilter(); // 如果调用Finish()之前还有key没有被添加到BloomFilter结构中, 就再调一下GenerateFilter()
  }

  // Append array of per-filter offsets
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }

  PutFixed32(&result_, array_offset);
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // 从字符串keys_中把每个key解析出来, 保存在tmp_keys_[], 用作policy_->CreateFilter()的参数.
  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  filter_offsets_.push_back(result_.size());
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
  size_t n = contents.size();
  // 参见FilterBlockBuilder::Finish(), contents至少包含2个数据: [array_offset](uint32), [kFilterBaseLg](uint8)
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array

  base_lg_ = contents[n - 1];
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5); // last_word即[array_offset]
  if (last_word > n - 5) return; // 除了[array_offset]和[kFilterBaseLg]外没有其他有效数据
  data_ = contents.data();
  offset_ = data_ + last_word; // offset_[] <=> FilterBlockBuilder::filter_offsets_[]
  num_ = (n - 5 - last_word) / 4; // num of entries in offset_[]
}

bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
    // 以下分析需要对照FilterBlockBuilder::Finish()得到的数据结构.
    // 把offset_看成uint32数组, 则: start = offset_[index], limit = offset_[index+1]
    // offset_ - data_ = 所有BloomFilter结构的总长度
    // 如果index对应与最后一个BloomFilter结构, 即offset_[index]是最后一项, 那么offset_[index+1]就等于[array_offset],
    // 即 limit = [array_offset] = offset_[0]在data_中的偏移地址 = 所有BloomFilter结构的总长度
    // 所以, limit不能超过offset_ - data_
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start); // limit - start = index指向的BloomFilter结构的长度
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
