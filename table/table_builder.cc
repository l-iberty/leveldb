// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <assert.h>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }

  Options options;
  Options index_block_options;
  WritableFile* file;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;
  BlockBuilder index_block;
  std::string last_key;
  int64_t num_entries;
  bool closed;  // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;

  // 直到遇到下一个DataBlock的第1个key时, 我们才为上一个DataBlock生成Index Block.
  // 这样做的好处是, 可以选择为Index Block选择较短的key. 例如, 上一个DataBlock
  // 的最后一个key是"the quick brown fox", 其后继DataBlock的第一个key是"the who",
  // 那么就可以选择"the r"作为作为上一个DataBlock的Index Block的key. 那么, 自然只有
  // 等到一个DataBlock中的KV数据全部添加完毕, 才能知道最后一个key是什么. 至于如何选择
  // 这个"较短的key", 就需要参考BytewiseComparatorImpl::FindShortestSeparator(),
  // 见util/comparator.cc, 我给出了详细的注释.

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  if (r->pending_index_entry) { // 为上一个DataBlock生成Index Block
    assert(r->data_block.empty());
    // 选择一个较短的key作为Index Block的key.
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    // pending_handle保存了上一个DataBlock的大小及其在sstable文件中的偏移地址, 参见TableBuilder::Flush(),
    // 现将其编码到字符串handle_encoding, 作为Index Block的value.
    r->pending_handle.EncodeTo(&handle_encoding);
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }

  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key); // 向FilterBlock添加key
  }

  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value); // 向DataBlock添加KV数据

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    // data_block已达到一定的规模(缺省值为4KB; 另外需要注意, 此时尚未调用data_block.Finish(),
    // 所以其中只包含前缀压缩的KV数据, 生成完整的DataBlock需要等到WriteBlock()), 将其持久化到
    // sstable文件; 然后生成这些key对应的filter结构, 但先不把filter结构持久化.
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle); // pending_handle保存当前DataBlock的大小及其在sstable文件中的偏移地址
  if (ok()) { // 文件写入成功
    r->pending_index_entry = true; // 当前DataBlock处理完毕, 下一个DataBlock的第1个key到来时就为其前驱DataBlock生成Index Block
    r->status = r->file->Flush(); // 刷新文件状态
  }
  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish(); // 生成DataBlock的序列化字符串

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

// 最终写入sstable文件的数据包括:
//  block_contents: uint8[n] (可以是各种类型的block, 比如DataBlock, FilterBlock)
//  type: uint8
//  crc: uint32
void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset); // rep_->offset = 本次写入位置的文件偏移 = 本次写入前的文件大小, 首次写入时为0.
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize]; // 1-byte type + 32-bit crc
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize; // rep_->offset += 本次写入的字节数
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

// 最终写入sstable文件的逻辑结构如下(详细解释参见mydoc/SSTable逻辑结构.md)：
// [ DataBlock_1 ][type][crc]
// [ DataBlock_2 ][type][crc]
// ...
// [ DataBlock_n ][type][crc]
// [ FilterBlock ]
// [ Meta Index Block ]
// [ Index Block ]
// [ Footer ]
Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush(); // 写入最后一个数据块: [DataBlock][type][crc]
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
    // filter_block_handle保存FilterBlock的大小及其在sstable文件中的偏移地址
  }

  // Write metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      // 将filter_block_handle保存的FilterBlock的大小及其在sstable文件中的偏移地址
      // 编码到字符串handle_encoding, 作为Meta Index Block的value.
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
    // metaindex_block_handle保存Meta Index Block的大小及其在sstable文件中的偏移地址
  }

  // Write index block
  // Index Block中的每一条KV数据都是前面若干个DataBlock的索引
  if (ok()) {
    if (r->pending_index_entry) { // 为最后一个DataBlock生成Index Block
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding); // pending_handle的含义参见TableBuilder::Add()
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
    // index_block_handle保存Index Block的大小及其在sstable文件中的偏移
  }

  // Write footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
