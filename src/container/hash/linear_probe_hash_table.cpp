//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "container/hash/linear_probe_hash_table.h"

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      num_buckets_(num_buckets),
      num_pages_((num_buckets - 1) / BLOCK_ARRAY_SIZE + 1),
      last_block_array_size_(num_buckets - (num_pages_ - 1) * BLOCK_ARRAY_SIZE),
      hash_fn_(std::move(hash_fn)) {
  auto page = buffer_pool_manager->NewPage(&header_page_id_);
  page->WLatch();

  InitHeaderPage(HeaderPageCast(page));

  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, true);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::InitHeaderPage(HashTableHeaderPage *header_page) {
  header_page->SetPageId(header_page_id_);
  header_page->SetSize(num_buckets_);

  page_ids_.clear();
  for (size_t i = 0; i < num_pages_; ++i) {
    page_id_t page_id;
    buffer_pool_manager_->NewPage(&page_id);
    buffer_pool_manager_->UnpinPage(page_id, false);
    header_page->AddBlockPageId(page_id);
    page_ids_.push_back(page_id);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetIndex(const KeyType &key) -> std::tuple<slot_index_t, block_index_t, slot_offset_t> {
  slot_index_t slot_index = hash_fn_.GetHash(key) % num_buckets_;
  block_index_t block_index = slot_index / BLOCK_ARRAY_SIZE;
  slot_offset_t bucket_index = slot_index % BLOCK_ARRAY_SIZE;
  return {slot_index, block_index, bucket_index};
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();

  auto [slot_index, block_index, bucket_index] = GetIndex(key);

  auto raw_block_page = buffer_pool_manager_->FetchPage(page_ids_[block_index]);
  raw_block_page->RLatch();
  auto block_page = BlockPageCast(raw_block_page);

  while (block_page->IsOccupied(bucket_index)) {
    if (block_page->IsReadable(bucket_index) && !comparator_(key, block_page->KeyAt(bucket_index))) {
      result->push_back(block_page->ValueAt(bucket_index));
    }

    StepForward(bucket_index, block_index, raw_block_page, block_page, LockType::READ);

    if (block_index * BLOCK_ARRAY_SIZE + bucket_index == slot_index) {
      break;
    }
  }

  // unlock
  raw_block_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(raw_block_page->GetPageId(), false);
  table_latch_.RUnlock();
  return result->size() > 0;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  auto success = InsertImpl(transaction, key, value);
  table_latch_.RUnlock();
  return success;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::InsertImpl(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // 根据 key 获取 slot_index, block_index, bucket_index
  auto [slot_index, block_index, bucket_index] = GetIndex(key);

  // 获取 key 对应的 block page
  auto raw_block_page = buffer_pool_manager_->FetchPage(page_ids_[block_index]);
  raw_block_page->WLatch();
  auto block_page = BlockPageCast(raw_block_page);

  bool success = true;
  while (!block_page->Insert(bucket_index, key, value)) {
    // 如果 key, value pair 已经存在，结束循环，返回 false
    // PS: 支持同 key，不同 value 的存储，检测是否存在 kv
    if (block_page->IsReadable(bucket_index) && IsMatch(block_page, bucket_index, key, value)) {
      success = false;
      break;
    }

    StepForward(bucket_index, block_index, raw_block_page, block_page, LockType::WRITE);

    // HASH 存储是一个环，如果一直 next 找空位到 slot_index 位置(相对起始位置，转一圈)
    // 则进行 HASH TABLE 扩容
    if (block_index * BLOCK_ARRAY_SIZE + bucket_index == slot_index) {
      raw_block_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(raw_block_page->GetPageId(), false);

      table_latch_.RUnlock();
      Resize(num_buckets_);
      table_latch_.RLock();

      // HASH 扩容完成，重新获取 slot_index, block_index, bucket_index
      std::tie(slot_index, block_index, bucket_index) = GetIndex(key);

      // 重写 page 信息
      raw_block_page = buffer_pool_manager_->FetchPage(page_ids_[block_index]);
      raw_block_page->WLatch();
      block_page = BlockPageCast(raw_block_page);
    }
  }
  raw_block_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(raw_block_page->GetPageId(), success);
  return success;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  // 使用 key, 获取 slot_index, block_index, bucket_index
  auto [slot_index, block_index, bucket_index] = GetIndex(key);
  auto raw_block_page = buffer_pool_manager_->FetchPage(page_ids_[block_index]);
  raw_block_page->WLatch();
  auto block_page = BlockPageCast(raw_block_page);

  bool success = false;
  while (block_page->IsOccupied(bucket_index)) {
    // 如果存在，移除 (k,v) pair
    if (IsMatch(block_page, bucket_index, key, value)) {
      if (block_page->IsReadable(bucket_index)) {
        block_page->Remove(bucket_index);
        success = true;
      } else {
        success = false;
      }
      break;
    }

    StepForward(bucket_index, block_index, raw_block_page, block_page, LockType::WRITE);

    // 如果回到 slot_index（相对起始位置），结束循环
    if (block_index * BLOCK_ARRAY_SIZE + bucket_index == slot_index) {
      break;
    }
  }

  // 释放锁，unpin page
  raw_block_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(raw_block_page->GetPageId(), success);
  table_latch_.RUnlock();
  return success;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  // 获取 table 写锁
  table_latch_.WLock();
  // 扩容 HASH 表为原来的 2 倍
  num_buckets_ = 2 * initial_size;
  num_pages_ = (num_buckets_ - 1) / BLOCK_ARRAY_SIZE + 1;
  last_block_array_size_ = num_buckets_ - (num_pages_ - 1) * BLOCK_ARRAY_SIZE;

  // 保存老的 Header Page
  auto old_header_page_id = header_page_id_;
  std::vector<page_id_t> old_page_ids(page_ids_);

  // 创建一个新的 Header Page, 并初始化
  auto raw_header_page = buffer_pool_manager_->NewPage(&header_page_id_);
  raw_header_page->WLatch();
  InitHeaderPage(HeaderPageCast(raw_header_page));

  // 移动 (key, value) pair 到新的 page
  for (size_t block_index = 0; block_index < num_pages_; ++block_index) {
    auto old_page_id = old_page_ids[block_index];
    auto raw_block_page = buffer_pool_manager_->FetchPage(old_page_id);
    raw_block_page->RLatch();
    auto block_page = BlockPageCast(raw_block_page);

    for (slot_offset_t bucket_index = 0; bucket_index < GetBlockArraySize(block_index); ++block_index) {
      if (block_page->IsReadable(bucket_index)) {
        InsertImpl(nullptr, block_page->KeyAt(bucket_index), block_page->ValueAt(block_index));
      }
    }

    // 删除 old 的 Page
    raw_block_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(old_page_id, false);
    buffer_pool_manager_->DeletePage(old_page_id);
  }

  // 删除 old 的 Header Page
  raw_header_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  buffer_pool_manager_->DeletePage(old_header_page_id);
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  table_latch_.RLock();
  size_t size = num_buckets_;
  table_latch_.RUnlock();
  return size;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::StepForward(slot_offset_t &bucket_index, block_index_t &block_index, Page *&raw_block_page,
                                  HASH_TABLE_BLOCK_TYPE *&block_page, LockType lockType) {
  if (++bucket_index != GetBlockArraySize(block_index)) {
    return;
  }

  // move to next block page
  if (lockType == LockType::READ) {
    raw_block_page->RUnlatch();
  } else {
    raw_block_page->WUnlatch();
  }
  buffer_pool_manager_->UnpinPage(page_ids_[block_index], false);

  // update index
  bucket_index = 0;
  block_index = (block_index + 1) % num_pages_;

  // update page
  raw_block_page = buffer_pool_manager_->FetchPage(page_ids_[block_index]);
  if (lockType == LockType::READ) {
    raw_block_page->RLatch();
  } else {
    raw_block_page->WLatch();
  }
  block_page = BlockPageCast(raw_block_page);
}

template class LinearProbeHashTable<int, int, IntComparator>;
template class LinearProbeHashTable<hash_t, TmpTuple, HashComparator>;
template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
