//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  std::lock_guard<std::shared_mutex> lock(latch_);
  Page *page;

  // 1.1    If P exists, pin it and return it immediately.
  auto p = page_table_.find(page_id);
  if (p != page_table_.end()) {
    page = &pages_[p->second];
    if (page->pin_count_++ == 0) {
      replacer_->Pin(p->second);
    }
    return page;
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  frame_id_t frame_id = GetFrameId();
  if (frame_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  page = &pages_[frame_id];
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->data_);
  }
  // 3.     Delete R from the page table and insert P.
  page_table_.erase(page->GetPageId());
  page_table_[page_id] = frame_id;
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  disk_manager_->ReadPage(page_id, page->data_);
  page->update(page_id, 1, false);
  replacer_->Pin(frame_id);
  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::shared_mutex> lock(latch_);
  // 1. If page does not exist, return false.
  auto p = page_table_.find(page_id);
  if (p == page_table_.end()) {
    return false;
  }
  // 2. Get the page, if the pin count of p is less than or equal to 0, return false.
  Page &page = pages_[p->second];
  if (page.pin_count_<=0) {
    return false;
  }
  // 3. Set the page's pin count value to 0 and unpin it in the replacer.
  page.is_dirty_ |= is_dirty;
  if (--page.pin_count_ == 0) {
    replacer_->Unpin(p->second);
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::shared_mutex> lock(latch_);

  // 1. If page not found, return false.
  auto p = page_table_.find(page_id);
  if (p == page_table_.end()) {
    return false;
  }
  // 2. If it is a dirty page, write is back to disk and update the dirty flag of page to false.
  Page &page = pages_[p->second];
  if (page.IsDirty()) {
    disk_manager_->WritePage(page_id, page.data_);
    page.is_dirty_= false;
  }
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  std::lock_guard<std::shared_mutex> lock(latch_);

  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  if (free_list_.empty() && replacer_->Size() == 0) {
    *page_id = INVALID_PAGE_ID;
    return nullptr;
  }
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t frame_id = GetFrameId();
  if (frame_id == INVALID_PAGE_ID) {
    *page_id = INVALID_PAGE_ID;
    return nullptr;
  }
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  Page *page = &pages_[frame_id];
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->data_);
  }
  *page_id = disk_manager_->AllocatePage();
  page_table_.erase(page->GetPageId());
  page_table_[*page_id] = frame_id;
  page->update(*page_id, 1, false, true);
  // 4.   Set the page ID output parameter. Return a pointer to P.
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0. Make sure you call DiskManager::DeallocatePage!
  std::lock_guard<std::shared_mutex> lock(latch_);

  // 1. Search the page table for the requested page (P).
  // If P does not exist, return true.
  auto p = page_table_.find(page_id);
  if (p == page_table_.end()) {
    return true;
  }
  // 2. If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  Page &page = pages_[p->second];
  if (page.pin_count_ > 0) {
    return false;
  }
  // 3. Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  disk_manager_->DeallocatePage(page_id);
  page_table_.erase(page_id);
  page.update(INVALID_PAGE_ID, 0, false, true);
  free_list_.push_back(p->second);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::lock_guard<std::shared_mutex> lock(latch_);
  for(size_t i = 0; i < pool_size_; ++i) {
    Page &page = pages_[i];
    if (page.page_id_ != INVALID_PAGE_ID && page.IsDirty()) {
      disk_manager_->WritePage(i, page.data_);
      page.is_dirty_ = false;
    }
  }
}

/**
 * Get victim frame id, if it is not possible to allocate a frame from the free_list, then choose one from the victim
 * and if none is found, return INVALID_PAGE_ID.
 * @return the frame id
 */
frame_id_t BufferPoolManager::GetFrameId() {
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Victim(&frame_id)) {
      return INVALID_PAGE_ID;
    }

    // 脏页 flush log 到磁盘
    if (enable_logging) {
      Page &page = pages_[frame_id];
      if (page.IsDirty() && page.GetLSN() > log_manager_->GetPersistentLSN()) {
        log_manager_->Flush();
      }
    }
  }
  return frame_id;
}
}  // namespace bustub
