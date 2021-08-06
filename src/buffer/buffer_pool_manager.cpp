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
#include "common/logger.h"

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
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  auto page = page_table_.find(page_id);
  if (page != page_table_.end()) {
    replacer_->Pin(page->second);
    pages_[page->second].pin_count_ += 1;
    return &pages_[page->second];
  }

  frame_id_t replacement;

  if (!free_list_.empty() > 0) {
    replacement = free_list_.front();
    free_list_.pop_front();
    replacer_->Pin(replacement);
    pages_[replacement].pin_count_ += 1;
    page_table_[page_id] = replacement;
    pages_[replacement].is_dirty_ = false;
    pages_[replacement].page_id_ = page_id;
    disk_manager_->ReadPage(page_id, pages_[replacement].data_);
    return &pages_[replacement];
  }

  auto found = replacer_->Victim(&replacement);
  if (!found) return nullptr;
  auto page_to_remove = pages_[replacement].GetPageId();

  if (pages_[replacement].IsDirty()) {
    if (!FlushPageImpl(page_to_remove)) return nullptr;
  }
  page_table_.erase(page_to_remove);
  page_table_[page_id] = replacement;
  pages_[replacement].is_dirty_ = false;
  pages_[replacement].page_id_ = page_id;
  replacer_->Pin(replacement);
  pages_[replacement].pin_count_ += 1;
  disk_manager_->ReadPage(page_id, pages_[replacement].data_);
  return &pages_[replacement];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  auto page = page_table_.find(page_id);
  if (page == page_table_.end()) return true;
  if (pages_[page->second].GetPinCount() <= 0) return false;
  pages_[page->second].pin_count_ -= 1;
  pages_[page->second].is_dirty_ = is_dirty;
  replacer_->Unpin(page->second);
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  auto page = page_table_.find(page_id);
  if (page == page_table_.end()) return false;
  if (!pages_[page->second].IsDirty()) return true;
  disk_manager_->WritePage(page_id, pages_[page->second].data_);
  pages_[page->second].is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  frame_id_t victim;
  if (!free_list_.empty() > 0) {
    victim = free_list_.front();
    free_list_.pop_front();
    *page_id = disk_manager_->AllocatePage();
    pages_[victim].ResetMemory();
    replacer_->Pin(victim);
    pages_[victim].pin_count_ = 1;
    page_table_[*page_id] = victim;
    pages_[victim].is_dirty_ = false;
    pages_[victim].page_id_ = *page_id;
    return &pages_[victim];
  }
  auto found = replacer_->Victim(&victim);
  if (!found) return nullptr;
  page_id_t page_to_remove = pages_[victim].GetPageId();

  if (pages_[victim].IsDirty()) {
    if (!FlushPageImpl(page_to_remove)) return nullptr;
  }
  page_table_.erase(page_to_remove);
  *page_id = disk_manager_->AllocatePage();
  pages_[victim].ResetMemory();
  page_table_[*page_id] = victim;
  replacer_->Pin(victim);
  pages_[victim].pin_count_ = 1;
  pages_[victim].is_dirty_ = false;
  pages_[victim].page_id_ = *page_id;
  return &pages_[victim];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  auto page = page_table_.find(page_id);
  if (page_id == INVALID_PAGE_ID || page == page_table_.end()) return true;
  if (pages_[page->second].GetPinCount() != 0) return false;
  disk_manager_->DeallocatePage(page_id);
  page_table_.erase(page_id);
  pages_[page->second].is_dirty_ = false;
  pages_[page->second].page_id_ = INVALID_PAGE_ID;
  free_list_.push_back(page->second);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].IsDirty()) {
      FlushPageImpl(pages_[i].page_id_);
    }
  }
}

}  // namespace bustub
