//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_header_page.cpp
//
// Identification: src/storage/page/hash_table_header_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_header_page.h"

namespace bustub {
page_id_t HashTableHeaderPage::GetBlockPageId(size_t index) { 
  if (index > this->GetSize()) {
    throw new Exception("Index " + std::to_string(index) + "is out of range. Size: " + std::to_string(this->GetSize()));
  }
  return this->block_page_ids_[index];
 }

page_id_t HashTableHeaderPage::GetPageId() const { return page_id_; }

void HashTableHeaderPage::SetPageId(bustub::page_id_t page_id) { page_id_ = page_id; }

lsn_t HashTableHeaderPage::GetLSN() const { return lsn_; }

void HashTableHeaderPage::SetLSN(lsn_t lsn) { lsn_ = lsn; }

void HashTableHeaderPage::AddBlockPageId(page_id_t page_id) {
if (next_ind_ >= this->GetSize()) {
    throw new Exception("Reach limit of block_page_ids_");
  }
  this->block_page_ids_[next_ind_] = page_id;
  next_ind_++;
}

size_t HashTableHeaderPage::NumBlocks() { return next_ind_; }

void HashTableHeaderPage::SetSize(size_t size) { size_ = size; }

size_t HashTableHeaderPage::GetSize() const { return size_; }

void HashTableHeaderPage::ResetIndex() { next_ind_ = 0; }  
}// namespace bustub
