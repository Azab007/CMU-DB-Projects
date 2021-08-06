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

#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include <math.h>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
      //header page
    Page *header = buffer_pool_manager->NewPage(&header_page_id_);
    auto headerPage = reinterpret_cast<HashTableHeaderPage*>(header->GetData());
    headerPage->SetPageId(header_page_id_);
    headerPage->SetSize(num_buckets);

    //block pages
    auto blocknum = ceil(num_buckets / BLOCK_ARRAY_SIZE);
    page_id_t blockPageId;
    for(unsigned i = 0; i< blocknum; i++){
      buffer_pool_manager->NewPage(&blockPageId);
      buffer_pool_manager->UnpinPage(blockPageId, true);
      buffer_pool_manager->FlushPage(blockPageId);
      headerPage->AddBlockPageId(blockPageId);
    }
    buffer_pool_manager->UnpinPage(header_page_id_, true);

 
  
    }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  Page *header = buffer_pool_manager_->FetchPage(header_page_id_);
    auto *headerPage = reinterpret_cast<HashTableHeaderPage*>(header->GetData());
    auto start = hash_fn_.GetHash(key) % headerPage->GetSize();
    auto finish = false;
      for (auto i = start;;i = (i+1) % headerPage->GetSize()){

      if(i == start){
        if(finish) break;
        finish = true;
      }

    auto blockIndex = start / BLOCK_ARRAY_SIZE;
    auto blockPageId = headerPage->GetBlockPageId(blockIndex);
    auto block = buffer_pool_manager_->FetchPage(blockPageId);
    auto blockPage = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(block->GetData());
    auto offset = i % BLOCK_ARRAY_SIZE;
      if(!blockPage->IsOccupied(offset)) break;
      
      if(blockPage->IsReadable(offset) && comparator_(blockPage->KeyAt(offset),key) == 0) {
        result->push_back(blockPage->ValueAt(offset));
      }
    }

    if (result->size() > 0){
      return true;
    }
    return false;

}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  Page *header = buffer_pool_manager_->FetchPage(header_page_id_);
  auto *headerPage = reinterpret_cast<HashTableHeaderPage*>(header->GetData());
  auto start = hash_fn_.GetHash(key) % headerPage->GetSize();
  auto finish = false;
    for (auto i = start;;i = (i+1) % headerPage->GetSize()){

    if(i == start){
      if(finish) break;
      finish = true;
    }
  auto blockIndex = start / BLOCK_ARRAY_SIZE;
  auto blockPageId = headerPage->GetBlockPageId(blockIndex);
  auto block = buffer_pool_manager_->FetchPage(blockPageId);
  auto blockPage = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(block->GetData());
  auto offset = i % BLOCK_ARRAY_SIZE;
    if(blockPage->IsOccupied(offset) && comparator_(blockPage->KeyAt(offset),key) == 0 && blockPage->ValueAt(offset) == value){
      return false;
    }

    if(blockPage->Insert(offset,key,value)) {
      return true;
    }

    
  }

  this->Resize(headerPage->GetSize());
  return this->Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  Page *header = buffer_pool_manager_->FetchPage(header_page_id_);
  auto *headerPage = reinterpret_cast<HashTableHeaderPage*>(header->GetData());
  auto start = hash_fn_.GetHash(key) % headerPage->GetSize();
  auto found = false;
  auto finish = false;
    for (auto i = start;;i = (i+1) % headerPage->GetSize()){

    if(i == start){
      if(finish) break;
      finish = true;
    }
  auto blockIndex = start / BLOCK_ARRAY_SIZE;
  auto blockPageId = headerPage->GetBlockPageId(blockIndex);
  auto block = buffer_pool_manager_->FetchPage(blockPageId);
  auto blockPage = reinterpret_cast<HASH_TABLE_BLOCK_TYPE *>(block->GetData());
  auto offset = i % BLOCK_ARRAY_SIZE;
    
    if(blockPage->IsOccupied(offset) && blockPage->IsReadable(offset) && comparator_(blockPage->KeyAt(offset),key) == 0 &&
    value == blockPage->ValueAt(offset)) {
      blockPage->Remove(offset);
      found = true;
    }
  }

  return found;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  Page *header = buffer_pool_manager_->FetchPage(header_page_id_);
  auto *headerPage = reinterpret_cast<HashTableHeaderPage*>(header->GetData());
  auto newSize = initial_size * 2;
  headerPage->SetSize(newSize);
  page_id_t blockPageId;
      for(unsigned i = 0; i< initial_size; i++){
        buffer_pool_manager_->NewPage(&blockPageId);
        headerPage->AddBlockPageId(blockPageId);
        buffer_pool_manager_->UnpinPage(blockPageId, true);
      }
      headerPage->ResetIndex();
      for (size_t idx = 0; idx < headerPage->NumBlocks(); idx++) {
      const auto &block = reinterpret_cast<HASH_TABLE_BLOCK_TYPE*>(buffer_pool_manager_->FetchPage(headerPage->GetBlockPageId(idx))->GetData()); 
      for (size_t pair_idx = 0; pair_idx < BLOCK_ARRAY_SIZE; pair_idx++) {
        this->Insert(nullptr, block->KeyAt(pair_idx), block->ValueAt(pair_idx));
      }
      this->buffer_pool_manager_->DeletePage(headerPage->GetBlockPageId(idx));
    }
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  Page *header = buffer_pool_manager_->FetchPage(header_page_id_);
  auto *headerPage = reinterpret_cast<HashTableHeaderPage*>(header->GetData());
  auto size = headerPage->GetSize();
  return size;
}


template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
