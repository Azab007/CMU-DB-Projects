//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_block_page.cpp
//
// Identification: src/storage/page/hash_table_block_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_block_page.h"
#include "storage/index/generic_key.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BLOCK_TYPE::KeyAt(slot_offset_t bucket_ind) const {
  if (!IsReadable(bucket_ind)) {
    throw std::runtime_error("Bucket " + std::to_string(bucket_ind) + " is not readable");
  }
  return array_[bucket_ind].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BLOCK_TYPE::ValueAt(slot_offset_t bucket_ind) const {
  if (!IsReadable(bucket_ind)) {
    throw std::runtime_error("Bucket " + std::to_string(bucket_ind) + " is not readable");
  }
  return array_[bucket_ind].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::Insert(slot_offset_t bucket_ind, const KeyType &key, const ValueType &value) {
  char expected = readable_[bucket_ind / 8];
  char desired = readable_[bucket_ind / 8] | (1 << (bucket_ind % 8));
  if (!readable_[bucket_ind / 8].compare_exchange_weak(expected, desired)) return false;
  if (expected == desired) return false;
  array_[bucket_ind] = std::make_pair(key, value);
  occupied_[bucket_ind / 8] |= (1 << (bucket_ind % 8));
  return true;

}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::Remove(slot_offset_t bucket_ind) {
  if (!IsReadable(bucket_ind)) {
    throw std::runtime_error("Bucket " + std::to_string(bucket_ind) + " is not readable");
  }
  char expected = readable_[bucket_ind / 8];
  char desired = readable_[bucket_ind / 8] & ~(1 << (bucket_ind % 8));
  readable_[bucket_ind / 8].compare_exchange_weak(expected, desired);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsOccupied(slot_offset_t bucket_ind) const {
  return (occupied_[bucket_ind / 8] >> (bucket_ind % 8)) & 1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsReadable(slot_offset_t bucket_ind) const {
  return (readable_[bucket_ind / 8] >> (bucket_ind % 8)) & 1;
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBlockPage<int, int, IntComparator>;
template class HashTableBlockPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBlockPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBlockPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBlockPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBlockPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
