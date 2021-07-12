//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  buffer_size = num_pages;
  clock_hand = 0;
  ref_bits = std::vector<bool>(num_pages, false);
  inclock_bits = std::vector<bool>(num_pages, false);
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  while (Size() != 0) {
    if (inclock_bits[clock_hand] && !ref_bits[clock_hand]) {
      *frame_id = clock_hand;
      inclock_bits[clock_hand] = false;
      clock_hand = (clock_hand + 1) % buffer_size;
      return true;
    } else if (inclock_bits[clock_hand] && ref_bits[clock_hand]) {
      ref_bits[clock_hand] = false;
      clock_hand = (clock_hand + 1) % buffer_size;

    } else {
      clock_hand = (clock_hand + 1) % buffer_size;
    }
  }
  return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  ref_bits[frame_id] = false;
  inclock_bits[frame_id] = false;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  ref_bits[frame_id] = true;
  inclock_bits[frame_id] = true;
}

size_t ClockReplacer::Size() {
  int cnt = 0;
  for (int i = 0; i < buffer_size; i++) {
    if (inclock_bits[i]) {
      cnt += 1;
    }
  }
  return cnt;
}

}  // namespace bustub
