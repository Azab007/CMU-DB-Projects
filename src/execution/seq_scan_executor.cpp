//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
     const auto &catalog = exec_ctx_->GetCatalog();
  this->table = catalog->GetTable(this->plan_->GetTableOid())->table_.get();
  this->iter = std::make_unique<TableIterator>(this->table->Begin(exec_ctx_->GetTransaction()));
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
    auto &iterator = *(this->iter);
  while (iterator != this->table->End()) {
    auto tup = *(iterator++);
    auto passed = true;
    if (this->plan_->GetPredicate() != nullptr) {
      passed = this->plan_->GetPredicate()->Evaluate(&tup, this->GetOutputSchema()).GetAs<bool>();
    }
    if (passed) {
      *tuple = Tuple(tup);
      return true;
    }
  }
  return false;
 }

}  // namespace bustub
