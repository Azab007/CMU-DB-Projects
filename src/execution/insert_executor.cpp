//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), exec_child(std::move(child_executor)) {}

// const Schema *InsertExecutor::GetOutputSchema() {
//   const auto &catalog = this->GetExecutorContext()->GetCatalog();
//   return &catalog->GetTable(this->plan_->TableOid())->schema_;
// }

void InsertExecutor::Init() {
    const auto &catalog = exec_ctx_->GetCatalog();
    this->table = catalog->GetTable(plan_->TableOid());
    if(!plan_->IsRawInsert()){
        exec_child->Init();
    }

}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
    Tuple t;
    RID r;
    if (plan_->IsRawInsert())    {
        auto num = plan_->RawValues().size();
        for(size_t i = 0; i < num; i++){
            t = Tuple(plan_->RawValuesAt(i), this->GetOutputSchema());
            auto done = this->table->table_->InsertTuple(t, &r, exec_ctx_->GetTransaction());
        //    LOG_DEBUG("Insert tuple successfully: %s", tuple->ToString(this->GetOutputSchema()).c_str());
            if (!done) return false;
        }
      //  LOG_DEBUG("Insert %d tuples into table", int(plan_->RawValues().size()));
        return true;
    
     }
    else{
        while (exec_child->Next(&t, &r)) {
           //LOG_DEBUG("Read tuple: %s", tuple->ToString(this->GetOutputSchema()).c_str());
            auto done = this->table->table_->InsertTuple(t, &r, exec_ctx_->GetTransaction());
            if (!done) return false;
        }
        return true;
    }
     


}  // namespace bustub
}