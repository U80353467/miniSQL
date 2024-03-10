//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"

/**
* TODO: Student Implement
*/
SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan){}

bool SeqScanExecutor::Init() {
  auto table_name = plan_->GetTableName();
  TableInfo* table_info;
  if (exec_ctx_->GetCatalog()->GetTable(table_name, table_info) == DB_TABLE_NOT_EXIST) {
    error_info.append("table not found");
    return false;
  }
  auto table_heap = table_info->GetTableHeap();
  table_iter_ = table_heap->Begin(exec_ctx_->GetTransaction());
  return true;
}

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
  if (table_iter_ == table_iter_.table_heap_->End()) {
    return false;
  }
  if (plan_->GetPredicate() == nullptr) {
    *row = *table_iter_;
    *rid = row->GetRowId();
    table_iter_++;
    return true;
  }
  while (!plan_->GetPredicate()->Evaluate(&*table_iter_).CompareEquals(Field(kTypeInt, 1))) {
    table_iter_++;
    if (table_iter_ == table_iter_.table_heap_->End()) {
      return false;
    }
  }
  *row = *table_iter_;
  *rid = row->GetRowId();
  table_iter_++;
  return true;
}
