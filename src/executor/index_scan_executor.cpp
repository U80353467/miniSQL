#include "executor/executors/index_scan_executor.h"
/**
* TODO: Student Implement
*/
IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

bool IndexScanExecutor::Init() {
  error_info.clear();
  auto table_name = plan_->GetTableName();
  TableInfo* table_info;
  if (exec_ctx_->GetCatalog()->GetTable(table_name, table_info) == DB_TABLE_NOT_EXIST) {
    error_info.append("table not found");
    return false;
  }
  is_predicate_single = false;
  table_heap = table_info->GetTableHeap();
  vector<IndexInfo *> index_infos = plan_->indexes_;
  rids.clear();
  AbstractExpressionRef expression = plan_->GetPredicate();
  if (expression->GetType() == ExpressionType::ComparisonExpression) {
    uint32_t column_id = dynamic_pointer_cast<ColumnValueExpression>(expression->GetChildAt(0))->GetColIdx();
    Field field = Field(dynamic_pointer_cast<ConstantValueExpression>(expression->GetChildAt(1))->val_);
    vector<Field> fields;
    fields.push_back(field);
    Row row = Row(fields);
    for (auto index_info : index_infos) {
      if (index_info->GetIndexKeySchema()->GetColumn(0) == table_info->GetSchema()->GetColumn(column_id)) {
        index_info->GetIndex()->ScanKey(row, rids, nullptr, dynamic_pointer_cast<ComparisonExpression>(expression)->GetComparisonType());
      }
    }
    is_predicate_single = true;
  } else {
    if (!plan_->need_filter_) {
      {
        AbstractExpressionRef expression_cmp = expression->GetChildAt(0);
        AbstractExpressionRef expression_cmp_ = expression->GetChildAt(1);
        uint32_t column_id = dynamic_pointer_cast<ColumnValueExpression>(expression_cmp->GetChildAt(0))->GetColIdx();
        uint32_t column_id_ = dynamic_pointer_cast<ColumnValueExpression>(expression_cmp_->GetChildAt(0))->GetColIdx();
        std::string cmp_str = dynamic_pointer_cast<ComparisonExpression>(expression_cmp)->GetComparisonType();
        std::string cmp_str_ = dynamic_pointer_cast<ComparisonExpression>(expression_cmp_)->GetComparisonType();
        if (column_id == column_id_ && cmp_str == ">" && cmp_str_ == "<") {
          Field field = Field(dynamic_pointer_cast<ConstantValueExpression>(expression_cmp->GetChildAt(1))->val_);
          vector<Field> fields;
          fields.push_back(field);
          Row row(fields);

          Field field_ = Field(dynamic_pointer_cast<ConstantValueExpression>(expression_cmp_->GetChildAt(1))->val_);
          vector<Field> fields_;
          fields_.push_back(field_);
          Row row_(fields_);
          for (auto index_info : index_infos) {
            if (index_info->GetIndexKeySchema()->GetColumn(0) == table_info->GetSchema()->GetColumn(column_id)) {
              index_info->GetIndex()->ScanKey(row, row_, rids, nullptr, "r");
            }
          }
          return true;
        }
      }
      vector<RowId> rids_left, rids_right;
      {
        AbstractExpressionRef expression_cmp = expression->GetChildAt(0);
        uint32_t column_id = dynamic_pointer_cast<ColumnValueExpression>(expression_cmp->GetChildAt(0))->GetColIdx();
        Field field = Field(dynamic_pointer_cast<ConstantValueExpression>(expression_cmp->GetChildAt(1))->val_);
        vector<Field> fields;
        fields.push_back(field);
        Row row(fields);
        for (auto index_info : index_infos) {
          if (index_info->GetIndexKeySchema()->GetColumn(0) == table_info->GetSchema()->GetColumn(column_id)) {
            index_info->GetIndex()->ScanKey(row, rids_left, nullptr, dynamic_pointer_cast<ComparisonExpression>(expression_cmp)->GetComparisonType());
          }
        }
      }
      {
        AbstractExpressionRef expression_cmp = expression->GetChildAt(1);
        uint32_t column_id = dynamic_pointer_cast<ColumnValueExpression>(expression_cmp->GetChildAt(0))->GetColIdx();
        Field field = Field(dynamic_pointer_cast<ConstantValueExpression>(expression_cmp->GetChildAt(1))->val_);
        vector<Field> fields;
        fields.push_back(field);
        Row row(fields);
        for (auto index_info : index_infos) {
          if (index_info->GetIndexKeySchema()->GetColumn(0) == table_info->GetSchema()->GetColumn(column_id)) {
            index_info->GetIndex()->ScanKey(row, rids_right, nullptr, dynamic_pointer_cast<ComparisonExpression>(expression_cmp)->GetComparisonType());
          }
        }
      }
      rids.resize(rids_left.size() + rids_right.size());
      sort(rids_left.begin(), rids_left.end());
      sort(rids_right.begin(), rids_right.end());
      set_intersection(rids_left.begin(), rids_left.end(), rids_right.begin(), rids_right.end(), rids.begin());
    } else {
      AbstractExpressionRef expression_cmp = expression->GetChildAt(0);
      uint32_t column_id = dynamic_pointer_cast<ColumnValueExpression>(expression_cmp->GetChildAt(0))->GetColIdx();
      Field field = Field(dynamic_pointer_cast<ConstantValueExpression>(expression_cmp->GetChildAt(1))->val_);
      vector<Field> fields;
      fields.push_back(field);
      Row row = Row(fields);
      for (auto index_info : index_infos) {
        if (index_info->GetIndexKeySchema()->GetColumn(0) == table_info->GetSchema()->GetColumn(column_id)) {
          index_info->GetIndex()->ScanKey(row, rids, nullptr, dynamic_pointer_cast<ComparisonExpression>(expression_cmp)->GetComparisonType());
        }
      }
    }
  }
  return true;
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
  if (!plan_->need_filter_ || is_predicate_single) {
    if (rids.size() == 0) {
      return false;
    }
    *rid = rids[0];
    row->SetRowId(*rid);
    if (row->GetRowId().GetPageId() < 0) {
      return false;
    }
    table_heap->GetTuple(row, exec_ctx_->GetTransaction());
    rids.erase(rids.begin());
    return true;
  } else {
    if (plan_->need_filter_) {
      if (rids.size() == 0) {
        return false;
      }
      while (true) {
        if (rids.size() == 0) {
          return false;
        }
        *rid = rids[0];
        row->SetRowId(*rid);
        table_heap->GetTuple(row, exec_ctx_->GetTransaction());
        if (plan_->GetPredicate()->Evaluate(row).CompareEquals(Field(kTypeInt, 1))) {
          rids.erase(rids.begin());
          return true;
        }
        rids.erase(rids.begin());
      }
    }
  }
}