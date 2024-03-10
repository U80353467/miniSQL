//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/**
* TODO: Student Implement
*/
bool UpdateExecutor::Init() {
  error_info.clear();
  child_executor_->Init();
  row_affected = 0;
  Row old_row, new_row;
  RowId old_rid;
  auto table_name = plan_->GetTableName();
  TableInfo* table_info;
  if (exec_ctx_->GetCatalog()->GetTable(table_name, table_info) == DB_TABLE_NOT_EXIST) {
    error_info.append("table not found");
    return false;
  }
  auto table_heap = table_info->GetTableHeap();

  vector<IndexInfo*> index_infos;
  vector<vector<Field>> old_indexes_fields, new_indexes_fields;
  exec_ctx_->GetCatalog()->GetTableIndexes(table_name, index_infos);

  while (child_executor_->Next(&old_row, &old_rid)) {
    new_row = GenerateUpdatedTuple(old_row);

    for (auto index_info : index_infos) {
      vector<Field> old_index_fields, new_index_fields;
      for (auto column : index_info->GetIndexKeySchema()->GetColumns()) {
        old_index_fields.push_back(*old_row.GetField(column->GetTableInd()));
        new_index_fields.push_back(*new_row.GetField(column->GetTableInd()));
      }
      old_indexes_fields.push_back(old_index_fields);
      new_indexes_fields.push_back(new_index_fields);
      vector<RowId> result;
      if(index_info->GetIndex()->ScanKey(Row(new_index_fields), result, nullptr) == DB_SUCCESS){
        if (!result.empty()) {
          error_info.append("duplicate key value violates unique constraint on ").append(new_index_fields[0].toString());
        }
      }
    }

    if (table_heap->UpdateTuple(new_row, old_rid, exec_ctx_->GetTransaction())) {
      for (uint32_t i = 0; i < index_infos.size(); i++) {
        if (index_infos[i]->GetIndex()->RemoveEntry(Row(old_indexes_fields[i]), old_rid, exec_ctx_->GetTransaction()) == DB_SUCCESS) {
          if (index_infos[i]->GetIndex()->InsertEntry(Row(new_indexes_fields[i]), old_rid, exec_ctx_->GetTransaction()) == DB_SUCCESS) {
            continue;
          }
        } else {
          error_info.append("update failed");
          return false;
        }
      }
    } else {
      error_info.append("update failed");
      return false;
    }
    ++row_affected;
  }
  return true;
}

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  if (row_affected != 0) {
    row = new Row();
    rid = new RowId();
    --row_affected;
    return true;
  }
  return false;
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
  vector<Field> fields;
  unordered_map<uint32_t, AbstractExpressionRef> update_attr;
  update_attr = plan_->GetUpdateAttr();
  for (uint32_t i = 0; i < src_row.GetFieldCount(); i++) {
    auto field = src_row.GetField(i);
    if (update_attr.find(i) != update_attr.end()) {
      AbstractExpressionRef expression = update_attr.at(i);
      if (expression->GetType() == ExpressionType::ConstantExpression) {
        auto column_expression = dynamic_pointer_cast<ConstantValueExpression>(expression);
        Field update_field = Field(column_expression->val_);
        fields.push_back(update_field);
      }
    } else {
      fields.push_back(*field);
    }
  }
  Row new_row = Row(fields);
  return new_row;
}