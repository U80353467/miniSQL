//
// Created by njz on 2023/1/29.
//

#include "executor/executors/delete_executor.h"

/**
* TODO: Student Implement
*/

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

bool DeleteExecutor::Init() {
  error_info.clear();
  child_executor_->Init();
  row_affected = 0;
  Row del_row;
  RowId del_rid;
  auto table_name = plan_->GetTableName();
  TableInfo* table_info;
  if (exec_ctx_->GetCatalog()->GetTable(table_name, table_info) == DB_TABLE_NOT_EXIST) {
    error_info.append("table not found");
    return false;
  }
  auto table_heap = table_info->GetTableHeap();

  vector<IndexInfo*> index_infos;
  vector<vector<Field>> indexes_fields;
  exec_ctx_->GetCatalog()->GetTableIndexes(table_name, index_infos);

  while (child_executor_->Next(&del_row, &del_rid)) {

    for (auto index_info : index_infos) {
      vector<Field> index_fields;
      for(auto column : index_info->GetIndexKeySchema()->GetColumns()){
        index_fields.push_back(*del_row.GetField(column->GetTableInd()));
      }
      indexes_fields.push_back(index_fields);
    }

    if (table_heap->MarkDelete(del_rid, exec_ctx_->GetTransaction())) {
      for (uint32_t i = 0; i < index_infos.size(); i++) {
        if (index_infos[i]->GetIndex()->RemoveEntry(Row(indexes_fields[i]), del_rid, exec_ctx_->GetTransaction()) == DB_SUCCESS) {
          continue;
        } else {
          error_info.append("Delete tuple in index failed.");
          return false;
        }
      }
    } else {
      error_info.append("Delete tuple in table failed.");
      return false;
    }
    ++row_affected;
  }
  return true;
}

bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  if (row_affected != 0) {
    row = new Row();
    rid = new RowId();
    --row_affected;
    return true;
  }
  return false;
}