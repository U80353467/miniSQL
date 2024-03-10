//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

bool InsertExecutor::Init() {
  error_info.clear();
  child_executor_->Init();
  Row row;
  RowId rid;
  child_executor_->Next(&row, &rid);
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

  if (row.GetFieldCount() != table_info->GetSchema()->GetColumnCount()) {
    error_info.append("Unmatched number of columns");
    return false;
  }
  Schema* table_schema = table_info->GetSchema();
  for (uint32_t i = 0; i < row.GetFieldCount(); ++i) {
    std::string column_name = table_schema->GetColumn(i)->GetName();
    if (row.GetField(i)->IsNull()) {
      if (!table_schema->GetColumn(i)->IsNullable()) {
        error_info.append("Column" + column_name + "cannot be null");
        return false;
      }
      continue;
    }
    std::string field_val = row.GetField(i)->toString();
    TypeId type_id = row.GetField(i)->GetTypeId();
    if (type_id == TypeId::kTypeInt) {
      std::istringstream iss(field_val);
      int n;
      iss >> std::noskipws >> n;
      bool is_integer = iss.eof() && !iss.fail();
      if (!is_integer) {
        error_info.append("Invalid value for column ").append(column_name).append(" : ").append(field_val);
        return false;
      }
    } else if (type_id == TypeId::kTypeFloat) {
      std::istringstream iss(field_val);
      float f;
      iss >> std::noskipws >> f;
      bool is_float = iss.eof() && !iss.fail();
      if (!is_float) {
        error_info.append("Invalid value for column ").append(column_name).append(" : ").append(field_val);
        return false;
      }
    } else if (type_id == TypeId::kTypeChar){
      uint32_t len = field_val.size();
      if (len <= 0 || len > table_schema->GetColumn(i)->GetLength()) {
        error_info.append("Invalid value for column ").append(column_name).append(" : ").append(field_val);
        return false;
      }
    } else {
      error_info.append("Invalid value for column ").append(column_name).append(" : ").append(field_val);
      return false;
    }
  }
  for (auto index_info : index_infos) {
    vector<Field> index_fields;
    for(auto column : index_info->GetIndexKeySchema()->GetColumns()){
      index_fields.push_back(*row.GetField(column->GetTableInd()));
    }
    indexes_fields.push_back(index_fields);
    vector<RowId> result;
    if(index_info->GetIndex()->ScanKey(Row(index_fields), result, nullptr) == DB_SUCCESS){
      if (!result.empty()) {
        error_info.append("duplicate key value violates unique constraint on ").append(index_fields[0].toString());
      }
    }
  }
  if (!error_info.empty()) {
    return false;
  }

  if (table_heap->InsertTuple(row, exec_ctx_->GetTransaction())) {
    for (uint32_t i = 0; i < index_infos.size(); i++) {
      if (index_infos[i]->GetIndex()->InsertEntry(Row(indexes_fields[i]), row.GetRowId(), exec_ctx_->GetTransaction()) == DB_SUCCESS) {
        continue;
      } else {
        error_info.append("Insert tuple in index failed");
        return false;
      }
    }
  } else {
    error_info.append("Insert tuple in table failed");
    return false;
  }
  row_affected = 1;
  return true;
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  if (row_affected != 0) {
    row = new Row();
    rid = new RowId();
    --row_affected;
    return true;
  }
  return false;
}