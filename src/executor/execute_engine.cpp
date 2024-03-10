#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    //strcpy(stdir->d_name, "executor_test.db");
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Transaction *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  bool is_success = executor->Init();
  if (is_success) {
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } else {
    std::cout << "Error Encountered in Executor Execution: " << executor->error_info << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if(!current_db_.empty())
    context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  if (current_db_.empty()) {
    std::cout << "No database selected." << std::endl;
    return DB_FAILED;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  if (DONTOUTPUT == true) {
    return DB_SUCCESS;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}
/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  auto start_time = std::chrono::system_clock::now();
  std::string new_db_name = ast->child_->val_;
  if (!new_db_name.empty()) {
    if (dbs_.find(new_db_name) == dbs_.end()) {
      DBStorageEngine *db = new DBStorageEngine(new_db_name);
      dbs_.emplace(new_db_name, db);
      auto stop_time = std::chrono::system_clock::now();
      double duration_time =
    double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
      std::stringstream ss;
      ResultWriter writer(ss);
      writer.BeginRow();
      writer.WriteCell("Database created.", 20);
      writer.EndRow();
      writer.EndInformation(1, duration_time, false);
      std::cout << writer.stream_.rdbuf();
      return DB_SUCCESS;
    } else {
      return DB_ALREADY_EXIST;
    }
  }
  std::cout << "Database name is empty." << std::endl;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  auto start_time = std::chrono::system_clock::now();
  std::string db_name = ast->child_->val_;
  if (!db_name.empty()) {
    if (dbs_.find(db_name) != dbs_.end()) {
      std::string db_file_name_ = "./databases/" + db_name;
      remove(db_file_name_.c_str());
      dbs_.erase(db_name);
      auto stop_time = std::chrono::system_clock::now();
      double duration_time = double(
          (std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
      std::stringstream ss;
      ResultWriter writer(ss);
      writer.BeginRow();
      writer.WriteCell("Database dropped.", 20);
      writer.EndRow();
      writer.EndInformation(1, duration_time, false);
      std::cout << writer.stream_.rdbuf();
      return DB_SUCCESS;
    } else {
      return DB_NOT_EXIST;
    }
  }
  std::cout << "Database name is empty." << std::endl;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    std::cout << "No database exists." << std::endl;
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  vector<int>data_width = {0};
  for (auto iter : dbs_) {
    if (iter.first.length() > data_width[0]) {
      data_width[0] = iter.first.length();
    }
  }
  std::stringstream ss;
  ResultWriter writer(ss);
  writer.BeginRow();
  writer.WriteHeaderCell("Database", data_width[0]);
  writer.EndRow();
  writer.Divider(data_width);
  for (auto iter : dbs_) {
    writer.BeginRow();
    writer.WriteCell(iter.first, data_width[0]);
    writer.EndRow();
    writer.Divider(data_width);
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  writer.EndInformation(dbs_.size(), duration_time, false);
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  std::string db_name = ast->child_->val_;
  if (!db_name.empty()) {
    if (dbs_.find(db_name) != dbs_.end()) {
      current_db_ = db_name;
      std::cout << "Database changed." << std::endl;
      return DB_SUCCESS;
    } else {
      return DB_NOT_EXIST;
    }
  }
  std::cout << "Database name is empty." << std::endl;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    std::cout << "No database selected." << std::endl;
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  vector<int>data_width = {0};
  vector<TableInfo*> tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  if (tables.empty()) {
    std::cout << "No table exists." << std::endl;
    return DB_FAILED;
  }
  data_width[0] = current_db_.size() + 10;
  vector<string> table_names;
  for (auto iter : tables) {
    table_names.push_back(iter->GetTableName());
    if (iter->GetTableName().length() > data_width[0]) {
      data_width[0] = iter->GetTableName().length();
    }
  }
  std::stringstream ss;
  ResultWriter writer(ss);
  writer.BeginRow();
  writer.WriteHeaderCell("Tables_in_" + current_db_, data_width[0]);
  writer.EndRow();
  writer.Divider(data_width);
  for (auto iter : table_names) {
    writer.BeginRow();
    writer.WriteCell(iter, data_width[0]);
    writer.EndRow();
    writer.Divider(data_width);
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  writer.EndInformation(table_names.size(), duration_time, false);
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if (current_db_.empty()) {
    std::cout << "No database selected." << std::endl;
    return DB_FAILED;
  }
  
  auto start_time = std::chrono::system_clock::now();
  std::string table_name = ast->child_->val_;
  if (!table_name.empty()) {
    vector<Column *> columns;
    pSyntaxNode column_list = ast->child_->next_;
    pSyntaxNode primary_column_list = column_list->child_;
    while (primary_column_list->next_ != nullptr) {
      primary_column_list = primary_column_list->next_;
    }
    bool has_primary_keys = false, has_unique_keys = false;
    unordered_map<char*, bool>primary_keys;
    unordered_map<char*, bool>unique_keys;
    if (primary_column_list->val_ != nullptr && strcmp(primary_column_list->val_, "primary keys") == 0) {
      has_primary_keys = true;
      pSyntaxNode primary_column = primary_column_list->child_;
      while (primary_column != nullptr) {
        primary_keys.emplace(primary_column->val_, true);
        primary_column = primary_column->next_;
      }
    }
    pSyntaxNode column = column_list->child_;
    uint32_t index = 0;
    while ((has_primary_keys && column->next_ != nullptr) || (!has_primary_keys && column != nullptr)) {
      bool is_unique = false;
      bool is_nullable = true;
      if (column->val_ != nullptr && strcmp(column->val_, "unique") == 0) {
        is_unique = true;
        has_unique_keys = true;
        unique_keys.emplace(column->child_->val_, true);
      }
      char* column_name = column->child_->val_;
      if (has_primary_keys && primary_keys.find(column_name) != primary_keys.end()) {
        is_unique = true;
        is_nullable = false;
      }
      pSyntaxNode type = column->child_->next_;
      TypeId type_id;
      uint32_t length;
      if (strcmp(type->val_, "int") == 0 || strcmp(type->val_, "float") == 0) {
        if (strcmp(type->val_, "int") == 0) {
          type_id = TypeId::kTypeInt;
        } else {
          type_id = TypeId::kTypeFloat;
        }
        Column * col = new Column(column_name, type_id, index, is_nullable, is_unique);
        columns.push_back(col);
      } else if (strcmp(type->val_, "char") == 0) {
        type_id = TypeId::kTypeChar;
        pSyntaxNode len = type->child_;
        char* endptr;
        length = strtol(len->val_, &endptr, 10);
        if (*endptr != '\0') {
          std::cout << "Length for char type must be a integer." << std::endl;
          return DB_FAILED;
        }
        if (length <= 0) {
          std::cout << "Length for char type must be greater than 0." << std::endl;
          return DB_FAILED;
        }
        Column * col = new Column(column_name, type_id, length, index, is_nullable, is_unique);
        columns.push_back(col);
      } else {
        std::cout << "Invalid data type." << std::endl;
        return DB_FAILED;
      }
      column = column->next_;
      ++index;
    }
    TableSchema * schema = new TableSchema(columns);
    TableInfo * table_info;
    dberr_t create_table_result = dbs_[current_db_]->catalog_mgr_->CreateTable(table_name, schema, context->GetTransaction(), table_info);
    if (create_table_result != DB_SUCCESS) {
      return create_table_result;
    }

    if (has_unique_keys) {
      for (auto iter : unique_keys) {
        std::string column_name = iter.first;
        vector<std::string> index_keys;
        index_keys.push_back(column_name);
        std::string index_name;
        index_name.append("__idx").append(column_name).append(table_name);
        IndexInfo * index_info;
        dberr_t create_index_result = dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, index_name, index_keys, context->GetTransaction(), index_info, "bptree");
        if (create_index_result != DB_SUCCESS) {
          return create_index_result;
        }
      }
    }

    if (has_primary_keys) {
      vector<std::string> index_keys;
      for (auto iter : primary_keys) {
        std::string column_name = iter.first;
        index_keys.push_back(column_name);
      }
      std::string index_name;
      index_name.append("__idxPri").append(table_name);
      IndexInfo * index_info;
      dberr_t create_index_result = dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, index_name, index_keys, context->GetTransaction(), index_info, "bptree");
      if (create_index_result != DB_SUCCESS) {
        return create_index_result;
      }
    }

    auto stop_time = std::chrono::system_clock::now();
    double duration_time =
        double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
    std::stringstream ss;
    ResultWriter writer(ss);
    writer.BeginRow();
    writer.WriteCell("Table created.", 14);
    writer.EndRow();
    writer.EndInformation(1, duration_time, false);
    std::cout << writer.stream_.rdbuf();
    return DB_SUCCESS;
  }
  std::cout << "Table name is empty." << std::endl;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if (current_db_.empty()) {
    std::cout << "No database selected." << std::endl;
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  std::string table_name = ast->child_->val_;
  if (!table_name.empty()) {
    dberr_t drop_table_result = dbs_[current_db_]->catalog_mgr_->DropTable(table_name);
    if (drop_table_result != DB_SUCCESS) {
      return drop_table_result;
    }
    auto stop_time = std::chrono::system_clock::now();
    double duration_time =
        double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
    std::stringstream ss;
    ResultWriter writer(ss);
    writer.BeginRow();
    writer.WriteCell("Table dropped.", 14);
    writer.EndRow();
    writer.EndInformation(1, duration_time, false);
    std::cout << writer.stream_.rdbuf();
    return DB_SUCCESS;
  }
  std::cout << "Table name is empty." << std::endl;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (current_db_.empty()) {
    std::cout << "No database selected." << std::endl;
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  vector<TableInfo*> tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  if (tables.empty()) {
    std::cout << "No table exists." << std::endl;
    return DB_FAILED;
  }
  vector<IndexInfo*> indexes;
  bool is_has_any_index = false;
  std::stringstream ss;
  ResultWriter writer(ss);
  for (auto table : tables) {
    vector<int>data_width = {0};
    std::string table_name = table->GetTableName();
    data_width[0] = table_name.size() + 11;
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, indexes);
    if (!indexes.empty()) {
      is_has_any_index = true;
      for (auto index : indexes) {
        if (index->GetIndexName().size() > data_width[0]) {
          data_width[0] = index->GetIndexName().size();
        }
      }
      writer.BeginRow();
      writer.WriteHeaderCell("Indexes_in_" + table_name, data_width[0]);
      writer.EndRow();
      writer.Divider(data_width);
      for (auto index : indexes) {
        writer.BeginRow();
        writer.WriteCell(index->GetIndexName(), data_width[0]);
        writer.EndRow();
        writer.Divider(data_width);
      }
      cout << writer.stream_.rdbuf();
    }
  }
  if (!is_has_any_index) {
    std::cout << "No index exists." << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  writer.EndInformation(indexes.size(), duration_time, false);
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if (current_db_.empty()) {
    std::cout << "No database selected." << std::endl;
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  std::string index_name = ast->child_->val_;
  std::string table_name = ast->child_->next_->val_;
  TableInfo* table_info;
  dberr_t get_table_result = dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info);
  if (get_table_result != DB_SUCCESS) {
    return get_table_result;
  }
  if (!table_name.empty() && !index_name.empty()) {
    vector<std::string> index_keys;
    pSyntaxNode key_list = ast->child_->next_->next_;
    pSyntaxNode key = key_list->child_;
    while (key != nullptr) {
      std::string key_name = key->val_;
      bool is_key_found = false;
      for (auto col : table_info->GetSchema()->GetColumns()) {
        if (col->GetName() == key_name) {
          index_keys.push_back(key_name);
          is_key_found = true;
          break;
        }
      }
      if (!is_key_found) {
        std::cout << "Unknown column '" << key_name << "' in 'field list'." << std::endl;
        return DB_FAILED;
      }
      key = key->next_;
    }
    IndexInfo * index_info;
    dberr_t create_index_result = dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, index_name, index_keys, context->GetTransaction(), index_info, "bptree");
    if (create_index_result != DB_SUCCESS) {
      return create_index_result;
    }
    auto stop_time = std::chrono::system_clock::now();
    double duration_time =
        double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
    std::stringstream ss;
    ResultWriter writer(ss);
    writer.BeginRow();
    writer.WriteCell("Index created.", 14);
    writer.EndRow();
    writer.EndInformation(1, duration_time, false);
    std::cout << writer.stream_.rdbuf();
    return DB_SUCCESS;
  }
  std::cout << "Table name or index name is empty." << std::endl;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if (current_db_.empty()) {
    std::cout << "No database selected." << std::endl;
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  std::string index_name = ast->child_->val_;
  std::string table_name;
  vector<TableInfo *>tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  if (tables.empty()) {
    std::cout << "No table exists." << std::endl;
    return DB_FAILED;
  }
  for (auto table : tables) {
    vector<IndexInfo *> indexes;
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table->GetTableName(), indexes);
    if (indexes.empty()) {
      continue;
    }
    for (auto index : indexes) {
      if (index->GetIndexName() == index_name) {
        table_name = table->GetTableName();
        break;
      }
    }
  }
  if (!index_name.empty()) {
    dberr_t drop_index_result = dbs_[current_db_]->catalog_mgr_->DropIndex(table_name, index_name);
    if (drop_index_result != DB_SUCCESS) {
      return drop_index_result;
    }
    auto stop_time = std::chrono::system_clock::now();
    double duration_time =
        double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
    std::stringstream ss;
    ResultWriter writer(ss);
    writer.BeginRow();
    writer.WriteCell("Index dropped.", 14);
    writer.EndRow();
    writer.EndInformation(1, duration_time, false);
    std::cout << writer.stream_.rdbuf();
    return DB_SUCCESS;
  }
  std::cout << "Index name is empty." << std::endl;
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  std::string file_path = ast->child_->val_;
  FILE *fp = fopen(file_path.c_str(), "r");
  if (fp == nullptr) {
    std::cout << "File not found." << std::endl;
    return DB_FAILED;
  }
  DONTOUTPUT = true;
  auto start_time = std::chrono::system_clock::now();
  char cmd[1024];
  while (true) {
    memset(cmd, 0, 1024);
    int i = 0;
    char ch;
    while ((ch = fgetc(fp)) != ';') {
      if (ch == EOF) {
        DONTOUTPUT = false;
        auto stop_time = std::chrono::system_clock::now();
        double duration_time =
          double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
        std::cout << duration_time << "ms elapsed." << std::endl;
        fclose(fp);
        return DB_SUCCESS;
      }
      cmd[i++] = ch;
    }
    cmd[i] = ch;

    fgetc(fp);
        // create buffer for sql input
    YY_BUFFER_STATE bp = yy_scan_string(cmd);
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);

    // init parser module
    MinisqlParserInit();

    // parse
    yyparse();

    // parse result handle
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
    }

    auto result = Execute(MinisqlGetParserRootNode());

    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();

    // quit condition
    ExecuteInformation(result);
    if (result == DB_QUIT) {
      DONTOUTPUT = false;
      fclose(fp);
      return DB_QUIT;
    }
  }
  DONTOUTPUT = false;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
 return DB_QUIT;
}

