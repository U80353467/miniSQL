#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
    ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
    MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
    buf += 4;
    MACH_WRITE_UINT32(buf, table_meta_pages_.size());
    buf += 4;
    MACH_WRITE_UINT32(buf, index_meta_pages_.size());
    buf += 4;
    for (auto iter : table_meta_pages_) {
        MACH_WRITE_TO(table_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
    for (auto iter : index_meta_pages_) {
        MACH_WRITE_TO(index_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
    // check valid
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += 4;
    ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
    // get table and index nums
    uint32_t table_nums = MACH_READ_UINT32(buf);
    buf += 4;
    uint32_t index_nums = MACH_READ_UINT32(buf);
    buf += 4;
    // create metadata and read value
    CatalogMeta *meta = new CatalogMeta();
    for (uint32_t i = 0; i < table_nums; i++) {
        auto table_id = MACH_READ_FROM(table_id_t, buf);
        buf += 4;
        auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
    }
    for (uint32_t i = 0; i < index_nums; i++) {
        auto index_id = MACH_READ_FROM(index_id_t, buf);
        buf += 4;
        auto index_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->index_meta_pages_.emplace(index_id, index_page_id);
    }
    return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  uint32_t total_size = 0;
  total_size += 4;  
  total_size += 4;  
  total_size += 4; 
  total_size += table_meta_pages_.size() * 8;
  total_size += index_meta_pages_.size() * 8;
  return total_size;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  if (init) {
    catalog_meta_ = CatalogMeta::NewInstance();
    auto page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_->SerializeTo(page->GetData());
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
    next_index_id_ = 0;
    next_table_id_ = 0;
  } else {
    auto page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = CatalogMeta::DeserializeFrom(page->GetData());
    for (auto iter : *(catalog_meta_->GetTableMetaPages())) {
      if (iter.second != INVALID_PAGE_ID) {
        LoadTable(iter.first, iter.second);
      }
    }
    for (auto iter : *(catalog_meta_->GetIndexMetaPages())) {
      if (iter.second != INVALID_PAGE_ID) {
        LoadIndex(iter.first, iter.second);
      }
    }
    next_index_id_ = catalog_meta_->GetNextIndexId();
    next_table_id_ = catalog_meta_->GetNextTableId();
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  }

}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  if (table_names_.find(table_name) != table_names_.end()) {
    return DB_TABLE_ALREADY_EXIST;
  }

  page_id_t new_table_page_id;
  auto new_table_page = buffer_pool_manager_->NewPage(new_table_page_id);

  catalog_meta_->table_meta_pages_.emplace(next_table_id_, new_table_page_id);
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

  page_id_t first_page_id;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(first_page_id));
  page->Init(first_page_id, INVALID_PAGE_ID, log_manager_, txn);
  buffer_pool_manager_->UnpinPage(first_page_id, true);

  table_info = TableInfo::Create();
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_);
  table_heap->SetFirstPageId(first_page_id);
  table_heap->SetNextFreePageId(first_page_id);
  TableMetadata *table_metadata = TableMetadata::Create(next_table_id_, table_name, table_heap->GetFirstPageId(), schema);
  table_info->Init(table_metadata, table_heap);
  table_metadata->SerializeTo(new_table_page->GetData());
  buffer_pool_manager_->UnpinPage(new_table_page_id, true);

  tables_.emplace(next_table_id_, table_info);
  table_names_.emplace(table_name, next_table_id_);
  next_table_id_++;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_info = tables_.at(table_names_.at(table_name));
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for (auto iter : tables_) {
    tables.push_back(iter.second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type) {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  if (index_names_[table_name].find(index_name) != index_names_[table_name].end()) {
    return DB_INDEX_ALREADY_EXIST;
  }
  TableInfo *table_info = tables_[table_names_[table_name]];
  uint32_t key_index = 0;
  std::vector<uint32_t> key_map;
  for (auto colomn_index : index_keys) {
    if (table_info->GetSchema()->GetColumnIndex(colomn_index, key_index) == DB_COLUMN_NAME_NOT_EXIST) {
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    key_map.push_back(key_index);
  }
  
  page_id_t new_index_page_id;
  auto new_index_page = buffer_pool_manager_->NewPage(new_index_page_id);

  catalog_meta_->index_meta_pages_.emplace(next_index_id_, new_index_page_id);
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

  index_info = IndexInfo::Create();
  IndexMetadata *index_metadata = IndexMetadata::Create(next_index_id_, index_name, table_names_[table_name], key_map);
  index_info->Init(index_metadata, table_info, buffer_pool_manager_);
  TableHeap *table_heap = tables_.at(table_names_.at(table_name))->GetTableHeap();
  for (auto iter = table_heap->Begin(txn); iter != table_heap->End(); iter++) {
    std::vector<Field> fields;
    for (auto key : key_map) {
      fields.push_back(*iter->GetField(key));
    }
    index_info->GetIndex()->InsertEntry(Row(fields), iter->GetRowId(), txn);
  }
  index_metadata->SerializeTo(new_index_page->GetData());

  indexes_.emplace(next_index_id_, index_info);
  index_names_[table_name].emplace(index_name, next_index_id_);
  next_index_id_++;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  if (index_names_.at(table_name).find(index_name) == index_names_.at(table_name).end()) {
    return DB_INDEX_NOT_FOUND;
  }
  index_info = indexes_.at(index_names_.at(table_name).at(index_name));
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  if (index_names_.find(table_name) == index_names_.end() || index_names_.at(table_name).empty()) {
    return DB_INDEX_NOT_FOUND;
  }
  for (auto iter : index_names_.at(table_name)) {
    indexes.push_back(indexes_.at(iter.second));
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_id_t table_id = table_names_[table_name];
  TableInfo *table_info = tables_[table_id];
  tables_.erase(table_names_[table_name]);
  table_names_.erase(table_name);

  for (auto iter : index_names_[table_name]) {
    DropIndex(table_name, iter.first);
  }

  table_info->GetTableHeap()->DeleteTable();

  buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_[table_id]);
  catalog_meta_->table_meta_pages_.erase(table_id);
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if (index_names_.find(table_name) == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  if (index_names_[table_name].find(index_name) == index_names_[table_name].end()) {
    return DB_INDEX_NOT_FOUND;
  }
  index_id_t index_id = index_names_[table_name][index_name];
  IndexInfo *index_info = indexes_[index_id];
  indexes_.erase(index_id);
  index_names_[table_name].erase(index_name);
  index_info->GetIndex()->Destroy();

  buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_[index_id]);
  catalog_meta_->index_meta_pages_.erase(index_id);
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  if (buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID)) {
    return DB_SUCCESS;
  }
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  auto table_page = buffer_pool_manager_->FetchPage(page_id);
  if (table_page == nullptr) {
    return DB_FAILED;
  }
  auto table_info = TableInfo::Create();
  TableMetadata *table_metadata = nullptr;
  table_metadata->DeserializeFrom(table_page->GetData(), table_metadata);
  TableHeap *table_heap = nullptr;
  table_heap = TableHeap::Create(buffer_pool_manager_, table_metadata->GetFirstPageId(), table_metadata->GetSchema(), log_manager_, lock_manager_);
  table_info->Init(table_metadata, table_heap);
  tables_.emplace(table_id, table_info);
  table_names_.emplace(table_metadata->GetTableName(), table_id);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  auto index_page = buffer_pool_manager_->FetchPage(page_id);
  if (index_page == nullptr) {
    return DB_FAILED;
  }
  auto index_info = IndexInfo::Create();
  IndexMetadata *index_metadata = nullptr;
  index_metadata->DeserializeFrom(index_page->GetData(), index_metadata);
  TableInfo *table_info = nullptr;
  table_info = tables_[index_metadata->GetTableId()];
  index_info->Init(index_metadata, table_info, buffer_pool_manager_);
  indexes_.emplace(index_id, index_info);
  index_names_[table_info->GetTableName()].emplace(index_metadata->GetIndexName(), index_id);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if (tables_.find(table_id) == tables_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_info = tables_[table_id];
  return DB_SUCCESS;
}