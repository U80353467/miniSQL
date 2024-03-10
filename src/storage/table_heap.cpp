#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  page_id_t page_id = next_free_page_id_;
  bool is_first_page = page_id == INVALID_PAGE_ID;
  page_id_t prev_page_id = INVALID_PAGE_ID;
  TablePage *page = nullptr;
  if (page_id != INVALID_PAGE_ID) {
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    if (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      buffer_pool_manager_->UnpinPage(page_id, true);
      return true;
    }
    page_id = page->GetNextPageId();
    prev_page_id = page->GetPageId();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  }

  page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(page_id));
  if (page == nullptr) {
    throw runtime_error("out of memory");
  }
  if (is_first_page) {
    first_page_id_ = page_id;
  } else {
    auto prev_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(prev_page_id));
    prev_page->SetNextPageId(page_id);
    buffer_pool_manager_->UnpinPage(prev_page_id, true);
  }
  next_free_page_id_ = page_id;
  page->Init(page_id, prev_page_id, log_manager_, txn);
  if (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
    buffer_pool_manager_->UnpinPage(page_id, true);
    return true;
  }
  buffer_pool_manager_->UnpinPage(page_id, false);
  return false;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    return false;
  }
  Row old_row = Row(rid);
  int update_result = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  if(update_result == 1) {
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
  }else if(update_result == 0){
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return false;
  }else if(update_result == -1){
    ApplyDelete(rid, txn);
    InsertTuple(const_cast<Row&>(row), txn);
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
  }
  return false;
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if (page == nullptr) {
    return false;
  }
  bool result = page->GetTuple(row, schema_, txn, lock_manager_);
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return result;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    if (first_page_id_ == INVALID_PAGE_ID) {
      return;
    }
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Transaction *txn) {
  return TableIterator(this, txn);
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  return TableIterator(this, (Row*)nullptr);
}
