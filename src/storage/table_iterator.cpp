#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator() = default;

TableIterator::TableIterator(const TableIterator &other) {
  table_heap_ = other.table_heap_;
  rid_ = other.rid_;
  row_ptr_ = new Row(*other.row_ptr_);
}

TableIterator::TableIterator(TableIterator &&other) noexcept {
  table_heap_ = other.table_heap_;
  rid_ = other.rid_;
  row_ptr_ = other.row_ptr_;
  other.table_heap_ = nullptr;
  rid_ = INVALID_ROWID;
  other.row_ptr_ = nullptr;
}

TableIterator::TableIterator(TableHeap *table_heap, Transaction *txn) {
  table_heap_ = table_heap;
  page_id_t page_id = table_heap_->GetFirstPageId();
  if (page_id == INVALID_PAGE_ID) {
    row_ptr_ = nullptr;
    return;
  }
  auto *page = reinterpret_cast<TablePage *>(table_heap->buffer_pool_manager_->FetchPage(page_id));
  if (!page->GetFirstTupleRid(&rid_)) {
    row_ptr_ = nullptr;
    return;
  }
  while (rid_ == INVALID_ROWID) {
    page_id = page->GetNextPageId();
    if (page_id == INVALID_PAGE_ID) {
      table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
      row_ptr_ = nullptr;
      return;
    }
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    page = reinterpret_cast<TablePage *>(table_heap->buffer_pool_manager_->FetchPage(page_id));
    page->GetFirstTupleRid(&rid_);
  }
  row_ptr_ = new Row(rid_);
  table_heap_->GetTuple(row_ptr_, txn);
  table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
}

TableIterator::TableIterator(TableHeap *table_heap, Row *row_ptr) {
  table_heap_ = table_heap;
  if (row_ptr == nullptr) {
    row_ptr_ = nullptr;
    return;
  }
  rid_ = row_ptr->GetRowId();
  row_ptr_ = row_ptr;
}

TableIterator::~TableIterator() {
}

bool TableIterator::operator==(const TableIterator &itr) const {
  if (row_ptr_ == nullptr) {
    if(itr.row_ptr_ == nullptr) {
      return true;
    }
    return false;
  }
  if (itr.row_ptr_ == nullptr) {
    return false;
  }
  return row_ptr_->GetRowId() == itr.row_ptr_->GetRowId();
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  return *row_ptr_;
}

Row *TableIterator::operator->() {
  return row_ptr_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  if (this != &itr) {
    table_heap_ = itr.table_heap_;
    rid_ = itr.rid_;
    row_ptr_ = itr.row_ptr_;
  }
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  page_id_t page_id = row_ptr_->GetRowId().GetPageId();
  TablePage *page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page_id));
  if (page->GetNextTupleRid(row_ptr_->GetRowId(), &rid_)) {
    row_ptr_->SetRowId(rid_);
    table_heap_->GetTuple(row_ptr_, nullptr);
    table_heap_->buffer_pool_manager_->UnpinPage(page_id, false);
    return *this;
  } else if ((page_id = page->GetNextPageId()) != INVALID_PAGE_ID){
    table_heap_->buffer_pool_manager_->UnpinPage(page_id, false);
    TablePage *page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page_id));
    while (page->GetFirstTupleRid(&rid_) && rid_ == INVALID_ROWID) {
      page_id = page->GetNextPageId();
      if (page_id == INVALID_PAGE_ID) {
        table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
        row_ptr_ = nullptr;
        return *this;
      }
      table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
      page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page_id));
    }
    row_ptr_->SetRowId(rid_);
    table_heap_->GetTuple(row_ptr_, nullptr);
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  } else {
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    row_ptr_ = nullptr;
  }
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator tmp(*this);
  ++(*this);
  return TableIterator(tmp);
}
