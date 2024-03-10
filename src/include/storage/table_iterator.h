#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"

class TableHeap;

class TableIterator {
public:
  // you may define your own constructor based on your member variables
  explicit TableIterator();

  explicit TableIterator(const TableIterator &other);

  explicit TableIterator(TableIterator &&other) noexcept;

  explicit TableIterator(TableHeap *table_heap, Transaction *txn);

  explicit TableIterator(TableHeap *table_heap, Row *row_ptr);

  virtual ~TableIterator();

  bool operator==(const TableIterator &itr) const;

  bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator=(const TableIterator &itr) noexcept;

  TableIterator &operator++();

  TableIterator operator++(int); 

private:
  // add your own private member variables here
  Row * row_ptr_;
  RowId rid_;
public:
  TableHeap * table_heap_;
};

#endif  // MINISQL_TABLE_ITERATOR_H
