#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t offset = 0;
  memcpy(buf + offset, &SCHEMA_MAGIC_NUM, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  uint32_t column_count = columns_.size();
  memcpy(buf + offset, &column_count, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  
  for (uint32_t i = 0; i < column_count; i++) {
    uint32_t column_size = columns_[i]->SerializeTo(buf + offset);
    offset += column_size;
  }
  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  uint32_t total_size = 0;
  total_size += sizeof(SCHEMA_MAGIC_NUM);
  uint32_t column_count = columns_.size();
  total_size += sizeof(column_count);
  for (uint32_t i = 0; i < column_count; i++) {
    total_size += columns_[i]->GetSerializedSize();
  }
  return total_size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  // replace with your code here
  uint32_t offset = 0;
  uint32_t magic_num;
  memcpy(&magic_num, buf + offset, sizeof(uint32_t));
  ASSERT(magic_num == SCHEMA_MAGIC_NUM, "Invalid magic number during deserialize.");
  offset += sizeof(uint32_t);

  uint32_t column_count;
  memcpy(&column_count, buf + offset, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  std::vector<Column *> columns;
  for (uint32_t i = 0; i < column_count; i++) {
    Column *column;
    uint32_t column_size = Column::DeserializeFrom(buf + offset, column);
    offset += column_size;
    columns.push_back(column);
  }
  schema = new Schema(columns, true);
  return offset;
}