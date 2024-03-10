#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t offset = 0;
  //memcpy(buf + offset, &ROW_MAGIC_NUM, sizeof(uint32_t));
  //offset += sizeof(uint32_t);

  uint32_t field_count = fields_.size();
  memcpy(buf + offset, &field_count, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  uint32_t null_map_size = (field_count + 7) / 8;
  char * null_map = new char[null_map_size];
  memset(null_map, 0, null_map_size);
  for (uint32_t i = 0; i < field_count; i++) {
    if (fields_[i]->IsNull()) {
      null_map[i / 8] |= (1 << (i % 8));
    }
  }

  memcpy(buf + offset, null_map, null_map_size);
  offset += null_map_size;
  for (uint32_t i = 0; i < field_count; i++) {
      uint32_t field_size = fields_[i]->SerializeTo(buf + offset);
      offset += field_size;
  }
  delete[] null_map;
  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  fields_.clear();
  ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
  uint32_t offset = 0;
  //uint32_t magic_num;
  //memcpy(&magic_num, buf + offset, sizeof(uint32_t));
  //offset += sizeof(uint32_t);
  //ASSERT(magic_num == ROW_MAGIC_NUM, "Invalid magic number during deserialize.");

  uint32_t field_count;
  memcpy(&field_count, buf + offset, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  uint32_t null_map_size = (field_count + 7) / 8;
  char * null_map = new char[null_map_size];
  memcpy(null_map, buf + offset, null_map_size);
  offset += null_map_size;

  for (uint32_t i = 0; i < field_count; i++) {
    Field * field;
    bool is_null = (null_map[i / 8] & (1 << (i % 8))) != 0;
    uint32_t field_size = Field::DeserializeFrom(buf + offset, schema->GetColumn(i)->GetType(), &field, is_null);
    offset += field_size;
    fields_.push_back(field);
  }

  delete[] null_map;
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t total_size = 0;
  //total_size += sizeof(uint32_t);
  
  uint32_t field_count = fields_.size();
  const uint32_t COLUMN_COUNT_SIZE = sizeof(uint32_t);
  total_size += COLUMN_COUNT_SIZE;
  uint32_t null_map_size = (field_count + 7) / 8;
  total_size += null_map_size;

  for (uint32_t i = 0; i < field_count; i++) {
      total_size += fields_[i]->GetSerializedSize();
  }

  return total_size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
