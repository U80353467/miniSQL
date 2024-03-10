#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t offset = 0;
  memcpy(buf + offset, &COLUMN_MAGIC_NUM, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  uint32_t name_len = name_.length();
  memcpy(buf + offset, &name_len, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  
  memcpy(buf + offset, name_.c_str(), name_len);
  offset += name_len;

  memcpy(buf + offset, &type_, sizeof(TypeId));
  offset += sizeof(TypeId);

  memcpy(buf + offset, &len_, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  memcpy(buf + offset, &table_ind_, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  memcpy(buf + offset, &nullable_, sizeof(bool));
  offset += sizeof(bool);

  memcpy(buf + offset, &unique_, sizeof(bool));
  offset += sizeof(bool);

  return offset;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  uint32_t total_size = 0;
  total_size += sizeof(COLUMN_MAGIC_NUM);  
  uint32_t name_len = name_.length();
  total_size += sizeof(name_len); 
  total_size += name_len;
  total_size += sizeof(TypeId); 
  total_size += sizeof(len_); 
  total_size += sizeof(table_ind_); 
  total_size += sizeof(nullable_);
  total_size += sizeof(unique_);
  return total_size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  // replace with your code here
  uint32_t offset = 0;
  uint32_t magic_num;
  memcpy(&magic_num, buf + offset, sizeof(uint32_t));
  ASSERT(magic_num == COLUMN_MAGIC_NUM, "Invalid magic number during deserialize.");
  offset += sizeof(uint32_t);

  uint32_t name_len;
  memcpy(&name_len, buf + offset, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  char * name = new char[name_len + 1];
  memcpy(name, buf + offset, name_len);
  name[name_len] = '\0';
  offset += name_len;
  
  TypeId type;
  memcpy(&type, buf + offset, sizeof(TypeId));
  offset += sizeof(TypeId);

  uint32_t len;
  memcpy(&len, buf + offset, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  
  uint32_t table_ind;
  memcpy(&table_ind, buf + offset, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  
  bool nullable;
  memcpy(&nullable, buf + offset, sizeof(bool));
  offset += sizeof(bool);

  bool unique;
  memcpy(&unique, buf + offset, sizeof(bool));
  offset += sizeof(bool);

  if (type != TypeId::kTypeChar) {
    column = new Column(name, type, table_ind, nullable, unique);
  } else {
    column = new Column(name, type, len, table_ind, nullable, unique);
  }

  return offset;
}
