#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(lru_list.size() > 0){
    *frame_id = lru_list.front();
    lru_list.pop_front();
    page_set.erase(*frame_id);
    return true;
  }
  return false;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  if(page_set.find(frame_id) != page_set.end()){
    lru_list.remove(frame_id);
    page_set.erase(frame_id);
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if(page_set.find(frame_id) == page_set.end()){
    lru_list.push_back(frame_id);
    page_set.insert(frame_id);
  }
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return lru_list.size();
}