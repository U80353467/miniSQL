#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if (page_allocated_ < GetMaxSupportedSize()) {
    page_allocated_++;
    page_offset = next_free_page_;
    bytes[page_offset / 8] |= (1 << (page_offset % 8));
    while (!IsPageFree(next_free_page_) && next_free_page_ < GetMaxSupportedSize()){
      next_free_page_++;
    }
    return true;
  }
  return false;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (page_allocated_ > 0 && page_offset < GetMaxSupportedSize() && !IsPageFree(page_offset)) {
    page_allocated_--;
    bytes[page_offset / 8] &= ~(1 << (page_offset % 8));
    if (page_offset < next_free_page_) {
      next_free_page_ = page_offset;
    }
    return true;
  }
  return false;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return (bytes[byte_index] & (1 << bit_index)) == 0;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;