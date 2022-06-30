#include "PriorityQueue.hh"

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>

#include <algorithm>
#include <phosg/Strings.hh>

using namespace std;

namespace sharedstructures {


PriorityQueue::PriorityQueue(shared_ptr<Allocator> allocator) : allocator(allocator) {
  auto g = this->allocator->lock(true);
  this->base_offset = this->allocator->allocate_object<QueueBase>();
}

PriorityQueue::PriorityQueue(shared_ptr<Allocator> allocator, uint64_t base_offset) :
    allocator(allocator), base_offset(base_offset) {
  if (!this->base_offset) {
    auto g = this->allocator->lock(false);
    this->base_offset = this->allocator->base_object_offset();
  }

  if (!this->base_offset) {
    auto g = this->allocator->lock(true);
    this->base_offset = this->allocator->base_object_offset();
    if (!this->base_offset) {
      // We can't use sizeof() here because the Node structure varies in size
      this->base_offset = this->allocator->allocate_object<QueueBase>();
      this->allocator->set_base_object_offset(this->base_offset);
    }
  }
}


shared_ptr<Allocator> PriorityQueue::get_allocator() const {
  return this->allocator;
}

uint64_t PriorityQueue::base() const {
  return this->base_offset;
}


void PriorityQueue::push(const void* data, size_t size) {
  auto g = this->allocator->lock(true);
  auto p = this->allocator->get_pool();
  auto base = this->queue_base();

  // Figure out if we need to reallocate the array
  uint64_t new_allocated_count;
  uint64_t new_offset;
  if (base->allocated_count == 0) {
    new_allocated_count = 32;
    new_offset = this->allocator->allocate(new_allocated_count * sizeof(uint64_t));
    base = this->queue_base();
  } else if (base->allocated_count == base->count) {
    new_allocated_count = base->allocated_count * 2;
    new_offset = this->allocator->allocate(new_allocated_count * sizeof(uint64_t));
    base = this->queue_base();
  } else {
    new_allocated_count = base->allocated_count;
    new_offset = base->array_offset;
  }

  // If we reallocated the array, copy the contents to the new array and update
  // the pointer and allocated count
  if (new_offset != base->array_offset) {
    uint64_t* old_array = p->at<uint64_t>(base->array_offset);
    uint64_t* new_array = p->at<uint64_t>(new_offset);
    for (uint64_t x = 0; x < base->count; x++) {
      new_array[x] = old_array[x];
    }
    base->array_offset = new_offset;
    base->allocated_count = new_allocated_count;
  }

  // Copy the item itself into the pool
  uint64_t item_data_offset = this->allocator->allocate(size);
  memcpy(p->at<void>(item_data_offset), data, size);

  // Add the pointer into the heap
  base = this->queue_base(); // allocate() above may have invalidated this
  this->array(base)[base->count] = item_data_offset;
  base->count++;

  this->sift_down_locked(0, base->count - 1);
}

void PriorityQueue::push(const string& data) {
  this->push(data.data(), data.size());
}


string PriorityQueue::pop() {
  auto g = this->allocator->lock(true);
  auto p = this->allocator->get_pool();
  auto base = this->queue_base();

  uint64_t returned_item_offset;

  if (base->count == 0) {
    throw out_of_range("queue is empty");

  } else if (base->count == 1) {
    returned_item_offset = this->array(base)[0];
    base->count = 0;

  } else { // 2 or more items
    uint64_t* array = this->array(base);

    // Put the last item into the first slot and sift it up appropriately
    returned_item_offset = array[0];
    array[0] = array[base->count - 1];
    base->count--;
    this->sift_up_locked(0);
  }

  const char* data = p->at<char>(returned_item_offset);
  string ret(data, this->allocator->block_size(returned_item_offset));
  this->allocator->free(returned_item_offset);
  return ret;
}

void PriorityQueue::clear() {
  auto g = this->allocator->lock(true);
  auto p = this->allocator->get_pool();
  auto base = this->queue_base();
  uint64_t* array = this->array(base);

  uint64_t count = base->count;
  base->count = 0;
  for (; count > 0; count--) {
    this->allocator->free(array[count - 1]);
  }
}

size_t PriorityQueue::size() const {
  auto g = this->allocator->lock(false);
  return this->queue_base()->count;
}


PriorityQueue::QueueBase::QueueBase() : count(0), allocated_count(0), array_offset(0) { }


PriorityQueue::QueueBase* PriorityQueue::queue_base() {
  return this->allocator->get_pool()->at<QueueBase>(this->base_offset);
}

const PriorityQueue::QueueBase* PriorityQueue::queue_base() const {
  return this->allocator->get_pool()->at<QueueBase>(this->base_offset);
}

uint64_t* PriorityQueue::array(QueueBase* base) {
  return this->allocator->get_pool()->at<uint64_t>(base->array_offset);
}

const uint64_t* PriorityQueue::array(const QueueBase* base) const {
  return this->allocator->get_pool()->at<const uint64_t>(base->array_offset);
}


bool PriorityQueue::less_locked(uint64_t a_offset, uint64_t b_offset) const {
  auto p = this->allocator->get_pool();
  uint64_t a_size = this->allocator->block_size(a_offset);
  uint64_t b_size = this->allocator->block_size(b_offset);
  const void* a = p->at<const void>(a_offset);
  const void* b = p->at<const void>(b_offset);
  int memcmp_res = memcmp(a, b, (a_size < b_size) ? a_size : b_size);
  if (memcmp_res < 0) {
    return true;
  } else if (memcmp_res > 0) {
    return false;
  } else {
    return (a_size < b_size);
  }
}


void PriorityQueue::sift_down_locked(uint64_t start_index, uint64_t target_index) {
  auto base = this->queue_base();
  uint64_t* array = this->array(base);

  uint64_t displaced_item = array[target_index];
  while (target_index > start_index) {
    uint64_t parent_index = (target_index - 1) >> 1;

    // If the parent is less than the target, we're done
    if (!this->less_locked(displaced_item, array[parent_index])) {
      break;
    }

    array[target_index] = array[parent_index];
    target_index = parent_index;
  }

  array[target_index] = displaced_item;
}

void PriorityQueue::sift_up_locked(uint64_t target_index) {
  auto base = this->queue_base();
  uint64_t* array = this->array(base);

  uint64_t original_target_index = target_index;
  uint64_t displaced_item = array[target_index];
  uint64_t left_child_index = 2 * target_index + 1;
  while (left_child_index < base->count) {
    uint64_t right_child_index = left_child_index + 1;
    uint64_t least_child_index = ((right_child_index < base->count) &&
        !this->less_locked(array[left_child_index], array[right_child_index])) ?
          right_child_index : left_child_index;

    array[target_index] = array[least_child_index];
    target_index = least_child_index;

    left_child_index = 2 * target_index + 1;
  }

  array[target_index] = displaced_item;
  this->sift_down_locked(original_target_index, target_index);
}


} // namespace sharedstructures
