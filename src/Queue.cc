#include "Queue.hh"

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>

#include <algorithm>
#include <phosg/Strings.hh>

using namespace std;

namespace sharedstructures {

Queue::Queue(shared_ptr<Allocator> allocator)
    : allocator(allocator) {
  auto g = this->allocator->lock(true);
  this->base_offset = this->allocator->allocate_object<QueueBase>();
  this->setup_base_locked();
}

Queue::Queue(shared_ptr<Allocator> allocator, uint64_t base_offset)
    : allocator(allocator),
      base_offset(base_offset) {
  if (!this->base_offset) {
    auto g = this->allocator->lock(false);
    this->base_offset = this->allocator->base_object_offset();
  }

  if (!this->base_offset) {
    auto g = this->allocator->lock(true);
    this->base_offset = this->allocator->base_object_offset();
    if (!this->base_offset) {
      this->base_offset = this->allocator->allocate_object<QueueBase>();
      this->allocator->set_base_object_offset(this->base_offset);
      this->setup_base_locked();
    }
  }
}

void Queue::setup_base_locked() {
  auto p = this->allocator->get_pool();
  QueueBase* base = p->at<QueueBase>(this->base_offset);

  base->front_node.prev_offset = 0;
  base->front_node.next_offset = p->at(&base->back_node);
  base->back_node.prev_offset = p->at(&base->front_node);
  base->back_node.next_offset = 0;
}

shared_ptr<Allocator> Queue::get_allocator() const {
  return this->allocator;
}

uint64_t Queue::base() const {
  return this->base_offset;
}

void Queue::link_node_locked(Node* new_node, Node* prev, Node* next) {
  auto p = this->allocator->get_pool();

  new_node->prev_offset = p->at(prev);
  new_node->next_offset = p->at(next);
  prev->next_offset = p->at(new_node);
  next->prev_offset = p->at(new_node);

  QueueBase* base = p->at<QueueBase>(this->base_offset);
  base->item_count++;
  base->total_value_bytes += this->allocator->block_size(p->at(new_node)) - sizeof(Node);
}

void Queue::unlink_node_locked(Node* node) {
  auto p = this->allocator->get_pool();

  Node* prev = p->at<Node>(node->prev_offset);
  Node* next = p->at<Node>(node->next_offset);
  prev->next_offset = node->next_offset;
  next->prev_offset = node->prev_offset;

  QueueBase* base = p->at<QueueBase>(this->base_offset);
  base->item_count--;
  base->total_value_bytes -= this->allocator->block_size(p->at(node)) - sizeof(Node);
}

Queue::Node* Queue::create_node_locked(const void* data, size_t size) {
  uint64_t new_node_offset = this->allocator->allocate_object<Node>(
      sizeof(Node) + size);
  Node* new_node = this->allocator->get_pool()->at<Node>(new_node_offset);
  memcpy(new_node->data, data, size);
  return new_node;
}

void Queue::push_back(const void* data, size_t size) {
  auto g = this->allocator->lock(true);
  auto p = this->allocator->get_pool();

  Node* new_node = this->create_node_locked(data, size);
  QueueBase* base = p->at<QueueBase>(this->base_offset);
  this->link_node_locked(new_node, p->at<Node>(base->back_node.prev_offset),
      &base->back_node);
}

void Queue::push_back(const string& data) {
  this->push_back(data.data(), data.size());
}

void Queue::push_front(const void* data, size_t size) {
  auto g = this->allocator->lock(true);
  auto p = this->allocator->get_pool();

  Node* new_node = this->create_node_locked(data, size);
  QueueBase* base = p->at<QueueBase>(this->base_offset);
  this->link_node_locked(new_node, &base->front_node,
      p->at<Node>(base->front_node.next_offset));
}

void Queue::push_front(const string& data) {
  this->push_front(data.data(), data.size());
}

void Queue::push(bool front, const void* data, size_t size) {
  if (front) {
    this->push_front(data, size);
  } else {
    this->push_back(data, size);
  }
}

void Queue::push(bool front, const string& data) {
  if (front) {
    this->push_front(data.data(), data.size());
  } else {
    this->push_back(data.data(), data.size());
  }
}

string Queue::pop_back() {
  auto g = this->allocator->lock(true);
  auto p = this->allocator->get_pool();

  QueueBase* base = p->at<QueueBase>(this->base_offset);
  Node* node = p->at<Node>(base->back_node.prev_offset);
  if (node->prev_offset == 0) {
    throw out_of_range("queue is empty");
  }
  this->unlink_node_locked(node);

  string ret(reinterpret_cast<const char*>(node->data),
      this->allocator->block_size(p->at(node)) - sizeof(Node));
  this->allocator->free_object_ptr(node);
  return ret;
}

string Queue::pop_front() {
  auto g = this->allocator->lock(true);
  auto p = this->allocator->get_pool();

  QueueBase* base = p->at<QueueBase>(this->base_offset);
  Node* node = p->at<Node>(base->front_node.next_offset);
  if (node->next_offset == 0) {
    throw out_of_range("queue is empty");
  }
  this->unlink_node_locked(node);

  string ret(reinterpret_cast<const char*>(node->data),
      this->allocator->block_size(p->at(node)) - sizeof(Node));
  this->allocator->free_object_ptr(node);
  return ret;
}

string Queue::pop(bool front) {
  if (front) {
    return this->pop_front();
  } else {
    return this->pop_back();
  }
}

size_t Queue::size() const {
  auto g = this->allocator->lock(false);
  return this->allocator->get_pool()->at<QueueBase>(this->base_offset)->item_count;
}

size_t Queue::bytes() const {
  auto g = this->allocator->lock(false);
  return this->allocator->get_pool()->at<QueueBase>(this->base_offset)->total_value_bytes;
}

Queue::QueueBase::QueueBase()
    : item_count(0),
      total_value_bytes(0) {}

void Queue::verify(bool print) {
  auto g = this->allocator->lock(false);
  auto p = this->allocator->get_pool();

  QueueBase* base = p->at<QueueBase>(this->base_offset);
  uint64_t prev_offset = 0;
  uint64_t current_offset;
  // We intentionally overflow a size_t here to start at a "negative" number, so
  // we won't count the front and back nodes
  size_t count = static_cast<size_t>(-2);
  size_t bytes = 0;
  for (current_offset = p->at(&base->front_node); current_offset;
       current_offset = p->at<Node>(current_offset)->next_offset, count++) {
    Node* current = p->at<Node>(current_offset);

    size_t node_data_bytes;
    if ((current != &base->front_node) && (current != &base->back_node)) {
      node_data_bytes = this->allocator->block_size(p->at(current)) - sizeof(Node);
    } else {
      node_data_bytes = 0;
    }
    bytes += node_data_bytes;

    if (print) {
      fprintf(stderr, "node %016" PRIX64 " has prev=%016" PRIX64 " next=%016" PRIX64 " with %zu data bytes\n",
          current_offset, current->prev_offset, current->next_offset, node_data_bytes);
    }
    if (current->prev_offset != prev_offset) {
      throw runtime_error("incorrect prev link");
    }
    prev_offset = current_offset;
  }
  if (prev_offset != p->at(&base->back_node)) {
    throw runtime_error("terminal node is not the back node");
  }
  if (print) {
    fprintf(stderr, "%zu data bytes in %zu items total\n", bytes, count);
  }
  if (count != base->item_count) {
    throw runtime_error(string_printf("item count is incorrect (%zu expected, %" PRIu64 " stored)",
        count, base->item_count));
  }
  if (bytes != base->total_value_bytes) {
    throw runtime_error(string_printf("item byte count is incorrect (%zu expected, %" PRIu64 " stored)",
        bytes, base->total_value_bytes));
  }
}

} // namespace sharedstructures
