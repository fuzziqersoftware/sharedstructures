#pragma once

#include <stdint.h>
#include <sys/uio.h> // for iov

#include <memory>
#include <string>
#include <utility>

#include "Allocator.hh"

namespace sharedstructures {


class Queue {
public:
  Queue() = delete;
  Queue(const Queue&) = delete;
  Queue(Queue&&) = delete;

  // unconditional create constructor - allocates a new queue. this constructor
  // doesn't affect the allocator's base object offset; you'll need to
  // explicitly pass a nonzero offset when opening this tree later. use the
  // base() method to get the required offset.
  explicit Queue(std::shared_ptr<Allocator> allocator);
  // create or open constructor.
  // - if base_offset != 0, opens an existing queue at that offset.
  // - if base_offset == 0, opens the queue at the allocator's base object
  //   offset, creating one if the base object offset is also 0.
  Queue(std::shared_ptr<Allocator> allocator, uint64_t base_offset);

  ~Queue() = default;

  // returns the allocator for this queue
  std::shared_ptr<Allocator> get_allocator() const;
  // returns the base offset for this queue
  uint64_t base() const;

  // adds an item to the queue
  void push_back(const void* data, size_t size);
  void push_back(const std::string& data);
  void push_front(const void* data, size_t size);
  void push_front(const std::string& data);
  void push(bool front, const void* data, size_t size);
  void push(bool front, const std::string& data);

  // removes an item from the queue
  std::string pop_back();
  std::string pop_front();
  std::string pop(bool front);

  // inspection methods
  size_t size() const; // item count
  size_t bytes() const; // total bytes in all objects, not counting overhead

  // used in unit tests to verify internal structure
  void verify(bool print = false);

private:
  std::shared_ptr<Allocator> allocator;
  uint64_t base_offset;

  struct Node {
    uint64_t prev_offset;
    uint64_t next_offset;
    uint8_t data[0];
  };

  struct QueueBase {
    uint64_t item_count;
    uint64_t total_value_bytes;

    Node front_node;
    Node back_node;

    QueueBase();
  };

  void setup_base_locked();

  void link_node_locked(Node* new_node, Node* prev, Node* next);
  void unlink_node_locked(Node* node);
  Node* create_node_locked(const void* data, size_t size);
};


} // namespace sharedstructures
