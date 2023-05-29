#pragma once

#include <stdint.h>

#include <memory>
#include <string>
#include <utility>

#include "Allocator.hh"

namespace sharedstructures {

class PriorityQueue {
public:
  PriorityQueue() = delete;
  PriorityQueue(const PriorityQueue&) = delete;
  PriorityQueue(PriorityQueue&&) = delete;

  // Unconditional create constructor - allocates a new priority queue. This
  // constructor doesn't affect the allocator's base object offset; you'll need
  // to explicitly pass a nonzero offset when opening this tree later. Use the
  // base() method to get the required offset.
  explicit PriorityQueue(std::shared_ptr<Allocator> allocator);
  // Create or open constructor.
  // - If base_offset != 0, opens an existing priority queue at that offset.
  // - If base_offset == 0, opens the priority queue  at the allocator's base
  //   object offset, creating one if the base object offset is also 0.
  PriorityQueue(std::shared_ptr<Allocator> allocator, uint64_t base_offset);

  ~PriorityQueue() = default;

  // Returns the allocator for this priority queue
  std::shared_ptr<Allocator> get_allocator() const;
  // Returns the base offset for this priority queue
  uint64_t base() const;

  // Inserts a new item in the queue
  void push(const void* data, size_t size);
  void push(const std::string& data);

  // Removes and returns the minimum item in the queue
  std::string pop();

  // TODO: implement replace() (like heapreplace and heappushpop in python)

  // Deletes everything from the queue
  void clear();

  // Inspection methods
  size_t size() const;

  // TODO: handle crashes. Currently duplicate pointers can be left behind;
  // probably we should somehow replace them with nulls and handle those in
  // accessors

private:
  std::shared_ptr<Allocator> allocator;
  uint64_t base_offset;

  struct QueueBase {
    uint64_t count;
    uint64_t allocated_count;
    uint64_t array_offset;

    QueueBase();
  } __attribute__((packed));

  QueueBase* queue_base();
  const QueueBase* queue_base() const;
  uint64_t* array(QueueBase* data);
  const uint64_t* array(const QueueBase* data) const;

  bool less_locked(uint64_t a_offset, uint64_t b_offset) const;

  void sift_down_locked(uint64_t start_index, uint64_t target_index);
  void sift_up_locked(uint64_t target_index);
};

} // namespace sharedstructures
