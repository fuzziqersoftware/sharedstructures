#pragma once

#include "Allocator.hh"

namespace sharedstructures {

class LogarithmicAllocator : public Allocator {
public:
  LogarithmicAllocator() = delete;
  LogarithmicAllocator(const LogarithmicAllocator&) = delete;
  LogarithmicAllocator(LogarithmicAllocator&&) = delete;
  explicit LogarithmicAllocator(std::shared_ptr<Pool> pool);
  ~LogarithmicAllocator() = default;

  virtual uint64_t allocate(size_t size);
  virtual void free(uint64_t x);

  virtual size_t block_size(uint64_t offset) const;

  virtual void set_base_object_offset(uint64_t offset);
  virtual uint64_t base_object_offset() const;

  virtual size_t bytes_allocated() const;
  virtual size_t bytes_free() const;

  virtual ProcessReadWriteLockGuard lock(bool writing) const;
  virtual bool is_locked(bool writing) const;

  // Debugging functions
  virtual void verify() const;
  void print(FILE* stream) const;

private:
  struct alignas(8) Data {
    alignas(8) std::atomic<uint64_t> size; // This is part of the Pool structure
    alignas(8) std::atomic<uint64_t> initialized;

    ProcessReadWriteLock data_lock;

    alignas(8) std::atomic<uint64_t> base_object_offset;
    alignas(8) std::atomic<uint64_t> bytes_allocated; // Sum of allocated block sizes
    alignas(8) std::atomic<uint64_t> bytes_committed; // Same as above, + the block structs

    // Minimum order is 4 (0x10) and maximum order is 57 (0x0200000000000000),
    // for a total of 54 orders
    static const int8_t minimum_order;
    static const int8_t maximum_order;
    alignas(8) std::atomic<uint64_t> free_head[54];
    alignas(8) std::atomic<uint64_t> free_tail[54];

    uint8_t arena[0];
  } __attribute__((packed));

  Data* data();
  const Data* data() const;

  struct alignas(8) FreeBlock {
    // High bit: allocated (must be 0); next 6 bits: order; rest: prev ptr
    alignas(8) uint64_t prev_order_allocated;
    alignas(8) uint64_t next;

    uint64_t prev() const;
    int8_t order() const;
    bool allocated() const;
  } __attribute__((packed));

  struct alignas(8) AllocatedBlock {
    // High bit: allocated (must be 1)
    alignas(8) uint64_t size_allocated;

    uint64_t size() const;
    bool allocated() const;
  } __attribute__((packed));

  union alignas(8) Block {
    FreeBlock free;
    AllocatedBlock allocated;
  } __attribute__((packed));

  virtual void repair();

  void create_free_block(uint64_t offset, int8_t order);
  void create_free_blocks(uint64_t offset, uint64_t size);
  uint64_t merge_blocks_at(uint64_t block_offset);
  void unlink_block(uint64_t block_offset);
};

} // namespace sharedstructures
