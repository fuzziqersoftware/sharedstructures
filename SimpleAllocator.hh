#pragma once

#include "Allocator.hh"

namespace sharedstructures {

class SimpleAllocator : public Allocator {
public:
  SimpleAllocator() = delete;
  SimpleAllocator(const SimpleAllocator&) = delete;
  SimpleAllocator(SimpleAllocator&&) = delete;
  explicit SimpleAllocator(std::shared_ptr<Pool> pool);
  ~SimpleAllocator() = default;


  // allocator functions.
  // there are three sets of these.
  // - allocate/free behave like malloc/free but deal with raw offsets instead
  //   of pointers.
  // - allocate_object/free_object behave like the new/delete operators (they
  //   call object constructors/destructors) but also deal with offsets instead
  //   of pointers.
  // - allocate_object_ptr and free_object_ptr deal with PoolPointer instances,
  //   but otherwise behave like allocate_object/free_object.

  virtual uint64_t allocate(size_t size);
  virtual void free(uint64_t x);

  virtual size_t block_size(uint64_t offset) const;

  virtual void set_base_object_offset(uint64_t offset);
  virtual uint64_t base_object_offset() const;

  virtual size_t bytes_allocated() const;
  virtual size_t bytes_free() const;

  // locks the entire pool
  virtual ProcessReadWriteLockGuard lock(bool writing) const;
  virtual bool is_locked(bool writing) const;


private:
  // pool structure

  struct Data {
    std::atomic<uint64_t> size; // this is part of the Pool structure

    std::atomic<uint8_t> initialized;

    ProcessReadWriteLock data_lock;

    std::atomic<uint64_t> base_object_offset;
    std::atomic<uint64_t> bytes_allocated; // sum of allocated block sizes
    std::atomic<uint64_t> bytes_committed; // same as above, + the block structs

    std::atomic<uint64_t> head;
    std::atomic<uint64_t> tail;

    uint8_t arena[0];
  };

  Data* data();
  const Data* data() const;


  // struct that describes an allocated block. inside the pool, these form a
  // doubly-linked list with variable-size elements.
  struct AllocatedBlock {
    uint64_t prev;
    uint64_t next;
    uint64_t size;

    uint64_t effective_size();
  };

  virtual void repair();
};

} // namespace sharedstructures
