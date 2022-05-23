#pragma once

#include <memory>

#include "Pool.hh"
#include "ProcessLock.hh"

namespace sharedstructures {

class Allocator {
protected:
  Allocator(std::shared_ptr<Pool>);

public:
  Allocator() = delete;
  Allocator(Pool&&) = delete;
  virtual ~Allocator() = default;


  // introspection functions
  std::shared_ptr<Pool> get_pool() const;


  // allocator functions.
  // there are three sets of these.
  // - allocate/free behave like malloc/free but deal with raw offsets instead
  //   of pointers.
  // - allocate_object/free_object behave like the new/delete operators (they
  //   call object constructors/destructors) but also deal with offsets instead
  //   of pointers.
  // - allocate_object_ptr and free_object_ptr deal with PoolPointer instances,
  //   but otherwise behave like allocate_object/free_object.
  // TODO: support shrinking the pool by truncating unused space at the end

  virtual uint64_t allocate(size_t size) = 0;

  // TODO: figure out why forwarding doesn't work here (we should use Args&&)
  template <typename T, typename... Args>
  uint64_t allocate_object(Args... args, size_t size = 0) {
    uint64_t off = this->allocate(size ? size : sizeof(T));
    new (this->pool->at<T>(off)) T(std::forward<Args>(args)...);
    return off;
  }
  template <typename T, typename... Args>
  Pool::PoolPointer<T> allocate_object_ptr(Args... args, size_t size = 0) {
    uint64_t off = this->allocate(size ? size : sizeof(T));
    new (this->pool->at<T>(off)) T(std::forward<Args>(args)...);
    return Pool::PoolPointer<T>(this->pool.get(), off);
  }

  virtual void free(uint64_t x) = 0;

  template <typename T> void free_object(uint64_t off) {
    T* x = (T*)off;
    x->T::~T();
    this->free(off);
  }
  template <typename T> void free_object_ptr(T* ptr) {
    this->free_object<T>(this->pool->at(ptr));
  }

  // returns the size of the allocated block starting at offset
  virtual size_t block_size(uint64_t offset) const = 0;


  // base object functions.
  // the base object is a single pointer stored in the pool's header. this can
  // be used to keep track of the main data structure that a pool contains, so
  // it doesn't need to be stored outside the pool (and given every time the
  // pool is opened).

  virtual void set_base_object_offset(uint64_t offset) = 0;
  virtual uint64_t base_object_offset() const = 0;


  // introspection functions.

  // returns the size of all allocated blocks, excluding overhead
  virtual size_t bytes_allocated() const = 0;
  // returns the number of bytes in free space
  virtual size_t bytes_free() const = 0;
  // overhead can be computed as size() - free_space() - allocated_space()


  // locking functions.

  virtual ProcessReadWriteLockGuard lock(bool writing) const = 0;
  virtual bool is_locked(bool writing) const = 0;


  // for debugging

  virtual void verify() const = 0;


protected:
  std::shared_ptr<Pool> pool;

  virtual void repair();
};

} // namespace sharedstructures
