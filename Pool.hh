#pragma once

#include <atomic>
#include <phosg/Filesystem.hh>
#include <string>

namespace sharedstructures {

// TODO: we probably shouldn't assume 64-bit pointers everywhere

class Pool {
public:
  Pool() = delete;
  Pool(const Pool&) = delete;
  Pool(Pool&&) = delete;
  // open or create an existing pool.
  // if file is true, the pool is backed by the file given by `name`. if file is
  // false, the pool is backed by a shared memory object named `name`. on mac os
  // x, file = false isn't supported (and we assume file == true always) because
  // shared memory objects can't be resized after creation. if max_size is not
  // 0, this process will not expand the pool beyond that size (but it can open
  // an existing pool larger than that size, and another process that opened the
  // pool with a larger max_size can expand it). note that creating a new pool
  // is not race-safe; if two processes try to create the pool at the same time,
  // one will "win" and create the pool normally, but the other will see an
  // inconsistent view of the pool and may fail in strange ways.
  explicit Pool(const std::string& name, size_t max_size = 0, bool file = true);
  ~Pool();

  const std::string& get_name() const;


  // expands the pool to the given size. if the given size is smaller than the
  // pool's size, does nothing.
  ssize_t expand(size_t new_size);

  // checks for expansions by other processes. generally you shouldn't need to
  // call this manually; the allocator should do it for you when you lock the
  // pool.
  void check_size_and_remap() const;

  // returns the size of the pool in bytes.
  size_t size() const;


  // basic accessor functions.
  // the return values of the functions in this section are invalidated by any
  // action that causes the pool to change size or be remapped. these are:
  // - allocate/allocate_object
  // - free/free_object
  // - read_lock/write_lock

  // converts an offset into a usable pointer
  template <typename T> T* at(uint64_t offset) {
    return (T*)((uint8_t*)this->data + offset);
  }
  template <typename T> const T* at(uint64_t offset) const {
    return (T*)((uint8_t*)this->data + offset);
  }

  // converts a usable pointer into an offset
  template <typename T> uint64_t at(const T* ptr) const {
    return (uint64_t)ptr - (uint64_t)this->data;
  }


  // full-featured accessor.
  // in most situations you can treat this like a normal pointer, except that
  // it's not affected by pool remaps (so you don't have to worry about calling
  // at<T>() again after each allocate/free call). this is slightly slower than
  // at<T>() since it requires extra memory accesses; if your code is very
  // performance-sensitive, use at<T>() instead.

  template <typename T>
  class PoolPointer {
  public:
    PoolPointer(Pool* pool, uint64_t offset) : pool(pool), offset(offset) { }
    PoolPointer(const PoolPointer&) = default;
    PoolPointer(PoolPointer&&) = default;
    ~PoolPointer() = default;

    T& operator*() {
      return *this->pool->template at<T>(this->offset);
    }
    const T& operator*() const {
      return *this->pool->template at<T>(this->offset);
    }
    T* operator->() {
      return this->pool->template at<T>(this->offset);
    }
    const T* operator->() const {
      return this->pool->template at<T>(this->offset);
    }
    T& operator[](size_t index) {
      return *this->pool->template at<T>(this->offset + index * sizeof(T));
    }
    const T& operator[](size_t index) const {
      return *this->pool->template at<T>(this->offset + index * sizeof(T));
    }
    operator T*() {
      return this->pool->template at<T>(this->offset);
    }
    operator T*() const {
      return this->pool->template at<T>(this->offset);
    }

  private:
    Pool* pool;
    uint64_t offset;
  };


  // utility functions.

  // deletes a pool (without opening it). if the pool is not open by any other
  // processes, it's deleted immediately. if it is open somewhere, it's deleted
  // when all processes have closed it.
  static bool delete_pool(const std::string& name, bool file = true);

private:
  struct Data {
    std::atomic<uint64_t> size;
  };

  std::string name;
  size_t max_size;

  scoped_fd fd;
  mutable size_t pool_size;

  mutable Data* data;
};

} // namespace sharedstructures
