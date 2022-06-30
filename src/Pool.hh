#pragma once

#include <atomic>
#include <phosg/Filesystem.hh>
#include <string>
#include <sys/mman.h>

// This mmap flag is required on OSX but doesn't exist on Linux
#ifndef PHOSG_MACOS
#define MAP_HASSEMAPHORE 0
#endif

namespace sharedstructures {

// TODO: This assumption is not always true.
#define PAGE_SIZE 4096

// TODO: We probably shouldn't assume 64-bit pointers everywhere either, but
// this seems safe for the foreseeable future.

class Pool {
public:
  Pool() = delete;
  Pool(const Pool&) = delete;
  Pool(Pool&&) = delete;
  // The Pool constructor opens an existing pool or creates a new one.
  // If file is true, the pool is backed by the file given by `name`. If file is
  // false, the pool is backed by a shared memory object named `name`. On macOS,
  // file = false isn't supported (and we assume file == true always) because
  // shared memory objects can't be resized after creation. If max_size is not
  // 0, this process will not expand the pool beyond that size (but it can open
  // an existing pool larger than that size, and another process that opened the
  // pool with a larger max_size can expand it). Note that creating a new pool
  // is not race-safe; if two processes try to create the pool at the same time,
  // one will "win" and create the pool normally, but the other may see an
  // inconsistent view of the pool and fail. The following errors can be thrown:
  // - cannot_open_file: the pool exists, but we can't open it. This can happen
  //   if someone else called delete_pool while we were trying to open it - try
  //   again.
  // - runtime_error: the pool exists, but its size is zero (if we were opening
  //   it), or we can't increase its size (if we created it). This can happen if
  //   multiple processes try to create the pool concurrently, or if the disk on
  //   which the pool is being created is out of space.
  // - bad_alloc: the pool exists and isn't empty, but we can't map it into our
  //   address space. Either it's too large or we're out of address space.
  explicit Pool(const std::string& name, size_t max_size = 0, bool file = true);
  ~Pool();

  const std::string& get_name() const;


  // Expands the pool to the given size. If the given size is smaller than the
  // pool's size, does nothing.
  void expand(size_t new_size);

  // Checks for expansions by other processes. Generally you shouldn't need to
  // call this manually; the allocator should do it for you when you lock the
  // pool.
  void check_size_and_remap() const;

  // Returns the size of the pool in bytes.
  size_t size() const;


  // Basic accessor functions.
  // The return values of the functions in this section are invalidated by any
  // action that causes the pool to change size or be remapped. These are:
  // - allocate/allocate_object
  // - free/free_object
  // - read_lock/write_lock

  // Converts an offset into a usable pointer
  template <typename T> T* at(uint64_t offset) {
    if (!this->data) {
      throw std::bad_alloc();
    }
    return (T*)((uint8_t*)this->data + offset);
  }
  template <typename T> const T* at(uint64_t offset) const {
    if (!this->data) {
      throw std::bad_alloc();
    }
    return (T*)((uint8_t*)this->data + offset);
  }

  // Converts a usable pointer into an offset
  template <typename T> uint64_t at(const T* ptr) const {
    if (!this->data) {
      throw std::bad_alloc();
    }
    return (uint64_t)ptr - (uint64_t)this->data;
  }


  // Temporary-state accessor functions.
  // These are necessary only in special situations - for example, if we lock
  // the pool and expand it, but it then doesn't fit in the current process'
  // address space. To unlock the pool after such an occurrence, we map only the
  // page containing the lock, clear it, and unmap the page immediately.
  template <typename T> T map_and_read_atomic(uint64_t offset) const {
    uint64_t page_offset = offset & ~(PAGE_SIZE - 1);
    uint64_t offset_within_page = offset ^ page_offset;

    // Map two pages if it spans a page boundary
    uint8_t page_count = 1 + ((offset_within_page + sizeof(T)) > PAGE_SIZE);
    void* data = mmap(nullptr, page_count * PAGE_SIZE, PROT_READ,
        MAP_SHARED | MAP_HASSEMAPHORE, this->fd, 0);
    if (data == MAP_FAILED) {
      throw std::bad_alloc();
    }
    std::atomic<T>* var = (std::atomic<T>*)((char*)data + offset_within_page);
    T ret = var->load();
    munmap(data, page_count * PAGE_SIZE);
    return ret;
  }

  template <typename T> void map_and_write_atomic(uint64_t offset, T value) {
    uint64_t page_offset = offset & ~(PAGE_SIZE - 1);
    uint64_t offset_within_page = offset ^ page_offset;

    // Map two pages if it spans a page boundary
    uint8_t page_count = 1 + ((offset_within_page + sizeof(T)) > PAGE_SIZE);
    void* data = mmap(nullptr, page_count * PAGE_SIZE, PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_HASSEMAPHORE, this->fd, 0);
    if (data == MAP_FAILED) {
      throw std::bad_alloc();
    }
    std::atomic<T>* var = (std::atomic<T>*)((char*)data + offset_within_page);
    var->store(value);
    munmap(data, page_count * PAGE_SIZE);
  }

  void map_and_call(uint64_t offset, size_t size,
      std::function<void(void*, size_t)> cb);


  // Full-featured accessor.
  // In most situations you can treat this like a normal pointer, except that
  // it's not affected by pool remaps (so you don't have to worry about calling
  // at<T>() again after each allocate/free call). This is slightly slower than
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


  // Deletes a pool (without opening it). If the pool is not open by any other
  // processes, it's deleted immediately; if it is open somewhere, it's deleted
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
