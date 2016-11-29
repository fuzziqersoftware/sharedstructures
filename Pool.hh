#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include <memory>
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
  // pool with a larger max_size can expand it).
  explicit Pool(const std::string& name, size_t max_size = 0, bool file = true);
  ~Pool();


  // accessor functions.
  // the return values of the functions in this section are invalidated by any
  // action that causes the pool to change size or be remapped. these are:
  // - allocate/allocate_object
  // - free/free_object
  // - read_lock/write_lock

  // converts an offset into a usable pointer
  template <typename T> T* at(uint64_t offset) {
    return (T*)(&this->data->data[offset]);
  }
  template <typename T> const T* at(uint64_t offset) const {
    return (T*)(&this->data->data[offset]);
  }

  // converts a usable pointer into an offset
  template <typename T> uint64_t at(const T* ptr) const {
    return (uint64_t)ptr - (uint64_t)this->data;
  }


  // allocator functions.
  // there are two sets of these. allocate/free behave like malloc/free but deal
  // with offsets instead of pointers. allocate_object/free_object behave like
  // the new/delete operators (they call object constructors/destructors) but
  // also deal with offsets instead of pointers.
  // TODO: the allocator algorithm is currently linear-time; this can be slow
  // when a large number of objects are allocated.
  // TODO: support shrinking the pool by truncating unused space at the end

  uint64_t allocate(size_t size);

  // TODO: figure out why forwarding doesn't work here (we should use Args&&)
  template <typename T, typename... Args>
  uint64_t allocate_object(Args... args, size_t size = 0) {
    uint64_t off = this->allocate(size ? size : sizeof(T));
    new (this->at<T>(off)) T(std::forward<Args>(args)...);
    return off;
  }

  void free(uint64_t x);

  template <typename T> void free_object(uint64_t off) {
    T* x = (T*)off;
    x->T::~T();
    this->free(off);
  }

  // returns the size of the allocated block starting at offset
  size_t block_size(uint64_t offset) const;


  // base object functions.
  // the base object is a single pointer stored in the pool's header. this can
  // be used to keep track of the main data structure that a pool contains, so
  // it doesn't need to be stored outside the pool (and given every time the
  // pool is opened).

  void set_base_object_offset(uint64_t offset);
  uint64_t base_object_offset() const;


  // introspection functions.

  // returns the size of the pool in bytes
  size_t size() const;
  // returns the size of all allocated blocks, excluding overhead
  size_t bytes_allocated() const;
  // returns the number of bytes in free space
  size_t bytes_free() const;
  // overhead can be computed as size() - free_space() - allocated_space()


  // utility functions.

  // deletes a pool (without opening it). if the pool is not open by any other
  // processes, it's deleted immediately. if it is open somewhere, it's deleted
  // when all processes have closed it.
  static bool delete_pool(const std::string& name);


  // locking functions.

  // TODO: currently we use a crude spinlock mechanism. we can't use better
  // pre-built libraries like pthreads ot std::mutex because they contain
  // pointers, and the pool can move around in a process' address space.

  class pool_rw_guard {
  public:
    pool_rw_guard() = delete;
    pool_rw_guard(const pool_rw_guard&) = delete;
    pool_rw_guard(pool_rw_guard&&);
    pool_rw_guard(const Pool* pool, bool writing);
    ~pool_rw_guard();

  private:
    bool writing;
    const Pool* pool;
  };

  // locks the entire pool for reading
  pool_rw_guard read_lock() const;
  // locks the entire pool for writing
  pool_rw_guard write_lock();


private:
  // basic stuff
  std::string name;
  size_t max_size;


  // pool structure

  union Data {
    uint8_t data[0];
    struct {
      std::atomic<uint64_t> size;
      std::atomic<uint8_t> resize_lock; // 0 = unlocked, 1 = locked
      std::atomic<uint8_t> read_lock; // 0 = unlocked, 1 = locked
      std::atomic<uint8_t> write_lock; // 0 = unlocked, 1 = locked
      std::atomic<uint64_t> num_readers;

      uint64_t head;
      uint64_t tail;

      std::atomic<uint64_t> base_object_offset;
      std::atomic<uint64_t> bytes_allocated;
      std::atomic<uint64_t> bytes_free;

      uint8_t arena[0];
    };
  };

  scoped_fd fd;
  mutable Data* data;
  mutable size_t pool_size;

  void check_size_and_remap() const;
  void expand_pool(size_t new_size);


  // locking primitives (super primitive)

  static void spinlock(std::atomic<uint8_t>*);
  static void unlock(std::atomic<uint8_t>*);


  // struct that describes an allocated block. inside the pool, these form a
  // doubly-linked list with variable-size elements.
  struct AllocatedBlock {
    // TODO: maybe we can make these uint32_t to save some space
    uint64_t prev;
    uint64_t next;
    uint64_t size;

    uint64_t effective_size();
  };
};

} // namespace sharedstructures
