#include "Pool.hh"

#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <phosg/Filesystem.hh>
#include <phosg/Strings.hh>

// this mmap flag is required on OSX but doesn't exist on Linux
#ifndef MAP_HASSEMAPHORE
#define MAP_HASSEMAPHORE 0
#endif

// TODO: this assumption might be wrong on some less-common architectures
#define PAGE_SIZE 4096

using namespace std;

namespace sharedstructures {


static int open_segment(const char* name, int type, mode_t mode, bool file) {
  if (file) {
    return open(name, type, mode);
  } else {
    return shm_open(name, type, mode);
  }
}

static int unlink_segment(const char* name, bool file) {
  if (file) {
    return unlink(name);
  } else {
    return shm_unlink(name);
  }
}


Pool::Pool(const string& name, size_t max_size, bool file) : name(name),
    max_size(max_size) {

  // on Linux, shared memory objects can be resized at any time just by calling
  // ftruncate again. but on OSX, ftruncate can be called only once for each
  // shared memory object, so we instead have to memory-map a file on disk.
  // furthermore, OSX and Linux define the shm_open function differently so we
  // can't use a function pointer to simplify usage
  if (MAP_HASSEMAPHORE) {
    file = true;
  }

  this->fd = open_segment(this->name.c_str(), O_RDWR | O_CREAT | O_EXCL, 0666,
      file);
  if (this->fd == -1 && errno == EEXIST) {
    this->fd = open_segment(this->name.c_str(), O_RDWR, 0666, file);
    if (this->fd == -1) {
      throw cannot_open_file(this->name);
    }

    // we did not create the shared memory object, so it should have a nonzero
    // size. map it all into memory at once
    this->pool_size = fstat(fd).st_size;
    this->data = (Data*)mmap(NULL, this->pool_size, PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_HASSEMAPHORE, this->fd, 0);
    if (!this->data) {
      throw runtime_error("mmap failed: " + string_for_error(errno));
    }

  } else {

    // we created the shared memory object, so its size is zero. resize it to
    // the minimum size and initialize the basic data structures. note that this
    // procedure is safe from a concurrency perspective because we use 0 as the
    // locked state for our mutexes.
    this->pool_size = PAGE_SIZE;
    if (ftruncate(this->fd, this->pool_size)) {
      unlink_segment(this->name.c_str(), file);
      throw runtime_error("can\'t resize memory map: " +
          string_for_error(errno));
    }

    this->data = (Data*)mmap(NULL, this->pool_size, PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_HASSEMAPHORE, this->fd, 0);
    if (!this->data) {
      unlink_segment(this->name.c_str(), file);
      throw runtime_error("mmap failed: " + string_for_error(errno));
    }

    // according to POSIX, ftruncate() should fill the new space with zeroes,
    // which means we would be holding all the locks already. if the environment
    // isn't POSIX-compliant then these might not be zeroed, which would mean
    // the locks could be available - in this case, creating a pool isn't safe
    // to do concurrently, but there's nothing we can do about that.
    this->data->resize_lock = 0;
    this->data->read_lock = 0;
    this->data->write_lock = 0;
    this->data->num_readers = 0;

    this->data->size = this->pool_size;

    // initialize the allocator
    this->data->head = 0;
    this->data->tail = 0;
    this->data->bytes_allocated = 0;
    this->data->bytes_free = this->data->size - sizeof(Data);

    // release all the locks
    this->unlock(&this->data->resize_lock);
    this->unlock(&this->data->read_lock);
    this->unlock(&this->data->write_lock);
  }
}

Pool::~Pool() {
  if (this->data) {
    munmap(this->data, this->pool_size);
  }
  // this->fd is closed automatically because it's a scoped_fd
}


uint64_t Pool::allocate(size_t size) {
  // need to store an AllocatedBlock too
  size_t needed_size = ((size + 7) & (~7)) + sizeof(AllocatedBlock);

  // the list is linked in order of memory address. we can return the first
  // available chunk of `size` bytes that we find; however, to minimize
  // fragmentation, we'll use the smallest available chunk that's bigger than
  // `size`. this implementation is essentially guaranteed to be slow, but this
  // library is designed for fast reads, not fast writes.

  // if there are no allocated blocks, then there's no search to be done
  if (this->data->head == 0 && this->data->tail == 0) {
    size_t free_size = this->data->size - sizeof(Data);
    if (free_size < needed_size) {
      this->expand(needed_size + sizeof(Data));
    }

    // just write the block struct, link it, and return
    AllocatedBlock* b = this->at<AllocatedBlock>(sizeof(Data));
    b->prev = 0;
    b->next = 0;
    b->size = size;
    this->data->head = sizeof(Data);
    this->data->tail = sizeof(Data);
    this->data->bytes_allocated += size;
    this->data->bytes_free -= (b->effective_size() + sizeof(AllocatedBlock));
    return sizeof(Data) + sizeof(AllocatedBlock);
  }

  // keep track of the best block we've found so far. the first candidate block
  // is the space between the header and the first block.
  uint64_t candidate_offset = (uint8_t*)&this->data->arena[0] -
      (uint8_t*)this->data;
  uint64_t candidate_size = this->data->head - candidate_offset;
  uint64_t candidate_link_location = 0;

  // loop over all the blocks, finding the space after each one. stop early if
  // we find a block of exactly the right size.
  uint64_t current_block = this->data->head;
  while (current_block && (needed_size != candidate_size)) {
    AllocatedBlock* b = this->at<AllocatedBlock>(current_block);

    // if !next, this is the last block - have to compute after_size differently
    uint64_t free_block_size;
    if (b->next) {
      free_block_size = b->next - current_block - b->effective_size() -
          sizeof(AllocatedBlock);
    } else {
      free_block_size = this->pool_size - current_block - b->effective_size() -
          sizeof(AllocatedBlock);
    }

    // if this block is big enough, remember it if it's the smallest one so far
    if ((free_block_size >= needed_size) &&
        ((candidate_size < needed_size) || (free_block_size < candidate_size))) {
      candidate_offset = current_block + sizeof(AllocatedBlock) + b->effective_size();
      candidate_size = free_block_size;
      candidate_link_location = current_block;
    }

    // advance to the next block
    current_block = b->next;
  }

  // if we didn't find any usable spaces, we'll have to expand the block and
  // allocate at the end
  if (candidate_size < needed_size) {
    AllocatedBlock* tail = this->at<AllocatedBlock>(this->data->tail);

    size_t new_pool_size = this->data->tail + sizeof(AllocatedBlock) +
        tail->effective_size() + needed_size;
    this->expand(new_pool_size);

    // tail pointer may be invalid after expand
    tail = this->at<AllocatedBlock>(this->data->tail);
    candidate_offset = this->data->tail + sizeof(AllocatedBlock) +
        tail->effective_size();
    candidate_link_location = this->data->tail;
  }

  // create the block and link it. there are 3 cases:
  // 1. link location is 0 - the block is before the head block
  // 2. link location == tail - the block is after the tail block
  // 3. anything else - the block is at neither the head nor tail
  AllocatedBlock* new_block = this->at<AllocatedBlock>(candidate_offset);
  if (candidate_link_location == 0) {
    new_block->prev = 0;
    new_block->next = this->data->head;
    this->at<AllocatedBlock>(new_block->next)->prev = candidate_offset;
    this->data->head = candidate_offset;
  } else if (candidate_link_location == this->data->tail) {
    new_block->prev = this->data->tail;
    new_block->next = 0;
    this->at<AllocatedBlock>(new_block->prev)->next = candidate_offset;
    this->data->tail = candidate_offset;
  } else {
    AllocatedBlock* prev = this->at<AllocatedBlock>(candidate_link_location);
    AllocatedBlock* next = this->at<AllocatedBlock>(prev->next);
    new_block->prev = candidate_link_location;
    new_block->next = prev->next;
    prev->next = candidate_offset;
    next->prev = candidate_offset;
  }
  new_block->size = size;
  this->data->bytes_allocated += size;
  this->data->bytes_free -= (new_block->effective_size() + sizeof(AllocatedBlock));

  // don't spend it all in once place...
  return candidate_offset + sizeof(AllocatedBlock);
}

void Pool::free(uint64_t offset) {
  if ((offset < sizeof(Data)) || (offset > this->pool_size - sizeof(AllocatedBlock))) {
    return; // herp derp
  }

  // we only have to update counts and remove the block from the linked list
  AllocatedBlock* block = this->at<AllocatedBlock>(offset - sizeof(AllocatedBlock));
  this->data->bytes_allocated -= block->size;
  this->data->bytes_free += (block->effective_size() + sizeof(AllocatedBlock));
  if (block->prev) {
    this->at<AllocatedBlock>(block->prev)->next = block->next;
  } else {
    this->data->head = block->next;
  }
  if (block->next) {
    this->at<AllocatedBlock>(block->next)->prev = block->prev;
  } else {
    this->data->tail = block->prev;
  }
}

size_t Pool::block_size(uint64_t offset) const {
  const AllocatedBlock* b = this->at<AllocatedBlock>(offset - sizeof(AllocatedBlock));
  return b->size;
}



void Pool::set_base_object_offset(uint64_t offset) {
  this->data->base_object_offset = offset;
}

uint64_t Pool::base_object_offset() const {
  return this->data->base_object_offset;
}



size_t Pool::size() const {
  return this->data->size;
}

size_t Pool::bytes_allocated() const {
  return this->data->bytes_allocated;
}

size_t Pool::bytes_free() const {
  return this->data->bytes_free;
}



bool Pool::delete_pool(const std::string& name, bool file) {
  int ret = unlink_segment(name.c_str(), file || MAP_HASSEMAPHORE);
  if (ret == 0) {
    return true;
  }
  if (errno == ENOENT) {
    return false;
  }
  throw runtime_error("can\'t delete pool: " + string_for_error(errno));
}



void Pool::check_size_and_remap() const {
  uint64_t new_pool_size = this->data->size.load();
  if (new_pool_size != this->pool_size) {
    munmap(this->data, this->pool_size);

    // remap the pool with the new size
    this->pool_size = new_pool_size;
    this->data = (Data*)mmap(NULL, this->pool_size, PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_HASSEMAPHORE, this->fd, 0);
    if (!this->data) {
      throw runtime_error("mmap failed: " + string_for_error(errno));
    }
  }
}

void Pool::expand(size_t new_size) {
  if (new_size < this->pool_size) {
    return;
  }

  // the new size must be a multiple of the page size, so round it up.
  new_size = (new_size + PAGE_SIZE - 1) & (~(PAGE_SIZE - 1));
  if (this->max_size && (new_size > this->max_size)) {
    throw runtime_error("can\'t expand pool beyond maximum size");
  }

  // we can't use a guard object here because the lock moves in our address
  // space during this procedure. the resize_lock doesn't protect the size
  // variable, its purpose is to make sure nobody sets the size again after we
  // do but before we call ftruncate.
  spinlock(&this->data->resize_lock);
  int ftruncate_ret = ftruncate(this->fd, new_size);
  this->data->bytes_free += (new_size - this->data->size);
  this->data->size = new_size;
  unlock(&this->data->resize_lock);
  if (ftruncate_ret) {
    throw runtime_error("can\'t resize memory map: " + string_for_error(errno));
  }

  // now the underlying shared memory object is larger; we need to recreate our
  // view of it
  this->check_size_and_remap(); // sets this->pool_size
}

Pool::pool_rw_guard::pool_rw_guard(pool_rw_guard&& other) :
    writing(other.writing), pool(other.pool) {
  other.pool = NULL;
}

Pool::pool_rw_guard::pool_rw_guard(const Pool* pool, bool writing) :
    writing(writing), pool(pool) {

  if (this->writing) {
    Pool::spinlock(&this->pool->data->write_lock);

  } else {
    Pool::spinlock(&this->pool->data->read_lock);
    if (++this->pool->data->num_readers == 1) {
      Pool::spinlock(&this->pool->data->write_lock);
    }
    Pool::unlock(&this->pool->data->read_lock);
  }
}

Pool::pool_rw_guard::~pool_rw_guard() {
  if (!this->pool) {
    return;
  }

  if (this->writing) {
    Pool::unlock(&this->pool->data->write_lock);

  } else {
    Pool::spinlock(&this->pool->data->read_lock);
    if (--this->pool->data->num_readers == 0) {
      Pool::unlock(&this->pool->data->write_lock);
    }
    Pool::unlock(&this->pool->data->read_lock);
  }
}


Pool::pool_rw_guard Pool::read_lock() const {
  this->check_size_and_remap();
  pool_rw_guard g(this, false);
  this->check_size_and_remap();
  return g;
}

Pool::pool_rw_guard Pool::write_lock() {
  this->check_size_and_remap();
  pool_rw_guard g(this, true);
  this->check_size_and_remap();
  return g;
}


void Pool::spinlock(std::atomic<uint8_t>* l) {
  uint8_t x;
  do {
    x = 1;
  } while (!l->compare_exchange_weak(x, 0));
}

void Pool::unlock(std::atomic<uint8_t>* l) {
  l->store(1);
}

uint64_t Pool::AllocatedBlock::effective_size() {
  return (this->size + 7) & (~7);
}

} // namespace sharedstructures
