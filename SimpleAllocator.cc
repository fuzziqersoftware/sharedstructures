#include "SimpleAllocator.hh"

#include <inttypes.h>
#include <stddef.h>

using namespace std;

namespace sharedstructures {


SimpleAllocator::SimpleAllocator(std::shared_ptr<Pool> pool) : Allocator(pool) {
  auto data = this->data();

  if (data->initialized) {
    return;
  }

  auto g = this->lock(true);
  data = this->data(); // may be invalidated by lock()

  if (data->initialized) {
    return;
  }

  data->initialized = 1;
  data->base_object_offset = 0;
  data->head = 0;
  data->tail = 0;
  data->bytes_allocated = 0;
  data->bytes_committed = sizeof(Data);
}


uint64_t SimpleAllocator::allocate(size_t size) {
  auto data = this->data();

  // need to store an AllocatedBlock too
  size_t needed_size = ((size + 7) & (~7)) + sizeof(AllocatedBlock);

  // the list is linked in order of memory address. we can return the first
  // available chunk of `size` bytes that we find; however, to minimize
  // fragmentation, we'll use the smallest available chunk that's bigger than
  // `size`. this implementation is essentially guaranteed to be slow, but this
  // library is designed for fast reads, not fast writes.

  // if there are no allocated blocks, then there's no search to be done
  if (data->head == 0 && data->tail == 0) {
    size_t free_size = data->size - sizeof(Data);
    if (free_size < needed_size) {
      this->pool->expand(needed_size + sizeof(Data));
      data = this->data();
    }

    // just write the block struct, link it, and return
    AllocatedBlock* b = this->pool->at<AllocatedBlock>(sizeof(Data));
    b->prev = 0;
    b->next = 0;
    b->size = size;
    data->head = sizeof(Data);
    data->tail = sizeof(Data);
    data->bytes_allocated += size;
    data->bytes_committed += (b->effective_size() + sizeof(AllocatedBlock));
    return sizeof(Data) + sizeof(AllocatedBlock);
  }

  // keep track of the best block we've found so far. the first candidate block
  // is the space between the header and the first block.
  uint64_t candidate_offset = (uint8_t*)&data->arena[0] - (uint8_t*)data;
  uint64_t candidate_size = data->head - candidate_offset;
  uint64_t candidate_link_location = 0;

  // loop over all the blocks, finding the space after each one. stop early if
  // we find a block of exactly the right size.
  uint64_t current_block = data->head;
  while (current_block && (needed_size != candidate_size)) {
    AllocatedBlock* b = this->pool->at<AllocatedBlock>(current_block);

    // if !next, this is the last block - have to compute after_size differently
    uint64_t free_block_size;
    if (b->next) {
      free_block_size = b->next - current_block - b->effective_size() -
          sizeof(AllocatedBlock);
    } else {
      free_block_size = this->pool->size() - current_block -
          b->effective_size() - sizeof(AllocatedBlock);
    }

    // if this block is big enough, remember it if it's the smallest one so far
    if ((free_block_size >= needed_size) &&
        ((candidate_size < needed_size) ||
         (free_block_size < candidate_size))) {
      candidate_offset = current_block + sizeof(AllocatedBlock) +
          b->effective_size();
      candidate_size = free_block_size;
      candidate_link_location = current_block;
    }

    // advance to the next block
    current_block = b->next;
  }

  // if we didn't find any usable spaces, we'll have to expand the block and
  // allocate at the end
  if (candidate_size < needed_size) {
    AllocatedBlock* tail = this->pool->at<AllocatedBlock>(data->tail);

    size_t new_pool_size = data->tail + sizeof(AllocatedBlock) +
        tail->effective_size() + needed_size;
    this->pool->expand(new_pool_size);
    data = this->data();

    // tail pointer may be invalid after expand
    tail = this->pool->at<AllocatedBlock>(data->tail);
    candidate_offset = data->tail + sizeof(AllocatedBlock) +
        tail->effective_size();
    candidate_link_location = data->tail;
  }

  // create the block and link it. there are 3 cases:
  // 1. link location is 0 - the block is before the head block
  // 2. link location == tail - the block is after the tail block
  // 3. anything else - the block is at neither the head nor tail
  // we always set next before prev and fill in new_block before changing
  // existing pointers because the pool is repaired (after a crash) by walking
  // from the head along the next pointers, so those should always be consistent
  AllocatedBlock* new_block = this->pool->at<AllocatedBlock>(candidate_offset);
  new_block->size = size;
  if (candidate_link_location == 0) {
    new_block->next = data->head;
    new_block->prev = 0;
    data->head = candidate_offset;
    this->pool->at<AllocatedBlock>(new_block->next)->prev = candidate_offset;
  } else if (candidate_link_location == data->tail) {
    new_block->next = 0;
    new_block->prev = data->tail;
    this->pool->at<AllocatedBlock>(new_block->prev)->next = candidate_offset;
    data->tail = candidate_offset;
  } else {
    AllocatedBlock* prev = this->pool->at<AllocatedBlock>(
        candidate_link_location);
    AllocatedBlock* next = this->pool->at<AllocatedBlock>(prev->next);
    new_block->next = prev->next;
    new_block->prev = candidate_link_location;
    prev->next = candidate_offset;
    next->prev = candidate_offset;
  }
  data->bytes_allocated += size;
  data->bytes_committed += new_block->effective_size() + sizeof(AllocatedBlock);

  // don't spend it all in once place...
  return candidate_offset + sizeof(AllocatedBlock);
}

void SimpleAllocator::free(uint64_t offset) {
  if ((offset < sizeof(Data)) ||
      (offset > this->pool->size() - sizeof(AllocatedBlock))) {
    return; // herp derp
  }

  auto data = this->data();

  // we only have to update counts and remove the block from the linked list
  AllocatedBlock* block = this->pool->at<AllocatedBlock>(
      offset - sizeof(AllocatedBlock));
  data->bytes_allocated -= block->size;
  data->bytes_committed -= (block->effective_size() + sizeof(AllocatedBlock));
  if (block->prev) {
    this->pool->at<AllocatedBlock>(block->prev)->next = block->next;
  } else {
    data->head = block->next;
  }
  if (block->next) {
    this->pool->at<AllocatedBlock>(block->next)->prev = block->prev;
  } else {
    data->tail = block->prev;
  }
}

size_t SimpleAllocator::block_size(uint64_t offset) const {
  const AllocatedBlock* b = this->pool->at<AllocatedBlock>(
      offset - sizeof(AllocatedBlock));
  return b->size;
}


void SimpleAllocator::set_base_object_offset(uint64_t offset) {
  this->data()->base_object_offset = offset;
}

uint64_t SimpleAllocator::base_object_offset() const {
  return this->data()->base_object_offset;
}


size_t SimpleAllocator::bytes_allocated() const {
  return this->data()->bytes_allocated;
}

size_t SimpleAllocator::bytes_free() const {
  auto data = this->data();
  return data->size - data->bytes_committed;
}


ProcessReadWriteLockGuard SimpleAllocator::lock(bool writing) const {
  this->pool->check_size_and_remap();

  ProcessReadWriteLockGuard::Behavior behavior = writing ?
      ProcessReadWriteLockGuard::Behavior::Write :
      ProcessReadWriteLockGuard::Behavior::ReadUnlessStolen;
  ProcessReadWriteLockGuard g(const_cast<Pool*>(this->pool.get()),
      offsetof(Data, data_lock), behavior);

  this->pool->check_size_and_remap();
  if (g.stolen_token()) {
    // if the lock was stolen, then we are holding it for writing and can call
    // repair(). but we may need to downgrade to a read lock afterward
    const_cast<SimpleAllocator*>(this)->repair();
    if (!writing) {
      g.downgrade();
    }
  }
  return g;
}

bool SimpleAllocator::is_locked(bool writing) const {
  return this->pool->at<ProcessReadWriteLock>(offsetof(Data, data_lock))->is_locked(writing);
}


void SimpleAllocator::verify() const {
  // TODO
}


SimpleAllocator::Data* SimpleAllocator::data() {
  return this->pool->at<Data>(0);
}

const SimpleAllocator::Data* SimpleAllocator::data() const {
  return this->pool->at<Data>(0);
}


void SimpleAllocator::repair() {
  auto data = this->data();

  uint64_t bytes_allocated = 0;
  uint64_t bytes_committed = sizeof(Data);

  uint64_t prev_offset = 0;
  uint64_t block_offset = data->head;
  while (block_offset) {

    AllocatedBlock* block = this->pool->at<AllocatedBlock>(block_offset);
    block->prev = prev_offset;

    bytes_allocated += block->size;
    bytes_committed += (block->effective_size() + sizeof(AllocatedBlock));

    prev_offset = block_offset;
    block_offset = block->next;
  }

  data->tail = prev_offset;
  data->bytes_allocated = bytes_allocated;
  data->bytes_committed = bytes_committed;
}


uint64_t SimpleAllocator::AllocatedBlock::effective_size() {
  return (this->size + 7) & (~7);
}

} // namespace sharedstructures
