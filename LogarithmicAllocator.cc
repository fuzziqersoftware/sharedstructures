#define _STDC_FORMAT_MACROS

#include "LogarithmicAllocator.hh"

#include <assert.h>
#include <inttypes.h>
#include <stddef.h>

#include <phosg/Strings.hh>

using namespace std;

namespace sharedstructures {


uint64_t LogarithmicAllocator::FreeBlock::prev() const {
  return this->prev_order_allocated & 0x01FFFFFFFFFFFFFF;
}

int8_t LogarithmicAllocator::FreeBlock::order() const {
  return (this->prev_order_allocated >> 57) & 0x3F;
}

bool LogarithmicAllocator::FreeBlock::allocated() const {
  return (this->prev_order_allocated >> 63) & 1;
}

uint64_t LogarithmicAllocator::AllocatedBlock::size() const {
  return this->size_allocated & 0x7FFFFFFFFFFFFFFF;
}

bool LogarithmicAllocator::AllocatedBlock::allocated() const {
  return (this->size_allocated >> 63) & 1;
}


// returns the size of an order (in bytes)
static uint64_t size_for_order(int8_t order) {
  return 1 << order;
}

// returns the smallest order of equal or greater size than the given size
static int8_t order_for_size(uint64_t size) {
  // if the size is an order size, just return that order size
  if ((size & (size - 1)) == 0) {
    return 63 - __builtin_clzll(size);

  // else, it's not an order size - return the next-larger order size
  } else {
    return 64 - __builtin_clzll(size);
  }
}

// returns the largest order of equal or smaller size than the given size
static int8_t largest_order_within_size(uint64_t size) {
  return 63 - __builtin_clzll(size);
}

// returns the largest order that offset can be a boundary of
static int8_t order_alignment_of_size(uint64_t offset) {
  // pick out the least-significant set bit and zero everything else, then
  // return the order for that size
  return order_for_size(((offset ^ (offset - 1)) + 1) >> 1);
}

// returns the first order boundary at or after the given offset
static uint64_t next_order_boundary(uint64_t offset, int8_t order) {
  uint64_t order_mask = size_for_order(order) - 1;
  if (offset & order_mask) {
    return (offset + order_mask) & (~order_mask);
  }
  return offset;
}


LogarithmicAllocator::LogarithmicAllocator(shared_ptr<Pool> pool) :
    Allocator(pool) {
  auto data = this->data();

  if (data->initialized) {
    return;
  }

  auto g = this->lock(true);
  data = this->data(); // may be invalidated by lock()

  if (data->initialized) {
    return;
  }

  uint64_t start_offset = next_order_boundary(sizeof(Data), Data::minimum_order);

  data->initialized = 1;
  data->base_object_offset = 0;
  data->bytes_allocated = 0;
  data->bytes_committed = start_offset;

  for (size_t x = 0; x < 60; x++) {
    data->free_head[x] = 0;
    data->free_tail[x] = 0;
  }

  // set up free blocks starting at the end of the Data struct
  this->create_free_blocks(start_offset, data->size - start_offset);
}


uint64_t LogarithmicAllocator::allocate(size_t size) {
  // make sure we hold the lock for writing
  assert(pool->at<ProcessReadWriteLock>(offsetof(Data, data_lock))
      ->is_locked(true));

  // need to store an AllocatedBlock too, and size must be a multiple of 8. this
  // means needed_size is >= 0x10.
  int8_t needed_order = order_for_size(size + sizeof(AllocatedBlock));
  if (needed_order < 0) {
    throw invalid_argument("size too small");
  }

  // check higher orders until we find one that has available space
  auto data = this->data();
  int8_t split_order = needed_order;
  for (; !data->free_head[split_order - Data::minimum_order] &&
         split_order <= Data::maximum_order; split_order++);

  // if there's available space in a possibly-higher order, allocate from it
  if (split_order < Data::maximum_order) {
    // there's a free block large enough to accommodate the request, but we
    // might need to split it until we get the size we want
    for (; split_order > needed_order; split_order--) {
      int8_t new_order = split_order - 1;
      uint64_t head_block_offset = data->free_head[
          split_order - Data::minimum_order];
      uint64_t new_block_offset = head_block_offset + size_for_order(new_order);

      // remove the original block from the higher order and add the two new
      // blocks to the lower order
      this->unlink_block(head_block_offset);
      this->create_free_block(head_block_offset, new_order);
      this->create_free_block(new_block_offset, new_order);
    }

    // now there's an available block in the needed order; set it up & return it
    uint64_t block_offset = data->free_head[needed_order - Data::minimum_order];
    Block* block = this->pool->at<Block>(block_offset);
    data->free_head[needed_order - Data::minimum_order] = block->free.next;
    if (block->free.next) {
      this->pool->at<FreeBlock>(block->free.next)->prev_order_allocated =
          ((uint64_t)needed_order) << 57;
    } else {
      data->free_tail[needed_order - Data::minimum_order] = 0;
    }

    // update counts
    data->bytes_allocated += size;
    data->bytes_committed += size_for_order(needed_order);

    // return the new block
    block->allocated.size_allocated = size | (1ULL << 63);
    return block_offset + sizeof(AllocatedBlock);
  }

  // if we get here, then we need to expand the pool - there are no blocks large
  // enough to satisfy the request.
  uint64_t original_size = this->pool->size();
  uint64_t order_size = size_for_order(needed_order);

  // the pool size may not be a multiple of the order size, so there may be a
  // block at the end that could be part of a newly-allocated block if we
  // expanded the pool. however, it can't be part of a higher-order block - only
  // lower-order blocks can be allocated within this space. so we start looking
  // at the next-lowest block order to see if anything is allocated.
  //
  // this logic is best illustrated by example. imagine the following memory
  // block layout (this is one chunk of memory, with all possible blocks of
  // all possible orders annotated, so a can be split into c and d, etc.):
  // ---------------------------------------------- ---------------------
  // aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbb bbbbbbbbbbbbbbbbbb...
  // ccccccccccccccccddddddddddddddddeeeeeeeeeeeeee eeffffffffffffffff...
  // gggggggghhhhhhhhiiiiiiiijjjjjjjjkkkkkkkkllllll llmmmmmmmmnnnnnnnn...
  // ooooppppqqqqrrrrssssttttuuuuvvvvwwwwxxxxyyyyzz zzAAAABBBBCCCCDDDD...
  // EEFFGGHHIIJJKKLLMMNNOOPPQQRRSSTTUUVVWWXXYYZZ11 223344556677889900...
  //
  // imagine the end of the pool is at the break between blocks 1 and 2, and
  // we're trying to allocate a block of size e. if blocks k, y, and 1 are
  // free, then we can do it by just extending the pool to the end of e and
  // returning e (importantly, b and e cannot already be allocated because they
  // don't fit within the pool as it stands now). but if any of k, w, x, y, U,
  // V, W, X, Y, Z, or 1 are allocated, then we can't do this and need to extend
  // the pool all the way to the end of f and return f instead. (note that l and
  // z cannot be allocated for the same reason that e and b can't be.)
  //
  // checking if any of these blocks are allocated is easier than it sounds:
  // - if k's order doesn't match the expected order of k, then k is split,
  //   and one of its child blocks must be allocated. this catches the case
  //   where w, x, U, V, W, or X are allocated.
  // - if k has the allocated flag set, then it's allocated (duh). this
  //   catches the case where k, w, or U are allocated.
  // - if neither of the above are true, then k is entirely free, and we can
  //   repeat the process to check if y (and Y and Z) and 1 are free in the
  //   same manner.
  uint64_t block_offset = original_size & (~(order_size - 1));
  int8_t block_order = largest_order_within_size(original_size - block_offset);
  while ((block_order >= Data::minimum_order) &&
         (block_offset < original_size)) {
    FreeBlock* block = this->pool->at<FreeBlock>(block_offset);
    if (block->allocated()) {
      break; // have to expand beyond this order
    }
    if (block->order() != block_order) {
      break; // this block isn't allocated, but a neighbor is
    }

    // if we get here, then the block we're looking at is free and is the
    // correct order - we need to check the next-lowest block order. (this is
    // how we "move" from checking k to checking y in the example above.)
    block_offset += size_for_order(block_order);
    block_order--;
  }

  // if all of the examined blocks are free, then we can allocate the new
  // block in their place, and expand the pool by only a little. the example
  // pool will then look like this, and we'll return e:
  // ------------------------------------------------ -------------------
  // aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbb bbbbbbbbbbbbbbbb...
  // ccccccccccccccccddddddddddddddddeeeeeeeeeeeeeeee ffffffffffffffff...
  // gggggggghhhhhhhhiiiiiiiijjjjjjjjkkkkkkkkllllllll mmmmmmmmnnnnnnnn...
  // ooooppppqqqqrrrrssssttttuuuuvvvvwwwwxxxxyyyyzzzz AAAABBBBCCCCDDDD...
  // EEFFGGHHIIJJKKLLMMNNOOPPQQRRSSTTUUVVWWXXYYZZ1122 3344556677889900...
  uint64_t allocated_block_offset;
  if (block_offset >= original_size) {
    allocated_block_offset = original_size & (~(order_size - 1));
  } else {
    allocated_block_offset = (original_size & (~(order_size - 1))) +
        size_for_order(needed_order);
  }

  // expand the pool before making any changes (this is important because
  // expansion can fail)
  this->pool->expand(allocated_block_offset + order_size);
  data = this->data();

  // if we're merging a bunch of blocks and allocating, unlink any blocks that
  // are in the area we want to allocate
  if (block_offset >= original_size) {
    uint64_t deleted_block_offset = allocated_block_offset;
    int8_t deleted_block_order = largest_order_within_size(
        original_size - allocated_block_offset);
    while (deleted_block_offset < original_size) {
      this->unlink_block(deleted_block_offset);
      deleted_block_offset += size_for_order(block_order);
      deleted_block_order--;
    }
  }

  // fill in the allocated block header. this has to be done before merging so
  // we don't try to merge it also
  AllocatedBlock* block = this->pool->at<AllocatedBlock>(
      allocated_block_offset);
  block->size_allocated = size | (1ULL << 63);

  // for an incomplete expansion, we reused some existing space at the end of
  // the pool, so there's nothing to merge. for full expansions, fill in the
  // unused space with free blocks and merge them if needed. for both types of
  // expansions, there may be new unused space after the allocated block, so
  // create free blocks in it. note that we don't have to merge anything in this
  // space since create_free_blocks already creates optimally-sized blocks for
  // the space it's given, and these blocks can't be merged at either end (the
  // low end is adjacent to the newly-allocated block, and the high end is at
  // the end of the pool).
  if (allocated_block_offset > original_size) {
    this->create_free_blocks(original_size,
        allocated_block_offset - original_size);
    this->merge_blocks_at(original_size);
  }
  uint64_t allocated_block_end = allocated_block_offset + order_size;
  this->create_free_blocks(allocated_block_end,
      data->size - allocated_block_end);

  // update counts and we're done
  data->bytes_allocated += size;
  data->bytes_committed += size_for_order(needed_order);
  return allocated_block_offset + sizeof(AllocatedBlock);
}

void LogarithmicAllocator::create_free_block(uint64_t offset, int8_t order) {
  atomic<uint64_t>* tail = &this->data()->free_tail[
      order - Data::minimum_order];

  // fill in the block struct
  FreeBlock* block = this->pool->at<FreeBlock>(offset);
  block->prev_order_allocated = ((uint64_t)order << 57) | *tail;
  block->next = 0;

  // link it appropriately
  if (*tail) {
    FreeBlock* prev_block = this->pool->at<FreeBlock>(*tail);
    assert(prev_block->order() == order);
    prev_block->next = offset;
    *tail = offset;
  } else {
    this->data()->free_head[order - Data::minimum_order] = offset;
    *tail = offset;
  }
}

void LogarithmicAllocator::create_free_blocks(uint64_t offset,
    uint64_t size) {
  uint64_t end_offset = offset + size;

  // create larger and larger blocks until we can't fit any more
  while (offset < end_offset) {
    int8_t order = order_alignment_of_size(offset);
    uint64_t order_size = size_for_order(order);
    if ((offset + order_size) > end_offset) {
      break;
    }

    this->create_free_block(offset, order);
    offset += order_size;
  }

  // create smaller and smaller blocks until we reach end_offset
  while (offset < end_offset) {
    int8_t order = largest_order_within_size(end_offset - offset);
    this->create_free_block(offset, order);
    offset += size_for_order(order);
  }
}

void LogarithmicAllocator::free(uint64_t offset) {
  // make sure we hold the lock for writing
  assert(pool->at<ProcessReadWriteLock>(offsetof(Data, data_lock))
      ->is_locked(true));

  auto data = this->data();
  if ((offset < sizeof(Data) + sizeof(AllocatedBlock)) ||
      (offset > data->size)) {
    return; // herp derp
  }

  uint64_t block_offset = offset - sizeof(AllocatedBlock);
  AllocatedBlock* allocated_block =
      this->pool->at<AllocatedBlock>(block_offset);
  if (!allocated_block->allocated()) {
    return;
  }

  // update counts
  uint64_t allocated_size = allocated_block->size();
  int8_t block_order = order_for_size(allocated_size + sizeof(AllocatedBlock));
  uint64_t block_size = size_for_order(block_order);

  data->bytes_allocated -= allocated_size;
  data->bytes_committed -= block_size;

  // return this block to the appropriate free list and merge if needed
  this->create_free_block(block_offset, block_order);
  this->merge_blocks_at(block_offset);
}

uint64_t LogarithmicAllocator::merge_blocks_at(uint64_t block_offset) {
  FreeBlock* block = this->pool->at<FreeBlock>(block_offset);
  int8_t block_order = block->order();

  // first, unlink the target block from its list
  this->unlink_block(block_offset);

  // now merge adjacent free blocks into each other until we can't anymore
  uint64_t min_offset = next_order_boundary(sizeof(Data), Data::minimum_order);
  auto data = this->data();
  for (;;) {
    uint64_t order_size = size_for_order(block_order);
    uint64_t other_block_offset = block_offset ^ order_size;
    if (other_block_offset < min_offset) {
      break; // can't merge the zero block
    }
    if (other_block_offset + order_size > data->size) {
      break; // other "block" extends beyond the pool boundary; can't merge
    }
    FreeBlock* other_block = this->pool->at<FreeBlock>(other_block_offset);
    if (other_block->allocated()) {
      break; // other block is allocated; can't merge
    }
    if (other_block->order() != block_order) {
      break; // other block is split (and partially-allocated); can't merge
    }

    // if we get here, then we can merge this block with the other. first remove
    // the other from its list
    this->unlink_block(other_block_offset);

    // the merged block's base offset is now the minimum of the two offsets
    if (other_block_offset < block_offset) {
      block_offset = other_block_offset;
      block = other_block;
    }

    // continue merging at a higher order
    block_order++;
  }

  // we're done merging; add this block to the list for its order
  this->create_free_block(block_offset, block_order);

  // return the offset following the last merged block (repair() needs this)
  return block_offset + order_for_size(block_order);
}

void LogarithmicAllocator::unlink_block(uint64_t block_offset) {
  FreeBlock* block = this->pool->at<FreeBlock>(block_offset);
  int8_t order = block->order();

  if (block->next) {
    this->pool->at<FreeBlock>(block->next)->prev_order_allocated =
        ((uint64_t)order << 57) | block->prev();
  } else {
    this->data()->free_tail[order - Data::minimum_order] = block->prev();
  }
  if (block->prev()) {
    this->pool->at<FreeBlock>(block->prev())->next = block->next;
  } else {
    this->data()->free_head[order - Data::minimum_order] = block->next;
  }
}


size_t LogarithmicAllocator::block_size(uint64_t offset) const {
  const AllocatedBlock* b = this->pool->at<AllocatedBlock>(
      offset - sizeof(AllocatedBlock));
  return b->size();
}


void LogarithmicAllocator::set_base_object_offset(uint64_t offset) {
  this->data()->base_object_offset = offset;
}

uint64_t LogarithmicAllocator::base_object_offset() const {
  return this->data()->base_object_offset;
}


size_t LogarithmicAllocator::bytes_allocated() const {
  return this->data()->bytes_allocated;
}

size_t LogarithmicAllocator::bytes_free() const {
  auto data = this->data();
  return data->size - data->bytes_committed;
}


ProcessReadWriteLockGuard LogarithmicAllocator::lock(bool writing) const {
  this->pool->check_size_and_remap();

  ProcessReadWriteLockGuard::Behavior behavior = writing ?
      ProcessReadWriteLockGuard::Behavior::Write :
      ProcessReadWriteLockGuard::Behavior::ReadUnlessStolen;
  ProcessReadWriteLockGuard g(const_cast<Pool*>(this->pool.get()),
      offsetof(Data, data_lock), behavior);

  this->pool->check_size_and_remap();
  if (g.stolen) {
    // if the lock was stolen, then we are holding it for writing and can call
    // repair(). but we may need to downgrade to a read lock afterward
    const_cast<LogarithmicAllocator*>(this)->repair();
    if (!writing) {
      g.downgrade();
    }
  }
  return g;
}

bool LogarithmicAllocator::is_locked(bool writing) const {
  return this->pool->at<ProcessReadWriteLock>(offsetof(Data, data_lock))->is_locked(writing);
}


LogarithmicAllocator::Data* LogarithmicAllocator::data() {
  return this->pool->at<Data>(0);
}

const LogarithmicAllocator::Data* LogarithmicAllocator::data() const {
  return this->pool->at<Data>(0);
}


void LogarithmicAllocator::verify() const {
  auto lock = this->lock(false);
  auto data = this->data();

  // check all blocks
  uint64_t bytes_allocated = 0;
  uint64_t bytes_committed = next_order_boundary(sizeof(Data), Data::minimum_order);
  uint64_t offset = next_order_boundary(sizeof(Data), Data::minimum_order);
  while (offset < data->size) {
    const Block* block = this->pool->at<Block>(offset);

    uint64_t next_offset;
    if (block->allocated.allocated()) {
      size_t committed_bytes = size_for_order(order_for_size(
          block->allocated.size() + sizeof(AllocatedBlock)));
      bytes_allocated += block->allocated.size();
      bytes_committed += committed_bytes;

      next_offset = offset + committed_bytes;

    } else { // free block
      const FreeBlock* fb = reinterpret_cast<const FreeBlock*>(block);
      if ((fb->order() < data->minimum_order) || (fb->order() > data->maximum_order)) {
        throw runtime_error(string_printf(
            "free block at %llX has incorrect order (%hhd not in range [%hhd,%hhd])",
            offset, fb->order(), data->minimum_order, data->maximum_order));
      }
      if (next_order_boundary(fb->prev(), fb->order()) != fb->prev()) {
        throw runtime_error(string_printf(
            "free block at %llX has misaligned prev link (%llX)", offset, fb->prev()));
      }
      if (next_order_boundary(fb->next, fb->order()) != fb->next) {
        throw runtime_error(string_printf(
            "free block at %llX has misaligned next link (%llX)", offset, fb->next));
      }

      next_offset = offset + size_for_order(fb->order());
    }

    if (next_offset <= offset) {
      throw runtime_error(string_printf(
          "%s block at %llX has incorrect size (next block at %llX)",
          block->allocated.allocated() ? "allocated" : "free", offset, next_offset));
    }
    offset = next_offset;
  }

  // check allocated/committed bytes
  if (data->bytes_allocated != bytes_allocated) {
    throw runtime_error(string_printf(
        "allocated byte count is incorrect (is %llX, should be %llX)",
        data->bytes_allocated.load(), bytes_allocated));
  }
  if (data->bytes_committed != bytes_committed) {
    throw runtime_error(string_printf(
        "committed byte count is incorrect (is %llX, should be %llX)",
        data->bytes_committed.load(), bytes_committed));
  }

  // check the free lists
  for (int8_t order = Data::minimum_order; order < Data::maximum_order;
       order++) {
    uint64_t offset = data->free_head[order - Data::minimum_order];
    uint64_t prev_offset = 0;
    while (offset) {
      FreeBlock* block = this->pool->at<FreeBlock>(offset);
      if (block->order() != order) {
        throw runtime_error(string_printf(
            "block at %llX has incorrect order (is %hhd, should be %hhd)",
            offset, block->order(), order));
      }
      if (block->allocated()) {
        throw runtime_error(string_printf(
            "block at %llX is linked and allocated", offset));
      }
      if (block->prev() != prev_offset) {
        throw runtime_error(string_printf(
            "block at %llX has incorrect prev link (is %llX, should be %llX)",
            offset, block->prev(), prev_offset));
      }
      prev_offset = offset;
      offset = block->next;
    }
    if (data->free_tail[order - Data::minimum_order] != prev_offset) {
      throw runtime_error(string_printf(
          "free list %hhd has incorrect tail link (is %llX, should be %llX)",
          order, data->free_tail[order - Data::minimum_order].load(),
          prev_offset));
    }
  }
}

void LogarithmicAllocator::repair() {

  // to rebuild the pool, we walk the entire space and rebuild the linked lists,
  // ignoring whatever might already be there
  uint64_t offset = next_order_boundary(sizeof(Data), Data::minimum_order);

  // clear all the lists
  auto* data = this->data();
  for (int8_t x = 0; x < Data::maximum_order - Data::minimum_order; x++) {
    data->free_head[x] = 0;
    data->free_tail[x] = 0;
  }

  // in the first pass, we make the linked list structure consistent again and
  // count allocated and committed bytes
  uint64_t bytes_allocated = 0, bytes_committed = offset;
  while (offset < data->size) {
    Block* block = this->pool->at<Block>(offset);

    // if it's allocated, it shouldn't be added to a list - just skip it
    int8_t order;
    if (block->allocated.allocated()) {
      order = order_for_size(block->allocated.size() + sizeof(AllocatedBlock));
      bytes_allocated += block->allocated.size();
      bytes_committed += size_for_order(order);

    // if it's not allocated, add it to the appropriate free list
    } else {
      order = block->free.order();
      this->create_free_block(offset, order);
    }

    // go to the next block
    offset += size_for_order(order);
  }

  data->bytes_allocated = bytes_allocated;
  data->bytes_committed = bytes_committed;

  // in the second pass, we merge any blocks that need merging (this can't be
  // done if the lists are inconsistent)
  while (offset < data->size) {
    Block* block = this->pool->at<Block>(offset);

    // if it's allocated, it can't be merged - just skip it
    int8_t order;
    if (block->allocated.allocated()) {
      order = order_for_size(block->allocated.size() + sizeof(AllocatedBlock));
      offset += size_for_order(order);

    // if it's not allocated, try to merge it
    } else {
      offset = this->merge_blocks_at(offset);
    }
  }
}


void LogarithmicAllocator::print(FILE* stream) const {
  auto data = this->data();

  fprintf(stream, "LogarithmicAllocator: size=%" PRIX64 " init=%" PRIu8
      " base=%" PRIX64 " alloc=%" PRIX64 " commit=%" PRIX64 "\n",
      data->size.load(), data->initialized.load(),
      data->base_object_offset.load(), data->bytes_allocated.load(),
      data->bytes_committed.load());
  for (int x = 0; x < Data::maximum_order - Data::minimum_order; x++) {
    uint64_t head = data->free_head[x];
    uint64_t tail = data->free_tail[x];
    if (!head && !tail) {
      continue;
    }
    fprintf(stream, "  Order %d: head=%" PRIX64 " tail=%" PRIX64 "\n",
        x + Data::minimum_order, head, tail);
  }

  uint64_t offset = next_order_boundary(sizeof(Data), Data::minimum_order);
  while (offset < data->size) {
    Block* block = this->pool->at<Block>(offset);
    if (block->allocated.allocated()) {
      fprintf(stream, "  Block-A %" PRIX64 ": size=%" PRIX64 "\n", offset,
          block->allocated.size());
      offset += size_for_order(order_for_size(
          block->allocated.size() + sizeof(AllocatedBlock)));
    } else {
      uint64_t block_size = size_for_order(block->free.order());
      fprintf(stream, "  Block-F %" PRIX64 ": prev=%" PRIX64 " next=%" PRIX64
          " order=%" PRIu8 " size=%" PRIX64 "\n",
          offset, block->free.prev(), block->free.next, block->free.order(),
          block_size);
      offset += block_size;
    }
  }
}


const int8_t LogarithmicAllocator::Data::minimum_order = 4;
const int8_t LogarithmicAllocator::Data::maximum_order = 57;

} // namespace sharedstructures
