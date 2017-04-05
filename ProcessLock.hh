#pragma once

#include "Pool.hh"

// this must be an odd number so that the alignment will make sense below
#define NUM_READER_SLOTS 65

namespace sharedstructures {


struct ProcessLock {
  std::atomic<int32_t> lock;
  int32_t __force_alignment__;

  bool is_locked() const;
};

struct ProcessReadWriteLock {
  std::atomic<int32_t> write_lock;
  std::atomic<int32_t> reader_tokens[NUM_READER_SLOTS];

  bool is_locked(bool writing) const;
};


class ProcessLockGuard {
public:
  ProcessLockGuard() = delete;
  ProcessLockGuard(const ProcessLockGuard&) = delete;
  ProcessLockGuard(ProcessLockGuard&&);
  ProcessLockGuard(Pool* pool, uint64_t offset);
  ~ProcessLockGuard();

  static size_t data_size();

  bool stolen; // true if the process holding the lock had crashed

private:
  Pool* pool;
  uint64_t offset;
};


class ProcessReadWriteLockGuard {
public:
  ProcessReadWriteLockGuard() = delete;
  ProcessReadWriteLockGuard(const ProcessReadWriteLockGuard&) = delete;
  ProcessReadWriteLockGuard(ProcessReadWriteLockGuard&&);
  ProcessReadWriteLockGuard(Pool* pool, uint64_t offset, bool writing);
  ~ProcessReadWriteLockGuard();

  static size_t data_size();

  bool stolen;

private:
  Pool* pool;
  uint64_t offset;
  int32_t reader_slot; // -1 if writing
};

} // namespace sharedstructures
