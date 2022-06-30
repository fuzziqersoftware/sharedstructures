#pragma once

#include "Pool.hh"

// This must be an odd number so that the alignment will make sense below
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
  size_t reader_count() const;
};


class ProcessLockGuard {
public:
  ProcessLockGuard() = delete;
  ProcessLockGuard(const ProcessLockGuard&) = delete;
  ProcessLockGuard(ProcessLockGuard&&);
  ProcessLockGuard(Pool* pool, uint64_t offset);
  ~ProcessLockGuard();

  static size_t data_size();

  int32_t token() const;
  int32_t stolen_token() const;

private:
  Pool* pool;
  uint64_t offset;
  int32_t lock_token;
  int32_t stolen_lock_token;
};


class ProcessReadWriteLockGuard {
public:
  enum class Behavior {
    Read = 0,
    Write,

    // If stolen, the returned lock is held for writing instead. The caller must
    // not forget to check for this case!
    ReadUnlessStolen,
  };

  ProcessReadWriteLockGuard() = delete;
  ProcessReadWriteLockGuard(const ProcessReadWriteLockGuard&) = delete;
  ProcessReadWriteLockGuard(ProcessReadWriteLockGuard&&);
  ProcessReadWriteLockGuard(Pool* pool, uint64_t offset, Behavior behavior);
  ~ProcessReadWriteLockGuard();

  static size_t data_size();

  void downgrade();
  int32_t token() const;
  int32_t stolen_token() const;

private:
  Pool* pool;
  uint64_t offset;
  int32_t reader_slot; // -1 if writing
  int32_t lock_token;
  int32_t stolen_lock_token;
};

} // namespace sharedstructures
