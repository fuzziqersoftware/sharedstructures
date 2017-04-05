#pragma once

#include "Pool.hh"

namespace sharedstructures {


class ProcessLockGuard {
public:
  ProcessLockGuard() = delete;
  ProcessLockGuard(const ProcessLockGuard&) = delete;
  ProcessLockGuard(ProcessLockGuard&&);
  ProcessLockGuard(Pool* pool, uint64_t offset);
  ~ProcessLockGuard();

  bool stolen; // true if the process holding the lock had crashed

private:
  Pool* pool;
  uint64_t offset;
};

} // namespace sharedstructures
