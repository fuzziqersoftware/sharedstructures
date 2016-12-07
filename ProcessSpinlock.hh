#pragma once

#include "Pool.hh"

namespace sharedstructures {


class ProcessSpinlockGuard {
public:
  ProcessSpinlockGuard() = delete;
  ProcessSpinlockGuard(const ProcessSpinlockGuard&) = delete;
  ProcessSpinlockGuard(ProcessSpinlockGuard&&);
  ProcessSpinlockGuard(Pool* pool, uint64_t offset);
  ~ProcessSpinlockGuard();

  bool stolen; // true if the process holding the lock had crashed

private:
  Pool* pool;
  uint64_t offset;
};

} // namespace sharedstructures
