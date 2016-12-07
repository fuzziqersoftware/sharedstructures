#include "ProcessSpinlock.hh"

#include <phosg/Process.hh>

using namespace std;

namespace sharedstructures {


ProcessSpinlockGuard::ProcessSpinlockGuard(ProcessSpinlockGuard&& other) :
    stolen(other.stolen), pool(other.pool), offset(other.offset) {
  other.pool = NULL;
}

ProcessSpinlockGuard::ProcessSpinlockGuard(Pool* pool, uint64_t offset) :
    stolen(false), pool(pool), offset(offset) {
  atomic<uint64_t>* lock = this->pool->at<atomic<uint64_t>>(offset);

  // heuristic: every 100 spins, we check if the process holding the lock is
  // still running; if it's not, we steal the lock (the process likely crashed)
  uint64_t desired_value = (this_process_start_time() << 20) | getpid_cached();
  uint64_t expected_value;
  bool lock_taken = false;
  while (!lock_taken) {

    // try 100 times to get the lock
    uint8_t spin_count = 0;
    while (!lock_taken && (spin_count < 100)) {
      expected_value = 0;
      lock_taken = lock->compare_exchange_weak(expected_value, desired_value);
      spin_count++;
    }

    // if we didn't get the lock, check if the process holding it is running
    if (!lock_taken) {
      pid_t pid = expected_value & ((1 << 20) - 1);
      uint64_t start_time_token = expected_value & ~((1 << 20) - 1);
      if ((start_time_for_pid(pid) << 20) != start_time_token) {
        // the holding process died; steal the lock from it. if we get the lock,
        // repair the allocator structures since they could be in an
        // inconsistent state. if we don't get the lock, then another process
        // got there first and we'll just keep waiting
        lock_taken = lock->compare_exchange_strong(expected_value, desired_value);
        if (lock_taken) {
          this->stolen = true;
        }
      }
    }
  }
}

ProcessSpinlockGuard::~ProcessSpinlockGuard() {
  atomic<uint64_t>* lock = this->pool->at<atomic<uint64_t>>(offset);
  lock->store(0);
}

} // namespace sharedstructures
