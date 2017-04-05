#include "ProcessLock.hh"

#ifdef LINUX
#include <linux/futex.h>
#include <sys/syscall.h>
#else
#include <sched.h>
#endif

#include <stdexcept>

#include <phosg/Process.hh>
#include <phosg/Strings.hh>

using namespace std;

namespace sharedstructures {

static const uint8_t PID_BITS = 18;
static const uint8_t START_TIME_BITS = 32 - PID_BITS;
static const uint8_t SPIN_LIMIT = 10;

static int32_t this_process_token() {
  return (this_process_start_time() << PID_BITS) | getpid_cached();
}

static int32_t start_time_for_token(int32_t token) {
  return (token & ~((1 << PID_BITS) - 1)) >> PID_BITS;
}

static pid_t pid_for_token(int32_t token) {
  return token & ((1 << PID_BITS) - 1);
}

static int32_t mask_start_time(int32_t start_time) {
  return start_time & ((1 << START_TIME_BITS) - 1);
}


#ifdef LINUX

static bool futex_wait(atomic<int>* lock, int expected_value,
    const struct timespec* timeout) {
  if (syscall(SYS_futex, lock, FUTEX_WAIT, expected_value, timeout, NULL, 0) == -1) {
    if ((errno != EAGAIN) && (errno != EINTR) && (errno != ETIMEDOUT) && (errno != EWOULDBLOCK)) {
      throw runtime_error("futex_wait failed: " + string_for_error(errno));
    }
    return false;
  }
  return true;
}

static void futex_wake(atomic<int>* lock, int num_wakes) {
  if (syscall(SYS_futex, lock, FUTEX_WAKE, 0, NULL, NULL, 0) == -1) {
    throw runtime_error("futex_wake failed: " + string_for_error(errno));
  }
}

static void release_and_wake(void* void_lock, size_t size) {
  atomic<int>* lock = reinterpret_cast<atomic<int>*>(void_lock);
  lock->store(0);
  futex_wake(lock, 1);
}

#endif


ProcessLockGuard::ProcessLockGuard(ProcessLockGuard&& other) :
    stolen(other.stolen), pool(other.pool), offset(other.offset) {
  other.pool = NULL;
}

ProcessLockGuard::ProcessLockGuard(Pool* pool, uint64_t offset) : stolen(false),
    pool(pool), offset(offset) {
  atomic<int>* lock = this->pool->at<atomic<int>>(this->offset);
  int desired_value = this_process_token();

#ifdef LINUX
  static const struct timespec timeout = {1, 0}; // 1 second

  for (;;) {
    int expected_value = 0;
    if (lock->compare_exchange_strong(expected_value, desired_value)) {
      return;
    }

    // someone else is holding the lock; wait for them to be done.
    // expected_value now contains the other process' token (not zero). if we
    // were not woken by FUTEX_WAKE, then another process may still be holding
    // the lock; check if it's running.
    if (!futex_wait(lock, expected_value, &timeout)) {
      pid_t pid = pid_for_token(expected_value);
      int32_t start_time = mask_start_time(start_time_for_token(expected_value));
      if (mask_start_time(start_time_for_pid(pid)) != start_time) {
        // the holding process died; steal the lock from it. if we get the lock,
        // repair the allocator structures since they could be in an
        // inconsistent state. if we don't get the lock, then another process
        // got there first and we'll just keep waiting
        if (lock->compare_exchange_strong(expected_value, desired_value)) {
          this->stolen = true;
          return;
        }
      }
    }
  }

#else
  for (;;) {
    // try several times to get the lock
    int expected_value;
    uint8_t spin_count = 0;
    while (spin_count < SPIN_LIMIT) {
      expected_value = 0;
      if (lock->compare_exchange_weak(expected_value, desired_value)) {
        return;
      }
      spin_count++;
    }

    // if we didn't get the lock, wait up to 1 second, then check if the holder
    // is still running

    // os x doesn't have futex
    // TODO: implement futex-like functionality on osx
    sched_yield();

    pid_t pid = pid_for_token(expected_value);
    uint64_t start_time_token = mask_start_time(start_time_for_token(expected_value));
    if (mask_start_time(start_time_for_pid(pid)) != start_time_token) {
      // the holding process died; steal the lock from it. if we get the lock,
      // repair the allocator structures since they could be in an
      // inconsistent state. if we don't get the lock, then another process
      // got there first and we'll just keep waiting
      if (lock->compare_exchange_strong(expected_value, desired_value)) {
        this->stolen = true;
        return;
      }
    }
  }

#endif
}

ProcessLockGuard::~ProcessLockGuard() {
  if (!this->pool) {
    return;
  }

  try {
    atomic<int>* lock = this->pool->at<atomic<int>>(this->offset);
    lock->store(0);
#ifdef LINUX
    futex_wake(lock, 1);
#endif
  } catch (const bad_alloc& e) {
    // this can happen if the pool was expanded and no longer fits in this
    // process' address space
#ifdef LINUX
    this->pool->map_and_call(this->offset, sizeof(int), &release_and_wake);
#else
    this->pool->map_and_write_atomic<int>(this->offset, 0);
#endif
  }
}

} // namespace sharedstructures
