#include "ProcessLock.hh"

#include <inttypes.h>
#include <limits.h>
#ifdef PHOSG_LINUX
#include <linux/futex.h>
#include <sys/syscall.h>
#else
#include <sched.h>
#endif

#include <stdexcept>

#include <phosg/Platform.hh>
#include <phosg/Process.hh>
#include <phosg/Strings.hh>

using namespace std;

namespace sharedstructures {

bool ProcessLock::is_locked() const {
  return this->lock.load() != 0;
}

bool ProcessReadWriteLock::is_locked(bool writing) const {
  if (writing) {
    return this->write_lock.load() != 0;
  }

  for (size_t x = 0; x < NUM_READER_SLOTS; x++) {
    if (this->reader_tokens[x].load() != 0) {
      return true;
    }
  }
  return false;
}

size_t ProcessReadWriteLock::reader_count() const {
  size_t count = 0;
  for (size_t x = 0; x < NUM_READER_SLOTS; x++) {
    count += (this->reader_tokens[x].load() != 0);
  }
  return count;
}

static const uint8_t PID_BITS = 18;
static const uint8_t START_TIME_BITS = 32 - PID_BITS;
static const uint8_t SPIN_LIMIT = 10;

static int32_t this_process_token() {
#ifdef PHOSG_MACOS
  int32_t start_time = this_process_start_time();
  if (!start_time) {
    throw runtime_error("cannot get start time for this process");
  }
  return (start_time << PID_BITS) | getpid_cached();
#else
  return getpid_cached();
#endif
}

#ifdef PHOSG_MACOS
static int32_t start_time_for_token(int32_t token) {
  return (token & ~((1 << PID_BITS) - 1)) >> PID_BITS;
}

static int32_t mask_start_time(int32_t start_time) {
  return start_time & ((1 << START_TIME_BITS) - 1);
}
#endif

static pid_t pid_for_token(int32_t token) {
  return token & ((1 << PID_BITS) - 1);
}

static bool process_for_token_is_running(int32_t token) {
  pid_t pid = pid_for_token(token);
#ifdef PHOSG_MACOS
  uint64_t start_time_token = start_time_for_token(token);
  uint64_t start_time = start_time_for_pid(pid);
  return (start_time != 0) &&
      (mask_start_time(start_time) == mask_start_time(start_time_token));
#else
  if (!pid_exists(pid)) {
    return false;
  }
  return !pid_is_zombie(pid);
#endif
}

#ifdef PHOSG_LINUX

static bool futex_wait(atomic<int32_t>* lock, int32_t expected_value,
    const struct timespec* timeout) {
  // Wait on a futex. Returns true if futex_wake was called by another process
  // or if the value didn't match the expected value at call time. Returns false
  // if the syscall was interrupted or the timeout expired. Neither of these
  // return values makes any guarantee about the futex's value, they just tell
  // what the most likely scenario is (is the value different or not?).
  if (syscall(SYS_futex, lock, FUTEX_WAIT, expected_value, timeout, nullptr, 0) == -1) {
    if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
      return true; // Value didn't match the input
    }
    if ((errno == EINTR) || (errno == ETIMEDOUT)) {
      return false; // Timed out or interrupted
    }
    // Some other thing
    throw runtime_error("futex_wait failed: " + string_for_error(errno));
  }
  return true;
}

static void futex_wake(atomic<int32_t>* lock, int32_t num_wakes) {
  if (syscall(SYS_futex, lock, FUTEX_WAKE, num_wakes, nullptr, nullptr, 0) == -1) {
    throw runtime_error("futex_wake failed: " + string_for_error(errno));
  }
}

#endif

static int32_t acquire_process_lock(atomic<int32_t>& lock, int32_t desired_value) {
  for (;;) {
    // Try several times to get the lock
    int32_t expected_value;
    uint64_t spin_count = 0;
    while (spin_count < SPIN_LIMIT) {
      expected_value = 0;
      if (lock.compare_exchange_weak(expected_value, desired_value)) {
        return 0;
      }
      spin_count++;
    }

#ifdef PHOSG_LINUX
    // Someone else is holding the lock; wait for them to be done.
    // expected_value now contains the other process' token (not zero). If the
    // futex value is likely to be different (because either futex_wake was
    // called or it's already different), don't bother checking if the other
    // process is running.
    static const struct timespec timeout = {1, 0}; // 1 second
    if (futex_wait(&lock, expected_value, &timeout)) {
      continue;
    }
#else
    // macOS doesn't have futex; we have to busy-wait.
    // TODO: implement futex-like functionality on macOS if possible (it must
    // be possible somehow)
    sched_yield();
#endif

    if (!process_for_token_is_running(expected_value)) {
      // The holding process died; steal the lock from it. If we get the lock,
      // repair the allocator structures since they could be in an
      // inconsistent state. If we don't get the lock, then another process
      // got there first and we'll just keep waiting
      if (lock.compare_exchange_strong(expected_value, desired_value)) {
        return expected_value;
      }
    }
  }
}

static void release_process_lock(atomic<int32_t>& lock, int32_t expected_token) {
  if (!lock.compare_exchange_strong(expected_token, 0)) {
    return;
  }
#ifdef PHOSG_LINUX
  // Wake them all - there will be a race for the lock if many processes are
  // waiting, but we don't keep the waiters in a queue so there isn't anything
  // better we can do here (for now)
  futex_wake(&lock, INT_MAX);
#endif
}

static bool wait_for_reader_release(atomic<int32_t>* lock) {
  for (;;) {
    int32_t existing_token = lock->load();
    if (!existing_token) {
      return false;
    }

#ifdef PHOSG_LINUX
    // existing_token now contains the other process' token (not zero). If the
    // futex value is likely to be different (because either futex_wake was
    // called or it's already different), don't bother checking if the other
    // process is running.
    static const struct timespec timeout = {0, 10000}; // 10ms
    if (futex_wait(lock, existing_token, &timeout)) {
      continue;
    }
#else
    // TODO: implement futex-like functionality on mac os if possible
    sched_yield();
#endif

    if (!process_for_token_is_running(existing_token)) {
      if (lock->compare_exchange_strong(existing_token, 0)) {
        return true;
      }
    }
  }
  return false;
}

static void wait_for_reader_drain(ProcessReadWriteLock* data, bool wait_all) {
  if (wait_all) {
    for (size_t x = 0; x < NUM_READER_SLOTS; x++) {
      int32_t existing_token = data->reader_tokens[x].load();
      if (existing_token == 0) {
        continue; // No process in this reader slot
      }

      // Wait for this reader to release. If they don't, then check if the
      // process is still running, and clear the lock if it's not. Because this
      // process was a reader, we don't need to repair the allocator state if we
      // cleared its lock.
      wait_for_reader_release(&data->reader_tokens[x]);
    }

  } else {
    for (;;) {
      // First check for an empty slot and return it if found
      for (size_t x = 0; x < NUM_READER_SLOTS; x++) {
        int32_t existing_token = data->reader_tokens[x].load();
        if (existing_token == 0) {
          return;
        }
      }

      // No empty slots; pick an "arbitrary" slot and wait on it
      int32_t reader_slot = getpid_cached() % NUM_READER_SLOTS;
      wait_for_reader_release(&data->reader_tokens[reader_slot]);
    }
  }
}

ProcessLockGuard::ProcessLockGuard(ProcessLockGuard&& other)
    : pool(other.pool),
      offset(other.offset),
      lock_token(other.lock_token),
      stolen_lock_token(other.stolen_lock_token) {
  other.pool = nullptr;
}

ProcessLockGuard::ProcessLockGuard(Pool* pool, uint64_t offset)
    : pool(pool),
      offset(offset),
      lock_token(this_process_token()),
      stolen_lock_token(0) {
  atomic<int32_t>* lock = this->pool->at<atomic<int32_t>>(this->offset);
  this->stolen_lock_token = acquire_process_lock(*lock, this->lock_token);
}

ProcessLockGuard::~ProcessLockGuard() {
  if (!this->pool) {
    return;
  }

  atomic<int32_t>* lock = this->pool->at<atomic<int32_t>>(this->offset);
  release_process_lock(*lock, this->lock_token);
}

size_t ProcessLockGuard::data_size() {
  // Everything must be 64-bit aligned, so even though we only use 32 bits, we
  // claim to use 64
  return sizeof(int64_t);
}

int32_t ProcessLockGuard::token() const {
  return this->lock_token;
}

int32_t ProcessLockGuard::stolen_token() const {
  return this->stolen_lock_token;
}

ProcessReadWriteLockGuard::ProcessReadWriteLockGuard(
    ProcessReadWriteLockGuard&& other)
    : pool(other.pool),
      offset(other.offset),
      reader_slot(other.reader_slot),
      lock_token(other.lock_token),
      stolen_lock_token(other.stolen_lock_token) {
  other.pool = nullptr;
}

ProcessReadWriteLockGuard::ProcessReadWriteLockGuard(Pool* pool,
    uint64_t offset, Behavior behavior)
    : pool(pool),
      offset(offset),
      lock_token(this_process_token()),
      stolen_lock_token(0) {
  auto* data = this->pool->at<ProcessReadWriteLock>(this->offset);

  if (behavior == Behavior::WRITE) {
    // Take the write lock, then wait for readers to drain or die. Because we're
    // holding the write lock, no new readers can be added
    this->reader_slot = -1;
    this->stolen_lock_token = acquire_process_lock(data->write_lock, this->lock_token);
    wait_for_reader_drain(data, true);

  } else {
    this->reader_slot = NUM_READER_SLOTS;

    int32_t reader_token = this_process_token();
    do {
      // Take the write lock, find an empty reader slot and take it, then
      // release the write lock. But if the caller needs to write when stolen
      // (for example, to call repair()), then return immediately
      this->stolen_lock_token = acquire_process_lock(data->write_lock, this->lock_token);
      if (this->stolen_lock_token && (behavior == Behavior::READ_UNLESS_STOLEN)) {
        this->reader_slot = -1;
        wait_for_reader_drain(data, true);
        return;
      }
      for (size_t x = 0; x < NUM_READER_SLOTS; x++) {
        int32_t expected_value = 0;
        if (data->reader_tokens[x].compare_exchange_strong(expected_value, reader_token)) {
          this->reader_slot = x;
          break;
        }
      }
      release_process_lock(data->write_lock, this->lock_token);

      // If there were no available reader slots, wait for any slot to drain and
      // try again
      if (this->reader_slot == NUM_READER_SLOTS) {
        wait_for_reader_drain(data, false);
      }
    } while (this->reader_slot == NUM_READER_SLOTS);
  }
}

ProcessReadWriteLockGuard::~ProcessReadWriteLockGuard() {
  if (!this->pool) {
    return;
  }

  auto* data = this->pool->at<ProcessReadWriteLock>(this->offset);
  if (this->reader_slot < 0) {
    release_process_lock(data->write_lock, this->lock_token);
  } else {
    release_process_lock(data->reader_tokens[this->reader_slot], this->lock_token);
  }
}

void ProcessReadWriteLockGuard::downgrade() {
  if (this->reader_slot != -1) {
    throw logic_error("attempted to downgrade a non-write lock");
  }

  auto* data = this->pool->at<ProcessReadWriteLock>(this->offset);

  int32_t reader_token = this_process_token();
  for (size_t x = 0; x < NUM_READER_SLOTS; x++) {
    int32_t expected_value = 0;
    if (data->reader_tokens[x].compare_exchange_strong(expected_value, reader_token)) {
      this->reader_slot = x;
      release_process_lock(data->write_lock, this->lock_token);
      return;
    }
  }

  // If we're holding a write lock, there had better not be any readers.
  // Shouldn't we have waited for them to drain before returning?!
  throw logic_error("write lock held with no available reader slots");
}

int32_t ProcessReadWriteLockGuard::token() const {
  return this->lock_token;
}

int32_t ProcessReadWriteLockGuard::stolen_token() const {
  return this->stolen_lock_token;
}

} // namespace sharedstructures
