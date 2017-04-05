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

static bool process_for_token_is_running(int32_t token) {
  pid_t pid = pid_for_token(token);
  uint64_t start_time_token = start_time_for_token(token);
  uint64_t start_time = start_time_for_pid(pid);
  return (start_time != 0) &&
         (mask_start_time(start_time) == mask_start_time(start_time_token));
}



#ifdef LINUX

static bool futex_wait(atomic<int32_t>* lock, int32_t expected_value,
    const struct timespec* timeout) {
  if (syscall(SYS_futex, lock, FUTEX_WAIT, expected_value, timeout, NULL, 0) == -1) {
    if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
      return true; // value didn't match the input
    }
    if ((errno == EINTR) || (errno == ETIMEDOUT)) {
      return false; // timed out or interrupted
    }
    // some other thing
    throw runtime_error("futex_wait failed: " + string_for_error(errno));
  }
  return true;
}

static void futex_wake(atomic<int32_t>* lock, int32_t num_wakes) {
  if (syscall(SYS_futex, lock, FUTEX_WAKE, 0, NULL, NULL, 0) == -1) {
    throw runtime_error("futex_wake failed: " + string_for_error(errno));
  }
}

static bool acquire_process_lock(atomic<int32_t>* lock) {
  static const struct timespec timeout = {1, 0}; // 1 second
  int32_t desired_value = this_process_token();

  for (;;) {
    int32_t expected_value = 0;
    if (lock->compare_exchange_strong(expected_value, desired_value)) {
      return false;
    }

    // someone else is holding the lock; wait for them to be done.
    // expected_value now contains the other process' token (not zero). if we
    // were not woken by FUTEX_WAKE, then another process may still be holding
    // the lock; check if it's running.
    if (!futex_wait(lock, expected_value, &timeout)) {
      if (!process_for_token_is_running(expected_value)) {
        // the holding process died; steal the lock from it. if we get the lock,
        // repair the allocator structures since they could be in an
        // inconsistent state. if we don't get the lock, then another process
        // got there first and we'll just keep waiting
        if (lock->compare_exchange_strong(expected_value, desired_value)) {
          return true;
        }
      }
    }
  }
}

static void release_process_lock(atomic<int32_t>* lock) {
  lock->store(0);
  futex_wake(lock, 1);
}

static bool wait_for_reader_release(atomic<int32_t>* lock,
    int32_t existing_token) {
  static const struct timespec timeout = {1, 0}; // 1 second

  while (!futex_wait(lock, existing_token, &timeout)) {
    if (!process_for_token_is_running(existing_token)) {
      lock->store(0);
      return true;
    }
  }
  return false;
}



#else // MACOSX

static bool acquire_process_lock(atomic<int32_t>* lock) {
  int32_t desired_value = this_process_token();

  for (;;) {
    // try several times to get the lock
    int32_t expected_value;
    uint8_t spin_count = 0;
    while (spin_count < SPIN_LIMIT) {
      expected_value = 0;
      if (lock->compare_exchange_weak(expected_value, desired_value)) {
        return false;
      }
      spin_count++;
    }

    // if we didn't get the lock, wait up to 1 second, then check if the holder
    // is still running

    // os x doesn't have futex
    // TODO: implement futex-like functionality on osx
    sched_yield();

    if (!process_for_token_is_running(expected_value)) {
      // the holding process died; steal the lock from it. if we get the lock,
      // repair the allocator structures since they could be in an
      // inconsistent state. if we don't get the lock, then another process
      // got there first and we'll just keep waiting
      if (lock->compare_exchange_strong(expected_value, desired_value)) {
        return true;
      }
    }
  }
}

static void release_process_lock(atomic<int32_t>* lock) {
  lock->store(0);
}

static bool wait_for_reader_release(atomic<int32_t>* lock,
    int32_t existing_token) {
  // TODO: do something better here (no futex on os x)
  while (lock->load()) {
    sched_yield();
    if (!process_for_token_is_running(existing_token)) {
      if (lock->compare_exchange_strong(existing_token, 0)) {
        return true;
      }
    }
  }
  return false;
}

#endif

static void release_cb(void* void_lock, size_t size) {
  atomic<int32_t>* lock = reinterpret_cast<atomic<int32_t>*>(void_lock);
  release_process_lock(lock);
}

static void wait_for_reader_drain(ProcessReadWriteLock* data, bool wait_all) {
  if (wait_all) {
    for (size_t x = 0; x < NUM_READER_SLOTS; x++) {
      int32_t existing_token = data->reader_tokens[x].load();
      if (existing_token == 0) {
        continue; // no process in this reader slot
      }

      // wait for this reader to release. if they don't, then check if the
      // process is still running, and clear the lock if it's not. because this
      // process was a reader, we don't need to repair the allocator state if we
      // cleared its lock.
      wait_for_reader_release(&data->reader_tokens[x], existing_token);
    }

  } else {
    // first check for an empty slot and return it if found
    for (size_t x = 0; x < NUM_READER_SLOTS; x++) {
      int32_t existing_token = data->reader_tokens[x].load();
      if (existing_token == 0) {
        return;
      }
    }

    // no empty slots; pick an "arbitrary" slot and wait on it
    int32_t reader_slot = getpid_cached() % NUM_READER_SLOTS;
    int32_t existing_token = data->reader_tokens[reader_slot].load();
    wait_for_reader_release(&data->reader_tokens[reader_slot], existing_token);
  }
}



ProcessLockGuard::ProcessLockGuard(ProcessLockGuard&& other) :
    stolen(other.stolen), pool(other.pool), offset(other.offset) {
  other.pool = NULL;
}

ProcessLockGuard::ProcessLockGuard(Pool* pool, uint64_t offset) : stolen(false),
    pool(pool), offset(offset) {
  atomic<int32_t>* lock = this->pool->at<atomic<int32_t>>(this->offset);
  this->stolen = acquire_process_lock(lock);
}

ProcessLockGuard::~ProcessLockGuard() {
  if (!this->pool) {
    return;
  }

  try {
    atomic<int32_t>* lock = this->pool->at<atomic<int32_t>>(this->offset);
    release_process_lock(lock);
  } catch (const bad_alloc& e) {
    // this can happen if the pool was expanded and no longer fits in this
    // process' address space
    this->pool->map_and_call(this->offset, sizeof(int32_t), &release_cb);
  }
}

size_t ProcessLockGuard::data_size() {
  // everything must be 64-bit aligned, so even though we only use 32 bits, we
  // claim to use 64
  return sizeof(int64_t);
}



ProcessReadWriteLockGuard::ProcessReadWriteLockGuard(
    ProcessReadWriteLockGuard&& other) : stolen(other.stolen), pool(other.pool),
    offset(other.offset), reader_slot(other.reader_slot) {
  other.pool = NULL;
}

ProcessReadWriteLockGuard::ProcessReadWriteLockGuard(Pool* pool,
    uint64_t offset, bool writing) : stolen(false), pool(pool), offset(offset) {
  auto* data = this->pool->at<ProcessReadWriteLock>(this->offset);

  if (writing) {
    // take the write lock, then wait for readers to drain or die. because we're
    // holding the write lock, no new readers can be added
    this->reader_slot = -1;
    this->stolen = acquire_process_lock(&data->write_lock);
    wait_for_reader_drain(data, true);

  } else {
    this->reader_slot = NUM_READER_SLOTS;

    int32_t reader_token = this_process_token();
    do {
      // take the write lock, find an empty reader slot and take it, then release
      // the write lock
      acquire_process_lock(&data->write_lock);
      for (size_t x = 0; x < NUM_READER_SLOTS; x++) {
        if (data->reader_tokens[x] == 0) {
          data->reader_tokens[x].store(reader_token);
          this->reader_slot = x;
          break;
        }
      }
      release_process_lock(&data->write_lock);

      // if there were no available reader slots, wait for any slot to drain and
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
    release_process_lock(&data->write_lock);
  } else {
    release_process_lock(&data->reader_tokens[this->reader_slot]);
  }
}

} // namespace sharedstructures
