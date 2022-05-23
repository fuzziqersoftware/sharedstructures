#define _STDC_FORMAT_MACROS

#include <errno.h>
#include <inttypes.h>
#include <sched.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <phosg/Process.hh>
#include <phosg/Strings.hh>
#include <phosg/Time.hh>
#include <phosg/UnitTest.hh>
#include <string>

#include "Pool.hh"
#include "ProcessLock.hh"

using namespace std;
using namespace sharedstructures;

using Behavior = ProcessReadWriteLockGuard::Behavior;

const string pool_name = "ProcessLockTest-pool";


shared_ptr<Pool> create_pool() {
  return shared_ptr<Pool>(new Pool(pool_name, 1024 * 1024));
}


unordered_set<pid_t> fork_children(size_t num_processes) {
  unordered_set<pid_t> child_pids;
  while ((child_pids.size() < num_processes) && !child_pids.count(0)) {
    pid_t pid = fork();
    if (pid == -1) {
      throw runtime_error("fork() failed: " + string_for_error(errno));
    } else {
      if (pid) {
        // parent
        printf("--   child process %d started\n", pid);
        child_pids.emplace(pid);
      } else {
        // child
        child_pids.clear();
        return child_pids;
      }
    }
  }
  return child_pids;
}


void wait_for_children(unordered_set<pid_t>& child_pids) {
  int num_failures = 0;
  int exit_status;
  pid_t exited_pid;
  while ((exited_pid = wait(&exit_status)) != -1) {
    child_pids.erase(exited_pid);
    if (WIFEXITED(exit_status) && (WEXITSTATUS(exit_status) == 0)) {
      printf("--   child %d terminated successfully\n", exited_pid);
    } else {
      printf("--   child %d failed (%d)\n", exited_pid, exit_status);
      num_failures++;
    }
  }

  expect_eq(true, child_pids.empty());
  expect_eq(0, num_failures);
}


void run_basic_test() {
  printf("-- basic\n");

  auto pool = create_pool();

  expect_eq(false, pool->at<ProcessLock>(0x18)->is_locked());
  {
    ProcessLockGuard g(pool.get(), 0x18);
    expect_eq(true, pool->at<ProcessLock>(0x18)->is_locked());
  }
  expect_eq(false, pool->at<ProcessLock>(0x18)->is_locked());

  expect_eq(false, pool->at<ProcessReadWriteLock>(0x18)->is_locked(false));
  expect_eq(false, pool->at<ProcessReadWriteLock>(0x18)->is_locked(true));
  expect_eq(0, pool->at<ProcessReadWriteLock>(0x18)->reader_count());
  {
    ProcessReadWriteLockGuard g(pool.get(), 0x18, Behavior::Read);
    expect_eq(true, pool->at<ProcessReadWriteLock>(0x18)->is_locked(false));
    expect_eq(false, pool->at<ProcessReadWriteLock>(0x18)->is_locked(true));
    expect_eq(1, pool->at<ProcessReadWriteLock>(0x18)->reader_count());
    expect_eq(0, g.stolen_token());
  }
  expect_eq(false, pool->at<ProcessReadWriteLock>(0x18)->is_locked(false));
  expect_eq(false, pool->at<ProcessReadWriteLock>(0x18)->is_locked(true));
  expect_eq(0, pool->at<ProcessReadWriteLock>(0x18)->reader_count());
  {
    ProcessReadWriteLockGuard g(pool.get(), 0x18, Behavior::Write);
    expect_eq(false, pool->at<ProcessReadWriteLock>(0x18)->is_locked(false));
    expect_eq(true, pool->at<ProcessReadWriteLock>(0x18)->is_locked(true));
    expect_eq(0, pool->at<ProcessReadWriteLock>(0x18)->reader_count());
    expect_eq(0, g.stolen_token());
  }
  expect_eq(false, pool->at<ProcessReadWriteLock>(0x18)->is_locked(false));
  expect_eq(false, pool->at<ProcessReadWriteLock>(0x18)->is_locked(true));
  expect_eq(0, pool->at<ProcessReadWriteLock>(0x18)->reader_count());
}


void run_lock_test() {
  printf("-- lock\n");

  unordered_set<pid_t> child_pids = fork_children(8);

  if (!child_pids.empty()) {
    wait_for_children(child_pids);
    return;
  }

  // child process: lock the pool and write the pid to the pool, then check that
  // it wasn't changed just before releasing the lock
  auto pool = create_pool();

  uint64_t start = now();
  uint64_t num_loops = 0;
  uint64_t num_after_loops = 0;
  uint64_t pid = getpid();
  uint64_t* pool_pid = pool->at<uint64_t>(0x8);
  uint64_t* pool_last_pid = pool->at<uint64_t>(0x10);
  while (now() < start + 1000000) {
    {
      ProcessLockGuard g(pool.get(), 0x18);
      expect_eq(0, *pool_pid);
      if (*pool_last_pid && (*pool_last_pid != pid)) {
        num_after_loops++;
      }
      *pool_pid = pid;
      *pool_last_pid = pid;

      // normally you shouldn't yield while holding the lock; we do this so
      // other processes get a chance to check the lock (which allows this test
      // to actually test mutual exclusion)
      sched_yield();

      expect_eq(pid, *pool_pid);
      *pool_pid = 0;
    }
    // don't grab the lock again immediately; give other processes a chance
    sched_yield();

    num_loops++;
  }

  printf("--   process %d terminated after %" PRIu64 " acquisitions (%" PRIu64 " after other processes)\n",
      getpid(), num_loops, num_after_loops);

  // we succeeded if we got the lock at all
  _exit(0);
}


void run_read_write_lock_test() {
  printf("-- read-write lock\n");

  unordered_set<pid_t> child_pids = fork_children(8);

  if (!child_pids.empty()) {
    wait_for_children(child_pids);
    return;
  }

  // child process: lock the pool and write the pid to the pool, then check that
  // it wasn't changed just before releasing the lock
  auto pool = create_pool();

  uint64_t start = now();
  uint64_t num_loops = 0;
  uint64_t num_after_loops = 0;
  uint64_t num_reads_after_self = 0;
  uint64_t num_reads_after_other = 0;
  uint64_t num_reads_after_none = 0;
  pid_t pid = getpid();
  int64_t* pool_pid = pool->at<int64_t>(0x08);
  int64_t* pool_last_pid = pool->at<int64_t>(0x10);
  while (now() < start + 1000000) {
    // lock the pool for writes, put our pid there, and let other processes read
    {
      ProcessReadWriteLockGuard g(pool.get(), 0x18, Behavior::Write);
      expect_eq(0, g.stolen_token());
      expect_eq(true, pool->at<ProcessReadWriteLock>(0x18)->is_locked(true));
      expect_eq(false, pool->at<ProcessReadWriteLock>(0x18)->is_locked(false));
      expect_eq(0, pool->at<ProcessReadWriteLock>(0x18)->reader_count());
      expect_eq(0, *pool_pid);
      if (*pool_last_pid && ((pid_t)*pool_last_pid != pid)) {
        num_after_loops++;
      }
      *pool_pid = pid;
      *pool_last_pid = pid;

      // normally you shouldn't yield while holding the lock; we do this so
      // other processes get a chance to check the lock (which allows this test
      // to actually test mutual exclusion)
      sched_yield();

      expect_eq(pid, *pool_pid);
      *pool_pid = 0;
    }
    // don't grab the lock again immediately; give other processes a chance
    sched_yield();

    // now read; check if the pid doesn't match our pid
    {
      ProcessReadWriteLockGuard g(pool.get(), 0x18, Behavior::Read);
      expect_eq(0, g.stolen_token());

      // we don't check if the lock is locked for writing - it's possible that
      // is_locked returns true if a writer is waiting for readers to drain
      expect_eq(true, pool->at<ProcessReadWriteLock>(0x18)->is_locked(false));

      // pool_pid should only be nonzero when the lock is held for writing (not
      // if a writer is waiting or absent)
      expect_eq(0, *pool_pid);
      if (*pool_last_pid == pid) {
        num_reads_after_self++;
      } else if (*pool_last_pid) {
        num_reads_after_other++;
      } else {
        num_reads_after_none++;
      }
    }

    num_loops++;
  }

  expect_eq(0, num_reads_after_none);

  printf("--   process %d terminated after %" PRIu64 " acquisitions (%" PRIu64
      " after others; %" PRIu64 " reads after self; %" PRIu64 " reads after others)\n",
      getpid(), num_loops, num_after_loops, num_reads_after_self, num_reads_after_other);

  // we succeeded if we got the lock at all
  _exit(0);
}


static const char* name_for_behavior(Behavior b) {
  switch (b) {
    case Behavior::Read:
      return "Read";
    case Behavior::Write:
      return "Write";
    case Behavior::ReadUnlessStolen:
      return "ReadUnlessStolen";
  }
  return "UnknownBehavior";
}

void run_write_crash_test_case(Behavior parent_behavior) {
  printf("-- write crash (parent %s)\n", name_for_behavior(parent_behavior));

  unordered_set<pid_t> child_pids = fork_children(1);
  auto pool = create_pool();

  if (child_pids.empty()) {
    printf("--   child acquiring write lock\n");
    ProcessReadWriteLockGuard g(pool.get(), 0x18, Behavior::Write);
    expect_eq(0, g.stolen_token());
    printf("--   child dying\n");
    _exit(0);

  } else {
    pid_t child_pid = *child_pids.begin();

    printf("--   parent waiting for lock to be acquired\n");
    auto* lock = pool->at<ProcessReadWriteLock>(0x18);
    while (!lock->is_locked(true) && pid_exists(child_pid)) {
      sched_yield();
    }
    expect_eq(0, pool->at<ProcessReadWriteLock>(0x18)->reader_count());

    // this should steal the lock even though the child exists as a zombie, and
    // should appear stolen even if locking for reading
    printf("--   parent acquiring lock\n");
    ProcessReadWriteLockGuard g(pool.get(), 0x18, parent_behavior);
    expect_ne(0, g.stolen_token());
    if (parent_behavior == Behavior::Read) {
      expect_eq(false, lock->is_locked(true));
      expect_eq(true, lock->is_locked(false));
    } else {
      expect_eq(true, lock->is_locked(true));
      expect_eq(false, lock->is_locked(false));
      g.downgrade();
      expect_eq(false, lock->is_locked(true));
      expect_eq(true, lock->is_locked(false));
    }

    // the child should have died with status 0
    wait_for_children(child_pids);
  }
}

void run_write_crash_test() {
  run_write_crash_test_case(Behavior::Read);
  run_write_crash_test_case(Behavior::Write);
  run_write_crash_test_case(Behavior::ReadUnlessStolen);
}

static unordered_set<pid_t> fill_reader_slots(shared_ptr<Pool> pool) {
  unordered_set<pid_t> all_zombie_pids;
  for (size_t x = 0; x < NUM_READER_SLOTS; x++) {
    unordered_set<pid_t> child_pids = fork_children(1);

    if (child_pids.empty()) {
      printf("--   child taking reader slot %zu\n", x);
      expect_eq(x, pool->at<ProcessReadWriteLock>(0x18)->reader_count());
      ProcessReadWriteLockGuard g(pool.get(), 0x18, Behavior::Read);
      expect_eq(0, g.stolen_token());
      expect_eq(x + 1, pool->at<ProcessReadWriteLock>(0x18)->reader_count());
      _exit(0);

    } else {
      pid_t child_pid = *child_pids.begin();
      while (start_time_for_pid(child_pid) != 0) {
        sched_yield();
      }
      all_zombie_pids.insert(child_pid);
    }
  }
  return all_zombie_pids;
}

void run_read_crash_test() {
  printf("-- read crash\n");

  auto pool = create_pool();

  // with all the reader slots full, any lock call should clear them all out.
  // the lock shouldn't appear stolen because the processes crashed while
  // reading, so no repairs are needed
  auto zombie_pids = fill_reader_slots(pool);
  expect_eq(NUM_READER_SLOTS, pool->at<ProcessReadWriteLock>(0x18)->reader_count());
  {
    ProcessReadWriteLockGuard g(pool.get(), 0x18, Behavior::Write);
    expect_eq(0, g.stolen_token());
  }
  expect_eq(0, pool->at<ProcessReadWriteLock>(0x18)->reader_count());
  wait_for_children(zombie_pids);

  // behavior should be similar when we lock the pool for reading, except we
  // shouldn't clear all the reader slots; we should clear only one
  zombie_pids = fill_reader_slots(pool);
  expect_eq(NUM_READER_SLOTS,
      pool->at<ProcessReadWriteLock>(0x18)->reader_count());
  {
    ProcessReadWriteLockGuard g(pool.get(), 0x18, Behavior::Read);
    expect_eq(0, g.stolen_token());
  }
  expect_eq(NUM_READER_SLOTS - 1,
      pool->at<ProcessReadWriteLock>(0x18)->reader_count());
  wait_for_children(zombie_pids);
}


int main(int, char**) {
  int retcode = 0;
  try {
    Pool::delete_pool(pool_name);
    {
      create_pool();
    }

    run_basic_test();
    run_lock_test();
    run_read_write_lock_test();
    run_write_crash_test();
    run_read_crash_test();
    printf("all tests passed\n");

    // only delete the pool if the tests pass; if they don't, we might want to
    // examine its contents
    Pool::delete_pool(pool_name);

  } catch (const exception& e) {
    printf("failure: %s\n", e.what());
    retcode = 1;
  }

  return retcode;
}
