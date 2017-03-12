#define _STDC_FORMAT_MACROS

#include <inttypes.h>
#include <sched.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <phosg/Process.hh>
#include <phosg/Time.hh>
#include <phosg/UnitTest.hh>
#include <string>

#include "Pool.hh"
#include "ProcessSpinlock.hh"

using namespace std;
using namespace sharedstructures;


void run_acquisitions_test() {
  printf("-- acquisitions (1 second)\n");

  {
    // creating a pool isn't race-safe, so we'll so it here (and close it)
    // before starting any child processes
    Pool p("test-pool", 1024 * 1024);
  }

  unordered_set<pid_t> child_pids;
  while ((child_pids.size() < 8) && !child_pids.count(0)) {
    pid_t pid = fork();
    if (pid == -1) {
      break;
    } else {
      if (pid) {
        printf("--   child process %d started\n", pid);
      }
      child_pids.emplace(pid);
    }
  }

  if (child_pids.count(0)) {
    // child process: lock the pool and write the pid to the pool, then check
    // that it wasn't changed just before releasing the lock
    shared_ptr<Pool> pool(new Pool("test-pool", 1024 * 1024));

    uint64_t start = now();
    uint64_t num_loops = 0;
    uint64_t num_after_loops = 0;
    pid_t pid = getpid();
    uint64_t* pool_pid = pool->at<uint64_t>(0x10);
    uint64_t* pool_last_pid = pool->at<uint64_t>(0x18);
    while (now() < start + 1000000) {
      {
        ProcessSpinlockGuard g(pool.get(), 0x08);
        expect_eq(0, *pool_pid);
        if (*pool_last_pid && ((pid_t)*pool_last_pid != pid)) {
          num_after_loops++;
        }
        *pool_pid = pid;
        *pool_last_pid = pid;

        // normally you shouldn't yield while holding the lock; we do this so
        // other processes get a chance to check the lock (which allows this
        // test to actually test mutual exclusion)
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

  } else {
    // parent process: wait for children to terminate
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
}


int main(int argc, char* argv[]) {
  int retcode = 0;
  try {
    Pool::delete_pool("test-pool");
    run_acquisitions_test();
    printf("all tests passed\n");

    // only delete the pool if the tests pass; if they don't, we might want to
    // examine its contents
    Pool::delete_pool("test-pool");

  } catch (const exception& e) {
    printf("failure: %s\n", e.what());
    retcode = 1;
  }

  return retcode;
}
