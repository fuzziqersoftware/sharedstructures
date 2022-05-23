#define __STDC_FORMAT_MACROS
#include <errno.h>
#include <inttypes.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <phosg/Strings.hh>
#include <phosg/Time.hh>
#include <phosg/UnitTest.hh>
#include <string>

#include "Pool.hh"
#include "IntVector.hh"

using namespace std;
using namespace sharedstructures;

const string pool_name = "IntVectorTest-cc-pool";


shared_ptr<IntVector> get_or_create_vector() {
  shared_ptr<Pool> pool(new Pool(pool_name));
  return shared_ptr<IntVector>(new IntVector(pool));
}



void print_vector(shared_ptr<IntVector> v) {
  size_t count = v->size();
  for (size_t x = 0; x < count; x++) {
    fprintf(stderr, "  v[%zu] == %" PRIu64 "\n", x, v->load(x));
  }
}



void run_basic_test() {
  printf("-- basic\n");

  auto v = get_or_create_vector();

  const ssize_t limit = 1024;

  expect_eq(0, v->size());
  v->expand(10);
  expect_eq(10, v->size());
  v->expand(5);
  expect_eq(10, v->size());
  v->expand(limit);
  expect_eq(limit, v->size());

  // load, store
  for (ssize_t x = 0; x < limit; x++) {
    expect_eq(0, v->load(x));
  }
  for (ssize_t x = 0; x < limit; x++) {
    v->store(x, x);
  }
  for (ssize_t x = 0; x < limit; x++) {
    expect_eq(x, v->load(x));
  }

  // exchange
  for (ssize_t x = 0; x < limit; x++) {
    expect_eq(x, v->exchange(x, x + 10));
  }
  for (ssize_t x = 0; x < limit; x++) {
    expect_eq(x + 10, v->load(x));
  }
  for (ssize_t x = 0; x < limit; x++) {
    expect_eq(x + 10, v->exchange(x, x));
  }
  for (ssize_t x = 0; x < limit; x++) {
    expect_eq(x, v->load(x));
  }

  // compare_exchange
  for (ssize_t x = 0; x < limit; x++) {
    expect_eq(x, v->compare_exchange(x, 10, 15));
  }
  for (ssize_t x = 0; x < limit; x++) {
    expect_eq((x == 10) ? 15 : x, v->load(x));
  }
  v->store(10, 10);

  // fetch_add, fetch_sub
  for (ssize_t x = 0; x < limit; x++) {
    expect_eq(x, v->fetch_add(x, 30));
  }
  for (ssize_t x = 0; x < limit; x++) {
    expect_eq(x + 30, v->load(x));
  }
  for (ssize_t x = 0; x < limit; x++) {
    expect_eq(x + 30, v->fetch_sub(x, 30));
  }
  for (ssize_t x = 0; x < limit; x++) {
    expect_eq(x, v->load(x));
  }

  // fetch_and, fetch_or
  for (ssize_t x = 0; x < limit; x++) {
    expect_eq(x, v->fetch_or(x, 0x7F));
  }
  for (ssize_t x = 0; x < limit; x++) {
    expect_eq(x | 0x7F, v->load(x));
  }
  for (ssize_t x = 0; x < limit; x++) {
    expect_eq(x | 0x7F, v->fetch_and(x, ~0x7F));
  }
  for (ssize_t x = 0; x < limit; x++) {
    expect_eq(x & ~0x7F, v->load(x));
  }

  // reset for xor test
  for (ssize_t x = 0; x < limit; x++) {
    v->store(x, x);
  }

  // fetch_xor
  for (ssize_t x = 0; x < 15; x++) {
    expect_eq(x, v->fetch_xor(x, 0x7F));
  }
  for (ssize_t x = 0; x < 15; x++) {
    expect_eq(x ^ 0x7F, v->load(x));
  }
  for (ssize_t x = 0; x < 15; x++) {
    expect_eq(x ^ 0x7F, v->fetch_xor(x, 0x7F));
  }
  for (ssize_t x = 0; x < 15; x++) {
    expect_eq(x, v->load(x));
  }

  // should be 3 pages
  expect_eq(4096 * 3, v->get_pool()->size());
}

void run_concurrent_readers_test() {
  printf("-- concurrent readers\n");

  unordered_set<pid_t> child_pids;
  while ((child_pids.size() < 8) && !child_pids.count(0)) {
    pid_t pid = fork();
    if (pid == -1) {
      break;
    } else {
      child_pids.emplace(pid);
    }
  }

  if (child_pids.count(0)) {
    // child process: try up to 3 seconds for value to go from 100 to 110
    auto v = get_or_create_vector();

    int64_t value = 100;
    uint64_t start_time = now();
    do {
      if (v->load(1) == value) {
        value++;
      }
      usleep(1); // yield to other processes
    } while ((value < 110) && (now() < (start_time + 3000000)));

    // we succeeded if we saw all the values from 100 to 110
    _exit(value != 110);

  } else {
    // parent process: increment the value from 100 to 110
    auto v = get_or_create_vector();

    for (int64_t value = 100; value < 110; value++) {
      usleep(100000);
      v->store(1, value);
    }

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


int main(int, char**) {
  int retcode = 0;

  try {
    Pool::delete_pool(pool_name);
    run_basic_test();
    run_concurrent_readers_test();
    printf("all tests passed\n");

  } catch (const exception& e) {
    printf("failure: %s\n", e.what());
    retcode = 1;
  }
  Pool::delete_pool(pool_name);

  return retcode;
}
