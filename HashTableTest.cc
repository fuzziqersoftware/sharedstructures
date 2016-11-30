#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <phosg/Time.hh>
#include <phosg/UnitTest.hh>
#include <string>

#include "HashTable.hh"

using namespace std;
using namespace sharedstructures;


void expect_key_missing(const HashTable& table, const void* k,
    size_t s) {
  try {
    table.at(k, s);
    expect(false);
  } catch (const out_of_range& e) { }
}


void verify_state(
    const unordered_map<string, string>& expected,
    const HashTable& table) {
  expect_eq(expected.size(), table.size());
  for (const auto& it : expected) {
    expect_eq(it.second, table.at(it.first.data(), it.first.size()));
  }
}


void run_basic_test() {
  printf("-- basic\n");

  shared_ptr<Pool> pool(new Pool("test-table"));
  HashTable table(pool, 0, 6);
  unordered_map<string, string> expected;
  size_t initial_pool_allocated = pool->bytes_allocated();

  expect_eq(0, table.size());
  expect_eq(6, table.bits());

  table.insert("key1", 4, "value1", 6);
  expected.emplace("key1", "value1");
  verify_state(expected, table);
  table.insert("key2", 4, "value2", 6);
  expected.emplace("key2", "value2");
  verify_state(expected, table);
  table.insert("key3", 4, "value3", 6);
  expected.emplace("key3", "value3");
  verify_state(expected, table);

  expect_eq(true, table.erase("key2", 4));
  expected.erase("key2");
  verify_state(expected, table);

  expect_eq(false, table.erase("key2", 4));
  verify_state(expected, table);

  table.insert("key1", 4, "value0", 6);
  expected["key1"] = "value0";
  verify_state(expected, table);

  table.clear();
  expected.clear();
  verify_state(expected, table);

  // the empty table should not leak any allocated memory
  expect_eq(initial_pool_allocated, pool->bytes_allocated());
}


void run_collision_test() {
  printf("-- collision\n");

  // writing 5 keys to a 4-slot hashtable forces a collision

  shared_ptr<Pool> pool(new Pool("test-table"));
  HashTable table(pool, 0, 2);
  unordered_map<string, string> expected;
  size_t initial_pool_allocated = pool->bytes_allocated();

  expect_eq(0, table.size());

  table.insert("key1", 4, "value1", 6);
  table.insert("key2", 4, "value2", 6);
  table.insert("key3", 4, "value3", 6);
  table.insert("key4", 4, "value4", 6);
  table.insert("key5", 4, "value5", 6);
  expected.emplace("key1", "value1");
  expected.emplace("key2", "value2");
  expected.emplace("key3", "value3");
  expected.emplace("key4", "value4");
  expected.emplace("key5", "value5");
  verify_state(expected, table);

  while (!expected.empty()) {
    auto it = expected.begin();
    table.erase(it->first);
    expected.erase(it);
    verify_state(expected, table);
  }

  // the empty table should not leak any allocated memory
  expect_eq(initial_pool_allocated, pool->bytes_allocated());
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
    // child process: try up to a second to get the key
    shared_ptr<Pool> pool(new Pool("test-table"));
    HashTable table(pool, 0, 4);

    int64_t value = 100;
    string value_str = to_string(value);
    uint64_t start_time = now();
    do {
      try {
        auto res = table.at("key1", 4);
        if (res == value_str) {
          value++;
          value_str = to_string(value);
        }
      } catch (const out_of_range& e) { }
      usleep(0); // yield to other processes
    } while ((value < 110) && (now() < (start_time + 1000000)));

    // we succeeded if we saw all the values from 100 to 110
    _exit(value != 110);

  } else {
    // parent process: write the key, then wait for children to terminate
    shared_ptr<Pool> pool(new Pool("test-table"));
    HashTable table(pool, 0, 4);

    for (int64_t value = 100; value < 110; value++) {
      usleep(50000);
      table.insert("key1", 4, to_string(value));
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



int main(int argc, char* argv[]) {
  int retcode = 0;

  try {
    Pool::delete_pool("test-table");
    run_basic_test();
    Pool::delete_pool("test-table");
    run_collision_test();
    Pool::delete_pool("test-table");
    run_concurrent_readers_test();
    printf("all tests passed\n");

  } catch (const exception& e) {
    printf("failure: %s\n", e.what());
    retcode = 1;
  }
  Pool::delete_pool("test-table");

  return retcode;
}
