#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <phosg/Time.hh>
#include <phosg/UnitTest.hh>
#include <string>

#include "Pool.hh"
#include "SimpleAllocator.hh"
#include "LogarithmicAllocator.hh"
#include "HashTable.hh"

using namespace std;
using namespace sharedstructures;


shared_ptr<Allocator> create_allocator(shared_ptr<Pool> pool,
    const string& allocator_type) {
  if (allocator_type == "simple") {
    return shared_ptr<Allocator>(new SimpleAllocator(pool));
  }
  if (allocator_type == "logarithmic") {
    return shared_ptr<Allocator>(new LogarithmicAllocator(pool));
  }
  throw invalid_argument("unknown allocator type: " + allocator_type);
}


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

  auto missing_elements = expected;
  for (const auto& it : table) {
    auto missing_it = missing_elements.find(it.first);
    expect_ne(missing_it, missing_elements.end());
    expect_eq(missing_it->second, it.second);
    missing_elements.erase(missing_it);
  }
  expect_eq(true, missing_elements.empty());
}


void run_basic_test(const string& allocator_type) {
  printf("-- [%s] basic\n", allocator_type.c_str());

  shared_ptr<Pool> pool(new Pool("test-table"));
  shared_ptr<Allocator> alloc = create_allocator(pool, allocator_type);
  HashTable table(alloc, 0, 6);

  unordered_map<string, string> expected;
  size_t initial_pool_allocated = alloc->bytes_allocated();

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
  expect_eq(initial_pool_allocated, alloc->bytes_allocated());
}


void run_collision_test(const string& allocator_type) {
  printf("-- [%s] collision\n", allocator_type.c_str());

  // writing 5 keys to a 4-slot hashtable forces a collision

  shared_ptr<Pool> pool(new Pool("test-table"));
  shared_ptr<Allocator> alloc = create_allocator(pool, allocator_type);
  HashTable table(alloc, 0, 2);

  unordered_map<string, string> expected;
  size_t initial_pool_allocated = alloc->bytes_allocated();

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
  expect_eq(initial_pool_allocated, alloc->bytes_allocated());
}


void run_conditional_writes_test(const string& allocator_type) {
  printf("-- [%s] conditional writes\n", allocator_type.c_str());

  shared_ptr<Pool> pool(new Pool("test-table"));
  shared_ptr<Allocator> alloc = create_allocator(pool, allocator_type);
  HashTable table(alloc, 0, 6);

  size_t initial_pool_allocated = table.get_allocator()->bytes_allocated();
  expect_eq(0, table.size());

  expect_eq(true, table.insert("key1", 4, "value1", 6));
  expect_eq(true, table.insert("key2", 4, "value2", 6));

  // check that conditions on the same key work
  {
    HashTable::CheckRequest check("key1", 4, "value2", 6);
    expect_eq(false, table.insert("key1", 4, "value1_1", 8, &check));
    expect_eq("value1", table.at("key1", 4));

    check.value = "value1";
    expect_eq(true, table.insert("key1", 4, "value1_1", 8, &check));
    expect_eq("value1_1", table.at("key1", 4));
  }

  // check that conditions on other keys work
  {
    HashTable::CheckRequest check("key2", 4, "value1");
    expect_eq(false, table.insert("key1", 4, "value1", 6, &check));
    expect_eq("value1_1", table.at("key1", 4));

    check.value = "value2";
    expect_eq(true, table.insert("key1", 4, "value1", 6, &check));
    expect_eq("value1", table.at("key1", 4));
  }

  // check that missing conditions work
  {
    HashTable::CheckRequest check("key2", 4);
    expect_eq(false, table.insert("key3", 4, "value3", 6, &check));
    expect_eq(false, table.exists("key3", 4));
  }
  {
    HashTable::CheckRequest check("key3", 4);
    expect_eq(true, table.insert("key3", 4, "value3", 6, &check));
    expect_eq("value3", table.at("key3", 4));
  }

  // check that conditional deletes work
  {
    HashTable::CheckRequest check("key1", 4, "value2", 6);
    expect_eq(false, table.erase("key1", 4, &check));
    expect_eq("value1", table.at("key1", 4));

    check.value = "value1";
    expect_eq(true, table.erase("key1", 4, &check));
    expect_eq(false, table.exists("key1", 4));
  }

  {
    HashTable::CheckRequest check("key3", 4);
    expect_eq(false, table.erase("key2", 4, &check));
    expect_eq("value2", table.at("key2", 4));
  }
  {
    HashTable::CheckRequest check("key1", 4);
    expect_eq(true, table.erase("key2", 4, &check));
    expect_eq(false, table.exists("key2", 4));
  }
  expect_eq(true, table.erase("key3", 4));

  // the empty table should not leak any allocated memory
  expect_eq(0, table.size());
  expect_eq(initial_pool_allocated, table.get_allocator()->bytes_allocated());
}


template <typename T>
static bool insert_typed(HashTable& table, const char* key, T v) {
  return table.insert(key, strlen(key), &v, sizeof(v));
}

template <typename T>
static T at_typed(HashTable& table, const char* key) {
  string ret = table.at(key, strlen(key));
  if (ret.size() != sizeof(T)) {
    throw out_of_range("key didn\'t match expected size");
  }
  return *(const T*)ret.data();
}

void run_incr_test(const string& allocator_type) {
  printf("-- [%s] incr\n", allocator_type.c_str());

  shared_ptr<Pool> pool(new Pool("test-table"));
  shared_ptr<Allocator> alloc = create_allocator(pool, allocator_type);
  HashTable table(alloc, 0, 6);

  size_t initial_pool_allocated = table.get_allocator()->bytes_allocated();

  // add some keys
  expect_eq(0, table.size());
  expect_eq(true, insert_typed<int8_t>(table, "int8", 40));
  expect_eq(true, insert_typed<int16_t>(table, "int16", 4000));
  expect_eq(true, insert_typed<int32_t>(table, "int32", 60000000));
  expect_eq(true, insert_typed<int64_t>(table, "int64", 800000000000000));
  expect_eq(true, insert_typed<float>(table, "float", 10.0));
  expect_eq(true, insert_typed<double>(table, "double", 15.5));
  expect_eq(true, table.insert("string", 6, "7 bytes", 7));
  expect_eq(7, table.size());

  // incr should create the key if it doesn't exist
  expect_eq(-10, table.incr("int8-2", (int64_t)-10));
  expect_eq(-4000, table.incr("int16-2", (int64_t)-4000));
  expect_eq(-60000000, table.incr("int32-2", (int64_t)-60000000));
  expect_eq(-800000000000000, table.incr("int64-2", (int64_t)-800000000000000));
  expect_eq(-10.0, table.incr("float-2", -10.0));
  expect_eq(-15.5, table.incr("double-2", -15.5));
  expect_eq(13, table.size());

  // all the keys should have the values we set, but the keys created by incr
  // should all be 64 bits
  expect_eq(40, at_typed<int8_t>(table, "int8"));
  expect_eq(4000, at_typed<int16_t>(table, "int16"));
  expect_eq(60000000, at_typed<int32_t>(table, "int32"));
  expect_eq(800000000000000, at_typed<int64_t>(table, "int64"));
  expect_eq(10.0, at_typed<float>(table, "float"));
  expect_eq(15.5, at_typed<double>(table, "double"));
  expect_eq(-10, at_typed<int64_t>(table, "int8-2"));
  expect_eq(-4000, at_typed<int64_t>(table, "int16-2"));
  expect_eq(-60000000, at_typed<int64_t>(table, "int32-2"));
  expect_eq(-800000000000000, at_typed<int64_t>(table, "int64-2"));
  expect_eq(-10.0, at_typed<double>(table, "float-2"));
  expect_eq(-15.5, at_typed<double>(table, "double-2"));
  expect_eq(13, table.size());

  // incr should return the new value of the key
  expect_eq(44, table.incr("int8", (int64_t)4));
  expect_eq(4010, table.incr("int16", (int64_t)10));
  expect_eq(60000100, table.incr("int32", (int64_t)100));
  expect_eq(800000000001000, table.incr("int64", (int64_t)1000));
  expect_eq(30.0, table.incr("float", 20.0));
  expect_eq(25.5, table.incr("double", 10.0));
  expect_eq(-14, table.incr("int8-2", (int64_t)-4));
  expect_eq(-4010, table.incr("int16-2", (int64_t)-10));
  expect_eq(-60000100, table.incr("int32-2", (int64_t)-100));
  expect_eq(-800000000001000, table.incr("int64-2", (int64_t)-1000));
  expect_eq(-30.0, table.incr("float-2", -20.0));
  expect_eq(-25.5, table.incr("double-2", -10.0));
  expect_eq(13, table.size());

  // test incr() on keys of the wrong type
  try {
    table.incr("string", 6, (int64_t)14);
    expect(false);
  } catch (const out_of_range& e) { }
  try {
    table.incr("string", 6, 15.0);
    expect(false);
  } catch (const out_of_range& e) { }

  // we're done here
  table.clear();
  expect_eq(0, table.size());

  // the empty table should not leak any allocated memory
  expect_eq(initial_pool_allocated, table.get_allocator()->bytes_allocated());
}


void run_concurrent_readers_test(const string& allocator_type) {
  printf("-- [%s] concurrent readers\n", allocator_type.c_str());

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
    shared_ptr<Allocator> alloc = create_allocator(pool, allocator_type);
    HashTable table(alloc, 0, 4);

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
      usleep(1); // yield to other processes
    } while ((value < 110) && (now() < (start_time + 1000000)));

    // we succeeded if we saw all the values from 100 to 110
    _exit(value != 110);

  } else {
    // parent process: write the key, then wait for children to terminate
    shared_ptr<Pool> pool(new Pool("test-table"));
    shared_ptr<Allocator> alloc = create_allocator(pool, allocator_type);
    HashTable table(alloc, 0, 4);

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
        printf("-- [%s]   child %d terminated successfully\n",
            allocator_type.c_str(), exited_pid);
      } else {
        printf("-- [%s]   child %d failed (%d)\n", allocator_type.c_str(),
            exited_pid, exit_status);
        num_failures++;
      }
    }

    expect_eq(true, child_pids.empty());
    expect_eq(0, num_failures);
  }
}



int main(int argc, char* argv[]) {
  int retcode = 0;

  vector<string> allocator_types({"simple", "logarithmic"});
  try {
    for (const string& allocator_type : allocator_types) {
      Pool::delete_pool("test-table");
      run_basic_test(allocator_type);
      Pool::delete_pool("test-table");
      run_conditional_writes_test(allocator_type);
      Pool::delete_pool("test-table");
      run_collision_test(allocator_type);
      Pool::delete_pool("test-table");
      run_incr_test(allocator_type);
      Pool::delete_pool("test-table");
      run_concurrent_readers_test(allocator_type);
    }
    printf("all tests passed\n");

  } catch (const exception& e) {
    printf("failure: %s\n", e.what());
    retcode = 1;
  }
  //Pool::delete_pool("test-table");

  return retcode;
}
