#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/wait.h>

#include <phosg/Time.hh>
#include <phosg/UnitTest.hh>
#include <string>

#include "PrefixTree.hh"

using namespace std;

using namespace sharedstructures;


shared_ptr<PrefixTree> get_or_create_tree(const char* name) {
  shared_ptr<Pool> pool(new Pool(name));
  return shared_ptr<PrefixTree>(new PrefixTree(pool, 0));
}


void expect_key_missing(const shared_ptr<PrefixTree> table, const void* k,
    size_t s) {
  try {
    table->at(k, s);
    expect(false);
  } catch (const out_of_range& e) { }
}


void verify_state(
    const unordered_map<string, PrefixTree::LookupResult>& expected,
    const shared_ptr<PrefixTree> table,
    size_t expected_node_size) {
  expect_eq(expected.size(), table->size());
  expect_eq(expected_node_size, table->node_size());
  for (const auto& it : expected) {
    expect_eq(it.second, table->at(it.first.data(), it.first.size()));
  }

  auto missing_elements = expected;
  for (const auto& it : *table) {
    auto missing_it = missing_elements.find(it.first);
    expect_ne(missing_it, missing_elements.end());
    expect_eq(missing_it->second, it.second);
    missing_elements.erase(missing_it);
  }
  expect_eq(true, missing_elements.empty());
}


void run_basic_test() {
  printf("-- basic\n");

  auto table = get_or_create_tree("test-table");

  size_t initial_pool_allocated = table->get_pool()->bytes_allocated();
  expect_eq(0, table->size());

  table->insert("key1", 4, "value1", 6);
  expect_eq(1, table->size());
  expect_eq(4, table->node_size());
  table->insert("key2", 4, "value2", 6);
  expect_eq(2, table->size());
  expect_eq(4, table->node_size());
  table->insert("key3", 4, "value3", 6);
  expect_eq(3, table->size());
  expect_eq(4, table->node_size());

  PrefixTree::LookupResult r;
  r.type = PrefixTree::ResultValueType::String;
  r.as_string = "value1";
  expect_eq(r, table->at("key1", 4));
  r.as_string = "value2";
  expect_eq(r, table->at("key2", 4));
  r.as_string = "value3";
  expect_eq(r, table->at("key3", 4));
  expect_eq(3, table->size());
  expect_eq(4, table->node_size());

  expect_eq(true, table->erase("key2", 4));
  expect_eq(2, table->size());
  expect_eq(4, table->node_size());
  expect_eq(false, table->erase("key2", 4));
  expect_eq(2, table->size());
  expect_eq(4, table->node_size());

  r.as_string = "value1";
  expect_eq(r, table->at("key1", 4));
  expect_key_missing(table, "key2", 4);
  r.as_string = "value3";
  expect_eq(r, table->at("key3", 4));
  expect_eq(2, table->size());
  expect_eq(4, table->node_size());

  table->insert("key1", 4, "value0", 6);
  expect_eq(2, table->size());
  expect_eq(4, table->node_size());

  r.as_string = "value0";
  expect_eq(r, table->at("key1", 4));
  expect_key_missing(table, "key2", 4);
  r.as_string = "value3";
  expect_eq(r, table->at("key3", 4));
  expect_eq(2, table->size());
  expect_eq(4, table->node_size());

  expect_eq(true, table->erase("key1", 4));
  expect_eq(1, table->size());
  expect_eq(4, table->node_size());
  expect_eq(true, table->erase("key3", 4));
  expect_eq(0, table->size());
  expect_eq(1, table->node_size());

  // the empty table should not leak any allocated memory
  expect_eq(initial_pool_allocated, table->get_pool()->bytes_allocated());
}

void run_reorganization_test() {
  printf("-- reorganization\n");

  auto table = get_or_create_tree("test-table");

  size_t initial_pool_allocated = table->get_pool()->bytes_allocated();

  // initial state: empty
  unordered_map<string, PrefixTree::LookupResult> expected_state;
  verify_state(expected_state, table, 1);

  // <> null
  //   a null
  //     b null
  //       (c) "abc"
  table->insert("abc", 3, "abc", 3);
  expected_state.emplace("abc", "abc");
  verify_state(expected_state, table, 3);

  // <> null
  //   a null
  //     b "ab"
  //       (c) "abc"
  table->insert("ab", 2, "ab", 2);
  expected_state.emplace("ab", "ab");
  verify_state(expected_state, table, 3);

  // <> null
  //   a null
  //     (b) "ab"
  table->erase("abc", 3);
  expected_state.erase("abc");
  verify_state(expected_state, table, 2);

  // <> ""
  //   a null
  //     (b) "ab"
  table->insert("", 0, "", 0);
  expected_state.emplace("", "");
  verify_state(expected_state, table, 2);

  // <> ""
  //   a null
  //     b "ab"
  //       c null
  //         (d) "abcd"
  table->insert("abcd", 4, "abcd", 4);
  expected_state.emplace("abcd", "abcd");
  verify_state(expected_state, table, 4);

  // <> ""
  //   a null
  //     b null
  //       c null
  //         (d) "abcd"
  table->erase("ab", 2);
  expected_state.erase("ab");
  verify_state(expected_state, table, 4);

  // <> ""
  //   a null
  //     b null
  //       c null
  //         d "abcd"
  //           (e) "abcde"
  table->insert("abcde", 5, "abcde", 5);
  expected_state.emplace("abcde", "abcde");
  verify_state(expected_state, table, 5);

  // <> ""
  //   a null
  //     b null
  //       c null
  //         d "abcd"
  //           (e) "abcde"
  //           (f) "abcdf"
  table->insert("abcdf", 5, "abcdf", 5);
  expected_state.emplace("abcdf", "abcdf");
  verify_state(expected_state, table, 5);

  // <> ""
  //   a null
  //     b null
  //       c null
  //         d "abcd"
  //           (e) "abcde"
  //           (f) "abcdf"
  //         (e) "abce"
  table->insert("abce", 4, "abce", 4);
  expected_state.emplace("abce", "abce");
  verify_state(expected_state, table, 5);

  // <> ""
  //   a null
  //     b null
  //       c null
  //         d "abcd"
  //           (e) "abcde"
  //           (f) "abcdf"
  //         e "abce"
  //           (f) "abcef"
  table->insert("abcef", 5, "abcef", 5);
  expected_state.emplace("abcef", "abcef");
  verify_state(expected_state, table, 6);

  // <> null
  table->clear();
  expected_state.clear();
  verify_state(expected_state, table, 1);

  // the empty table should not leak any allocated memory
  expect_eq(initial_pool_allocated, table->get_pool()->bytes_allocated());
}

void run_types_test() {
  printf("-- types\n");

  // uncomment this to easily see the difference between result types
  // TODO: make this a helper function or something
  //PrefixTree::LookupResult l(2.38);
  //auto r = table->at("key-double", 10);
  //string ls = l.str(), rs = r.str();
  //fprintf(stderr, "%s vs %s\n", ls.c_str(), rs.c_str());

  auto table = get_or_create_tree("test-table");

  size_t initial_pool_allocated = table->get_pool()->bytes_allocated();

  expect_eq(0, table->size());
  expect_eq(1, table->node_size());

  // write a bunch of keys of different types
  table->insert("key-string", 10, "value-string", 12);
  table->insert("key-int", 7, (int64_t)(1024 * 1024 * -3));
  table->insert("key-int-long", 12, (int64_t)0x9999999999999999);
  table->insert("key-double", 10, 2.38);
  table->insert("key-true", 8, true);
  table->insert("key-false", 9, false);
  table->insert("key-null", 8);

  expect_eq(7, table->size());
  expect_eq(32, table->node_size());

  // get their values again
  try {
    table->at("key-missing", 11);
    expect(false);
  } catch (const out_of_range& e) { }
  expect_eq(PrefixTree::LookupResult("value-string"),
      table->at("key-string", 10));
  expect_eq(PrefixTree::LookupResult((int64_t)1024 * 1024 * -3),
      table->at("key-int", 7));
  expect_eq(PrefixTree::LookupResult((int64_t)0x9999999999999999),
      table->at("key-int-long", 12));
  expect_eq(PrefixTree::LookupResult(2.38), table->at("key-double", 10));
  expect_eq(PrefixTree::LookupResult(true), table->at("key-true", 8));
  expect_eq(PrefixTree::LookupResult(false), table->at("key-false", 9));
  expect_eq(PrefixTree::LookupResult(), table->at("key-null", 8));

  // make sure type() returns the same types as at()
  // note: type() doesn't throw for missing keys, but at() does
  expect_eq(PrefixTree::ResultValueType::Missing,
      table->type("key-missing", 11));
  expect_eq(PrefixTree::ResultValueType::String, table->type("key-string", 10));
  expect_eq(PrefixTree::ResultValueType::Int, table->type("key-int", 7));
  expect_eq(PrefixTree::ResultValueType::Int, table->type("key-int-long", 12));
  expect_eq(PrefixTree::ResultValueType::Double,
      table->type("key-double", 10));
  expect_eq(PrefixTree::ResultValueType::Bool, table->type("key-true", 8));
  expect_eq(PrefixTree::ResultValueType::Bool, table->type("key-false", 9));
  expect_eq(PrefixTree::ResultValueType::Null, table->type("key-null", 8));

  // make sure exists() returns true for all the keys we expect
  expect_eq(false, table->exists("key-missing", 11));
  expect_eq(true, table->exists("key-string", 10));
  expect_eq(true, table->exists("key-int", 7));
  expect_eq(true, table->exists("key-int-long", 12));
  expect_eq(true, table->exists("key-double", 10));
  expect_eq(true, table->exists("key-true", 8));
  expect_eq(true, table->exists("key-false", 9));
  expect_eq(true, table->exists("key-null", 8));

  table->clear();
  expect_eq(0, table->size());
  expect_eq(1, table->node_size());

  // the empty table should not leak any allocated memory
  expect_eq(initial_pool_allocated, table->get_pool()->bytes_allocated());
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
    auto table = get_or_create_tree("test-table");

    int64_t value = 100;
    uint64_t start_time = now();
    do {
      try {
        auto res = table->at("key1", 4);
        if (res == PrefixTree::LookupResult((int64_t)value)) {
          value++;
        }
      } catch (const out_of_range& e) {
      }
    } while ((value < 110) && (now() < (start_time + 1000000)));

    // we succeeded if we saw all the values from 100 to 110
    _exit(value != 110);

  } else {
    // parent process: write the key, then wait for children to terminate
    auto table = get_or_create_tree("test-table");

    for (int64_t value = 100; value < 110; value++) {
      usleep(50000);
      table->insert("key1", 4, (int64_t)value);
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
    run_reorganization_test();
    run_types_test();
    run_concurrent_readers_test();
    printf("all tests passed\n");

  } catch (const exception& e) {
    printf("failure: %s\n", e.what());
    retcode = 1;
  }
  Pool::delete_pool("test-table");

  return retcode;
}
