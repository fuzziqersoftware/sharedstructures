#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/mman.h>

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


void run_basic_test() {
  printf("-- basic\n");

  HashTable table("test-table");

  expect_eq(0, table.size());
  expect_ne(0, table.pool_size());
  expect_eq(0, table.pool_size() & 4095);

  expect_eq(true, table.insert("key1", 4, "value1", 6));
  expect_eq(1, table.size());
  expect_eq(true, table.insert("key2", 4, "value2", 6));
  expect_eq(2, table.size());
  expect_eq(true, table.insert("key3", 4, "value3", 6));
  expect_eq(3, table.size());

  expect_eq("value1", table.at("key1", 4));
  expect_eq("value2", table.at("key2", 4));
  expect_eq("value3", table.at("key3", 4));
  expect_eq(3, table.size());

  expect_eq(true, table.erase("key2", 4));
  expect_eq(2, table.size());

  expect_eq("value1", table.at("key1", 4));
  expect_key_missing(table, "key2", 4);
  expect_eq("value3", table.at("key3", 4));
  expect_eq(2, table.size());

  expect_eq(false, table.insert("key1", 4, "value0", 6, false));
  expect_eq(2, table.size());

  expect_eq("value1", table.at("key1", 4));
  expect_key_missing(table, "key2", 4);
  expect_eq("value3", table.at("key3", 4));
  expect_eq(2, table.size());

  expect_eq(true, table.insert("key1", 4, "value0", 6, true));
  expect_eq(2, table.size());

  expect_eq("value0", table.at("key1", 4));
  expect_key_missing(table, "key2", 4);
  expect_eq("value3", table.at("key3", 4));
  expect_eq(2, table.size());
}


int main(int argc, char* argv[]) {
  int retcode = 0;

  try {
    Pool::delete_pool("test-table");
    run_basic_test();
    printf("all tests passed\n");

  } catch (const exception& e) {
    printf("failure: %s\n", e.what());
    retcode = 1;
  }
  Pool::delete_pool("test-table");

  return retcode;
}
