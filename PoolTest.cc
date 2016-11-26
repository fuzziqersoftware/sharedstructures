#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/mman.h>
#include <unistd.h>

#include <phosg/UnitTest.hh>
#include <string>

#include "Pool.hh"

using namespace std;
using namespace sharedstructures;


class TestClass {
public:
  TestClass() : member_variable(1) {
    this->instance_count++;
  }
  ~TestClass() {
    this->instance_count--;
  }

  size_t member_variable;

  static size_t instance_count;
};

size_t TestClass::instance_count;


void check_fill_area(void* ptr, size_t size) {
  uint8_t* data = (uint8_t*)ptr;
  for (size_t x = 0; x < size; x++) {
    data[x] = x & 0xFF;
  }
  for (size_t x = 0; x < size; x++) {
    expect_eq(x & 0xFF, data[x]);
  }
}


void run_basic_test() {
  printf("-- basic\n");

  Pool pool("test-pool", 1024 * 1024); // 1MB
  size_t orig_free_bytes = pool.bytes_free();
  expect_eq(0, pool.bytes_allocated());
  expect_ne(0, orig_free_bytes);
  expect_eq(0, pool.base_object_offset());
  expect_eq(4096, pool.size());

  // basic allocate/free
  uint64_t off = pool.allocate(100);
  expect_ne(0, off);
  expect_eq(100, pool.block_size(off));
  expect_eq(100, pool.bytes_allocated());
  expect_lt(pool.bytes_free(), orig_free_bytes - 100);
  expect_eq(0, pool.base_object_offset());

  // make sure the block is writable, lolz
  char* data = pool.at<char>(off);
  data[0] = 1;
  pool.free(off);
  expect_eq(0, pool.bytes_allocated());
  expect_eq(orig_free_bytes, pool.bytes_free());
  expect_eq(0, pool.base_object_offset());

  // make sure allocate_object/free_object call constructors/destructors
  expect_eq(0, TestClass::instance_count);
  TestClass* t = pool.at<TestClass>(pool.allocate_object<TestClass>());
  expect_eq(1, TestClass::instance_count);
  pool.free_object<TestClass>(pool.at(t));
  expect_eq(0, TestClass::instance_count);

  // allocate 128KB (this should cause an expansion)
  off = pool.allocate(1024 * 128);
  expect_ne(0, off);
  expect_eq(1024 * 128, pool.block_size(off));
  expect_lt(1024 * 128, pool.size());
  pool.free(off);
  expect_eq(1024 * 128 + 4096, pool.size());
}

void run_expansion_boundary_test_with_size(Pool& pool, size_t size) {
  size_t free_bytes = pool.bytes_free();
  uint64_t data = pool.allocate(size);
  check_fill_area(pool.at<void>(data), size);
  pool.free(data);
  expect_eq(free_bytes, pool.bytes_free());
}

void run_expansion_boundary_test() {
  printf("-- expansion boundaries\n");

  Pool pool("test-pool", 1024 * 1024); // 1MB
  run_expansion_boundary_test_with_size(pool, pool.bytes_free() - 0x20);
  run_expansion_boundary_test_with_size(pool, pool.bytes_free() - 0x18);
  run_expansion_boundary_test_with_size(pool, pool.bytes_free() - 0x10);
  run_expansion_boundary_test_with_size(pool, pool.bytes_free() - 0x08);
  run_expansion_boundary_test_with_size(pool, pool.bytes_free() + 0x00);
  run_expansion_boundary_test_with_size(pool, pool.bytes_free() + 0x08);
}


int main(int argc, char* argv[]) {
  int retcode = 0;

  try {
    Pool::delete_pool("test-pool");
    run_basic_test();
    run_expansion_boundary_test();
    printf("all tests passed\n");

  } catch (const exception& e) {
    printf("failure: %s\n", e.what());
    retcode = 1;
  }
  Pool::delete_pool("test-pool");

  return retcode;
}
