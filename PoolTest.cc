#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <phosg/Process.hh>
#include <phosg/Time.hh>
#include <phosg/UnitTest.hh>
#include <string>

#include "Pool.hh"

using namespace std;
using namespace sharedstructures;


class TestClass {
public:
  TestClass() : member_variable(1), f_calls(0), const_f_calls(0) {
    this->instance_count++;
  }
  ~TestClass() {
    this->instance_count--;
  }

  void f() {
    this->f_calls++;
  }

  void f() const {
    this->const_f_calls++;
  }

  size_t member_variable;
  size_t f_calls;
  mutable size_t const_f_calls;

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
  check_fill_area(data, 100);
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

void run_smart_pointer_test() {
  printf("-- smart pointer\n");

  Pool pool("test-pool", 1024 * 1024); // 1MB

  // make sure allocate_object/free_object call constructors/destructors
  expect_eq(0, TestClass::instance_count);
  auto t = pool.allocate_object_ptr<TestClass>();
  expect_eq(1, TestClass::instance_count);

  expect_eq(0, t->f_calls);
  expect_eq(0, t->const_f_calls);
  t->f();
  expect_eq(1, t->f_calls);
  expect_eq(0, t->const_f_calls);

  const Pool::PoolPointer<TestClass> t2 = t;
  expect_eq(1, t2->f_calls);
  expect_eq(0, t2->const_f_calls);
  t2->f();
  expect_eq(1, t->f_calls);
  expect_eq(1, t->const_f_calls);

  pool.free_object_ptr<TestClass>(t);
  expect_eq(0, TestClass::instance_count);
}

void run_expansion_boundary_test_with_size(Pool& pool, size_t size) {
  size_t free_bytes = pool.bytes_free();
  uint64_t data = pool.allocate(size);
  check_fill_area(pool.at<void>(data), size);
  pool.free(data);
  expect_le(free_bytes, pool.bytes_free());
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

void run_lock_test() {
  printf("-- lock\n");

  Pool pool("test-pool", 1024 * 1024); // 1MB

  // these values are implementation-dependent, sigh
  uint64_t* lock_ptr = pool.at<uint64_t>(8);
  uint64_t locked_value = (this_process_start_time() << 20) | getpid_cached();
  expect_eq(0, *lock_ptr);
  {
    auto g = pool.lock();
    expect_eq(locked_value, *lock_ptr);
  }
  expect_eq(0, *lock_ptr);

  uint64_t start_time = now();
  pid_t child_pid = fork();
  if (!child_pid) {
    {
      auto g = pool.lock();
      usleep(1000000);
    }
    _exit(0);
  }
  usleep(100000); // give the child time to take the lock

  // we should have to wait to get the lock; the child process is holding it
  uint64_t end_time;
  {
    auto g = pool.lock();
    end_time = now();
  }
  expect_ge(end_time - start_time, 1000000);

  // make sure the child exited cleanly
  int exit_status;
  expect_eq(child_pid, waitpid(child_pid, &exit_status, 0));
  expect_eq(true, WIFEXITED(exit_status));
  expect_eq(0, WEXITSTATUS(exit_status));
}

void run_crash_test() {
  printf("-- crash\n");

  uint64_t bytes_allocated;
  uint64_t bytes_free;
  unordered_map<uint64_t, string> offset_to_data;
  {
    Pool pool("test-pool", 1024 * 1024);

    while (offset_to_data.size() < 100) {
      auto g = pool.lock();
      uint64_t offset = pool.allocate(2048);

      string data;
      while (data.size() < 2048) {
        data += (char)rand();
      }

      memcpy(pool.at<void>(offset), data.data(), data.size());

      offset_to_data.emplace(offset, move(data));
    }

    bytes_allocated = pool.bytes_allocated();
    bytes_free = pool.bytes_free();
  }

  // child: open a pool, lock it, and be killed with SIGKILL
  pid_t pid = fork();
  if (!pid) {
    Pool pool("test-pool", 1024 * 1024);
    auto g = pool.lock();

    sigset_t sigs;
    sigemptyset(&sigs);
    for (;;) {
      sigsuspend(&sigs);
    }
  }

  // parent: wait a bit, kill the child with SIGKILL, make sure the pool is
  // still consistent (and not locked)
  Pool pool("test-pool", 1024 * 1024); // 1MB
  try {
    usleep(500000);
    expect_eq(true, pool.is_locked());
  } catch (const exception& e) {
    kill(pid, SIGKILL);
    throw;
  }
  kill(pid, SIGKILL);

  // note: on linux, we can still get the start_time of a zombie process, so we
  // wait() on it here
  int exit_status;
  expect_eq(pid, waitpid(pid, &exit_status, 0));
  expect_eq(true, WIFSIGNALED(exit_status));
  expect_eq(SIGKILL, WTERMSIG(exit_status));

  // even though the pool is still locked, we should be able to lock the pool
  // because the child was killed
  expect_eq(true, pool.is_locked());
  auto g = pool.lock();
  expect_eq(bytes_allocated, pool.bytes_allocated());
  expect_eq(bytes_free, pool.bytes_free());
  for (const auto& it : offset_to_data) {
    expect_eq(0, memcmp(pool.at<void>(it.first), it.second.data(),
        it.second.size()));
    pool.free(it.first);
  }
}


int main(int argc, char* argv[]) {
  int retcode = 0;

  try {
    Pool::delete_pool("test-pool");
    run_basic_test();
    run_smart_pointer_test();
    run_expansion_boundary_test();
    run_lock_test();
    run_crash_test();
    printf("all tests passed\n");

  } catch (const exception& e) {
    printf("failure: %s\n", e.what());
    retcode = 1;
  }
  Pool::delete_pool("test-pool");

  return retcode;
}
