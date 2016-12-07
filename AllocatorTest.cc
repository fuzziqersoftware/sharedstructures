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

#include "SimpleAllocator.hh"

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


shared_ptr<Allocator> create_allocator(shared_ptr<Pool> pool,
    const string& allocator_type) {
  if (allocator_type == "simple") {
    return shared_ptr<Allocator>(new SimpleAllocator(pool));
  }
  throw invalid_argument("unknown allocator type: " + allocator_type);
}


void run_basic_test(const string& allocator_type) {
  printf("-- basic\n");

  shared_ptr<Pool> pool(new Pool("test-pool", 1024 * 1024));
  auto alloc = create_allocator(pool, allocator_type);

  size_t orig_free_bytes = alloc->bytes_free();
  expect_eq(0, alloc->bytes_allocated());
  expect_ne(0, orig_free_bytes);
  expect_eq(0, alloc->base_object_offset());
  expect_eq(4096, pool->size());

  // basic allocate/free
  uint64_t off = alloc->allocate(100);
  expect_ne(0, off);
  expect_eq(100, alloc->block_size(off));
  expect_eq(100, alloc->bytes_allocated());
  expect_lt(alloc->bytes_free(), orig_free_bytes - 100);
  expect_eq(0, alloc->base_object_offset());

  // make sure the block is writable, lolz
  char* data = pool->at<char>(off);
  check_fill_area(data, 100);
  alloc->free(off);
  expect_eq(0, alloc->bytes_allocated());
  expect_eq(orig_free_bytes, alloc->bytes_free());
  expect_eq(0, alloc->base_object_offset());

  // make sure allocate_object/free_object call constructors/destructors
  expect_eq(0, TestClass::instance_count);
  TestClass* t = pool->at<TestClass>(alloc->allocate_object<TestClass>());
  expect_eq(1, TestClass::instance_count);
  alloc->free_object<TestClass>(pool->at(t));
  expect_eq(0, TestClass::instance_count);

  // allocate 128KB (this should cause an expansion)
  off = alloc->allocate(1024 * 128);
  expect_ne(0, off);
  expect_eq(1024 * 128, alloc->block_size(off));
  expect_lt(1024 * 128, pool->size());
  alloc->free(off);
  expect_eq(1024 * 128 + 4096, pool->size());
}

void run_smart_pointer_test(const string& allocator_type) {
  printf("-- smart pointer\n");

  shared_ptr<Pool> pool(new Pool("test-pool", 1024 * 1024));
  auto alloc = create_allocator(pool, allocator_type);

  // make sure allocate_object/free_object call constructors/destructors
  expect_eq(0, TestClass::instance_count);
  auto t = alloc->allocate_object_ptr<TestClass>();
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

  alloc->free_object_ptr<TestClass>(t);
  expect_eq(0, TestClass::instance_count);
}

void run_expansion_boundary_test_with_size(shared_ptr<Allocator>& alloc,
    size_t size) {
  size_t free_bytes = alloc->bytes_free();
  uint64_t data = alloc->allocate(size);
  check_fill_area(alloc->get_pool()->at<void>(data), size);
  alloc->free(data);
  expect_le(free_bytes, alloc->bytes_free());
}

void run_expansion_boundary_test(const string& allocator_type) {
  printf("-- expansion boundaries\n");

  shared_ptr<Pool> pool(new Pool("test-pool", 1024 * 1024));
  auto alloc = create_allocator(pool, allocator_type);

  run_expansion_boundary_test_with_size(alloc, alloc->bytes_free() - 0x20);
  run_expansion_boundary_test_with_size(alloc, alloc->bytes_free() - 0x18);
  run_expansion_boundary_test_with_size(alloc, alloc->bytes_free() - 0x10);
  run_expansion_boundary_test_with_size(alloc, alloc->bytes_free() - 0x08);
  run_expansion_boundary_test_with_size(alloc, alloc->bytes_free() + 0x00);
  run_expansion_boundary_test_with_size(alloc, alloc->bytes_free() + 0x08);
}

void run_lock_test(const string& allocator_type) {
  printf("-- lock\n");

  shared_ptr<Pool> pool(new Pool("test-pool", 1024 * 1024));
  auto alloc = create_allocator(pool, allocator_type);

  uint64_t start_time = now();
  pid_t child_pid = fork();
  if (!child_pid) {
    {
      auto g = alloc->lock();
      usleep(1000000);
    }
    _exit(0);
  }
  usleep(100000); // give the child time to take the lock

  // we should have to wait to get the lock; the child process is holding it
  uint64_t end_time;
  {
    auto g = alloc->lock();
    end_time = now();
  }
  expect_ge(end_time - start_time, 1000000);

  // make sure the child exited cleanly
  int exit_status;
  expect_eq(child_pid, waitpid(child_pid, &exit_status, 0));
  expect_eq(true, WIFEXITED(exit_status));
  expect_eq(0, WEXITSTATUS(exit_status));
}

void run_crash_test(const string& allocator_type) {
  printf("-- crash\n");

  uint64_t bytes_allocated;
  uint64_t bytes_free;
  unordered_map<uint64_t, string> offset_to_data;
  {
    shared_ptr<Pool> pool(new Pool("test-pool", 1024 * 1024));
    auto alloc = create_allocator(pool, allocator_type);

    while (offset_to_data.size() < 100) {
      auto g = alloc->lock();
      uint64_t offset = alloc->allocate(2048);

      string data;
      while (data.size() < 2048) {
        data += (char)rand();
      }

      memcpy(pool->at<void>(offset), data.data(), data.size());

      offset_to_data.emplace(offset, move(data));
    }

    bytes_allocated = alloc->bytes_allocated();
    bytes_free = alloc->bytes_free();
  }

  // child: open a pool, lock it, and be killed with SIGKILL
  pid_t pid = fork();
  if (!pid) {
    shared_ptr<Pool> pool(new Pool("test-pool", 1024 * 1024));
    auto alloc = create_allocator(pool, allocator_type);

    auto g = alloc->lock();

    sigset_t sigs;
    sigemptyset(&sigs);
    for (;;) {
      sigsuspend(&sigs);
    }
  }

  // parent: wait a bit, kill the child with SIGKILL, make sure the pool is
  // still consistent (and not locked)
  shared_ptr<Pool> pool(new Pool("test-pool", 1024 * 1024));
  auto alloc = create_allocator(pool, allocator_type);
  try {
    usleep(500000);
    expect_eq(true, alloc->is_locked());
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
  expect_eq(true, alloc->is_locked());
  auto g = alloc->lock();
  expect_eq(bytes_allocated, alloc->bytes_allocated());
  expect_eq(bytes_free, alloc->bytes_free());
  for (const auto& it : offset_to_data) {
    expect_eq(0, memcmp(pool->at<void>(it.first), it.second.data(),
        it.second.size()));
    alloc->free(it.first);
  }
}


int main(int argc, char* argv[]) {
  int retcode = 0;

  vector<string> allocator_types({
    "simple",
  });

  try {
    for (const auto& allocator_type : allocator_types) {
      Pool::delete_pool("test-pool");
      run_basic_test(allocator_type);
      run_smart_pointer_test(allocator_type);
      run_expansion_boundary_test(allocator_type);
      run_lock_test(allocator_type);
      run_crash_test(allocator_type);
    }
    printf("all tests passed\n");

  } catch (const exception& e) {
    printf("failure: %s\n", e.what());
    retcode = 1;
  }
  Pool::delete_pool("test-pool");

  return retcode;
}
