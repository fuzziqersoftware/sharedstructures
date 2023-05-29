#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <phosg/Process.hh>
#include <phosg/Time.hh>
#include <phosg/UnitTest.hh>
#include <string>

#include "LogarithmicAllocator.hh"
#include "SimpleAllocator.hh"

using namespace std;
using namespace sharedstructures;

const string pool_name_prefix = "AllocatorTest-cc-pool-";

class TestClass {
public:
  TestClass()
      : member_variable(1),
        f_calls(0),
        const_f_calls(0) {
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
  if (allocator_type == "logarithmic") {
    return shared_ptr<Allocator>(new LogarithmicAllocator(pool));
  }
  throw invalid_argument("unknown allocator type: " + allocator_type);
}

void run_basic_test(const string& allocator_type) {
  printf("-- [%s] basic\n", allocator_type.c_str());

  shared_ptr<Pool> pool(new Pool(pool_name_prefix + allocator_type, 1024 * 1024));
  auto alloc = create_allocator(pool, allocator_type);
  auto g = alloc->lock(true);

  size_t orig_free_bytes = alloc->bytes_free();
  expect_eq(0, alloc->bytes_allocated());
  expect_ne(0, orig_free_bytes);
  expect_eq(0, alloc->base_object_offset());
  expect_eq(4096, pool->size());

  // Basic allocate/free
  uint64_t off = alloc->allocate(100);
  expect_ne(0, off);
  expect_eq(100, alloc->block_size(off));
  expect_eq(100, alloc->bytes_allocated());
  expect_lt(alloc->bytes_free(), orig_free_bytes - 100);
  expect_eq(0, alloc->base_object_offset());

  // Make sure the block is writable
  char* data = pool->at<char>(off);
  check_fill_area(data, 100);
  alloc->free(off);
  expect_eq(0, alloc->bytes_allocated());
  expect_eq(orig_free_bytes, alloc->bytes_free());
  expect_eq(0, alloc->base_object_offset());

  // Make sure allocate_object/free_object call constructors/destructors
  expect_eq(0, TestClass::instance_count);
  TestClass* t = pool->at<TestClass>(alloc->allocate_object<TestClass>());
  expect_eq(1, TestClass::instance_count);
  alloc->free_object<TestClass>(pool->at(t));
  expect_eq(0, TestClass::instance_count);

  // Allocate 8KB (this should cause an expansion)
  off = alloc->allocate(1024 * 8);
  expect_ne(0, off);
  expect_eq(1024 * 8, alloc->block_size(off));
  expect_lt(1024 * 8, pool->size());
  alloc->free(off);
  expect_ge(pool->size(), 1024 * 8 + 4096);
}

void run_smart_pointer_test(const string& allocator_type) {
  printf("-- [%s] smart pointer\n", allocator_type.c_str());

  shared_ptr<Pool> pool(new Pool(pool_name_prefix + allocator_type, 1024 * 1024));
  auto alloc = create_allocator(pool, allocator_type);
  auto g = alloc->lock(true);

  // Make sure allocate_object/free_object call constructors/destructors
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
  printf("-- [%s] expansion boundaries\n", allocator_type.c_str());

  shared_ptr<Pool> pool(new Pool(pool_name_prefix + allocator_type, 1024 * 1024));
  auto alloc = create_allocator(pool, allocator_type);
  auto g = alloc->lock(true);

  run_expansion_boundary_test_with_size(alloc, alloc->bytes_free() - 0x20);
  run_expansion_boundary_test_with_size(alloc, alloc->bytes_free() - 0x18);
  run_expansion_boundary_test_with_size(alloc, alloc->bytes_free() - 0x10);
  run_expansion_boundary_test_with_size(alloc, alloc->bytes_free() - 0x08);
  run_expansion_boundary_test_with_size(alloc, alloc->bytes_free() + 0x00);
}

void run_lock_test(const string& allocator_type) {
  printf("-- [%s] lock\n", allocator_type.c_str());

  shared_ptr<Pool> pool(new Pool(pool_name_prefix + allocator_type, 1024 * 1024));
  auto alloc = create_allocator(pool, allocator_type);

  uint64_t start_time = now();
  pid_t child_pid = fork();
  if (!child_pid) {
    {
      auto g = alloc->lock(true);
      usleep(1000000);
    }
    _exit(0);
  }
  fprintf(stderr, "parent waiting\n");
  usleep(100000); // Give the child time to take the lock

  // We should have to wait to get the lock; the child process is holding it
  uint64_t end_time;
  {
    fprintf(stderr, "parent locking\n");
    auto g = alloc->lock(true);
    fprintf(stderr, "parent holds lock\n");
    end_time = now();
  }
  expect_ge(end_time - start_time, 1000000);

  // Make sure the child exited cleanly
  int exit_status;
  expect_eq(child_pid, waitpid(child_pid, &exit_status, 0));
  expect_eq(true, WIFEXITED(exit_status));
  expect_eq(0, WEXITSTATUS(exit_status));
}

void run_crash_test(const string& allocator_type) {
  printf("-- [%s] crash\n", allocator_type.c_str());

  uint64_t bytes_allocated;
  uint64_t bytes_free;
  unordered_map<uint64_t, string> offset_to_data;
  {
    shared_ptr<Pool> pool(new Pool(pool_name_prefix + allocator_type, 1024 * 1024));
    auto alloc = create_allocator(pool, allocator_type);

    while (offset_to_data.size() < 100) {
      auto g = alloc->lock(true);
      expect_eq(0, g.stolen_token());
      uint64_t offset = alloc->allocate(2048);

      string data;
      while (data.size() < 2048) {
        data += (char)rand();
      }

      memcpy(pool->at<void>(offset), data.data(), data.size());

      offset_to_data.emplace(offset, std::move(data));
    }

    bytes_allocated = alloc->bytes_allocated();
    bytes_free = alloc->bytes_free();
  }

  // Child: open a pool, lock it, and be killed with SIGKILL
  pid_t pid = fork();
  if (!pid) {
    shared_ptr<Pool> pool(new Pool(pool_name_prefix + allocator_type, 1024 * 1024));
    auto alloc = create_allocator(pool, allocator_type);

    auto g = alloc->lock(true);
    expect_eq(0, g.stolen_token());

    sigset_t sigs;
    sigemptyset(&sigs);
    for (;;) {
      sigsuspend(&sigs);
    }
  }

  // Parent: wait a bit, kill the child with SIGKILL, make sure the pool is
  // still consistent (and not locked)
  shared_ptr<Pool> pool(new Pool(pool_name_prefix + allocator_type, 1024 * 1024));
  auto alloc = create_allocator(pool, allocator_type);
  try {
    usleep(500000);
    expect_eq(true, alloc->is_locked(true));
  } catch (const exception& e) {
    kill(pid, SIGKILL);
    throw;
  }
  kill(pid, SIGKILL);

  // Note: on linux, we can still get the start_time of a zombie process (and
  // is how sharedstructures knows to clean up the lock), so wait() on it here
  int exit_status;
  expect_eq(pid, waitpid(pid, &exit_status, 0));
  expect_eq(true, WIFSIGNALED(exit_status));
  expect_eq(SIGKILL, WTERMSIG(exit_status));

  // Even though the pool is still locked, we should be able to lock the pool
  // because the child was killed, and the lock should show that it was stolen
  expect_eq(true, alloc->is_locked(true));
  auto g = alloc->lock(true);
  expect_ne(0, g.stolen_token());
  expect_eq(bytes_allocated, alloc->bytes_allocated());
  expect_eq(bytes_free, alloc->bytes_free());
  for (const auto& it : offset_to_data) {
    expect_eq(0, memcmp(pool->at<void>(it.first), it.second.data(), it.second.size()));
    alloc->free(it.first);
  }
}

int main(int, char**) {
  int retcode = 0;

  vector<string> allocator_types({
      "simple",
      "logarithmic",
  });

  try {
    for (const auto& allocator_type : allocator_types) {
      Pool::delete_pool(pool_name_prefix + allocator_type);
      run_basic_test(allocator_type);
      run_smart_pointer_test(allocator_type);
      run_expansion_boundary_test(allocator_type);
      run_lock_test(allocator_type);
      run_crash_test(allocator_type);
    }
    printf("all tests passed\n");

    // Only delete the pool if the tests pass; if they don't, we might want to
    // examine its contents
    for (const auto& allocator_type : allocator_types) {
      Pool::delete_pool(pool_name_prefix + allocator_type);
    }

  } catch (const exception& e) {
    printf("failure: %s\n", e.what());
    retcode = 1;
  }

  return retcode;
}
