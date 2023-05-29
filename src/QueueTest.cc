#define __STDC_FORMAT_MACROS
#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <phosg/Strings.hh>
#include <phosg/Time.hh>
#include <phosg/UnitTest.hh>
#include <string>

#include "LogarithmicAllocator.hh"
#include "Pool.hh"
#include "Queue.hh"
#include "SimpleAllocator.hh"

using namespace std;

using namespace sharedstructures;

const string pool_name_prefix = "QueueTest-cc-pool-";

shared_ptr<Allocator> create_allocator(
    shared_ptr<Pool> pool, const string& allocator_type) {
  if (allocator_type == "simple") {
    return shared_ptr<Allocator>(new SimpleAllocator(pool));
  }
  if (allocator_type == "logarithmic") {
    return shared_ptr<Allocator>(new LogarithmicAllocator(pool));
  }
  throw invalid_argument("unknown allocator type: " + allocator_type);
}

shared_ptr<Queue> get_or_create_queue(
    const string& name, const string& allocator_type) {
  shared_ptr<Pool> pool(new Pool(name));
  shared_ptr<Allocator> alloc = create_allocator(pool, allocator_type);
  return shared_ptr<Queue>(new Queue(alloc, 0));
}

void run_basic_tests(const string& allocator_type) {
  printf("-- [%s] basic\n", allocator_type.c_str());

  auto q = get_or_create_queue(pool_name_prefix + allocator_type, allocator_type);

  size_t initial_pool_allocated = q->get_allocator()->bytes_allocated();
  expect_eq(0, q->size());
  expect_eq(0, q->bytes());

  auto test_queue = [&](bool reverse) {
    printf("-- [%s]   %s queue operation\n", allocator_type.c_str(), reverse ? "reverse" : "forward");

    q->push(reverse, "v1");
    q->verify();
    expect_eq(1, q->size());
    expect_eq(2, q->bytes());
    q->push(reverse, "val2");
    q->verify();
    expect_eq(2, q->size());
    expect_eq(6, q->bytes());
    q->push(reverse, "value-3");
    q->verify();
    expect_eq(3, q->size());
    expect_eq(13, q->bytes());

    expect_eq("v1", q->pop(!reverse));
    q->verify();
    expect_eq(2, q->size());
    expect_eq(11, q->bytes());
    expect_eq("val2", q->pop(!reverse));
    q->verify();
    expect_eq(1, q->size());
    expect_eq(7, q->bytes());
    expect_eq("value-3", q->pop(!reverse));
    q->verify();
    expect_eq(0, q->size());
    expect_eq(0, q->bytes());
    expect_raises<out_of_range>([&]() {
      q->pop(!reverse);
    });
    q->verify();
  };

  test_queue(false);
  test_queue(true);

  auto test_stack = [&](bool front) {
    printf("-- [%s]   %s stack operation\n", allocator_type.c_str(), front ? "front" : "back");

    q->push(front, "v1");
    q->verify();
    expect_eq(1, q->size());
    expect_eq(2, q->bytes());
    q->push(front, "val2");
    q->verify();
    expect_eq(2, q->size());
    expect_eq(6, q->bytes());
    q->push(front, "value-3");
    q->verify();
    expect_eq(3, q->size());
    expect_eq(13, q->bytes());

    expect_eq("value-3", q->pop(front));
    q->verify();
    expect_eq(2, q->size());
    expect_eq(6, q->bytes());
    expect_eq("val2", q->pop(front));
    q->verify();
    expect_eq(1, q->size());
    expect_eq(2, q->bytes());
    expect_eq("v1", q->pop(front));
    q->verify();
    expect_eq(0, q->size());
    expect_eq(0, q->bytes());
    expect_raises<out_of_range>([&]() {
      q->pop(front);
    });
    q->verify();
  };

  test_stack(false);
  test_stack(true);

  // The empty queue should not leak any allocated memory
  expect_eq(initial_pool_allocated, q->get_allocator()->bytes_allocated());
}

void run_concurrent_producers_test(const string& allocator_type) {
  printf("-- [%s] concurrent producers\n", allocator_type.c_str());

  // Make sure the queue is initialized
  get_or_create_queue(pool_name_prefix + allocator_type, allocator_type);

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
    // Child process: generate the numbers [0, 100) prefixed with our pid
    auto q = get_or_create_queue(pool_name_prefix + allocator_type, allocator_type);

    char buffer[32];
    pid_t pid = getpid();
    for (size_t x = 0; x < 10; x++) {
      size_t size = sprintf(buffer, "%d-%zu", pid, x);
      q->push_back(buffer, size);
    }

    _exit(0);

  } else {
    // Parent process: read everything from the queue until all processes
    // terminate
    auto q = get_or_create_queue(pool_name_prefix + allocator_type, allocator_type);

    unordered_map<pid_t, ssize_t> latest_value_from_process;
    for (pid_t pid : child_pids) {
      latest_value_from_process.emplace(pid, -1);
    }

    size_t num_failures = 0;
    while (!child_pids.empty()) {
      try {
        auto data = q->pop_front();
        auto tokens = split(data, '-');
        expect_eq(2, tokens.size());

        pid_t pid = stoi(tokens[0]);
        ssize_t value = stoi(tokens[1]);
        auto it = latest_value_from_process.find(pid);
        expect_ne(latest_value_from_process.end(), it);
        expect_lt(it->second, value);
        it->second = value;

      } catch (const out_of_range&) { // Queue is empty
        int exit_status;
        pid_t exited_pid;
        if ((exited_pid = waitpid(-1, &exit_status, WNOHANG)) != 0) {
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
      }
    }

    expect(child_pids.empty());
    expect_eq(0, num_failures);
  }
}

void run_concurrent_consumers_test(const string& allocator_type) {
  printf("-- [%s] concurrent consumers\n", allocator_type.c_str());

  // Make sure the queue is initialized
  get_or_create_queue(pool_name_prefix + allocator_type, allocator_type);

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
    // Child process: read numbers and expect them to be in increasing order
    // stop when -1 is received
    auto q = get_or_create_queue(pool_name_prefix + allocator_type, allocator_type);

    ssize_t prev_value = -1;
    ssize_t value = 0;
    for (;;) {
      try {
        string item = q->pop_front();
        value = stoi(item);
        if (value == -1) {
          break;
        }
      } catch (const out_of_range&) {
        usleep(1);
        continue;
      }
      if (value < prev_value) {
        fprintf(stderr, "value !< prev_value (%zd !< %zd)\n", value, prev_value);
        expect(false);
      }
      // expect_gt(value, prev_value);
      prev_value = value;
    };

    _exit(0);

  } else {
    // Parent process: push numbers [0, 1000), then wait for children
    auto q = get_or_create_queue(pool_name_prefix + allocator_type, allocator_type);

    for (int64_t v = 0; v < 1000; v++) {
      char buffer[32];
      size_t len = sprintf(buffer, "%" PRId64, v);
      q->push_back(buffer, len);
    }

    for (size_t x = 0; x < child_pids.size(); x++) {
      q->push_back("-1", 2);
    }

    size_t num_failures = 0;
    while (!child_pids.empty()) {
      int exit_status;
      pid_t exited_pid;
      if ((exited_pid = wait(&exit_status)) != -1) {
        child_pids.erase(exited_pid);
        bool exited = WIFEXITED(exit_status);
        bool signaled = WIFSIGNALED(exit_status);
        int status = WEXITSTATUS(exit_status);
        int signal = WTERMSIG(exit_status);
        if (exited && (exit_status == 0)) {
          printf("-- [%s]   child %d terminated successfully\n",
              allocator_type.c_str(), exited_pid);
        } else {
          printf("-- [%s]   child %d failed (%d; exited=%d signaled=%d status=%d signal=%d)\n",
              allocator_type.c_str(), exited_pid, exit_status, exited, signaled, status, signal);
          num_failures++;
        }
      }
    }

    expect(child_pids.empty());
    expect_eq(0, num_failures);
  }
}

int main(int, char**) {
  int retcode = 0;

  vector<string> allocator_types({"simple", "logarithmic"});
  try {
    for (const auto& allocator_type : allocator_types) {
      Pool::delete_pool(pool_name_prefix + allocator_type);
      run_basic_tests(allocator_type);
      Pool::delete_pool(pool_name_prefix + allocator_type);
      run_concurrent_producers_test(allocator_type);
      Pool::delete_pool(pool_name_prefix + allocator_type);
      run_concurrent_consumers_test(allocator_type);
    }
    printf("all tests passed\n");

  } catch (const exception& e) {
    printf("failure: %s\n", e.what());
    retcode = 1;
  }
  for (const auto& allocator_type : allocator_types) {
    Pool::delete_pool(pool_name_prefix + allocator_type);
  }

  return retcode;
}
