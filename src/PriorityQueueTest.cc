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
#include "SimpleAllocator.hh"
#include "LogarithmicAllocator.hh"
#include "PriorityQueue.hh"

using namespace std;

using namespace sharedstructures;

const string pool_name_prefix = "PriorityQueueTest-cc-pool-";


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


shared_ptr<PriorityQueue> get_or_create_queue(const string& name,
    const string& allocator_type) {
  shared_ptr<Pool> pool(new Pool(name));
  shared_ptr<Allocator> alloc = create_allocator(pool, allocator_type);
  return shared_ptr<PriorityQueue>(new PriorityQueue(alloc, 0));
}


void run_basic_test(const string& allocator_type) {
  printf("-- [%s] basic\n", allocator_type.c_str());

  auto q = get_or_create_queue(pool_name_prefix + allocator_type, allocator_type);

  q->push("v1");
  expect_eq(1, q->size());
  q->push("v3");
  expect_eq(2, q->size());
  q->push("v2");
  expect_eq(3, q->size());
  q->push("v20");
  expect_eq(4, q->size());

  expect_eq("v1", q->pop());
  expect_eq(3, q->size());
  expect_eq("v2", q->pop());
  expect_eq(2, q->size());
  expect_eq("v20", q->pop());
  expect_eq(1, q->size());
  expect_eq("v3", q->pop());
  expect_eq(0, q->size());
  try {
    q->pop();
    expect(false);
  } catch (const out_of_range&) { }

  // TODO: test that no item data blocks are leaked
}

void run_heapsort(const string& allocator_type) {
  printf("-- [%s] heapsort\n", allocator_type.c_str());

  auto q = get_or_create_queue(pool_name_prefix + allocator_type, allocator_type);

  // Put the numbers 1-1000 in the queue
  for (size_t x = 1000; x > 0; x--) {
    char buf[8];
    int size = sprintf(buf, "%04zu", x);
    q->push(buf, size);
  }

  // We should get them back in increasing order
  for (size_t x = 1; x <= 1000; x++) {
    char buf[8];
    sprintf(buf, "%04zu", x);
    expect_eq(buf, q->pop());
  }

  expect_eq(0, q->size());
  try {
    q->pop();
    expect(false);
  } catch (const out_of_range&) { }

  // TODO: test that no item data blocks are leaked
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
    for (size_t x = 0; x < 100; x++) {
      size_t size = sprintf(buffer, "%d-%04zu", pid, x);
      q->push(buffer, size);
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
        auto data = q->pop();
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
    expect_eq(0, q->size());
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
        string item = q->pop();
        if (item == "END") {
          break;
        }
        value = stoi(item);
      } catch (const out_of_range&) {
        usleep(1);
        continue;
      }
      if (value < prev_value) {
        fprintf(stderr, "value !< prev_value (%zd !< %zd)\n", value, prev_value);
        expect(false);
      }
      //expect_gt(value, prev_value);
      prev_value = value;
    };

    _exit(0);

  } else {
    // Parent process: push numbers [0, 1000), then wait for children
    auto q = get_or_create_queue(pool_name_prefix + allocator_type, allocator_type);

    for (int64_t v = 0; v < 1000; v++) {
      char buffer[32];
      size_t len = sprintf(buffer, "%04" PRId64, v);
      q->push(buffer, len);
    }

    for (size_t x = 0; x < child_pids.size(); x++) {
      q->push("END", 3);
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
    expect_eq(0, q->size());
  }
}


int main(int, char**) {
  int retcode = 0;

  vector<string> allocator_types({"simple", "logarithmic"});
  try {
    for (const auto& allocator_type : allocator_types) {
      Pool::delete_pool(pool_name_prefix + allocator_type);
      run_basic_test(allocator_type);
      Pool::delete_pool(pool_name_prefix + allocator_type);
      run_heapsort(allocator_type);
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
