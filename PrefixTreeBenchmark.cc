#define __STDC_FORMAT_MACROS
#include <errno.h>
#include <inttypes.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <algorithm>
#include <phosg/Time.hh>
#include <phosg/UnitTest.hh>
#include <string>

#include "Pool.hh"
#include "LogarithmicAllocator.hh"
#include "SimpleAllocator.hh"
#include "PrefixTree.hh"

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


template <typename T>
struct Stats {
  size_t count;
  T sum;
  T mean;
  T min;
  T max;
  T p01;
  T p10;
  T p50;
  T p90;
  T p99;
};

template <typename T>
struct Stats<T> get_stats(const vector<T>& x) {
  Stats<T> ret;

  ret.count = x.size();
  ret.min = x.front();
  ret.max = x.back();
  ret.p01 = x[(1 * ret.count) / 100];
  ret.p10 = x[(10 * ret.count) / 100];
  ret.p50 = x[(50 * ret.count) / 100];
  ret.p90 = x[(90 * ret.count) / 100];
  ret.p99 = x[(99 * ret.count) / 100];

  ret.sum = 0;
  for (const auto& y : x) {
    ret.sum += y;
  }
  ret.mean = ret.sum / ret.count;

  return ret;
}

template <typename T>
void print_stats(FILE* f, const char* title, struct Stats<T>& stats) {
  fprintf(stdout, "%s: count=%zu avg=%" PRIu64 " min=%" PRIu64 " p01=%" PRIu64
      " p10=%" PRIu64 " p50=%" PRIu64 " p90=%" PRIu64 " p99=%" PRIu64 " max=%"
      PRIu64 "\n", title, stats.count, stats.mean, stats.min, stats.p01,
      stats.p10, stats.p50, stats.p90, stats.p99, stats.max);
}

void print_stats(FILE* f, const char* title, struct Stats<double>& stats) {
  fprintf(stdout, "%s: count=%zu avg=%lg min=%lg p01=%lg p10=%lg p50=%lg "
      "p90=%lg p99=%lg max=%lg\n", title, stats.count, stats.mean, stats.min,
      stats.p01, stats.p10, stats.p50, stats.p90, stats.p99, stats.max);
}


void print_usage(const char* argv0) {
  fprintf(stderr,
    "usage: %s -X<allocator-type> [options]\n"
    "  options:\n"
    "    -l<max-pool-size> : pool will grow up to this size (bytes)\n"
    "    -s<min-alloc-size> : allocations will be at least this many bytes each\n"
    "    -S<max-alloc-size> : allocations will be at most this many bytes each\n"
    "    -P<pool-name> : filename for the pool\n"
    "    -A : preallocate the entire pool up to max-pool-size\n", argv0);
}


int main(int argc, char** argv) {

  size_t max_size = 32 * 1024 * 1024;
  uint64_t report_interval = 100;
  bool preallocate = false;
  string allocator_type;
  string pool_name = "benchmark-pool";
  for (int x = 1; x < argc; x++) {
    if (argv[x][0] == '-') {
      if (argv[x][1] == 'l') {
        max_size = strtoull(&argv[x][2], NULL, 0);
      } else if (argv[x][1] == 'X') {
        allocator_type = &argv[x][2];
      } else if (argv[x][1] == 'P') {
        pool_name = &argv[x][2];
      } else if (argv[x][1] == 'A') {
        preallocate = true;
      } else {
        fprintf(stderr, "unknown argument: %s\n", argv[x]);
        print_usage(argv[0]);
        return 1;
      }
    } else {
      fprintf(stderr, "unknown argument: %s\n", argv[x]);
      print_usage(argv[0]);
      return 1;
    }
  }

  if (allocator_type.empty()) {
    print_usage(argv[0]);
    return 1;
  }

  srand(time(NULL));

  Pool::delete_pool(pool_name);
  shared_ptr<Pool> pool(new Pool(pool_name));
  if (preallocate) {
    pool->expand(max_size);
  }
  shared_ptr<Allocator> alloc = create_allocator(pool, allocator_type);
  PrefixTree t(alloc, 0);

  vector<double> efficiencies;
  vector<uint64_t> insert_times;
  char key_str[20];
  while (pool->size() <= max_size) {

    if (t.size() % report_interval == 0) {
      double efficiency = (float)alloc->bytes_allocated() /
          (pool->size() - alloc->bytes_free());
      efficiencies.emplace_back(efficiency);
      fprintf(stderr, "insert #%zu: %zu bytes in pool, %g efficiency\n",
          t.size(), pool->size(), efficiency);
    }

    size_t key_len = sprintf(key_str, "%zu", t.size());
    uint64_t start = now();
    t.insert(key_str, key_len);
    uint64_t end = now();
    insert_times.emplace_back(end - start);
  }

  vector<uint64_t> get_times;
  for (size_t x = 0; x < t.size(); x++) {
    if (x % report_interval == 0) {
      fprintf(stderr, "get #%zu\n", x);
    }

    size_t key_len = sprintf(key_str, "%zu", x);
    uint64_t start = now();
    auto res = t.at(key_str, key_len);
    uint64_t end = now();

    expect_eq(res.type, PrefixTree::ResultValueType::Null);
    get_times.emplace_back(end - start);
  }

  if (!efficiencies.empty()) {
    sort(efficiencies.begin(), efficiencies.end());
    auto efficiency_stats = get_stats(efficiencies);
    print_stats(stdout, "efficiency", efficiency_stats);
  }

  if (!insert_times.empty()) {
    sort(insert_times.begin(), insert_times.end());
    auto insert_times_stats = get_stats(insert_times);
    print_stats(stdout, "insert usecs", insert_times_stats);
  }

  if (!get_times.empty()) {
    sort(get_times.begin(), get_times.end());
    auto get_times_stats = get_stats(get_times);
    print_stats(stdout, "get usecs", get_times_stats);
  }

  return 0;
}
