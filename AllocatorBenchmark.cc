#define __STDC_FORMAT_MACROS
#include <errno.h>
#include <inttypes.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <phosg/Time.hh>
#include <phosg/UnitTest.hh>
#include <string>

#include "Pool.hh"
#include "LogarithmicAllocator.hh"
#include "SimpleAllocator.hh"

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


int main(int argc, char** argv) {

  size_t max_size = 32 * 1024 * 1024;
  size_t min_alloc_size = 1, max_alloc_size = 1024;
  uint64_t report_interval = 100;
  string allocator_type;
  string pool_name = "benchmark-pool";
  for (int x = 1; x < argc; x++) {
    if (argv[x][0] == '-') {
      if (argv[x][1] == 'l') {
        max_size = strtoull(&argv[x][2], NULL, 0);
      } else if (argv[x][1] == 's') {
        min_alloc_size = strtoull(&argv[x][2], NULL, 0);
      } else if (argv[x][1] == 'S') {
        max_alloc_size = strtoull(&argv[x][2], NULL, 0);
      } else if (argv[x][1] == 'X') {
        allocator_type = &argv[x][2];
      } else if (argv[x][1] == 'P') {
        pool_name = &argv[x][2];
      } else {
        fprintf(stderr, "unknown argument: %s\n", argv[x]);
        return 1;
      }
    } else {
      fprintf(stderr, "unknown argument: %s\n", argv[x]);
      return 1;
    }
  }

  srand(time(NULL));

  Pool::delete_pool(pool_name);
  shared_ptr<Pool> pool(new Pool(pool_name));
  shared_ptr<Allocator> alloc = create_allocator(pool, allocator_type);

  vector<double> efficiencies;
  vector<uint64_t> alloc_times;
  unordered_set<uint64_t> allocated_regions;
  size_t allocated_size = 0;
  while (pool->size() <= max_size) {
    if (allocated_regions.size() % report_interval == 0) {
      double efficiency = (float)alloc->bytes_allocated() / (pool->size() - alloc->bytes_free());
      efficiencies.emplace_back(efficiency);
      fprintf(stderr, "allocation #%zu: %zu allocated, %zu free, %zu total, %g efficiency\n",
          allocated_regions.size(), allocated_size, alloc->bytes_free(),
          pool->size(), efficiency);
    }

    size_t size = min_alloc_size + (rand() % (max_alloc_size - min_alloc_size));

    uint64_t start = now();
    uint64_t offset = alloc->allocate(size);
    uint64_t end = now();
    alloc_times.emplace_back(end - start);
    allocated_regions.emplace(offset);
    allocated_size += size;

    expect_eq(allocated_size, alloc->bytes_allocated());
  }

  vector<uint64_t> free_times;
  while (!allocated_regions.empty()) {
    auto it = allocated_regions.begin();
    uint64_t offset = *it;
    uint64_t size = alloc->block_size(offset);
    uint64_t start = now();
    alloc->free(offset);
    uint64_t end = now();
    free_times.emplace_back(end - start);
    allocated_regions.erase(it);

    allocated_size -= size;
    expect_eq(allocated_size, alloc->bytes_allocated());

    if (allocated_regions.size() % report_interval == 0) {
      double efficiency = (float)alloc->bytes_allocated() / (pool->size() - alloc->bytes_free());
      efficiencies.emplace_back(efficiency);
      fprintf(stderr, "free #%zu: %zu allocated, %zu free, %zu total, %g efficiency\n",
          allocated_regions.size(), allocated_size, alloc->bytes_free(),
          pool->size(), efficiency);
    }
  }

  sort(efficiencies.begin(), efficiencies.end());
  auto efficiency_stats = get_stats(efficiencies);
  fprintf(stdout, "efficiency: avg=%lg min=%lg p01=%lg p10=%lg p50=%lg p90=%lg p99=%lg max=%lg\n",
      efficiency_stats.mean, efficiency_stats.min, efficiency_stats.p01,
      efficiency_stats.p10, efficiency_stats.p50, efficiency_stats.p90,
      efficiency_stats.p99, efficiency_stats.max);

  sort(alloc_times.begin(), alloc_times.end());
  auto alloc_time_stats = get_stats(alloc_times);
  fprintf(stdout, "alloc usecs: avg=%" PRIu64 " min=%" PRIu64 " p01=%" PRIu64 " p10=%" PRIu64 " p50=%" PRIu64 " p90=%" PRIu64 " p99=%" PRIu64 " max=%" PRIu64 "\n",
      alloc_time_stats.mean, alloc_time_stats.min, alloc_time_stats.p01,
      alloc_time_stats.p10, alloc_time_stats.p50, alloc_time_stats.p90,
      alloc_time_stats.p99, alloc_time_stats.max);

  sort(free_times.begin(), free_times.end());
  auto free_time_stats = get_stats(free_times);
  fprintf(stdout, "free usecs: avg=%" PRIu64 " min=%" PRIu64 " p01=%" PRIu64 " p10=%" PRIu64 " p50=%" PRIu64 " p90=%" PRIu64 " p99=%" PRIu64 " max=%" PRIu64 "\n",
      free_time_stats.mean, free_time_stats.min, free_time_stats.p01,
      free_time_stats.p10, free_time_stats.p50, free_time_stats.p90,
      free_time_stats.p99, free_time_stats.max);

  return 0;
}
