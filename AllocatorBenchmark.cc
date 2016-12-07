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
#include "SimpleAllocator.hh"

using namespace std;

using namespace sharedstructures;


int main(int argc, char** argv) {

  srand(time(NULL));

  size_t min_alloc_size = 0, max_alloc_size = 1024;
  uint64_t report_interval = 100;

  Pool::delete_pool("benchmark-pool");
  shared_ptr<Pool> pool(new Pool("benchmark-pool"));
  SimpleAllocator alloc(pool);
  pool->expand(32 * 1024 * 1024);

  unordered_set<uint64_t> allocated_regions;
  size_t allocated_size = 0;
  uint64_t alloc_time = 0;
  while (pool->size() <= 32 * 1024 * 1024) {
    if (allocated_regions.size() % report_interval == 0) {
      double efficiency = (float)alloc.bytes_allocated() / (pool->size() - alloc.bytes_free());
      fprintf(stderr, "allocation #%zu (%" PRIu64 " nsec/alloc): %zu allocated, %zu free, %zu total, %g efficiency\n",
          allocated_regions.size(), (alloc_time * 1000) / report_interval, allocated_size,
          alloc.bytes_free(), pool->size(), efficiency);
      alloc_time = 0;
    }

    size_t size = min_alloc_size + (rand() % (max_alloc_size - min_alloc_size));

    uint64_t start = now();
    uint64_t offset = alloc.allocate(size);
    uint64_t end = now();
    alloc_time += (end - start);
    allocated_regions.emplace(offset);
    allocated_size += size;

    expect_eq(allocated_size, alloc.bytes_allocated());
  }

  alloc_time = 0;
  while (!allocated_regions.empty()) {
    auto it = allocated_regions.begin();
    uint64_t offset = *it;
    uint64_t size = alloc.block_size(offset);
    uint64_t start = now();
    alloc.free(offset);
    uint64_t end = now();
    alloc_time += (end - start);
    allocated_regions.erase(it);

    allocated_size -= size;
    expect_eq(allocated_size, alloc.bytes_allocated());

    if (allocated_regions.size() % report_interval == 0) {
      double efficiency = (float)alloc.bytes_allocated() / (pool->size() - alloc.bytes_free());
      fprintf(stderr, "free #%zu (%" PRIu64 " nsec/free): %zu allocated, %zu free, %zu total, %g efficiency\n",
          allocated_regions.size(), (alloc_time * 1000) / report_interval, allocated_size,
          alloc.bytes_free(), pool->size(), efficiency);
      alloc_time = 0;
    }
  }

  return 0;
}
