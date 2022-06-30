#pragma once

#include <stdint.h>

#include <memory>
#include <string>
#include <utility>

#include "Pool.hh"

namespace sharedstructures {


class IntVector {
public:
  IntVector() = delete;
  IntVector(const IntVector&) = delete;
  IntVector(IntVector&&) = delete;

  explicit IntVector(std::shared_ptr<Pool> pool);

  ~IntVector() = default;

  // Returns the pool for this int vector
  std::shared_ptr<Pool> get_pool() const;

  // Returns the length of the vector
  size_t size() const;

  // Expands the vector (vectors cannot be shrunk!)
  void expand(size_t new_size);

  // Atomic operations
  int64_t load(size_t index);
  void store(size_t index, int64_t value);
  int64_t exchange(size_t index, int64_t value);
  int64_t compare_exchange(size_t index, int64_t expected, int64_t desired);
  int64_t fetch_add(size_t index, int64_t delta);
  int64_t fetch_sub(size_t index, int64_t delta);
  int64_t fetch_and(size_t index, int64_t mask);
  int64_t fetch_or(size_t index, int64_t mask);
  int64_t fetch_xor(size_t index, int64_t mask);

private:
  std::shared_ptr<Pool> pool;

  struct VectorBase {
    std::atomic<uint64_t> pool_size; // This shadows Pool::Data
    std::atomic<uint64_t> count;
    std::atomic<int64_t> data[0];
  };

  VectorBase* base();
  const VectorBase* base() const;
  std::atomic<int64_t>* at(size_t index);
};


} // namespace sharedstructures
