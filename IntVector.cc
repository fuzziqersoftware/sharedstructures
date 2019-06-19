#include "IntVector.hh"

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>

#include <algorithm>
#include <phosg/Strings.hh>

using namespace std;
using namespace sharedstructures;

namespace sharedstructures {


IntVector::IntVector(shared_ptr<Pool> pool) : pool(pool) { }

shared_ptr<Pool> IntVector::get_pool() const {
  return this->pool;
}

size_t IntVector::size() const {
  return this->base()->count;
}

void IntVector::expand(size_t new_count) {
  size_t required_pool_size = sizeof(IntVector::VectorBase) + new_count * sizeof(atomic<int64_t>);

  uint64_t orig_count = this->base()->count.load();
  while (orig_count < new_count) {
    this->pool->check_size_and_remap();
    this->pool->expand(required_pool_size);
    if (this->base()->count.compare_exchange_strong(orig_count, new_count)) {
      break;
    }
  }
}

int64_t IntVector::load(size_t index) {
  return this->at(index)->load();
}

void IntVector::store(size_t index, int64_t value) {
  this->at(index)->store(value);
}

int64_t IntVector::exchange(size_t index, int64_t value) {
  return this->at(index)->exchange(value);
}

int64_t IntVector::compare_exchange(size_t index, int64_t expected,
    int64_t desired) {
  this->at(index)->compare_exchange_strong(expected, desired);
  return expected;
}

int64_t IntVector::fetch_add(size_t index, int64_t delta) {
  return this->at(index)->fetch_add(delta);
}

int64_t IntVector::fetch_sub(size_t index, int64_t delta) {
  return this->at(index)->fetch_sub(delta);
}

int64_t IntVector::fetch_and(size_t index, int64_t mask) {
  return this->at(index)->fetch_and(mask);
}

int64_t IntVector::fetch_or(size_t index, int64_t mask) {
  return this->at(index)->fetch_or(mask);
}

int64_t IntVector::fetch_xor(size_t index, int64_t mask) {
  return this->at(index)->fetch_xor(mask);
}

IntVector::VectorBase* IntVector::base() {
  return this->pool->at<VectorBase>(0);
}

const IntVector::VectorBase* IntVector::base() const {
  return this->pool->at<VectorBase>(0);
}

atomic<int64_t>* IntVector::at(size_t index) {
  this->pool->check_size_and_remap();
  auto* base = this->base();
  if (index >= base->count) {
    throw out_of_range("index out of range");
  }
  return &base->data[index];
}


} // namespace sharedstructures
