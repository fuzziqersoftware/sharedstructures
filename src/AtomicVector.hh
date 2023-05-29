#pragma once

#include <stdint.h>

#include <atomic>
#include <memory>
#include <phosg/Encoding.hh>
#include <stdexcept>
#include <string>
#include <utility>

#include "Pool.hh"

namespace sharedstructures {

template <typename T>
class AtomicVector {
public:
  static constexpr size_t bits = bits_for_type<std::atomic<T>>;

  AtomicVector() = delete;
  AtomicVector(const AtomicVector&) = delete;
  AtomicVector(AtomicVector&&) = delete;

  explicit AtomicVector(std::shared_ptr<Pool> pool)
      : pool(pool) {
    if (!std::atomic<T>::is_always_lock_free) {
      throw std::logic_error("AtomicVector<T> cannot be used on systems where atomic<T> uses locks");
    }
  }

  ~AtomicVector() = default;

  // Returns the pool for this vector
  std::shared_ptr<Pool> get_pool() const {
    return this->pool;
  }

  // Returns the length (in elements) of the vector
  size_t size() const {
    return this->base()->count;
  }

  // Expands the vector (vectors cannot be shrunk!)
  void expand(size_t new_count) {
    size_t required_pool_size = sizeof(VectorBase) + new_count * sizeof(std::atomic<T>);

    uint64_t orig_count = this->base()->count.load();
    while (orig_count < new_count) {
      this->pool->check_size_and_remap();
      this->pool->expand(required_pool_size);
      if (this->base()->count.compare_exchange_strong(orig_count, new_count)) {
        break;
      }
    }
  }
  void expand_bits(size_t new_bit_count) {
    // Round the bit count up to a multiple of the base type (T)
    this->expand((new_bit_count + (this->bits - 1)) / this->bits);
  }

  // Atomic operations
  T load(size_t index) {
    return this->at(index).load();
  }
  void store(size_t index, T value) {
    this->at(index).store(value);
  }
  T exchange(size_t index, T value) {
    return this->at(index).exchange(value);
  }
  T compare_exchange(size_t index, T expected, T desired) {
    this->at(index).compare_exchange_strong(expected, desired);
    return expected;
  }
  T fetch_add(size_t index, T delta) {
    return this->at(index).fetch_add(delta);
  }
  T fetch_sub(size_t index, T delta) {
    return this->at(index).fetch_sub(delta);
  }
  T fetch_and(size_t index, T mask) {
    return this->at(index).fetch_and(mask);
  }
  T fetch_or(size_t index, T mask) {
    return this->at(index).fetch_or(mask);
  }
  T fetch_xor(size_t index, T mask) {
    return this->at(index).fetch_xor(mask);
  }

  // Atomic single-bit operations
  static T mask_for_bit_index(size_t bit_index) {
    return (static_cast<T>(1) << (AtomicVector::bits - (bit_index % AtomicVector::bits) - 1));
  }
  bool load_bit(size_t bit_index) {
    return (this->load(bit_index / this->bits) & this->mask_for_bit_index(bit_index));
  }
  bool set_bit(size_t bit_index, bool v) {
    T mask = this->mask_for_bit_index(bit_index);
    if (v) {
      return this->fetch_or(bit_index / this->bits, mask) & mask;
    } else {
      return this->fetch_and(bit_index / this->bits, ~mask) & mask;
    }
  }
  bool toggle_bit(size_t bit_index) {
    T mask = this->mask_for_bit_index(bit_index);
    return !(this->fetch_xor(bit_index / this->bits, mask) & mask);
  }

private:
  std::shared_ptr<Pool> pool;

  struct VectorBase {
    std::atomic<uint64_t> pool_size; // This shadows Pool::Data
    std::atomic<uint64_t> count;
    std::atomic<T> data[0];
  } __attribute__((packed));

  VectorBase* base() {
    return this->pool->template at<VectorBase>(0);
  }

  const VectorBase* base() const {
    return this->pool->template at<VectorBase>(0);
  }

  std::atomic<T>& at(size_t index) {
    this->pool->check_size_and_remap();
    auto* base = this->base();
    if (index >= base->count) {
      throw std::out_of_range("index out of range");
    }
    return base->data[index];
  }
};

} // namespace sharedstructures
