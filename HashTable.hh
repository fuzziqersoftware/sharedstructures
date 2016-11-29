#include <stdint.h>

#include <memory>
#include <string>

#include "Pool.hh"

namespace sharedstructures {


class HashTable {
public:
  HashTable() = delete;
  HashTable(const HashTable&) = delete;
  HashTable(HashTable&&) = delete;

  // create constructor - allocates and initializes a new hash table in pool.
  explicit HashTable(std::shared_ptr<Pool> pool, uint8_t bits);
  // (conditional) create constructor.
  // opens an existing HashTable in the given pool. if base_offset is 0, opens
  // the HashTable at the pool's base offset. if the pool's base offset is also
  // 0, creates a new HashTable and sets the pool's base offset to the new
  // table's base offset.
  HashTable(std::shared_ptr<Pool> pool, uint64_t base_offset, uint8_t bits);

  // returns the pool for this hash table
  std::shared_ptr<Pool> get_pool() const;
  // returns the base offset for this hasb table. if it was automatically
  // allocated with the conditional create constructor, this will tell you how
  // to open it again later.
  uint64_t base() const;

  // inserts/overwrites a key with a string value.
  void insert(const void* k, size_t k_size, const void* v, size_t v_size);
  void insert(const void* k, size_t k_size, const std::string& v);
  void insert(const std::string& k, const void* v, size_t v_size);
  void insert(const std::string& k, const std::string& v);

  // deletes a key.
  bool erase(const void* k, size_t k_size);
  bool erase(const std::string& k);

  // deletes all the keys in the hash table.
  void clear();

  // checks if a key exists.
  bool exists(const void* k, size_t k_size) const;
  bool exists(const std::string& k) const;

  // returns the value of a key. throws std::out_of_range if the key is missing.
  std::string at(const void* k, size_t k_size) const;
  std::string at(const std::string& k) const;

  // inspection methods.
  size_t size() const; // key count
  uint8_t bits() const; // hash bucket count factor

  void print(FILE* stream) const;

private:
  std::shared_ptr<Pool> pool;
  uint64_t base_offset;

  // TODO: implement secondary tables (for rehashing)

  struct Slot {
    uint64_t key_offset;
    uint64_t key_size;
    // there's no value_offset because the value is just after the key.
    // there's no value_size because we can infer it from pool_block_size and
    // key_size.
    // if key_offset is 0, then this slot is empty.
    // if key_size is (uint64_t)-1, then this slot contains indirect slots
  };

  struct IndirectValue {
    uint64_t next;
    uint64_t key_offset;
    uint64_t key_size;
  };

  struct HashTableBase {
    uint8_t bits[2];
    uint64_t slots_offset[2];
    uint64_t item_count[2];
  };

  uint64_t create_hash_base(uint8_t bits);
  std::pair<uint64_t, uint64_t> walk_indirect_list(uint64_t indirect_offset,
      const void* k, size_t k_size) const;
  std::pair<uint64_t, uint64_t> walk_tables(const void* k, size_t k_size,
      uint64_t hash) const;
};

} // namespace sharedstructures
