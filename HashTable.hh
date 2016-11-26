#include <stdint.h>

#include <string>

#include "Pool.hh"

namespace sharedstructures {


class HashTable {
public:
  HashTable() = delete;
  HashTable(const HashTable&) = delete;
  HashTable(HashTable&&) = delete;
  // TODO: make this take a Pool object instead of constructing one internally
  explicit HashTable(const std::string& pool_name, size_t max_size = 0);
  ~HashTable() = default;

  bool insert(const std::string& k, const std::string& v,
      bool overwrite = true);
  bool insert(const void* k, size_t k_size, const void* v, size_t v_size,
      bool overwrite = true);

  bool erase(const std::string& k);
  bool erase(const void* k, size_t k_size);

  std::string at(const std::string& k) const;
  std::string at(const void* k, size_t k_size) const;

  size_t pool_size() const;
  size_t size() const;

private:
  Pool pool;

  // unfortunately we can't use most prebuilt libraries like STL containers here
  // because the memory can move (we have to use offsets instead of pointers)
  // TODO: implement secondary tables (for rehashing)
  // TODO: implement linking to store multiple items in one bucket

  struct HashTableData {
    struct Slot {
      uint64_t key_offset;
      uint64_t key_size;
      // there's no value_offset because the value is just after the key.
      // there's no value_size because we can infer it from pool_block_size and
      // key_size.
    };

    uint8_t bits;
    uint64_t item_count;
    uint64_t next_table_offset;
    Slot slots[0];

    HashTableData(uint8_t bits);
    static size_t allocation_size(uint8_t bits);
  };

  uint64_t create_hash_header(uint8_t bits);
  bool hash_insert(const void* key_data, size_t key_size,
      const void* value_data, size_t value_size, bool overwrite);
  bool hash_delete(const void* key_data, size_t key_size);
  std::pair<uint64_t, uint64_t> hash_lookup(const void* key_data,
      size_t key_size) const;
};

} // namespace sharedstructures
