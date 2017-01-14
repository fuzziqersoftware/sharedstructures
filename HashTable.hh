#pragma once

#include <stdint.h>

#include <memory>
#include <string>

#include "Allocator.hh"

namespace sharedstructures {


class HashTableIterator;


class HashTable {
public:
  HashTable() = delete;
  HashTable(const HashTable&) = delete;
  HashTable(HashTable&&) = delete;

  // create constructor - allocates and initializes a new hash table.
  HashTable(std::shared_ptr<Allocator> allocator, uint8_t bits);
  // (conditional) create constructor.
  // opens an existing HashTable using the given allocator. if base_offset is 0,
  // opens the HashTable at the allocator's base offset. if the allocator's base
  // offset is also 0, creates a new HashTable and sets the allocator's base
  // offset to the new table's base offset.
  HashTable(std::shared_ptr<Allocator> allocator, uint64_t base_offset,
      uint8_t bits);

  // returns the allocator for this hash table
  std::shared_ptr<Allocator> get_allocator() const;
  // returns the base offset for this hasb table. if it was automatically
  // allocated with the conditional create constructor, this will tell you how
  // to open it again later.
  uint64_t base() const;

  // to do a conditional write, instantiate one of these and pass it to insert()
  // or erase(). don't modify the key or key_size members of one of these
  // objects after constructing it.
  struct CheckRequest {
    const void* key;
    size_t key_size;
    const void* value;
    size_t value_size;

    uint64_t key_hash;

    CheckRequest() = delete;
    CheckRequest(const void* key, size_t key_size, const void* value,
        size_t value_size);
    CheckRequest(const void* key, size_t key_size, const std::string& value);
    CheckRequest(const void* key, size_t key_size); // checks for missing
    CheckRequest(const std::string& key, const void* value, size_t value_size);
    CheckRequest(const std::string& key, const std::string& value);
    CheckRequest(const std::string& key); // checks for missing
  };

  // inserts/overwrites a key with a string value.
  bool insert(const void* k, size_t k_size, const void* v, size_t v_size,
      const CheckRequest* check = NULL);
  bool insert(const void* k, size_t k_size, const std::string& v,
      const CheckRequest* check = NULL);
  bool insert(const std::string& k, const void* v, size_t v_size,
      const CheckRequest* check = NULL);
  bool insert(const std::string& k, const std::string& v,
      const CheckRequest* check = NULL);

  // deletes a key.
  bool erase(const void* k, size_t k_size, const CheckRequest* check = NULL);
  bool erase(const std::string& k, const CheckRequest* check = NULL);

  // deletes all the keys in the hash table.
  void clear();

  // checks if a key exists.
  bool exists(const void* k, size_t k_size) const;
  bool exists(const std::string& k) const;

  // returns the value of a key. throws std::out_of_range if the key is missing.
  std::string at(const void* k, size_t k_size) const;
  std::string at(const std::string& k) const;

  // these functions return the contents of a slot, which contains zero or more
  // key-value pairs. to iterate the table, call this function for all values in
  // [0, 1 << table.bits() - 1].
  std::vector<std::pair<std::string, std::string>> get_slot_contents(
      uint64_t slot_index) const;

  // these functions implement standard C++ iteration.
  HashTableIterator begin() const;
  HashTableIterator end() const;

  // inspection methods.
  size_t size() const; // key count
  uint8_t bits() const; // hash bucket count factor

  void print(FILE* stream) const;

private:
  std::shared_ptr<Allocator> allocator;
  uint64_t base_offset;

  // TODO: implement secondary tables (for rehashing)

  struct Slot {
    uint64_t key_offset;
    uint64_t key_size;
    // there's no value_offset because the value is just after the key.
    // there's no value_size because we can infer it from the block_size and
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
    uint8_t bits;
    uint64_t slots_offset;
    uint64_t item_count;
  };

  uint64_t create_hash_base(uint8_t bits);
  std::pair<uint64_t, uint64_t> walk_indirect_list(uint64_t indirect_offset,
      const void* k, size_t k_size) const;
  std::pair<uint64_t, uint64_t> walk_tables(const void* k, size_t k_size,
      uint64_t hash) const;

  bool execute_check(const CheckRequest& check) const;

  std::pair<std::string, std::string> next_key_value_internal(
      const void* current, size_t size, bool return_value) const;
};


class HashTableIterator {
public:
  HashTableIterator() = delete;
  HashTableIterator(const HashTableIterator& other) = default;
  HashTableIterator(HashTableIterator&& other) = default;
  HashTableIterator(const HashTable* table, uint64_t slot_index);
  ~HashTableIterator() = default;

  bool operator==(const HashTableIterator& other) const;
  bool operator!=(const HashTableIterator& other) const;
  HashTableIterator& operator++();
  HashTableIterator operator++(int);
  const std::pair<std::string, std::string>& operator*() const;

private:
  const HashTable* table;
  uint64_t slot_index;
  uint64_t result_index;
  std::vector<std::pair<std::string, std::string>> slot_contents;

  void advance_to_nonempty_slot();
};


} // namespace sharedstructures
