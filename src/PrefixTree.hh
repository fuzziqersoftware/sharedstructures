#pragma once

#include <stdint.h>
#include <sys/uio.h> // for iov

#include <memory>
#include <string>
#include <utility>

#include "Allocator.hh"

namespace sharedstructures {


class PrefixTreeIterator;

class PrefixTree {
public:
  PrefixTree() = delete;
  PrefixTree(const PrefixTree&) = delete;
  PrefixTree(PrefixTree&&) = delete;

  // Unconditional create constructor - allocates a new prefix tree. This
  // constructor doesn't affect the allocator's base object offset; you'll need
  // to explicitly pass a nonzero offset when opening this tree later. use the
  // base() method to get the required offset.
  explicit PrefixTree(std::shared_ptr<Allocator> allocator);
  // Create or open constructor.
  // - if base_offset != 0, opens an existing prefix tree at that offset.
  // - if base_offset == 0, opens the prefix tree at the allocator's base object
  //   offset, creating one if the base object offset is also 0.
  PrefixTree(std::shared_ptr<Allocator> allocator, uint64_t base_offset);

  ~PrefixTree() = default;

  // Returns the allocator for this prefix tree
  std::shared_ptr<Allocator> get_allocator() const;
  // Returns the base offset for this prefix tree
  uint64_t base() const;

  enum class ResultValueType {
    Missing = 0,
    String  = 1,
    Int     = 2,
    Double  = 3,
    Bool    = 4,
    Null    = 5,
  };

  struct LookupResult {
    ResultValueType type;
    std::string as_string;
    int64_t as_int;
    double as_double;
    bool as_bool;

    LookupResult(ResultValueType t); // Missing (this is never returned by at())
    LookupResult(); // Null
    LookupResult(bool b); // Bool
    LookupResult(int64_t i); // Int
    LookupResult(double d); // Double
    LookupResult(const char* s); // String
    LookupResult(const void* s, size_t size); // String
    LookupResult(const std::string& s); // String

    bool operator==(const LookupResult& other) const;
    bool operator!=(const LookupResult& other) const;

    std::string str() const;
  };

  // To do a check-and-set, instantiate one of these and pass it to insert() or
  // erase().
  struct CheckRequest {
    const void* key;
    size_t key_size;
    LookupResult value;

    CheckRequest();

    // There's no std::string variant of this because it would ambiguate two
    // cases: (const char*, size_t) and (const string&, int64_t); just pass
    // s.data() and s.size() instead
    template <typename... Args>
    CheckRequest(const void* key, size_t key_size, Args... args) :
        key(key), key_size(key_size), value(std::forward<Args>(args)...) { }
  };

  // Inserts/overwrites a key with a string value
  bool insert(const void* k, size_t k_size, const void* v, size_t v_size,
      const CheckRequest* check = nullptr);
  bool insert(const void* k, size_t k_size, const std::string& v,
      const CheckRequest* check = nullptr);
  bool insert(const std::string& k, const void* v, size_t v_size,
      const CheckRequest* check = nullptr);
  bool insert(const std::string& k, const std::string& v,
      const CheckRequest* check = nullptr);
  bool insert(const void* k, size_t k_size, const struct iovec *iov,
      size_t iovcnt, const CheckRequest* check = nullptr);
  bool insert(const std::string& k, const struct iovec *iov, size_t iovcnt,
      const CheckRequest* check = nullptr);

  // Inserts/overwrites a key with an integer value
  bool insert(const void* k, size_t k_size, int64_t v,
      const CheckRequest* check = nullptr);
  bool insert(const std::string& k, int64_t v,
      const CheckRequest* check = nullptr);

  // Inserts/overwrites a key with a floating-point value
  bool insert(const void* k, size_t k_size, double v,
      const CheckRequest* check = nullptr);
  bool insert(const std::string& k, double v, const CheckRequest* check = nullptr);

  // Inserts/overwrites a key with a boolean value
  bool insert(const void* k, size_t k_size, bool v,
      const CheckRequest* check = nullptr);
  bool insert(const std::string& k, bool v, const CheckRequest* check = nullptr);

  // Inserts/overwrites a key with a null value
  bool insert(const void* k, size_t k_size, const CheckRequest* check = nullptr);
  bool insert(const std::string& k, const CheckRequest* check = nullptr);

  // Inserts/overwrites a key with the result of a previous lookup
  bool insert(const void* k, size_t k_size, const LookupResult& res,
      const CheckRequest* check = nullptr);
  bool insert(const std::string& k, const LookupResult& res,
      const CheckRequest* check = nullptr);

  // Atomically increments the value of a numeric key, returning the new value.
  // If the key is missing, creates it with the given value. If the key is the
  // wrong type, throws out_of_range.
  int64_t incr(const void* k, size_t k_size, int64_t delta);
  int64_t incr(const std::string& k, int64_t delta);
  double incr(const void* k, size_t k_size, double delta);
  double incr(const std::string& k, double delta);

  // Deletes a key
  bool erase(const void* k, size_t k_size, const CheckRequest* check = nullptr);
  bool erase(const std::string& k, const CheckRequest* check = nullptr);

  // Deletes all the keys in the prefix tree
  void clear();

  // Checks if a key exists
  bool exists(const void* k, size_t k_size);
  bool exists(const std::string& k);

  // Returns the type of a key, or Missing if it doesn't exist. This is slightly
  // slower than exists() for keys that aren't Int, Bool or Null since it
  // requires an extra memory access.
  ResultValueType type(const void* k, size_t k_size) const;
  ResultValueType type(const std::string& k) const;

  // Returns the value of a key. Throws std::out_of_range if the key is missing.
  LookupResult at(const void* k, size_t k_size) const;
  LookupResult at(const std::string& key) const;

  // These functions return the key after the given key, along with that key's
  // value (in the case of next_key_value). To iterate the entire tree, call one
  // of these functions with no arguments, then keep calling it and passing the
  // previous call's return value until it throws std::out_of_range. It's safe
  // to modify the tree during such iterations, but concurrent modifications
  // will not be visible during the iteration unless they occur later than the
  // current key (lexicographically). It's also safe to switch between calling
  // the different next_* functions during iteration.
  std::string next_key(const void* current = nullptr, size_t size = 0) const;
  std::string next_key(const std::string& current) const;
  std::pair<std::string, LookupResult> next_key_value(
      const void* current = nullptr, size_t size = 0) const;
  std::pair<std::string, LookupResult> next_key_value(
      const std::string& current) const;

  // These functions implement standard C++ iteration
  PrefixTreeIterator begin() const;
  PrefixTreeIterator end() const;
  PrefixTreeIterator find(const void* key, size_t size) const;
  PrefixTreeIterator find(const std::string& key) const;
  PrefixTreeIterator lower_bound(const void* key, size_t size) const;
  PrefixTreeIterator lower_bound(const std::string& key) const;
  PrefixTreeIterator upper_bound(const void* key, size_t size) const;
  PrefixTreeIterator upper_bound(const std::string& key) const;

  // Inspection methods.
  size_t size() const; // Key count
  size_t node_size() const; // Node count
  // Bytes used by the subtree rooted at prefix
  size_t bytes_for_prefix(const void* prefix, size_t p_size) const;
  size_t bytes_for_prefix(const std::string& prefix) const;
  // Nodes contained in the subtree rooted at prefix
  size_t nodes_for_prefix(const void* prefix, size_t p_size) const;
  size_t nodes_for_prefix(const std::string& prefix) const;

  // Prints the tree's structure to the given stream. The optional arguments are
  // used for recursive calls; external callers shouldn't need to pass them.
  // This method does not lock the tree; it's intended only for debugging. Don't
  // call this on a tree that may be open in other processes.
  void print(FILE* stream, uint8_t k = 0, uint64_t node_offset = 0,
      uint64_t indent = 0) const;
  // Returns the tree's structure as a string. This isn't the same as what
  // print() outputs; it's not human-readable, but is ascii-encodable and
  // contains no whitespace. This method does not lock the tree; it's intended
  // only for debugging. Don't call this on a tree that may be open in other
  // processes.
  std::string get_structure() const;

private:
  std::shared_ptr<Allocator> allocator;
  uint64_t base_offset;

  // The tree's structure is a recursive set of Node objects. Each Node has a
  // value slot as well as 1-256 child slots, depending on the range of subnodes
  // allocated. The root node always exists and has 256 slots. To find the value
  // for a key, we start at the root node and move to the node specified by the
  // value slot for the next character in the key. If we end up at a node, then
  // the key's value is whatever is in the node's value slot. If the last
  // character in the key leaves us at a slot with a value, then that is the
  // key's value. Otherwise, the key isn't in the tree.

  struct Node {
    uint8_t start;
    uint8_t end;
    uint8_t parent_slot;
    uint8_t unused[5]; // Force an 8-byte alignment for the rest of the struct

    uint64_t value;
    uint64_t children[0];

    // Sets start, end, value, parent; doesn't initialize children
    Node(uint8_t start, uint8_t end, uint8_t parent_slot, uint64_t value);
    // Sets everything, including children. Creates a node with one slot.
    Node(uint8_t slot, uint8_t parent_slot, uint64_t value);
    // Sets everything, including children. Creates a node with all slots.
    Node();

    bool has_children() const;

    static size_t size_for_range(uint8_t start, uint8_t end);
    static size_t full_size();
  };

  enum class StoredValueType {
    // Here we take advantage of the restriction that all allocations are
    // aligned on a 8-byte boundary. This leaves the 3 low bits of all offsets
    // available, and we can store information about the object's type there.
    // The lowest 3 bits of a slot's contents determine what type of data is
    // stored in the slot, according to the below values:

    // SubNode refers to a Node structure. The high 61 bits of the contents are
    // the Node's offset. But if the offset is 0, the slot is empty. (This means
    // that a slot is empty if and only if its contents == 0.)
    SubNode     = 0,

    // String refers to a raw data buffer. The high 61 bits of the contents are
    // the buffer's offset. The length isn't stored explicitly anywhere; it's
    // retrieved from the allocator. For zero-length strings, the buffer offset
    // is 0 and no buffer is allocated.
    String      = 1,

    // Int is a 61-bit inlined integer. The lowest 3 bits are 010, and the other
    // bits are the integer's value. When read, it's sign-extended to 64 bits.
    Int         = 2,

    // LongInt is a 64-bit integer. This type is only used when the top 4 bits
    // of the value don't all match (so sign-extension from 61 bits would
    // produce the wrong result). The high 61 bits of the slot contents are the
    // offset of an 8-byte region that holds the integer's value.
    LongInt     = 3,

    // Double is a floating-point value. It's implemented similarly to LongInt,
    // except the value is a double instead of an int64_t, and the value 0.0 is
    // stored with no extra storage (the offset is 0).
    Double      = 4,

    // Trivial is a singleton value. The high 61 bits identify which singleton
    // is stored here: 0 = false, 1 = true, 2 = null.
    Trivial     = 5,

    // ShortString is a 7-byte or shorter string. The string's contents are
    // stored in the high 7 bytes of the slot contents; the low byte stores the
    // type in the low 3 bits and the string's length (0-7) in the next 3 bits.
    // The high 2 bits of the low byte are unused.
    ShortString = 6,

    // StoredValueTypes can be up to 7 (this is a 3-bit field). Type 7 is
    // currently unused.
  };

  struct TreeBase {
    // Note: if fields are added here, update the size in the constructor
    uint64_t item_count;
    uint64_t node_count;
    Node root;

    TreeBase();
  };

  void increment_item_count(ssize_t delta);
  void increment_node_count(ssize_t delta);

  struct Traversal {
    uint64_t value_slot_offset;
    std::vector<uint64_t> node_offsets;
  };

  Traversal traverse(const void* k, size_t s, bool return_values_only,
      bool with_nodes, bool create);
  Traversal traverse(const void* k, size_t s, bool return_values_only,
      bool with_nodes) const;

  bool execute_check(const CheckRequest& check) const;

  std::pair<std::string, LookupResult> next_key_value_internal(
      const void* current, size_t size, bool return_value) const;

  LookupResult lookup_result_for_contents(uint64_t contents) const;

  size_t bytes_for_contents(uint64_t contents) const;
  size_t nodes_for_contents(uint64_t contents) const;

  void clear_node(uint64_t node_offset);
  void clear_value_slot(uint64_t slot_offset);

  std::string get_structure_for_contents(uint64_t contents) const;

  static int64_t int_value_for_contents(uint64_t s);
  static uint64_t value_for_contents(uint64_t s);
  static StoredValueType type_for_contents(uint64_t s);
  static bool slot_has_child(uint64_t s);
};


class PrefixTreeIterator {
public:
  PrefixTreeIterator() = delete;
  PrefixTreeIterator(const PrefixTreeIterator& other) = default;
  PrefixTreeIterator(PrefixTreeIterator&& other) = default;
  PrefixTreeIterator(const PrefixTree* tree); // for end()
  PrefixTreeIterator(const PrefixTree* tree, const std::string* location);
  PrefixTreeIterator(const PrefixTree* tree, const std::string& key,
      const PrefixTree::LookupResult& value);
  ~PrefixTreeIterator() = default;

  bool operator==(const PrefixTreeIterator& other) const;
  bool operator!=(const PrefixTreeIterator& other) const;
  PrefixTreeIterator& operator++();
  PrefixTreeIterator operator++(int);

  const std::pair<std::string, PrefixTree::LookupResult>& operator*() const;
  const std::pair<std::string, PrefixTree::LookupResult>* operator->() const;

private:
  const PrefixTree* tree;
  std::pair<std::string, PrefixTree::LookupResult> current_result;
  bool complete;
};


} // namespace sharedstructures
