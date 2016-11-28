#include <stdint.h>
#include <sys/uio.h> // for iov

#include <memory>
#include <string>
#include <utility>

#include "Pool.hh"

namespace sharedstructures {


class PrefixTreeIterator;

class PrefixTree {
public:
  PrefixTree() = delete;
  PrefixTree(const PrefixTree&) = delete;
  PrefixTree(PrefixTree&&) = delete;

  // create constructor - allocates and initializes a new prefix tree in pool.
  explicit PrefixTree(std::shared_ptr<Pool> pool);
  // (conditional) create constructor.
  // - if base_offset != 0, opens an existing prefix tree in pool.
  // - if base_offset == 0, opens the prefix tree at the pool's base object
  //   offset, creating one if the base object offset is also 0.
  PrefixTree(std::shared_ptr<Pool> pool, uint64_t base_offset);

  ~PrefixTree() = default;

  // returns the pool for this prefix tree
  std::shared_ptr<Pool> get_pool() const;
  // returns the base offset for this prefix tree. if it was automatically
  // allocated with the conditional create constructor, this will tell you how
  // to open it again later.
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

  // inserts/overwrites a key with a string value.
  void insert(const void* k, size_t k_size, const void* v, size_t v_size);
  void insert(const void* k, size_t k_size, const std::string& v);
  void insert(const std::string& k, const void* v, size_t v_size);
  void insert(const std::string& k, const std::string& v);
  void insert(const void* k, size_t k_size, const struct iovec *iov,
      size_t iovcnt);
  void insert(const std::string& k, const struct iovec *iov, size_t iovcnt);

  // inserts/overwrites a key with an integer value.
  void insert(const void* k, size_t k_size, int64_t v);
  void insert(const std::string& k, int64_t v);

  // inserts/overwrites a key with a floating-point value.
  void insert(const void* k, size_t k_size, double v);
  void insert(const std::string& k, double v);

  // inserts/overwrites a key with a boolean value.
  void insert(const void* k, size_t k_size, bool v);
  void insert(const std::string& k, bool v);

  // inserts/overwrites a key with a null value.
  void insert(const void* k, size_t k_size);
  void insert(const std::string& k);

  // inserts/overwrites a key with the result of a previous lookup.
  void insert(const void* k, size_t k_size, const LookupResult& res);
  void insert(const std::string& k, const LookupResult& res);

  // deletes a key.
  bool erase(const void* k, size_t k_size);
  bool erase(const std::string& k);

  // deletes all the keys in the prefix tree.
  void clear();

  // checks if a key exists.
  bool exists(const void* k, size_t k_size);
  bool exists(const std::string& k);

  // returns the type of a key, or Missing if it doesn't exist. this is slightly
  // slower than exists() for keys that aren't Int, Bool or Null since it
  // requires an extra memory access.
  ResultValueType type(const void* k, size_t k_size) const;
  ResultValueType type(const std::string& k) const;

  // returns the value of a key. throws std::out_of_range if the key is missing.
  LookupResult at(const void* k, size_t k_size) const;
  LookupResult at(const std::string& key) const;

  // these functions return the key after the given key, along with that key's
  // value (in the case of next_key_value). to iterate the tree, call one of
  // these functions with no arguments, then keep calling it and passing the
  // previous call's return value until it throws std::out_of_range. it's safe
  // to modify the tree during such iterations. it's also safe to switch between
  // calling the different next_* functions during iteration.
  std::string next_key(const void* current = NULL, size_t size = 0) const;
  std::string next_key(const std::string& current) const;
  std::pair<std::string, LookupResult> next_key_value(
      const void* current = NULL, size_t size = 0) const;
  std::pair<std::string, LookupResult> next_key_value(
      const std::string& current) const;

  // these functions implement standard C++ iteration.
  PrefixTreeIterator begin() const;
  PrefixTreeIterator end() const;

  // inspection methods.
  size_t size() const; // key count
  size_t node_size() const; // node count

  void print(FILE* stream, uint8_t k = 0, uint64_t node_offset = 0, uint64_t indent = 0) const;

private:
  std::shared_ptr<Pool> pool;
  uint64_t base_offset;

  struct Node {
    uint8_t start;
    uint8_t end;
    uint8_t parent_slot;
    uint64_t parent_offset;

    uint64_t value;
    uint64_t children[0];

    // sets start, end, value, parent; doesn't initialize children
    Node(uint8_t start, uint8_t end, uint8_t parent_slot,
        uint64_t parent_offset, uint64_t value);
    // sets everything, including children. creates a node with one slot.
    Node(uint8_t slot, uint8_t parent_slot, uint64_t parent_offset,
        uint64_t value);
    // sets everything, including children. creates a node with all slots.
    Node();

    bool has_children() const;

    static size_t size_for_range(uint8_t start, uint8_t end);
    static size_t full_size();
  };

  struct NumberData {
    union {
      int64_t as_int;
      double as_double;
    };
    bool is_double;
  };

  enum class StoredValueType {
    SubNode = 0,
    String  = 1,
    Int     = 2, // 61-bit inlined int; for 64-bit ints and floats we use Number
    Number  = 3,
    Trivial = 4, // inlined int; 0=false, 1=true, 2=null
    // can be up to 7 (this is a 3-bit field)
  };

  struct TreeBase {
    // note: if fields are added here, update the size in the constructor
    size_t item_count;
    size_t node_count;
    Node root;

    TreeBase();
  };

  void increment_item_count(ssize_t delta);
  void increment_node_count(ssize_t delta);

  std::pair<uint64_t, uint64_t> traverse(const void* k, size_t k_size,
      bool create);
  std::pair<uint64_t, uint64_t> traverse(const void* k, size_t s) const;

  std::pair<std::string, LookupResult> next_key_value_internal(
      const void* current, size_t size, bool return_value) const;

  LookupResult lookup_result_for_contents(uint64_t contents) const;

  void clear_node(uint64_t node_offset);
  void clear_value_slot(uint64_t slot_offset);

  static uint64_t value_for_slot_contents(uint64_t s);
  static StoredValueType type_for_slot_contents(uint64_t s);
  static bool slot_has_child(uint64_t s);
};


class PrefixTreeIterator {
public:
  PrefixTreeIterator() = delete;
  PrefixTreeIterator(const PrefixTreeIterator& other) = default;
  PrefixTreeIterator(PrefixTreeIterator&& other) = default;
  PrefixTreeIterator(const PrefixTree* tree); // for end()
  PrefixTreeIterator(const PrefixTree* tree, const std::string* location); // for begin()/find()
  ~PrefixTreeIterator() = default;

  bool operator==(const PrefixTreeIterator& other) const;
  bool operator!=(const PrefixTreeIterator& other) const;
  PrefixTreeIterator& operator++();
  PrefixTreeIterator operator++(int);
  const std::pair<std::string, PrefixTree::LookupResult>& operator*() const;

private:
  const PrefixTree* tree;
  std::pair<std::string, PrefixTree::LookupResult> current_result;
  bool complete;
};


} // namespace sharedstructures
