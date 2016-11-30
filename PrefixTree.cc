#include "PrefixTree.hh"

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <string.h>
#include <unistd.h>

#include <algorithm>
#include <phosg/Strings.hh>

using namespace std;

namespace sharedstructures {


PrefixTree::PrefixTree(shared_ptr<Pool> pool) : pool(pool) {
  auto g = this->pool->write_lock();
  this->base_offset = this->pool->allocate_object<TreeBase>(
      2 * sizeof(uint64_t) + Node::full_size());
}

PrefixTree::PrefixTree(shared_ptr<Pool> pool, uint64_t base_offset) :
    pool(pool), base_offset(base_offset) {
  if (!this->base_offset) {
    auto g = this->pool->read_lock();
    this->base_offset = this->pool->base_object_offset();
  }

  if (!this->base_offset) {
    auto g = this->pool->write_lock();
    this->base_offset = this->pool->base_object_offset();
    if (!this->base_offset) {
      this->base_offset = this->pool->allocate_object<TreeBase>(
          2 * sizeof(uint64_t) + Node::full_size());
      this->pool->set_base_object_offset(this->base_offset);
    }
  }
}


shared_ptr<Pool> PrefixTree::get_pool() const {
  return this->pool;
}

uint64_t PrefixTree::base() const {
  return this->base_offset;
}


PrefixTree::LookupResult::LookupResult() : type(ResultValueType::Null) { }
PrefixTree::LookupResult::LookupResult(const char* s) :
    type(ResultValueType::String), as_string(s) { }
PrefixTree::LookupResult::LookupResult(const void* s, size_t size) :
    type(ResultValueType::String), as_string((const char*)s, size) { }
PrefixTree::LookupResult::LookupResult(const string& s) :
    type(ResultValueType::String), as_string(s) { }
PrefixTree::LookupResult::LookupResult(int64_t i) : type(ResultValueType::Int),
    as_int(i) { }
PrefixTree::LookupResult::LookupResult(double d) :
    type(ResultValueType::Double), as_double(d) { }
PrefixTree::LookupResult::LookupResult(bool b) : type(ResultValueType::Bool),
    as_bool(b) { }


bool PrefixTree::LookupResult::operator==(const LookupResult& other) const {
  if (this->type != other.type) {
    return false;
  }
  switch (this->type) {
    case ResultValueType::Missing:
      // there's no way to construct one of these with a Missing type
      throw invalid_argument("LookupResult has Missing type");
    case ResultValueType::String:
      return this->as_string == other.as_string;
    case ResultValueType::Int:
      return this->as_int == other.as_int;
    case ResultValueType::Double:
      return this->as_double == other.as_double;
    case ResultValueType::Bool:
      return this->as_bool == other.as_bool;
    case ResultValueType::Null:
      return true;
    default:
      return false;
  }
}

bool PrefixTree::LookupResult::operator!=(const LookupResult& other) const {
  return !this->operator==(other);
}

string PrefixTree::LookupResult::str() const {
  switch (this->type) {
    case ResultValueType::Missing:
      // there's no way to construct one of these with a Missing type
      throw invalid_argument("LookupResult has Missing type");
    case ResultValueType::String:
      return string_printf("<String:%s>", this->as_string.c_str());
    case ResultValueType::Int:
      return string_printf("<Int:%lld>", this->as_int);
    case ResultValueType::Double:
      return string_printf("<Double:%lf>", this->as_double);
    case ResultValueType::Bool:
      return string_printf("<Bool:%d>", this->as_bool);
    case ResultValueType::Null:
      return "<Null>";
    default:
      return "<UnknownType>";
  }
}


void PrefixTree::insert(const void* k, size_t k_size, const void* v,
    size_t v_size) {
  auto g = this->pool->write_lock();

  auto traverse_ret = this->traverse(k, k_size, true);
  uint64_t value_slot_offset = traverse_ret.first;

  this->clear_value_slot(value_slot_offset);

  // empty strings are stored with no allocated memory (but the type is String)
  if (v_size == 0) {
    *this->pool->at<uint64_t>(value_slot_offset) =
        (int64_t)StoredValueType::String;

  } else {
    uint64_t value_offset = this->pool->allocate(v_size);
    memcpy(this->pool->at<char>(value_offset), v, v_size);

    *this->pool->at<uint64_t>(value_slot_offset) = value_offset |
        (int64_t)StoredValueType::String;
  }

  this->increment_item_count(1);
}

void PrefixTree::insert(const void* k, size_t k_size, const string& v) {
  this->insert(k, k_size, v.data(), v.size());
}

void PrefixTree::insert(const string& k, const void* v, size_t v_size) {
  this->insert(k.data(), k.size(), v, v_size);
}

void PrefixTree::insert(const string& k, const string& v) {
  this->insert(k.data(), k.size(), v.data(), v.size());
}

void PrefixTree::insert(const void* k, size_t k_size, const struct iovec* iov,
    size_t iov_count) {

  size_t v_size = 0;
  for (size_t x = 0; x < iov_count; x++) {
    v_size += iov[x].iov_len;
  }

  auto g = this->pool->write_lock();

  auto traverse_ret = this->traverse(k, k_size, true);
  uint64_t value_slot_offset = traverse_ret.first;

  this->clear_value_slot(value_slot_offset);

  // empty strings are stored with no allocated memory (but the type is String)
  if (v_size == 0) {
    *this->pool->at<uint64_t>(value_slot_offset) =
        (int64_t)StoredValueType::String;

  } else {
    uint64_t value_offset = this->pool->allocate(v_size);
    size_t bytes_written = 0;
    for (size_t x = 0; x < iov_count; x++) {
      memcpy(this->pool->at<char>(value_offset + bytes_written),
          iov[x].iov_base, iov[x].iov_len);
      bytes_written += iov[x].iov_len;
    }

    *this->pool->at<uint64_t>(value_slot_offset) = value_offset |
        (int64_t)StoredValueType::String;
  }

  this->increment_item_count(1);
}

void PrefixTree::insert(const string& k, const struct iovec* iov,
    size_t iov_count) {
  this->insert(k.data(), k.size(), iov, iov_count);
}

void PrefixTree::insert(const void* k, size_t k_size, int64_t v) {
  auto g = this->pool->write_lock();

  auto traverse_ret = this->traverse(k, k_size, true);
  uint64_t value_slot_offset = traverse_ret.first;

  this->clear_value_slot(value_slot_offset);

  // if the first 4 bits of the value match, then sign-extension will work when
  // retrieving the value, so it's safe to store it in the slot directly
  uint8_t high_bits = (v >> 60) & 0x0F;
  if ((high_bits == 0x00) || (high_bits == 0x0F)) {
    *this->pool->at<int64_t>(value_slot_offset) = (v << 3) |
      (int64_t)StoredValueType::Int;

  // otherwise, we have to explicitly allocate space for it :(
  } else {
    uint64_t value_offset = this->pool->allocate(sizeof(NumberData));
    NumberData* num = this->pool->at<NumberData>(value_offset);
    num->as_int = v;
    num->is_double = false;
    *this->pool->at<uint64_t>(value_slot_offset) = value_offset |
        (int64_t)StoredValueType::Number;
  }

  this->increment_item_count(1);
}

void PrefixTree::insert(const string& k, int64_t v) {
  this->insert(k.data(), k.size(), v);
}

void PrefixTree::insert(const void* k, size_t k_size, double v) {
  auto g = this->pool->write_lock();

  auto traverse_ret = this->traverse(k, k_size, true);
  uint64_t value_slot_offset = traverse_ret.first;

  // if the value already exists and is a double, just reuse the storage
  uint64_t contents = *this->pool->at<uint64_t>(value_slot_offset);
  if (this->type_for_slot_contents(contents) == StoredValueType::Number) {
    uint64_t value_offset = this->value_for_slot_contents(contents);
    NumberData* num = this->pool->at<NumberData>(value_offset);
    num->as_double = v;
    num->is_double = true;

  // if the value is zero, we can store it a little more efficiently
  } else if (v == 0.0) {
    this->clear_value_slot(value_slot_offset);
    *this->pool->at<uint64_t>(value_slot_offset) = (int64_t)StoredValueType::Number;
    this->increment_item_count(1);

  } else {
    uint64_t value_offset = this->pool->allocate(sizeof(NumberData));
    NumberData* num = this->pool->at<NumberData>(value_offset);
    num->as_double = v;
    num->is_double = true;

    this->clear_value_slot(value_slot_offset);
    *this->pool->at<uint64_t>(value_slot_offset) = value_offset |
        (int64_t)StoredValueType::Number;

    this->increment_item_count(1);
  }
}

void PrefixTree::insert(const string& k, double v) {
  this->insert(k.data(), k.size(), v);
}

void PrefixTree::insert(const void* k, size_t k_size, bool v) {
  auto g = this->pool->write_lock();

  auto traverse_ret = this->traverse(k, k_size, true);
  uint64_t value_slot_offset = traverse_ret.first;

  this->clear_value_slot(value_slot_offset);
  *this->pool->at<uint64_t>(value_slot_offset) = ((int)v << 3) |
      (int64_t)StoredValueType::Trivial;

  this->increment_item_count(1);
}

void PrefixTree::insert(const string& k, bool v) {
  this->insert(k.data(), k.size(), v);
}

void PrefixTree::insert(const void* k, size_t k_size) {
  auto g = this->pool->write_lock();

  auto traverse_ret = this->traverse(k, k_size, true);
  uint64_t value_slot_offset = traverse_ret.first;

  this->clear_value_slot(value_slot_offset);
  *this->pool->at<uint64_t>(value_slot_offset) = (2 << 3) |
      (int64_t)StoredValueType::Trivial;

  this->increment_item_count(1);
}

void PrefixTree::insert(const string& k) {
  this->insert(k.data(), k.size());
}

void PrefixTree::insert(const void* k, size_t k_size, const LookupResult& r) {
  switch (r.type) {
    case ResultValueType::Missing:
      // there's no way to construct one of these with a Missing type. uh... I
      // guess this means the key should be deleted?
      this->erase(k, k_size);
      break;
    case ResultValueType::String:
      this->insert(k, k_size, r.as_string.data(), r.as_string.size());
      break;
    case ResultValueType::Int:
      this->insert(k, k_size, r.as_int);
      break;
    case ResultValueType::Double:
      this->insert(k, k_size, r.as_double);
      break;
    case ResultValueType::Bool:
      this->insert(k, k_size, r.as_bool);
      break;
    case ResultValueType::Null:
      this->insert(k, k_size);
      break;
    default:
      throw invalid_argument("insert with LookupResult of unknown type");
  }
}

void PrefixTree::insert(const string& k, const LookupResult& r) {
  this->insert(k.data(), k.size(), r);
}


int64_t PrefixTree::incr(const void* k, size_t k_size, int64_t delta) {
  auto g = this->pool->write_lock();

  // get or create the value slot
  auto traverse_ret = this->traverse(k, k_size, true);
  uint64_t value_slot_offset = traverse_ret.first;
  uint64_t contents = *this->pool->at<uint64_t>(value_slot_offset);
  StoredValueType type = this->type_for_slot_contents(contents);

  int64_t value;
  if (type == StoredValueType::Int) {
    // value is stored directly in the slot
    value = this->value_for_slot_contents(contents) >> 3;
    if (value & 0x1000000000000000) {
      value |= 0xE000000000000000; // sign-extend the last 3 bits
    }

  } else if (type == StoredValueType::Number) {
    // value is stored indirectly in a NumberData struct
    NumberData* num = this->pool->at<NumberData>(this->value_for_slot_contents(contents));
    if (num->is_double) {
      // key exists but is a Double
      throw out_of_range(string((const char*)k, k_size));
    }
    value = num->as_int;

  } else if (!contents) {
    // key didn't exist; we'll create it now
    value = 0;
    this->increment_item_count(1);

  } else {
    // key exists but is the wrong type
    throw out_of_range(string((const char*)k, k_size));
  }

  value += delta;

  // if the first 4 bits of the value match, then the resulting type is Int
  uint8_t high_bits = (value >> 60) & 0x0F;
  if ((high_bits == 0x00) || (high_bits == 0x0F)) {
    // delete the NumberData struct if present and replace it with an inline Int
    if (type == StoredValueType::Number) {
      this->pool->free(this->value_for_slot_contents(contents));
    }
    *this->pool->at<int64_t>(value_slot_offset) = (value << 3) |
        (int64_t)StoredValueType::Int;

  // otherwise, the resulting type is Number
  } else {
    // if the type is already Number, just update it
    if (type == StoredValueType::Number) {
      this->pool->at<NumberData>(this->value_for_slot_contents(contents))->as_int = value;

    // else, create a Number, put the value there, and link the slot to it
    } else {
      uint64_t value_offset = this->pool->allocate(sizeof(NumberData));
      NumberData* num = this->pool->at<NumberData>(value_offset);
      num->as_int = value;
      num->is_double = false;
      *this->pool->at<uint64_t>(value_slot_offset) = value_offset |
          (int64_t)StoredValueType::Number;
    }
  }

  return value;
}

int64_t PrefixTree::incr(const string& k, int64_t delta) {
  return this->incr(k.data(), k.size(), delta);
}

double PrefixTree::incr(const void* k, size_t k_size, double delta) {
  auto g = this->pool->write_lock();

  // get or create the value slot
  auto traverse_ret = this->traverse(k, k_size, true);
  uint64_t value_slot_offset = traverse_ret.first;
  uint64_t contents = *this->pool->at<uint64_t>(value_slot_offset);
  StoredValueType type = this->type_for_slot_contents(contents);

  if (type == StoredValueType::Number) {
    // value is stored indirectly in a NumberData struct
    NumberData* num = this->pool->at<NumberData>(this->value_for_slot_contents(contents));
    if (!num->is_double) {
      // key exists but is an Int
      throw out_of_range(string((const char*)k, k_size));
    }
    num->as_double += delta;
    return num->as_double;

  } else if (!contents) {
    // key didn't exist; we'll create it now
    uint64_t value_offset = this->pool->allocate(sizeof(NumberData));
    NumberData* num = this->pool->at<NumberData>(value_offset);
    num->as_double = delta;
    num->is_double = true;
    *this->pool->at<uint64_t>(value_slot_offset) = value_offset |
        (int64_t)StoredValueType::Number;

    this->increment_item_count(1);
    return delta;

  } else {
    // key exists but is the wrong type
    throw out_of_range(string((const char*)k, k_size));
  }
}

double PrefixTree::incr(const string& k, double delta) {
  return this->incr(k.data(), k.size(), delta);
}


bool PrefixTree::erase(const void* k, size_t k_size) {
  auto g = this->pool->write_lock();

  auto traverse_ret = this->traverse(k, k_size, false);
  uint64_t value_slot_offset = traverse_ret.first;
  uint64_t node_offset = traverse_ret.second;

  if (value_slot_offset == 0) {
    return false; // key already doesn't exist
  }
  bool ret = (value_slot_offset != 0);

  this->clear_value_slot(value_slot_offset);

  // delete all empty nodes on the path, except the root
  Node* node = this->pool->at<Node>(node_offset);
  while (node_offset && node->parent_offset && !node->has_children()) {

    // the node has no children, but may have a value. unlink this node from the
    // parent and move its value to its slot in the parent
    Node* parent_node = this->pool->at<Node>(node->parent_offset);
    parent_node->children[node->parent_slot - parent_node->start] = node->value;

    // move up to the parent and delete the child node
    uint64_t parent_offset = node->parent_offset;
    this->pool->free_object<Node>(node_offset);
    node_offset = parent_offset;
    node = this->pool->at<Node>(node_offset);

    this->increment_node_count(-1);
  }

  return ret;
}

bool PrefixTree::erase(const string& key) {
  return this->erase(key.data(), key.size());
}


void PrefixTree::clear() {
  auto g = this->pool->write_lock();
  this->clear_node(this->base_offset + offsetof(TreeBase, root));
}


bool PrefixTree::exists(const void* k, size_t k_size) {
  auto g = this->pool->read_lock();
  return this->traverse(k, k_size, false).first;
}

bool PrefixTree::exists(const string& key) {
  return this->exists(key.data(), key.size());
}


PrefixTree::ResultValueType PrefixTree::type(const void* k, size_t k_size) const {
  auto g = this->pool->read_lock();

  auto traverse_ret = this->traverse(k, k_size);
  uint64_t value_slot_offset = traverse_ret.first;

  if (!value_slot_offset) {
    return ResultValueType::Missing;
  }

  uint64_t contents = *this->pool->at<uint64_t>(value_slot_offset);
  switch (this->type_for_slot_contents(contents)) {
    case StoredValueType::SubNode:
      return ResultValueType::Missing;

    case StoredValueType::String:
      return ResultValueType::String;

    case StoredValueType::Int:
      return ResultValueType::Int;

    case StoredValueType::Number: {
      uint64_t offset = this->value_for_slot_contents(contents);
      NumberData* num = this->pool->at<NumberData>(offset);
      return num->is_double ? ResultValueType::Double : ResultValueType::Int;
    }

    case StoredValueType::Trivial:
      if (this->value_for_slot_contents(contents) == (2 << 3)) {
        return ResultValueType::Null;
      }
      return ResultValueType::Bool;
  }
  throw invalid_argument("unknown stored value type");
}

PrefixTree::ResultValueType PrefixTree::type(const string& key) const {
  return this->type(key.data(), key.size());
}


PrefixTree::LookupResult PrefixTree::at(const void* k, size_t k_size) const {
  auto g = this->pool->read_lock();

  auto traverse_ret = this->traverse(k, k_size);

  uint64_t value_slot_offset = traverse_ret.first;
  if (!value_slot_offset) {
    throw out_of_range(string((const char*)k, k_size));
  }

  uint64_t contents = *this->pool->at<uint64_t>(value_slot_offset);
  if (!contents) {
    throw out_of_range(string((const char*)k, k_size));
  }

  return this->lookup_result_for_contents(contents);
}

PrefixTree::LookupResult PrefixTree::at(const string& key) const {
  return this->at(key.data(), key.size());
}


string PrefixTree::next_key(const void* current, size_t size) const {
  return this->next_key_value_internal(current, size, false).first;
}

string PrefixTree::next_key(const string& current) const {
  return this->next_key_value_internal(current.data(), current.size(), false).first;
}

pair<string, PrefixTree::LookupResult> PrefixTree::next_key_value(
    const void* current, size_t size) const {
  return this->next_key_value_internal(current, size, true);
}

pair<string, PrefixTree::LookupResult> PrefixTree::next_key_value(
    const string& current) const {
  return this->next_key_value_internal(current.data(), current.size(), true);
}

PrefixTreeIterator PrefixTree::begin() const {
  return PrefixTreeIterator(this, NULL);
}

PrefixTreeIterator PrefixTree::end() const {
  return PrefixTreeIterator(this);
}


size_t PrefixTree::size() const {
  auto g = this->pool->read_lock();
  return this->pool->at<TreeBase>(this->base_offset)->item_count;
}

size_t PrefixTree::node_size() const {
  auto g = this->pool->read_lock();
  return this->pool->at<TreeBase>(this->base_offset)->node_count;
}


static void print_indent(FILE* stream, uint64_t indent) {
  while (indent) {
    fputc(' ', stream);
    indent--;
  }
}

void PrefixTree::print(FILE* stream, uint8_t k, uint64_t node_offset,
    uint64_t indent) const {
  if (!node_offset) {
    node_offset = this->base_offset + offsetof(TreeBase, root);
  }
  const Node* n = this->pool->at<Node>(node_offset);
  print_indent(stream, indent);
  fprintf(stream, "%02hhX @ %" PRIu64 " (%02hhX, %02hhX), from=(%" PRIu64 ":%02hhX)",
      k, node_offset, n->start, n->end, n->parent_offset, n->parent_slot);
  if (n->value) {
    StoredValueType t = this->type_for_slot_contents(n->value);
    fprintf(stream, " +%d@%" PRIu64 "\n", (int)t, this->value_for_slot_contents(n->value));
  } else {
    fputc('\n', stream);
  }
  for (uint16_t x = 0; x < (n->end - n->start + 1); x++) {
    uint64_t contents = n->children[x];
    StoredValueType type = this->type_for_slot_contents(contents);
    if (type != StoredValueType::SubNode) {
      print_indent(stream, indent + 2);
      uint64_t value = this->value_for_slot_contents(contents);
      fprintf(stream, "(%X) +%d@%" PRIu64 "\n", x + n->start, (int)type, value);
    } else if (contents) {
      this->print(stream, x + n->start, contents, indent + 2);
    }
  }
}


PrefixTree::Node::Node(uint8_t start, uint8_t end, uint8_t parent_slot,
    uint64_t parent_offset, uint64_t value) : start(start), end(end),
    parent_slot(parent_slot), parent_offset(parent_offset), value(value) { }

PrefixTree::Node::Node(uint8_t slot, uint8_t parent_slot,
    uint64_t parent_offset, uint64_t value) : start(slot), end(slot),
    parent_slot(parent_slot), parent_offset(parent_offset), value(value) {
  this->children[0] = 0;
}

PrefixTree::Node::Node() : start(0x00), end(0xFF), parent_slot(0),
    parent_offset(0), value(0) {
  for (uint16_t x = 0; x < 0x100; x++) {
    this->children[x] = 0;
  }
}

bool PrefixTree::Node::has_children() const {
  for (size_t x = 0; x < (size_t)(this->end - this->start + 1); x++) {
    if (this->children[x]) {
      return true;
    }
  }
  return false;
}

size_t PrefixTree::Node::size_for_range(uint8_t start, uint8_t end) {
  return sizeof(Node) + (end - start + 1) * sizeof(uint64_t);
}

size_t PrefixTree::Node::full_size() {
  return sizeof(Node) + 0x100 * sizeof(uint64_t);
}


PrefixTree::TreeBase::TreeBase() : item_count(0), node_count(1), root() { }


void PrefixTree::increment_item_count(ssize_t delta) {
  this->pool->at<TreeBase>(this->base_offset)->item_count += delta;
}

void PrefixTree::increment_node_count(ssize_t delta) {
  this->pool->at<TreeBase>(this->base_offset)->node_count += delta;
}


pair<uint64_t, uint64_t> PrefixTree::traverse(const void* k, size_t s,
    bool create) {
  uint8_t* k_data = (uint8_t*)k;
  uint8_t* k_end = k_data + s;

  uint64_t node_offset = this->base_offset + offsetof(TreeBase, root);

  // follow links to the leaf node
  while (k_data != k_end) {
    Node* node = this->pool->at<Node>(node_offset);
    if (*k_data < node->start || *k_data > node->end) {
      // out of range
      break;
    }

    uint64_t next_node_offset = node->children[*k_data - node->start];

    // if the next node is missing, then the key doesn't exist - we're done here
    if (!next_node_offset) {
      break;
    }

    // if the next node is a value, return it only if we're at the end
    // of the key. if it's not the end, we may have to make some changes
    if (this->type_for_slot_contents(next_node_offset) != StoredValueType::SubNode) {
      if (k_data == k_end - 1) {
        return make_pair(this->pool->at(&node->children[*k_data - node->start]),
            node_offset);
      } else {
        break;
      }
    }

    // the next node is a subnode, not a value - move down to it
    node_offset = next_node_offset;
    k_data++;
  }

  // if the node was found and it's not a value, return the value field
  if (k_data == k_end) {
    return make_pair(node_offset + offsetof(Node, value), node_offset);
  }

  // the node wasn't found; fail if we're not supposed to create it
  if (!create) {
    return make_pair(0, node_offset);
  }

  // if we get here, then the node doesn't exist and we should create it

  // first check if the current node has enough available range, and replace it
  // if not. note that we don't check if previous_node is missing (or check if
  // k_data == k) because the root node is always complete (has 256 slots), so
  // we'll never need to extend its range.
  {
    Node* node = this->pool->at<Node>(node_offset);
    bool extend_start = (*k_data < node->start);
    bool needs_extend = extend_start || (*k_data > node->end);
    if (needs_extend) {
      // make a new node
      uint8_t new_start = extend_start ? *k_data : node->start;
      uint8_t new_end = (!extend_start) ? *k_data : node->end;
      uint64_t new_node_offset = this->pool->allocate_object<Node, uint8_t, uint8_t, uint8_t, uint64_t, uint64_t>(
          new_start, new_end, node->parent_slot, node->parent_offset, node->value,
          Node::size_for_range(new_start, new_end));
      node = this->pool->at<Node>(node_offset); // may be invalidated by allocate()
      Node* new_node = this->pool->at<Node>(new_node_offset);
      Node* parent_node = this->pool->at<Node>(node->parent_offset);

      // copy the relevant data from the old node and update parent_offset for
      // all the child nodes
      uint16_t x = new_node->start;
      if (extend_start) {
        for (; x < node->start; x++) {
          new_node->children[x - new_node->start] = 0;
        }
        for (; x <= new_node->end; x++) {
          uint64_t contents = node->children[x - node->start];
          new_node->children[x - new_node->start] = contents;
          if (contents && (this->type_for_slot_contents(contents) == StoredValueType::SubNode)) {
            this->pool->at<Node>(contents)->parent_offset = new_node_offset;
          }
        }
      } else {
        for (; x <= node->end; x++) {
          uint64_t contents = node->children[x - node->start];
          new_node->children[x - new_node->start] = contents;
          if (contents && (this->type_for_slot_contents(contents) == StoredValueType::SubNode)) {
            this->pool->at<Node>(contents)->parent_offset = new_node_offset;
          }
        }
        for (; x <= new_node->end; x++) {
          new_node->children[x - new_node->start] = 0;
        }
      }

      // move the new node into place and delete the old node
      parent_node->children[new_node->parent_slot - parent_node->start] = new_node_offset;
      this->pool->free_object<Node>(node_offset);
      node_offset = new_node_offset;
    }
  }

  // now the current node contains a slot that we want to follow but it's empty,
  // so we'll create all the nodes we need. we won't create the last node
  // because we'll just stick the value in that slot.
  while (k_data != k_end - 1) {
    // allocate a node and make the current node point to it
    Node* node = this->pool->at<Node>(node_offset);
    uint64_t new_node_value = node->children[*k_data - node->start];
    uint64_t new_node_offset = this->pool->allocate_object<Node, uint8_t, uint8_t, uint64_t, uint64_t>(
        k_data[1], *k_data, node_offset, new_node_value,
        Node::size_for_range(*k_data, *k_data));

    // link to the new node from the parent
    node = this->pool->at<Node>(node_offset);
    node->children[*k_data - node->start] = new_node_offset;

    this->increment_node_count(1);

    // move down to that node
    node_offset = new_node_offset;
    k_data++;
  }

  // now `node` is the node that contains the slot we want
  Node* node = this->pool->at<Node>(node_offset);
  return make_pair(this->pool->at(&node->children[*k_data - node->start]),
      node_offset);
}

pair<uint64_t, uint64_t> PrefixTree::traverse(const void* k, size_t s) const {
  // traverse() is fairly long, so we don't want to duplicate implementation;
  // if create is false, it can be thought of as a const method because it
  // doesn't modify the tree in that case
  return const_cast<PrefixTree*>(this)->traverse(k, s, false);
}


pair<string, PrefixTree::LookupResult> PrefixTree::next_key_value_internal(
    const void* current, size_t size, bool return_value) const {
  uint64_t node_offset = this->base_offset + offsetof(TreeBase, root);
  int16_t slot_id = 0;
  size_t key_length = 0;

  auto g = this->pool->read_lock();

  // if current is NULL, then we're just starting the iteration - check the root
  // node's value, then find the next nonempty slot if needed
  if (!current) {
    Node* node = this->pool->at<Node>(node_offset);
    if (node->value) {
      return make_pair("", return_value ? lookup_result_for_contents(node->value) :
          LookupResult());
    }

  // current is not NULL - we're continuing iteration, or starting with a prefix
  } else {
    uint8_t* k_data = (uint8_t*)current;
    uint8_t* k_end = k_data + size;

    // follow links to the leaf node as far as possible
    while (k_data != k_end) {
      Node* node = this->pool->at<Node>(node_offset);

      // if current is before anything in this node, then we have to iterate the
      // node's children, but not the node itself (the node's value is at some
      // prefix of current, so it's not after current).
      if (*k_data < node->start) {
        slot_id = node->start;
        break;
      }
      // if current is after anything in this node, then we don't iterate the node
      // at all - we'll have to unwind the stack
      if (*k_data > node->end) {
        slot_id = 0x100;
        break;
      }

      // if the slot contains a value instead of a subnode, we're done here;
      // we'll start by examining the following slot
      uint64_t next_node_offset = node->children[*k_data - node->start];
      if (this->type_for_slot_contents(next_node_offset) != StoredValueType::SubNode) {
        slot_id = *k_data + 1;
        break;
      }

      // slot contains a subnode, not a value - move down to it
      node_offset = next_node_offset;
      k_data++;
      key_length++;
    }
  }

  // we found the position in the tree that's immediately after the given key.
  // now find the next non-null value in the tree at or after that position.
  uint64_t value = 0;
  while (node_offset) {
    Node* node = this->pool->at<Node>(node_offset);

    // check the node's value if we need to
    if (slot_id < 0) {
      if (node->value) {
        value = node->value;
        break;
      }
      slot_id = node->start;

    } else if (slot_id < node->start) {
      slot_id = node->start;
    }

    // if we're done with this node, go to the next slot in the parent node
    if (slot_id > node->end) {
      node_offset = node->parent_offset;
      slot_id = node->parent_slot + 1;
      key_length--;
      continue;
    }

    // if the slot is empty, keep going in this node
    uint64_t contents = node->children[slot_id - node->start];
    if (!contents) {
      slot_id++;
      continue;
    }

    // if the slot contains a value, we're done
    StoredValueType type = this->type_for_slot_contents(contents);
    if (type != StoredValueType::SubNode) {
      value = contents;
      break;
    }

    // the slot contains a subnode, so move to it and check if it has a value
    node_offset = contents;
    slot_id = -1;
    key_length++;
  }

  // if we didn't find a value, we're done iterating the tree
  if (!value) {
    throw out_of_range("done iterating tree");
  }

  // we did find a value - generate the key and return the key/value pair
  string key;
  key.reserve(key_length);
  if (slot_id >= 0) {
    key += (char)slot_id;
  }
  Node* node = this->pool->at<Node>(node_offset);
  while (node->parent_offset) {
    key += (char)node->parent_slot;
    node = this->pool->at<Node>(node->parent_offset);
  }
  reverse(key.begin(), key.end());

  return make_pair(key, return_value ? lookup_result_for_contents(value) :
      LookupResult());
}


PrefixTree::LookupResult PrefixTree::lookup_result_for_contents(
    uint64_t contents) const {
  switch (this->type_for_slot_contents(contents)) {
    case StoredValueType::SubNode:
      throw out_of_range("");
      break;

    case StoredValueType::String: {
      uint64_t data_offset = this->value_for_slot_contents(contents);
      if (data_offset) {
        return LookupResult(this->pool->at<char>(data_offset),
            this->pool->block_size(data_offset));
      }
      return LookupResult("", 0);
    }

    case StoredValueType::Int: {
      int64_t v = this->value_for_slot_contents(contents) >> 3;
      if (v & 0x1000000000000000) {
        v |= 0xE000000000000000; // sign-extend the last 3 bits
      }
      return LookupResult(v);
    }

    case StoredValueType::Number: {
      uint64_t num_offset = this->value_for_slot_contents(contents);
      if (!num_offset) {
        return LookupResult(0.0);
      }
      const NumberData* num = this->pool->at<NumberData>(num_offset);
      if (num->is_double) {
        return LookupResult(num->as_double);
      }
      return LookupResult(num->as_int);
    }

    case StoredValueType::Trivial: {
      uint64_t trivial_id = this->value_for_slot_contents(contents) >> 3;
      if (trivial_id == 2) {
        return LookupResult(); // Null
      }
      return LookupResult(trivial_id ? true : false);
    }
  }
  throw invalid_argument("slot has unknown type");
}


void PrefixTree::clear_node(uint64_t node_offset) {
  this->clear_value_slot(node_offset + offsetof(Node, value));

  Node* node = this->pool->at<Node>(node_offset);
  uint8_t start = node->start, end = node->end;
  for (uint16_t x = 0; x < end - start + 1; x++) {
    this->clear_value_slot(node_offset + offsetof(Node, children) + (x * sizeof(uint64_t)));
  }
}

void PrefixTree::clear_value_slot(uint64_t slot_offset) {
  uint64_t* slot = this->pool->at<uint64_t>(slot_offset);
  if (*slot == 0) {
    return; // slot is already empty
  }

  switch (this->type_for_slot_contents(*slot)) {
    case StoredValueType::SubNode: {
      // delete the entire subtree recursively
      uint64_t node_offset = this->value_for_slot_contents(*slot);
      this->clear_node(node_offset);
      this->pool->free(node_offset);
      *slot = 0;
      this->increment_node_count(-1);
      break;
    }

    case StoredValueType::String:
    case StoredValueType::Number: {
      // these types all point to a buffer; just free it and clear the pointer.
      // the buffer can be null though (e.g. for empty strings or zero-valued
      // doubles)
      uint64_t value_offset = this->value_for_slot_contents(*slot);
      if (value_offset) {
        this->pool->free(value_offset);
      }
      *slot = 0;
      this->increment_item_count(-1);
      break;
    }

    case StoredValueType::Int:
    case StoredValueType::Trivial:
      // these types don't have allocated storage; just clear the value
      *slot = 0;
      this->increment_item_count(-1);
      break;
  }
}


uint64_t PrefixTree::value_for_slot_contents(uint64_t s) {
  return s & (~7);
}

PrefixTree::StoredValueType PrefixTree::type_for_slot_contents(
    uint64_t s) {
  return (StoredValueType)(s & 7);
}


PrefixTreeIterator::PrefixTreeIterator(const PrefixTree* tree) : tree(tree),
    complete(true) { }

PrefixTreeIterator::PrefixTreeIterator(const PrefixTree* tree,
    const string* location) : tree(tree), complete(false) {
  try {
    if (location) {
      this->current_result = this->tree->next_key_value(*location);
    } else {
      this->current_result = this->tree->next_key_value();
    }
  } catch (const out_of_range& e) {
    this->complete = true;
  }
}

bool PrefixTreeIterator::operator==(const PrefixTreeIterator& other) const {
  if (this->complete) {
    return other.complete;
  }
  if (other.complete) {
    return false;
  }
  return this->current_result.first == other.current_result.first;
}

bool PrefixTreeIterator::operator!=(const PrefixTreeIterator& other) const {
  return !(this->operator==(other));
}

PrefixTreeIterator& PrefixTreeIterator::operator++() {
  if (this->complete) {
    throw invalid_argument("can\'t advance iterator beyond end position");
  }
  try {
    this->current_result = this->tree->next_key_value(this->current_result.first);
  } catch (const out_of_range& e) {
    this->complete = true;
  }
  return *this;
}

PrefixTreeIterator PrefixTreeIterator::operator++(int) {
  PrefixTreeIterator ret = *this;
  this->operator++();
  return ret;
}

const pair<string, PrefixTree::LookupResult>& PrefixTreeIterator::operator*() const {
  return this->current_result;
}


} // namespace sharedstructures
