#include "HashTable.hh"

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <string.h>

using namespace std;

namespace sharedstructures {


static uint64_t fnv1a64(const void* data, size_t size,
    uint64_t hash = 0xCBF29CE484222325) {

  const uint8_t *data_ptr = (const uint8_t*)data;
  const uint8_t *end_ptr = data_ptr + size;

  for (; data_ptr != end_ptr; data_ptr++) {
    hash = (hash ^ (uint64_t)*data_ptr) * 0x00000100000001B3;
  }
  return hash;
}


HashTable::HashTable(shared_ptr<Allocator> allocator, uint8_t bits) :
    allocator(allocator) {
  auto g = this->allocator->lock(true);
  this->base_offset = this->create_hash_base(bits);
}

HashTable::HashTable(shared_ptr<Allocator> allocator, uint64_t base_offset,
    uint8_t bits) : allocator(allocator), base_offset(base_offset) {
  if (!this->base_offset) {
    auto g = this->allocator->lock(false);
    this->base_offset = this->allocator->base_object_offset();
  }

  if (!this->base_offset) {

    auto g = this->allocator->lock(true);
    this->base_offset = this->allocator->base_object_offset();
    if (!this->base_offset) {
      this->base_offset = this->create_hash_base(bits);
      this->allocator->set_base_object_offset(this->base_offset);
    }
  }
}


shared_ptr<Allocator> HashTable::get_allocator() const {
  return this->allocator;
}

uint64_t HashTable::base() const {
  return this->base_offset;
}


HashTable::CheckRequest::CheckRequest(const void* key, size_t key_size,
    const void* value, size_t value_size) : key(key), key_size(key_size),
    value(value), value_size(value_size),
    key_hash(fnv1a64(this->key, this->key_size)) { }
HashTable::CheckRequest::CheckRequest(const void* key, size_t key_size,
    const std::string& value) :
    CheckRequest(key, key_size, value.data(), value.size()) { }
HashTable::CheckRequest::CheckRequest(const void* key, size_t key_size) :
    CheckRequest(key, key_size, nullptr, 0) { }
HashTable::CheckRequest::CheckRequest(const std::string& key, const void* value,
    size_t value_size) :
    CheckRequest(key.data(), key.size(), value, value_size) { }
HashTable::CheckRequest::CheckRequest(const std::string& key,
    const std::string& value) :
    CheckRequest(key.data(), key.size(), value.data(), value.size()) { }
HashTable::CheckRequest::CheckRequest(const std::string& key) :
    CheckRequest(key.data(), key.size(), nullptr, 0) { }


bool HashTable::insert(const void* k, size_t k_size, const void* v,
    size_t v_size, const CheckRequest* check) {
  uint64_t hash = fnv1a64(k, k_size);

  auto g = this->allocator->lock(true);

  if (check && !this->execute_check(*check)) {
    return false;
  }

  auto p = this->allocator->get_pool();

  // create the new key-value pair and copy the data in
  uint64_t new_kv_pair_offset = this->allocator->allocate(k_size + v_size);
  memcpy(p->at<void>(new_kv_pair_offset), k, k_size);
  memcpy(p->at<void>(new_kv_pair_offset + k_size), v, v_size);

  // get the slot pointer
  HashTableBase* table = p->at<HashTableBase>(this->base_offset);
  uint64_t slot_offset = table->slots_offset +
      (hash & ((1 << table->bits) - 1)) * sizeof(Slot);
  Slot* slot = p->at<Slot>(slot_offset);

  // if the slot is empty, just link it to the value
  if (!slot->key_offset) {
    // link it in the slot
    slot->key_offset = new_kv_pair_offset;
    slot->key_size = k_size;
    table->item_count++;
    return true;
  }

  // if the slot contains a direct value...
  if (!(slot->key_offset & 1)) {

    // if the key matches the key we're inserting, free the old buffer and
    // replace it with the new one
    if ((slot->key_size == k_size) &&
        !memcmp(p->at<void>(slot->key_offset), k, k_size)) {
      this->allocator->free(slot->key_offset);
      slot = p->at<Slot>(slot_offset); // may be invalidated
      slot->key_offset = new_kv_pair_offset;
      slot->key_size = k_size;

    // the key doesn't match. convert this to an indirect value
    } else {
      uint64_t existing_offset = this->allocator->allocate(
          sizeof(IndirectValue));
      uint64_t created_offset = this->allocator->allocate(
          sizeof(IndirectValue));
      IndirectValue* existing = p->at<IndirectValue>(existing_offset);
      IndirectValue* created = p->at<IndirectValue>(created_offset);
      slot = p->at<Slot>(slot_offset); // may be invalidated
      table = p->at<HashTableBase>(this->base_offset); // may be invalidated

      created->next = 0;
      created->key_offset = new_kv_pair_offset;
      created->key_size = k_size;
      existing->next = created_offset;
      existing->key_offset = slot->key_offset;
      existing->key_size = slot->key_size;
      slot->key_offset = existing_offset | 1;
      slot->key_size = 0;
      table->item_count++;
    }

  // the slot contains indirect values
  } else {
    // walk the list, looking for keys that match the one we're inserting
    auto walk_ret = walk_indirect_list(slot->key_offset & (~1), k, k_size);

    // if we found a match, just replace the buffer pointer on it
    if (walk_ret.second) {
      IndirectValue* indirect = p->at<IndirectValue>(walk_ret.second);
      this->allocator->free(indirect->key_offset);
      indirect = p->at<IndirectValue>(walk_ret.second);
      indirect->key_offset = new_kv_pair_offset;
      indirect->key_size = k_size;

    // no match; allocate a new indirect value at the end
    } else {
      uint64_t created_offset = this->allocator->allocate(
          sizeof(IndirectValue));
      IndirectValue* prev = p->at<IndirectValue>(walk_ret.first);
      IndirectValue* created = p->at<IndirectValue>(created_offset);
      table = p->at<HashTableBase>(this->base_offset); // may be invalidated

      prev->next = created_offset;
      created->next = 0;
      created->key_offset = new_kv_pair_offset;
      created->key_size = k_size;
      table->item_count++;
    }
  }

  return true;
}

bool HashTable::insert(const void* k, size_t k_size, const string& v,
    const CheckRequest* check) {
  return this->insert(k, k_size, v.data(), v.size(), check);
}

bool HashTable::insert(const string& k, const void* v, size_t v_size,
    const CheckRequest* check) {
  return this->insert(k.data(), k.size(), v, v_size, check);
}

bool HashTable::insert(const string& k, const string& v,
    const CheckRequest* check) {
  return this->insert(k.data(), k.size(), v.data(), v.size(), check);
}

int64_t HashTable::incr(const void* k, size_t k_size, int64_t delta) {
  // TODO: reduce code duplication here with insert() and incr(double)

  uint64_t hash = fnv1a64(k, k_size);

  auto g = this->allocator->lock(true);
  auto p = this->allocator->get_pool();

  // get the slot pointer
  HashTableBase* table = p->at<HashTableBase>(this->base_offset);
  uint64_t slot_offset = table->slots_offset +
      (hash & ((1 << table->bits) - 1)) * sizeof(Slot);
  Slot* slot = p->at<Slot>(slot_offset);

  // if the slot is empty, create a new 64-bit value
  if (!slot->key_offset) {
    // create the new key-value pair in the slot and copy the data in
    slot->key_offset = this->allocator->allocate(k_size + sizeof(int64_t));
    memcpy(p->at<void>(slot->key_offset), k, k_size);
    memcpy(p->at<void>(slot->key_offset + k_size), &delta, sizeof(int64_t));
    slot->key_size = k_size;
    table->item_count++;
    return delta;
  }

  // if the slot contains a direct value...
  if (!(slot->key_offset & 1)) {

    // if the key matches the key we're inserting, check and increment the value
    if ((slot->key_size == k_size) &&
        !memcmp(p->at<void>(slot->key_offset), k, k_size)) {
      uint64_t v_offset = slot->key_offset + slot->key_size;
      uint64_t v_size = this->allocator->block_size(slot->key_offset) -
          slot->key_size;
      if (v_size == 1) {
        *p->at<int8_t>(v_offset) += delta;
        return *p->at<int8_t>(v_offset);
      } else if (v_size == 2) {
        *p->at<int16_t>(v_offset) += delta;
        return *p->at<int16_t>(v_offset);
      } else if (v_size == 4) {
        *p->at<int32_t>(v_offset) += delta;
        return *p->at<int32_t>(v_offset);
      } else if (v_size == 8) {
        *p->at<int64_t>(v_offset) += delta;
        return *p->at<int64_t>(v_offset);
      } else {
        throw out_of_range("incr() against key of wrong size");
      }

    // the key doesn't match. convert this to an indirect value
    } else {
      uint64_t existing_offset = this->allocator->allocate(
          sizeof(IndirectValue));
      uint64_t created_offset = this->allocator->allocate(
          sizeof(IndirectValue));
      IndirectValue* existing = p->at<IndirectValue>(existing_offset);
      IndirectValue* created = p->at<IndirectValue>(created_offset);
      slot = p->at<Slot>(slot_offset); // may be invalidated
      table = p->at<HashTableBase>(this->base_offset); // may be invalidated

      created->next = 0;
      created->key_offset = this->allocator->allocate(k_size + sizeof(int64_t));
      memcpy(p->at<void>(created->key_offset), k, k_size);
      memcpy(p->at<void>(created->key_offset + k_size), &delta,
          sizeof(int64_t));
      created->key_size = k_size;

      existing->next = created_offset;
      existing->key_offset = slot->key_offset;
      existing->key_size = slot->key_size;

      slot->key_offset = existing_offset | 1;
      slot->key_size = 0;
      table->item_count++;

      return delta;
    }

  // the slot contains indirect values
  } else {
    // walk the list, looking for keys that match the one we're inserting
    auto walk_ret = walk_indirect_list(slot->key_offset & (~1), k, k_size);

    // if we found a match, check and increment the value
    if (walk_ret.second) {
      IndirectValue* indirect = p->at<IndirectValue>(walk_ret.second);
      uint64_t v_offset = indirect->key_offset + indirect->key_offset;
      uint64_t v_size = this->allocator->block_size(indirect->key_offset) -
          indirect->key_size;
      if (v_size == 1) {
        *p->at<int8_t>(v_offset) += delta;
        return *p->at<int8_t>(v_offset);
      } else if (v_size == 2) {
        *p->at<int16_t>(v_offset) += delta;
        return *p->at<int16_t>(v_offset);
      } else if (v_size == 4) {
        *p->at<int32_t>(v_offset) += delta;
        return *p->at<int32_t>(v_offset);
      } else if (v_size == 8) {
        *p->at<int64_t>(v_offset) += delta;
        return *p->at<int64_t>(v_offset);
      } else {
        throw out_of_range("incr() against key of wrong size");
      }

    // no match; allocate a new indirect value at the end
    } else {
      uint64_t created_offset = this->allocator->allocate(
          sizeof(IndirectValue));
      IndirectValue* prev = p->at<IndirectValue>(walk_ret.first);
      IndirectValue* created = p->at<IndirectValue>(created_offset);
      table = p->at<HashTableBase>(this->base_offset); // may be invalidated

      prev->next = created_offset;
      created->next = 0;
      created->key_offset = this->allocator->allocate(k_size + sizeof(int64_t));
      memcpy(p->at<void>(created->key_offset), k, k_size);
      memcpy(p->at<void>(created->key_offset + k_size), &delta,
          sizeof(int64_t));
      created->key_size = k_size;
      table->item_count++;

      return delta;
    }
  }
}

int64_t HashTable::incr(const std::string& k, int64_t delta) {
  return this->incr(k.data(), k.size(), delta);
}

double HashTable::incr(const void* k, size_t k_size, double delta) {
  // TODO: reduce code duplication here with insert() and incr(int64_t)

  uint64_t hash = fnv1a64(k, k_size);

  auto g = this->allocator->lock(true);
  auto p = this->allocator->get_pool();

  // get the slot pointer
  HashTableBase* table = p->at<HashTableBase>(this->base_offset);
  uint64_t slot_offset = table->slots_offset +
      (hash & ((1 << table->bits) - 1)) * sizeof(Slot);
  Slot* slot = p->at<Slot>(slot_offset);

  // if the slot is empty, create a new 64-bit value
  if (!slot->key_offset) {
    // create the new key-value pair in the slot and copy the data in
    slot->key_offset = this->allocator->allocate(k_size + sizeof(double));
    memcpy(p->at<void>(slot->key_offset), k, k_size);
    memcpy(p->at<void>(slot->key_offset + k_size), &delta, sizeof(double));
    slot->key_size = k_size;
    table->item_count++;
    return delta;
  }

  // if the slot contains a direct value...
  if (!(slot->key_offset & 1)) {

    // if the key matches the key we're inserting, check and increment the value
    if ((slot->key_size == k_size) &&
        !memcmp(p->at<void>(slot->key_offset), k, k_size)) {
      uint64_t v_offset = slot->key_offset + slot->key_size;
      uint64_t v_size = this->allocator->block_size(slot->key_offset) -
          slot->key_size;
      if (v_size == 4) {
        *p->at<float>(v_offset) += delta;
        return *p->at<float>(v_offset);
      } else if (v_size == 8) {
        *p->at<double>(v_offset) += delta;
        return *p->at<double>(v_offset);
      } else {
        throw out_of_range("incr() against key of wrong size");
      }

    // the key doesn't match. convert this to an indirect value
    } else {
      uint64_t existing_offset = this->allocator->allocate(
          sizeof(IndirectValue));
      uint64_t created_offset = this->allocator->allocate(
          sizeof(IndirectValue));
      IndirectValue* existing = p->at<IndirectValue>(existing_offset);
      IndirectValue* created = p->at<IndirectValue>(created_offset);
      slot = p->at<Slot>(slot_offset); // may be invalidated
      table = p->at<HashTableBase>(this->base_offset); // may be invalidated

      created->next = 0;
      created->key_offset = this->allocator->allocate(k_size + sizeof(double));
      memcpy(p->at<void>(created->key_offset), k, k_size);
      memcpy(p->at<void>(created->key_offset + k_size), &delta, sizeof(double));
      created->key_size = k_size;

      existing->next = created_offset;
      existing->key_offset = slot->key_offset;
      existing->key_size = slot->key_size;

      slot->key_offset = existing_offset | 1;
      slot->key_size = 0;
      table->item_count++;

      return delta;
    }

  // the slot contains indirect values
  } else {
    // walk the list, looking for keys that match the one we're inserting
    auto walk_ret = walk_indirect_list(slot->key_offset & (~1), k, k_size);

    // if we found a match, check and increment the value
    if (walk_ret.second) {
      IndirectValue* indirect = p->at<IndirectValue>(walk_ret.second);
      uint64_t v_offset = indirect->key_offset + indirect->key_offset;
      uint64_t v_size = this->allocator->block_size(indirect->key_offset) -
          indirect->key_size;
      if (v_size == 4) {
        *p->at<float>(v_offset) += delta;
        return *p->at<float>(v_offset);
      } else if (v_size == 8) {
        *p->at<double>(v_offset) += delta;
        return *p->at<double>(v_offset);
      } else {
        throw out_of_range("incr() against key of wrong size");
      }

    // no match; allocate a new indirect value at the end
    } else {
      uint64_t created_offset = this->allocator->allocate(
          sizeof(IndirectValue));
      IndirectValue* prev = p->at<IndirectValue>(walk_ret.first);
      IndirectValue* created = p->at<IndirectValue>(created_offset);
      table = p->at<HashTableBase>(this->base_offset); // may be invalidated

      prev->next = created_offset;
      created->next = 0;
      created->key_offset = this->allocator->allocate(k_size + sizeof(double));
      memcpy(p->at<void>(created->key_offset), k, k_size);
      memcpy(p->at<void>(created->key_offset + k_size), &delta, sizeof(double));
      created->key_size = k_size;
      table->item_count++;

      return delta;
    }
  }
}

double HashTable::incr(const std::string& k, double delta) {
  return this->incr(k.data(), k.size(), delta);
}

bool HashTable::erase(const void* k, size_t k_size, const CheckRequest* check) {
  uint64_t hash = fnv1a64(k, k_size);

  auto g = this->allocator->lock(true);

  if (check && !this->execute_check(*check)) {
    return false;
  }

  auto p = this->allocator->get_pool();

  uint64_t deleted_offset = 0;

  // get the slot pointer
  HashTableBase* table = p->at<HashTableBase>(this->base_offset);
  uint64_t slot_offset = table->slots_offset +
      (hash & ((1 << table->bits) - 1)) * sizeof(Slot);
  Slot* slot = p->at<Slot>(slot_offset);

  // if the slot is empty, there's nothing to delete
  if (!slot->key_offset) {
    return false;
  }

  // if the slot contains a direct value...
  if (!(slot->key_offset & 1)) {
    // if the key matches the key we're deleting, free the buffer and clear
    // the slot
    if ((slot->key_size == k_size) &&
        !memcmp(p->at<void>(slot->key_offset), k, k_size)) {
      if (deleted_offset != slot->key_offset) {
        this->allocator->free(slot->key_offset);
        deleted_offset = slot->key_offset;
        slot = p->at<Slot>(slot_offset);
      }
      slot->key_offset = 0;
      slot->key_size = 0;

      table = p->at<HashTableBase>(this->base_offset);
      table->item_count--;
    }

  // the slot contains indirect values
  } else {
    // walk the list, looking for keys that match the one we're inserting
    auto walk_ret = this->walk_indirect_list(slot->key_offset & (~1), k,
        k_size);

    // if we found a match, unlink and delete it
    if (walk_ret.second) {
      IndirectValue* indirect = p->at<IndirectValue>(walk_ret.second);
      if (walk_ret.first) {
        IndirectValue* prev = p->at<IndirectValue>(walk_ret.first);
        prev->next = indirect->next;
      } else {
        slot->key_offset = indirect->next;
      }
      if (deleted_offset != indirect->key_offset) {
        this->allocator->free(indirect->key_offset);
        deleted_offset = indirect->key_offset;
      }
      this->allocator->free(walk_ret.second);
      slot = p->at<Slot>(slot_offset);

      // if there is now only one indirect value, convert it to a direct value
      uint64_t indirect_offset = slot->key_offset;
      indirect = p->at<IndirectValue>(indirect_offset);
      if (slot->key_offset && !indirect->next) {
        slot->key_offset = indirect->key_offset;
        slot->key_size = indirect->key_size;
        this->allocator->free(indirect_offset);
      }

      table = p->at<HashTableBase>(this->base_offset);
      table->item_count--;
    }
  }

  return (deleted_offset != 0);
}

bool HashTable::erase(const std::string& k, const CheckRequest* check) {
  return this->erase(k.data(), k.size(), check);
}


void HashTable::clear() {
  auto g = this->allocator->lock(true);
  auto p = this->allocator->get_pool();

  HashTableBase* h = p->at<HashTableBase>(this->base_offset);
  uint64_t slots_offset = h->slots_offset;

  for (size_t slot_id = 0; slot_id < (size_t)(1 << h->bits); slot_id++) {
    Slot* slot = p->at<Slot>(h->slots_offset + slot_id * sizeof(Slot));
    if (!slot->key_offset) {
      continue;
    }

    // if it's an indirect value, delete the entire chain
    if (slot->key_offset & 1) {
      uint64_t indirect_offset = slot->key_offset & (~1);
      while (indirect_offset) {
        IndirectValue* indirect = p->at<IndirectValue>(indirect_offset);
        uint64_t next_offset = indirect->next;

        this->allocator->free(indirect->key_offset);
        this->allocator->free(indirect_offset);
        indirect_offset = next_offset;
      }

    // not an indirect value - just delete the buffer
    } else {
      this->allocator->free(slot->key_offset);
    }

    // clear the slot
    slot = p->at<Slot>(slots_offset + slot_id * sizeof(Slot));
    slot->key_offset = 0;
    slot->key_size = 0;
  }

  h = p->at<HashTableBase>(this->base_offset);
  h->item_count = 0;
}


bool HashTable::exists(const void* k, size_t k_size) const {
  uint64_t hash = fnv1a64(k, k_size);

  auto g = this->allocator->lock(false);
  auto walk_ret = this->walk_tables(k, k_size, hash);
  return (walk_ret.first != 0);
}

bool HashTable::exists(const std::string& k) const {
  return this->exists(k.data(), k.size());
}


string HashTable::at(const void* k, size_t k_size) const {
  uint64_t hash = fnv1a64(k, k_size);

  {
    auto g = this->allocator->lock(false);
    auto walk_ret = this->walk_tables(k, k_size, hash);
    if (walk_ret.first) {
      return string(this->allocator->get_pool()->at<char>(walk_ret.first),
          walk_ret.second);
    }
  }
  throw out_of_range(string((char*)k, k_size));
}

string HashTable::at(const std::string& k) const {
  return this->at(k.data(), k.size());
}


vector<pair<string, string>> HashTable::get_slot_contents(
    uint64_t slot_index) const {
  vector<pair<string, string>> ret;

  auto g = this->allocator->lock(false);
  auto p = this->allocator->get_pool();

  const HashTableBase* table = p->at<HashTableBase>(this->base_offset);
  uint64_t slot_offset = table->slots_offset + slot_index * sizeof(Slot);
  const Slot* slot = p->at<Slot>(slot_offset);

  // if the slot is empty, we're done
  if (!slot->key_offset) {
    return ret;
  }

  // if the slot contains a direct value, just return it
  if (!(slot->key_offset & 1)) {
    const char* key = p->at<const char>(slot->key_offset);
    const char* value = p->at<const char>(slot->key_offset + slot->key_size);
    ret.emplace_back(make_pair(string(key, slot->key_size),
        string(value, this->allocator->block_size(slot->key_offset) - slot->key_size)));

  // the slot contains indirect values; walk the list and return them all
  } else {
    uint64_t indirect_offset = slot->key_offset & (~1);
    IndirectValue* indirect = p->at<IndirectValue>(indirect_offset);
    for (; indirect_offset; indirect_offset = indirect->next) {
      indirect = p->at<IndirectValue>(indirect_offset);

      const char* key = p->at<const char>(indirect->key_offset);
      const char* value = p->at<const char>(indirect->key_offset + indirect->key_size);
      ret.emplace_back(make_pair(string(key, indirect->key_size),
          string(value, this->allocator->block_size(indirect->key_offset) - indirect->key_size)));
    }
  }

  return ret;
}

HashTableIterator HashTable::begin() const {
  return HashTableIterator(this, 0);
}

HashTableIterator HashTable::end() const {
  return HashTableIterator(this, 1 << this->bits());
}


size_t HashTable::size() const {
  auto g = this->allocator->lock(false);
  return this->allocator->get_pool()->at<HashTableBase>(
      this->base_offset)->item_count;
}

uint8_t HashTable::bits() const {
  auto g = this->allocator->lock(false);
  return this->allocator->get_pool()->at<HashTableBase>(
      this->base_offset)->bits;
}

void HashTable::print(FILE* stream) const {
  auto g = this->allocator->lock(false);
  auto p = this->allocator->get_pool();

  const HashTableBase* h = p->at<HashTableBase>(this->base_offset);

  const Slot* slots = p->at<Slot>(h->slots_offset);
  fprintf(stream, "Table: bits=%hhu, slots@%" PRIu64 "\n", h->bits,
      h->slots_offset);

  for (size_t slot_id = 0; slot_id < (size_t)(1 << h->bits); slot_id++) {
    if (!slots[slot_id].key_offset) {
      continue;

    } else if (!(slots[slot_id].key_offset & 1)) {
      fprintf(stream, "  Slot %zu: value=%" PRIu64 ":%" PRIu64 "\n", slot_id,
          slots[slot_id].key_offset, slots[slot_id].key_size);

    } else {
      fprintf(stream, "  Slot %zu: indirect\n", slot_id);

      uint64_t indirect_offset = slots[slot_id].key_offset & (~1);
      while (indirect_offset) {
        const IndirectValue* indirect = p->at<IndirectValue>(indirect_offset);
        fprintf(stream, "    Indirect: @%" PRIu64 ", next=%" PRIu64
            ", value=%" PRIu64 ":%" PRIu64 "\n", indirect_offset,
            indirect->next, indirect->key_offset, indirect->key_size);
        indirect_offset = indirect->next;
      }
    }
  }
}


uint64_t HashTable::create_hash_base(uint8_t bits) {
  if (bits < 2) {
    throw invalid_argument("bits must be >= 2");
  }

  auto p = this->allocator->get_pool();

  uint64_t base_offset = this->allocator->allocate(sizeof(HashTableBase));
  uint64_t slots_offset = this->allocator->allocate(sizeof(Slot) * (1 << bits));

  HashTableBase* h = p->at<HashTableBase>(base_offset);
  h->bits = bits;
  h->slots_offset = slots_offset;
  h->item_count = 0;

  Slot* slots = p->at<Slot>(slots_offset);
  for (size_t x = 0; x < (size_t)(1 << bits); x++) {
    slots[x].key_offset = 0;
    slots[x].key_size = 0;
  }

  return base_offset;
}


pair<uint64_t, uint64_t> HashTable::walk_indirect_list(uint64_t indirect_offset,
    const void* k, size_t k_size) const {
  auto p = this->allocator->get_pool();

  uint64_t prev_indirect_offset = 0;
  while (indirect_offset) {
    IndirectValue* indirect = p->at<IndirectValue>(indirect_offset);
    if ((indirect->key_size == k_size) &&
        !memcmp(p->at<void>(indirect->key_offset), k, k_size)) {
      break;

    } else {
      prev_indirect_offset = indirect_offset;
      indirect_offset = indirect->next;
    }
  }
  return make_pair(prev_indirect_offset, indirect_offset);
}

pair<uint64_t, uint64_t> HashTable::walk_tables(const void* k, size_t k_size,
    uint64_t hash) const {
  auto p = this->allocator->get_pool();

  const HashTableBase* table = p->at<HashTableBase>(this->base_offset);
  uint64_t slots_offset = table->slots_offset;
  if (!slots_offset) {
    return make_pair(0, 0);
  }

  uint64_t slot_offset = slots_offset +
      (hash & ((1 << table->bits) - 1)) * sizeof(Slot);
  Slot* slot = p->at<Slot>(slot_offset);

  // if the slot is empty, the key doesn't exist
  if (!slot->key_offset) {
    return make_pair(0, 0);
  }

  // if the slot contains a direct value...
  if (!(slot->key_offset & 1)) {
    // if the key matches the key we're looking for, return it
    if ((slot->key_size == k_size) &&
        !memcmp(p->at<void>(slot->key_offset), k, k_size)) {
      return make_pair(slot->key_offset + slot->key_size,
          this->allocator->block_size(slot->key_offset) - slot->key_size);
    }

  // the slot contains indirect values
  } else {
    // walk the list, looking for keys that match the one we're looking for
    auto walk_ret = this->walk_indirect_list(slot->key_offset & (~1), k,
        k_size);

    // if we found a match, return its value
    if (walk_ret.second) {
      IndirectValue* indirect = p->at<IndirectValue>(walk_ret.second);
      return make_pair(indirect->key_offset + indirect->key_size,
          this->allocator->block_size(indirect->key_offset) -
            indirect->key_size);
    }
  }

  return make_pair(0, 0);
}


bool HashTable::execute_check(const CheckRequest& check) const {
  auto walk_ret = this->walk_tables(check.key, check.key_size, check.key_hash);

  if (walk_ret.first) {
    if (!check.value || (check.value_size != walk_ret.second)) {
      return false;
    }
    const char* data = this->allocator->get_pool()->at<char>(walk_ret.first);
    return !memcmp(data, check.value, check.value_size);

  } else {
    return (check.value == nullptr);
  }
}


HashTableIterator::HashTableIterator(const HashTable* table,
    uint64_t slot_index) : table(table), slot_index(slot_index),
    result_index(0), slot_contents() {
  if (this->slot_index < (1ULL << this->table->bits())) {
    this->slot_contents = this->table->get_slot_contents(this->slot_index);
    this->advance_to_nonempty_slot();
  }
}

bool HashTableIterator::operator==(const HashTableIterator& other) const {

  if ((this->table != other.table) || (this->slot_index != other.slot_index)) {
    return false;
  }
  bool this_finished = this->result_index >= this->slot_contents.size();
  bool other_finished = other.result_index >= other.slot_contents.size();
  if (this_finished != other_finished) {
    return false;
  }
  if (this_finished) {
    return true;
  }
  return this->slot_contents[this->result_index].first ==
      other.slot_contents[other.result_index].first;
}

bool HashTableIterator::operator!=(const HashTableIterator& other) const {
  return !(this->operator==(other));
}

HashTableIterator& HashTableIterator::operator++() {
  if (this->slot_index >= (1ULL << this->table->bits())) {
    throw invalid_argument("can\'t advance iterator beyond end position");
  }

  // if there are still results in the current slot contents buffer, advance to
  // the next one
  if (this->result_index < this->slot_contents.size()) {
    this->result_index++;
  }
  this->advance_to_nonempty_slot();

  return *this;
}

HashTableIterator HashTableIterator::operator++(int) {
  HashTableIterator ret = *this;
  this->operator++();
  return ret;
}

const pair<string, string>& HashTableIterator::operator*() const {
  return this->slot_contents[this->result_index];
}

void HashTableIterator::advance_to_nonempty_slot() {
  while (this->result_index >= this->slot_contents.size()) {
    this->result_index = 0;
    this->slot_index++;
    if (this->slot_index < (1ULL << this->table->bits())) {
      this->slot_contents = this->table->get_slot_contents(this->slot_index);
    } else {
      this->slot_contents.clear();
      break;
    }
  }
}

} // namespace sharedstructures
