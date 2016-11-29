#include "HashTable.hh"

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


HashTable::HashTable(shared_ptr<Pool> pool, uint8_t bits) : pool(pool) {
  auto g = this->pool->write_lock();
  this->base_offset = this->create_hash_base(bits);
}

HashTable::HashTable(shared_ptr<Pool> pool, uint64_t base_offset, uint8_t bits)
    : pool(pool), base_offset(base_offset) {
  if (!this->base_offset) {
    auto g = this->pool->read_lock();
    this->base_offset = this->pool->base_object_offset();
  }

  if (!this->base_offset) {

    auto g = this->pool->write_lock();
    this->base_offset = this->pool->base_object_offset();
    if (!this->base_offset) {
      this->base_offset = this->create_hash_base(bits);
      this->pool->set_base_object_offset(this->base_offset);
    }
  }
}


shared_ptr<Pool> HashTable::get_pool() const {
  return this->pool;
}

uint64_t HashTable::base() const {
  return this->base_offset;
}


void HashTable::insert(const void* k, size_t k_size, const void* v,
    size_t v_size) {
  uint64_t hash = fnv1a64(k, k_size);

  auto g = this->pool->write_lock();

  // create the new key-value pair and copy the data in
  uint64_t new_kv_pair_offset = this->pool->allocate(k_size + v_size);
  memcpy(this->pool->at<void>(new_kv_pair_offset), k, k_size);
  memcpy(this->pool->at<void>(new_kv_pair_offset + k_size), v, v_size);

  // for each hash table...
  for (size_t table_index = 0; table_index < 2; table_index++) {
    HashTableBase* table = this->pool->at<HashTableBase>(this->base_offset);
    uint64_t slots_offset = table->slots_offset[table_index];
    if (!slots_offset) {
      continue;
    }

    uint8_t table_bits = table->bits[table_index];
    uint64_t slot_offset = slots_offset + (hash & ((1 << table_bits) - 1)) * sizeof(Slot);
    Slot* slot = this->pool->at<Slot>(slot_offset);

    // if the slot is empty, just link it to the value
    if (!slot->key_offset) {
      // link it in the slot
      slot->key_offset = new_kv_pair_offset;
      slot->key_size = k_size;
      table->item_count[table_index]++;
      continue;
    }

    // if the slot contains a direct value...
    if (!(slot->key_offset & 1)) {

      // if the key matches the key we're inserting, free the old buffer and
      // replace it with the new one
      if ((slot->key_size == k_size) &&
          !memcmp(this->pool->at<void>(slot->key_offset), k, k_size)) {
        this->pool->free(slot->key_offset);
        slot = this->pool->at<Slot>(slot_offset); // may be invalidated
        slot->key_offset = new_kv_pair_offset;
        slot->key_size = k_size;

      // the key doesn't match. convert this to an indirect value
      } else {
        uint64_t existing_offset = this->pool->allocate(sizeof(IndirectValue));
        uint64_t created_offset = this->pool->allocate(sizeof(IndirectValue));
        IndirectValue* existing = this->pool->at<IndirectValue>(existing_offset);
        IndirectValue* created = this->pool->at<IndirectValue>(created_offset);
        slot = this->pool->at<Slot>(slot_offset); // may be invalidated
        table = this->pool->at<HashTableBase>(this->base_offset); // may be invalidated

        created->next = 0;
        created->key_offset = new_kv_pair_offset;
        created->key_size = k_size;
        existing->next = created_offset;
        existing->key_offset = slot->key_offset;
        existing->key_size = slot->key_size;
        slot->key_offset = existing_offset | 1;
        slot->key_size = 0;
        table->item_count[table_index]++;
      }

    // the slot contains indirect values
    } else {
      // walk the list, looking for keys that match the one we're inserting
      auto walk_ret = walk_indirect_list(slot->key_offset & (~1), k, k_size);

      // if we found a match, just replace the buffer pointer on it
      if (walk_ret.second) {
        IndirectValue* indirect = this->pool->at<IndirectValue>(walk_ret.second);
        this->pool->free(indirect->key_offset);
        indirect = this->pool->at<IndirectValue>(walk_ret.second);
        indirect->key_offset = new_kv_pair_offset;
        indirect->key_size = k_size;

      // no match; allocate a new indirect value at the end
      } else {
        uint64_t created_offset = this->pool->allocate(sizeof(IndirectValue));
        IndirectValue* prev = this->pool->at<IndirectValue>(walk_ret.first);
        IndirectValue* created = this->pool->at<IndirectValue>(created_offset);
        table = this->pool->at<HashTableBase>(this->base_offset); // may be invalidated

        prev->next = created_offset;
        created->next = 0;
        created->key_offset = new_kv_pair_offset;
        created->key_size = k_size;
        table->item_count[table_index]++;
      }
    }
  }
}

void HashTable::insert(const void* k, size_t k_size, const string& v) {
  this->insert(k, k_size, v.data(), v.size());
}

void HashTable::insert(const string& k, const void* v, size_t v_size) {
  this->insert(k.data(), k.size(), v, v_size);
}

void HashTable::insert(const string& k, const string& v) {
  this->insert(k.data(), k.size(), v.data(), v.size());
}

bool HashTable::erase(const void* k, size_t k_size) {
  uint64_t hash = fnv1a64(k, k_size);

  auto g = this->pool->write_lock();

  uint64_t deleted_offset = 0;

  // for each hash table...
  for (size_t table_index = 0; table_index < 2; table_index++) {
    HashTableBase* table = this->pool->at<HashTableBase>(this->base_offset);
    uint64_t slots_offset = table->slots_offset[table_index];
    if (!slots_offset) {
      continue;
    }

    uint8_t table_bits = table->bits[table_index];
    uint64_t slot_offset = slots_offset + (hash & ((1 << table_bits) - 1)) * sizeof(Slot);
    Slot* slot = this->pool->at<Slot>(slot_offset);

    // if the slot is empty, there's nothing to delete
    if (!slot->key_offset) {
      continue;
    }

    // if the slot contains a direct value...
    if (!(slot->key_offset & 1)) {
      // if the key matches the key we're deleting, free the buffer and clear
      // the slot
      if ((slot->key_size == k_size) &&
          !memcmp(this->pool->at<void>(slot->key_offset), k, k_size)) {
        if (deleted_offset != slot->key_offset) {
          this->pool->free(slot->key_offset);
          deleted_offset = slot->key_offset;
          slot = this->pool->at<Slot>(slot_offset);
        }
        slot->key_offset = 0;
        slot->key_size = 0;

        table = this->pool->at<HashTableBase>(this->base_offset);
        table->item_count[table_index]--;
      }

    // the slot contains indirect values
    } else {
      // walk the list, looking for keys that match the one we're inserting
      auto walk_ret = this->walk_indirect_list(slot->key_offset & (~1), k, k_size);

      // if we found a match, unlink and delete it
      if (walk_ret.second) {
        IndirectValue* indirect = this->pool->at<IndirectValue>(walk_ret.second);
        if (walk_ret.first) {
          IndirectValue* prev = this->pool->at<IndirectValue>(walk_ret.first);
          prev->next = indirect->next;
        } else {
          slot->key_offset = indirect->next;
        }
        if (deleted_offset != indirect->key_offset) {
          this->pool->free(indirect->key_offset);
          deleted_offset = indirect->key_offset;
        }
        this->pool->free(walk_ret.second);
        slot = this->pool->at<Slot>(slot_offset);

        // if there is now only one indirect value, convert it to a direct value
        uint64_t indirect_offset = slot->key_offset;
        indirect = this->pool->at<IndirectValue>(indirect_offset);
        if (slot->key_offset && !indirect->next) {
          slot->key_offset = indirect->key_offset;
          slot->key_size = indirect->key_size;
          this->pool->free(indirect_offset);
        }

        table = this->pool->at<HashTableBase>(this->base_offset);
        table->item_count[table_index]--;
      }
    }
  }

  return (deleted_offset != 0);
}

bool HashTable::erase(const std::string& k) {
  return this->erase(k.data(), k.size());
}


void HashTable::clear() {
  auto g = this->pool->write_lock();

  for (size_t table_index = 0; table_index < 2; table_index++) {
    HashTableBase* h = this->pool->at<HashTableBase>(this->base_offset);

    uint8_t bits = h->bits[table_index];
    if (!bits) {
      continue;
    }

    uint64_t slots_offset = h->slots_offset[table_index];
    for (size_t slot_id = 0; slot_id < (1 << bits); slot_id++) {
      Slot* slot = this->pool->at<Slot>(slots_offset + slot_id * sizeof(Slot));
      if (!slot->key_offset) {
        continue;
      }

      // if it's an indirect value, delete the entire chain
      if (slot->key_offset & 1) {
        uint64_t indirect_offset = slot->key_offset & (~1);
        while (indirect_offset) {
          IndirectValue* indirect = this->pool->at<IndirectValue>(indirect_offset);
          uint64_t next_offset = indirect->next;

          // the first table is guaranteed to contain all the keys, so we only
          // free buffers from the first table
          if (table_index == 0) {
            this->pool->free(indirect->key_offset);
          }
          this->pool->free(indirect_offset);
          indirect_offset = next_offset;
        }

      // not an indirect value - just delete the buffer
      } else {
        if (table_index == 0) {
          this->pool->free(slot->key_offset);
        }
      }

      // clear the slot
      slot = this->pool->at<Slot>(slots_offset + slot_id * sizeof(Slot));
      slot->key_offset = 0;
      slot->key_size = 0;
    }

    h = this->pool->at<HashTableBase>(this->base_offset);
    h->item_count[table_index] = 0;
  }
}


bool HashTable::exists(const void* k, size_t k_size) const {
  uint64_t hash = fnv1a64(k, k_size);

  auto g = this->pool->read_lock();
  auto walk_ret = this->walk_tables(k, k_size, hash);
  return (walk_ret.first != 0);
}

bool HashTable::exists(const std::string& k) const {
  return this->exists(k.data(), k.size());
}


string HashTable::at(const void* k, size_t k_size) const {
  uint64_t hash = fnv1a64(k, k_size);

  {
    auto g = this->pool->read_lock();
    auto walk_ret = this->walk_tables(k, k_size, hash);
    if (walk_ret.first) {
      return string(this->pool->at<char>(walk_ret.first), walk_ret.second);
    }
  }
  throw out_of_range(string((char*)k, k_size));
}

string HashTable::at(const std::string& k) const {
  return this->at(k.data(), k.size());
}


size_t HashTable::size() const {
  auto g = this->pool->read_lock();
  return this->pool->at<HashTableBase>(this->base_offset)->item_count[0];
}

void HashTable::print(FILE* stream) const {
  auto g = this->pool->read_lock();

  const HashTableBase* h = this->pool->at<HashTableBase>(this->base_offset);
  for (size_t table_index = 0; table_index < 2; table_index++) {
    uint8_t bits = h->bits[table_index];
    if (!bits) {
      fprintf(stream, "Table %zu is not present\n", table_index);
      continue;
    }

    uint64_t slots_offset = h->slots_offset[table_index];
    const Slot* slots = this->pool->at<Slot>(slots_offset);
    fprintf(stream, "Table %zu: bits=%hhu, slots@%llX\n", table_index, bits,
        slots_offset);

    for (size_t slot_id = 0; slot_id < (1 << bits); slot_id++) {
      if (!slots[slot_id].key_offset) {
        //fprintf(stream, "  Slot %zu: empty\n", slot_id);

      } else if (!(slots[slot_id].key_offset & 1)) {
        fprintf(stream, "  Slot %zu: value=%llX:%llX\n", slot_id,
            slots[slot_id].key_offset, slots[slot_id].key_size);

      } else {
        fprintf(stream, "  Slot %zu: indirect\n", slot_id);

        uint64_t indirect_offset = slots[slot_id].key_offset & (~1);
        while (indirect_offset) {
          const IndirectValue* indirect = this->pool->at<IndirectValue>(indirect_offset);
          fprintf(stream, "    Indirect: @%llX, next=%llX, value=%llX:%llX\n",
              indirect_offset, indirect->next, indirect->key_offset,
              indirect->key_size);
          indirect_offset = indirect->next;
        }
      }
    }
  }
}


uint64_t HashTable::create_hash_base(uint8_t bits) {
  if (bits < 2) {
    throw invalid_argument("bits must be >= 2");
  }

  uint64_t base_offset = this->pool->allocate(sizeof(HashTableBase));
  uint64_t slots_offset = this->pool->allocate(sizeof(Slot) * (1 << bits));

  HashTableBase* h = this->pool->at<HashTableBase>(base_offset);
  h->bits[0] = bits;
  h->bits[1] = 0;
  h->slots_offset[0] = slots_offset;
  h->slots_offset[1] = 0;
  h->item_count[0] = 0;
  h->item_count[1] = 0;

  Slot* slots = this->pool->at<Slot>(slots_offset);
  for (size_t x = 0; x < (1 << bits); x++) {
    slots[x].key_offset = 0;
    slots[x].key_size = 0;
  }

  return base_offset;
}


pair<uint64_t, uint64_t> HashTable::walk_indirect_list(uint64_t indirect_offset,
    const void* k, size_t k_size) const {
  uint64_t prev_indirect_offset = 0;
  while (indirect_offset) {
    IndirectValue* indirect = this->pool->at<IndirectValue>(indirect_offset);
    if ((indirect->key_size == k_size) &&
        !memcmp(this->pool->at<void>(indirect->key_offset), k, k_size)) {
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
  // for each hash table...
  for (ssize_t table_index = 1; table_index >= 0; table_index--) {
    const HashTableBase* table = this->pool->at<HashTableBase>(this->base_offset);
    uint64_t slots_offset = table->slots_offset[table_index];
    if (!slots_offset) {
      continue;
    }

    uint8_t table_bits = table->bits[table_index];
    uint64_t slot_offset = slots_offset + (hash & ((1 << table_bits) - 1)) * sizeof(Slot);
    Slot* slot = this->pool->at<Slot>(slot_offset);

    // if the slot is empty, check the next table
    if (!slot->key_offset) {
      continue;
    }

    // if the slot contains a direct value...
    if (!(slot->key_offset & 1)) {
      // if the key matches the key we're looking for, return it
      if ((slot->key_size == k_size) &&
          !memcmp(this->pool->at<void>(slot->key_offset), k, k_size)) {
        return make_pair(slot->key_offset + slot->key_size,
            this->pool->block_size(slot->key_offset) - slot->key_size);
      }

    // the slot contains indirect values
    } else {
      // walk the list, looking for keys that match the one we're looking for
      auto walk_ret = this->walk_indirect_list(slot->key_offset & (~1), k, k_size);

      // if we found a match, return its value
      if (walk_ret.second) {
        IndirectValue* indirect = this->pool->at<IndirectValue>(walk_ret.second);
        return make_pair(indirect->key_offset + indirect->key_size,
            this->pool->block_size(indirect->key_offset) - indirect->key_size);
      }
    }
  }

  return make_pair(0, 0);
}

} // namespace sharedstructures
