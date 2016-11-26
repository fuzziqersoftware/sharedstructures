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


HashTable::HashTable(const string& name, size_t max_size) :
    pool(name, max_size) {

  uint64_t base;
  {
    auto g = pool.read_lock();
    base = this->pool.base_object_offset();
  }

  if (!base) {
    auto g = pool.write_lock();
    if (!this->pool.base_object_offset()) {
      uint8_t default_hash_bits = 6;
      this->pool.set_base_object_offset(this->create_hash_header(
          default_hash_bits));
    }
  }
}


bool HashTable::insert(const string& k, const string& v, bool overwrite) {
  return this->insert(k.data(), k.size(), v.data(), v.size());
}

bool HashTable::insert(const void* k, size_t k_size, const void* v,
    size_t v_size, bool overwrite) {
  auto g = pool.write_lock();
  return this->hash_insert(k, k_size, v, v_size, overwrite);
}


bool HashTable::erase(const std::string& k) {
  return this->erase(k.data(), k.size());
}

bool HashTable::erase(const void* k, size_t k_size) {
  auto g = pool.write_lock();
  return this->hash_delete(k, k_size);
}


string HashTable::at(const std::string& k) const {
  return this->at(k.data(), k.size());
}

string HashTable::at(const void* k, size_t k_size) const {
  {
    auto g = pool.read_lock();
    auto ret = this->hash_lookup(k, k_size);
    if (ret.first) {
      return string(this->pool.at<char>(ret.first), ret.second);
    }
  }
  throw out_of_range(string((char*)k, k_size));
}

size_t HashTable::pool_size() const {
  return this->pool.size();
}

size_t HashTable::size() const {
  auto g = pool.read_lock();
  return this->pool.at<HashTableData>(this->pool.base_object_offset())->item_count;
}



HashTable::HashTableData::HashTableData(uint8_t bits) : bits(bits),
    item_count(0), next_table_offset(0) {
  for (uint64_t x = 0; x < (1 << this->bits); x++) {
    this->slots[x].key_offset = 0;
    this->slots[x].key_size = 0;
  }
}

size_t HashTable::HashTableData::allocation_size(uint8_t bits) {
  return sizeof(HashTableData) + (1 << bits) * sizeof(Slot);
}

uint64_t HashTable::create_hash_header(uint8_t bits) {
  return this->pool.allocate_object<HashTableData, uint8_t>(bits,
      HashTableData::allocation_size(bits));
}

bool HashTable::hash_insert(const void* key_data, size_t key_size,
    const void* value_data, size_t value_size, bool overwrite) {

  uint64_t hash = fnv1a64(key_data, key_size);

  HashTableData* t = this->pool.at<HashTableData>(this->pool.base_object_offset());
  HashTableData::Slot* slot = &t->slots[hash & ((1 << t->bits) - 1)];

  // the slot isn't empty; overwrite it only if overwrite is true
  if (slot->key_offset && overwrite) {
    this->pool.free(slot->key_offset);
    slot->key_offset = 0;
    t->item_count--;
  }

  // if the slot is empty, allocate space for the object, copy its contents in,
  // and store pointers to it in the slot
  if (slot->key_offset == 0) {
    uint64_t kv_pair_offset = this->pool.allocate(key_size + value_size);
    memcpy(this->pool.at<void>(kv_pair_offset), key_data, key_size);
    memcpy(this->pool.at<void>(kv_pair_offset + key_size), value_data,
        value_size);
    slot->key_offset = kv_pair_offset;
    slot->key_size = key_size;
    t->item_count++;
    return true;
  }

  // TODO: implement this case
  return false;
}

bool HashTable::hash_delete(const void* key_data, size_t key_size) {
  uint64_t hash = fnv1a64(key_data, key_size);

  HashTableData* t = this->pool.at<HashTableData>(this->pool.base_object_offset());
  HashTableData::Slot* slot = &t->slots[hash & ((1 << t->bits) - 1)];

  // if the slot is empty, there's nothing to delete
  if (!slot->key_offset) {
    return false;
  }

  // if the slot isn't empty, check if the key matches the one being deleted
  if (slot->key_size != key_size) {
    return false;
  }
  if (memcmp(this->pool.at<void>(slot->key_offset), key_data, key_size)) {
    return false;
  }

  // found it - let's delete
  this->pool.free(slot->key_offset);
  slot->key_offset = 0;
  slot->key_size = 0;
  t->item_count--;
  return true;
}

pair<uint64_t, uint64_t> HashTable::hash_lookup(const void* key_data,
    size_t key_size) const {
  uint64_t hash = fnv1a64(key_data, key_size);

  const HashTableData* t = this->pool.at<HashTableData>(this->pool.base_object_offset());
  const HashTableData::Slot* slot = &t->slots[hash & ((1 << t->bits) - 1)];

  // if the slot is empty, there's nothing to delete
  if (!slot->key_offset) {
    return make_pair(0, 0);
  }

  // if the slot isn't empty, check if the key matches the one being deleted
  if (slot->key_size != key_size) {
    return make_pair(0, 0);
  }
  if (memcmp(this->pool.at<void>(slot->key_offset), key_data, key_size)) {
    return make_pair(0, 0);
  }

  // found it
  size_t item_size = this->pool.block_size(slot->key_offset);
  return make_pair(slot->key_offset + slot->key_size,
      item_size - slot->key_size);
}

} // namespace sharedstructures
