#include "Allocator.hh"

using namespace std;

namespace sharedstructures {


Allocator::Allocator(shared_ptr<Pool> pool) : pool(pool) { }

shared_ptr<Pool> Allocator::get_pool() const {
  return this->pool;
}

void Allocator::repair() { }

} // namespace sharedstructures
