# sharedstructures <img align="right" src="s-sharedstructures.png" />

sharedstructures is a C++ and Python 3 library for storing data structures in automatically-created, dynamically-sized shared memory objects. This library can be used to share complex data between processes in a performant way.

This library currently supports these data structures:
- Hash tables with binary string keys and values
- Prefix trees with binary string keys and typed values (supported types are null, bool, int, float, and binary string)
- Queues (doubly-linked lists) with binary string values
- Priority queues (binary heaps) with binary string values
- Atomic 64-bit integer arrays

## Building

- Build and install phosg (https://github.com/fuzziqersoftware/phosg).
- Run `cmake . && make`. This builds the C++ library, and builds the Python library if you have Python development files installed. (CMake will print a warning if it can't find Python.)
- Run `make test` to make sure everything works. There are occasionally spurious failures in one or two of the tests due to races that happen in the test code (not in sharedstructures itself) - I'll fix this at some point.
- Run `sudo make install` if you want to.

If it doesn't work on your system, let me know. I've built and tested it on macOS 12.3.1 and Ubuntu 20.04.

## Data structures

Data structure objects can be used on top of an Allocator or Pool object. See examples below for usage information.

### Mapping types

**HashTable** implements a binary-safe map of strings to strings. HashTables have a fixed bucket count at creation time and cannot be resized dynamically. This issue will be fixed in the future.

**PrefixTree** implements a binary-safe map of strings to values of any of the following types:
- Strings
- Integers
- Floating-point numbers
- Boolean values
- Null (this is not the same as the key not existing - a key can exist and have a null value)

Both HashTable and PrefixTree support getting and setting individual keys, iteration over all or part of the map, conditional writes (check-and-set, check-and-delete), and atomic increments. Note that atomic increments on HashTables are supported only in C++ and not in Python, but all other operations (including atomic increments on PrefixTrees) are supported in both languages.

### Array/list types

**Queue** implements a doubly-linked list of binary strings. Items may only be accessed at the ends of the list, not at arbitrary positions within the list.

**PriorityQueue** implements a heap of binary strings. Supports adding arbitrary strings to the heap and removing the minimum item (lexicographically earliest string).

**IntVector** implements an array of 64-bit signed integers, supporting various atomic operations on them. Unlike the other data structures, IntVector operates directly on top of a Pool object and does not have an Allocator. This is necessary because it implements only lock-free operations, and Allocators require using locks. This also means that a Pool containing an IntVector may not contain any other data structures.

The header files (HashTable.hh, PrefixTree.hh, Queue.hh, PriorityQueue.hh, and IntVector.hh) document how to use these objects. Take a look at the test source (HashTableTest.cc, PrefixTreeTest.cc, QueueTest.cc, PriorityQueueTest.cc, and IntVectorTest.cc) for usage examples.

## Basic usage

In the C++ examples below, you'll see Pool and LogarithmicAllocator objects; these usually don't need any configuration or changes from the example code. They're described later in this document.

### Mapping types

Hash tables have a number of unimplemented convenience features; for example, the bucket count cannot be changed without rebuilding the entire table manually. For sharing mappings/dictionaries, most use cases should use a prefix tree instead.

The following C++ code opens a prefix tree object, creating it if it doesn't exist:

    #include <memory>
    #include <sharedstructures/Pool.hh>
    #include <sharedstructures/LogarithmicAllocator.hh>
    #include <sharedstructures/PrefixTree.hh>

    using namespace sharedstructures;

    std::shared_ptr<Pool> pool(new Pool(filename));
    std::shared_ptr<Allocator> alloc(new LogarithmicAllocator(pool));
    std::shared_ptr<PrefixTree> tree(new PrefixTree(alloc, 0));

    // Note: k1, k2, etc. are all std::string
    tree->insert(k1, v1);  // Insert or overwrite a value
    PrefixTree::LookupResult v2 = tree->at(k2);  // Retrieve a value
    bool was_erased = tree->erase(k3);  // Delete a value
    bool key_exists = tree->exists(k4);  // Check if a key exists
    size_t count = tree->size();  // Get the number of key-value pairs

See PrefixTree.hh for more details on how to use the tree object. The HashTable interface is similar.

Python usage is more intuitive - sharedstructures.PrefixTree objects behave mostly like dicts (but read the "Python wrapper semantics" section below):

    import sharedstructures

    tree = sharedstructures.PrefixTree(filename, 'logarithmic', 0)

    tree[k1] = v1  # Insert or overwrite a value
    v2 = tree[k2]  # Retrieve a value
    del tree[k3]  # Delete a value
    key_exists = (k4 in tree)  # Check if a key exists
    num_keys = len(tree)  # Get the number of key-value pairs

### Queues

In C++, you can add std::string objects and raw pointer/size pairs to the queue, but it only returns std::string objects. Use it like this:

    #include <memory>
    #include <sharedstructures/Pool.hh>
    #include <sharedstructures/LogarithmicAllocator.hh>
    #include <sharedstructures/Queue.hh>

    using namespace sharedstructures;

    std::shared_ptr<Pool> pool(new Pool(filename));
    std::shared_ptr<Allocator> alloc(new LogarithmicAllocator(pool));
    std::shared_ptr<Queue> q(new Queue(alloc, 0));

    // Add items to the queue
    q->push_back(item1);  // (std::string)
    q->push_back(item1_data, item1_size);  // (const void*, size_t)
    q->push_front(item2);  // (std::string)
    q->push_front(item2_data, item2_size);  // (const void*, size_t)

    // Remove items from the queue (throws std::out_of_range if empty)
    std::string item3 = q->pop_back();
    std::string item4 = q->pop_front();

    // Get the number of items in the queue
    size_t count = q->size();

In Python, the wrapper will transparently marshal and unmarshal values passed to and from Queues. However, this means that you can't easily pass values to and from programs written in other languages. To bypass the serialize/deserialize steps (and therefore only be allowed to pass/return bytes objects in Python), use raw=True. Python usage looks like this:

    import sharedstructures

    q = sharedstructures.Queue(filename, 'logarithmic', 0)

    # Add items to the queue
    q.push_back(item1)  # or q.append(item1); use raw=True to skip serialization
    q.push_front(item2)  # or q.appendleft(item2); use raw=True to skip serialization

    # Remove items from the queue (raises IndexError if empty)
    item3 = q.pop_back()  # or q.pop(); use raw=True to skip deserialization
    item4 = q.pop_front()  # or q.popleft(); use raw=True to skip deserialization

    # Get the number of items in the queue
    num_items = len(q)

### Priority queues

In C++, you can add std::string objects and pointer/size pairs to the queue, but it only returns std::string objects. Use it like this:

    #include <memory>
    #include <sharedstructures/Pool.hh>
    #include <sharedstructures/LogarithmicAllocator.hh>
    #include <sharedstructures/PriorityQueue.hh>

    using namespace sharedstructures;

    std::shared_ptr<Pool> pool(new Pool(filename));
    std::shared_ptr<Allocator> alloc(new LogarithmicAllocator(pool));
    std::shared_ptr<PriorityQueue> q(new PriorityQueue(alloc, 0));

    // Add items to the queue
    q->push(item1);  // std::string
    q->push(item1_data, item1_size);  // (void*, size_t)

    // Remove and return the minimum item from the queue (throws std::out_of_range if empty)
    std::string item3 = q->pop();

    // Get the number of items in the queue
    size_t count = q->size();

Unlike Queue, you can't add arbitrary Python objects to a PriorityQueue; the Python module only accepts and returns bytes objects. Use it like this:

    import sharedstructures

    q = sharedstructures.PriorityQueue(filename, 'logarithmic', 0)

    # Add items to the queue (items must be bytes objects)
    q.push(item1)
    q.push(item2)

    # Remove items from the queue (raises IndexError if empty)
    item3 = q.pop()

    # Get the number of items in the queue
    num_items = len(q)

Usually the priority should be an ordered, constant-width field at the beginning of each item. For example, to use a 64-bit integer priority, prepend the return value of `struct.pack(">Q", priority)` to each item, and manually strip it from items returned by `.pop()`.

### Atomic and integer vectors

Unlike the other data structures, atomic vectors do not use an underlying allocator; instead, they manage memory in the pool internally. This means that an AtomicVector or IntVector cannot share a pool with any other data structures.

The following C++ code opens an atomic vector object, creating it if it doesn't exist. Note that you have to call `.expand()` at least once on a newly-created vector, since new vectors have no fields by default.

    #include <memory>
    #include <sharedstructures/Pool.hh>
    #include <sharedstructures/AtomicVector.hh>

    using namespace sharedstructures;

    std::shared_ptr<Pool> pool(new Pool(filename));
    std::shared_ptr<AtomicVector<uint64_t>> v(new AtomicVector<uint64_t>(pool));

    v->expand(new_count);  // Add fields to the vector
    size_t count = v->size();  // Get the number of fields in the vector
    int64_t value = v->load(index);  // Retrieve a value
    v->store(index, value);  // Overwrite a value
    int64_t old_value = v->exchange(index, new_value);  // Swap a value
    int64_t old_value = v->compare_exchange(index, expected_value, new_value);  // Compare and swap a value
    int64_t old_value = v->fetch_add(index, delta);  // Add to a value
    int64_t old_value = v->fetch_sub(index, delta);  // Subtract from a value
    int64_t old_value = v->fetch_and(index, mask);  // Bitwise-and a value
    int64_t old_value = v->fetch_or(index, mask);  // Bitwise-or a value
    int64_t old_value = v->fetch_xor(index, mask);  // Bitwise-xor a value
    bool value = v->load_bit(bit_index);  // Read a single bit
    v->store_bit(bit_index, value);  // Write a single bit
    bool original_value = v->xor_bit(bit_index);  // Flip a single bit (and return the old value)

The Python interface for IntVector is similar to the C++ interface:

    import sharedstructures

    v = sharedstructures.IntVector(filename)

    v.expand(new_count)  # Add fields to the vector
    count = len(v)  # Get the number of fields in the vector
    value = v.load(index)  # Retrieve a value
    v.store(index, value)  # Overwrite a value
    old_value = v.exchange(index, new_value)  # Swap a value
    old_value = v.compare_exchange(index, expected_value, new_value)  # Compare and swap a value
    old_value = v.add(index, delta)  # Add to a value
    old_value = v.subtract(index, delta)  # Subtract from a value
    old_value = v.bitwise_and(index, mask)  # Bitwise-and a value
    old_value = v.bitwise_or(index, mask)  # Bitwise-or a value
    old_value = v.bitwise_xor(index, mask)  # Bitwise-xor a value
    value = v.load_bit(bit_index)  # Read a single bit
    v.store_bit(bit_index, value)  # Write a single bit
    original_value = v.xor_bit(bit_index)  # Flip a single bit (and return the old value)

## Interfaces and objects

The Pool object (Pool.hh) implements a raw expandable memory pool. Unlike standard memory semantics, it deals with relative pointers ("offsets") since the pool base address can move in the process' address space. Offsets can be converted to usable pointers with the Pool::PoolPointer member class, which handles the offset logic internally and behaves like a normal pointer externally. Performance-sensitive callers can use `Pool::at<T>` instead, but its return values can be invalidated by pool expansion.

You'll usually want to use some kind of allocator on top of the Pool object. The Allocator object manages pool expansion and assignment of regions for the application's needs. There are currently two allocators implemented:
- SimpleAllocator achieves high space efficiency and constant-time frees, but allocations take up to linear time in the number of existing blocks.
- LogarithmicAllocator compromises space efficiency for speed; it wastes more memory, but both allocations and frees take logarithmic time in the size of the pool.

The allocator type of a pool can't be changed after creating it, so choose the allocator type based on what the access patterns will be. Use SimpleAllocator if you need high space efficiency or are sharing read-only data (at the cost of slow writes), or use LogarithmicAllocator if you need both reads and writes to be fast and can sacrifice some space for speed.

### Mapping iteration semantics

Iteration over either of the mapping structures only locks the structure while advancing the iterator, so it's possible for an iteration to see an inconsistent view of the data structure due to concurrent modifications by other processes.

Iterating a HashTable produces items in pseudorandom order. If an item exists in the table for the duration of the iteration, then it will be returned; if it's created or deleted during the iteration, then it may or may not be returned. Similarly, if a key's value is changed during the iteration, then either its new or old value may be returned.

Iterating a PrefixTree produces items in lexicographic order. Concurrent changes after the current key (lexicographically) will be visible, changes at or before the current key will not. Additionally, PrefixTree supports `lower_bound` and `upper_bound` in C++, and has `keys_from`, `values_from`, and `items_from` in Python to support starting iteration at places within the tree.

For both structures, the iterator objects cache one or more results on the iterator object itself, so values can't be modified through the iterator object.

Queues, PriorityQueues, and IntVectors are not iterable.

## Python wrapper semantics

HashTable, PrefixTree, Queue, PriorityQueue, and IntVector can also be used from Python with the included module. For HashTable and PrefixTree, keys can be accessed directly with the subscript operator (`t[k] = value`; `value = t[k]`; `del t[k]`). Keys must be bytes objects; TypeError is raised if some other type is given for a key. For Queue, PriorityQueue, and IntVector, items cannot be accessed using the subscript operator; you have to call the appropriate functions on the object instead.

For HashTables, PrefixTrees, and Queues (but not PriorityQueues), the Python wrapper transparently marshals objects that aren't basic types - which means you can store tuples, dicts, lists, etc. in HashTables, PrefixTrees, and Queues, though this will be inefficient for large objects. Storing numeric values and True/False/None in a PrefixTree will use the tree's corresponding native types, so they can be easily accessed from non-Python programs.

There are a few quirks to watch out for:
- With HashTable and PrefixTree, modifying complex values in-place will silently fail because `t[k]` returns a copy of the value at `k`, since it's generally not safe to directly modify values without holding the pool lock. Statements like `t[k1] = {}; t[k1][k2] = 17` won't work - after doing this, `t[k1]` will still be an empty dictionary.
- With HashTable and PrefixTree, strings and numeric values *can* be modified "in-place" because Python implements this using separate load and store operations - so `t[k] += 1` works, but is vulnerable to data races when multiple processes are accessing the structure. PrefixTree supports atomic increments on numeric keys by using `t.incr(k, delta)`.
- HashTable and PrefixTree aren't subclasses of dict. They can be converted to (non-shared) dicts by doing `dict(t.items())`. This may not produce a consistent snapshot though; see "Mapping iteration semantics" above.
- Queue, PriorityQueue, and IntVector aren't subclasses of list. Unlike the mapping types, there's no easy way to convert them to non-shared lists since they aren't iterable.

## Thread safety

The allocators are not thread-safe by default, but they include a global read-write lock that the caller can access to achieve thread-safety. To use it, do the following:

    {
      auto g = allocator->lock(false); // false = read lock
      // Now the pool cannot be expanded or remapped within this process, and
      // it's safe to read from the pool
    }
    // Now it's no longer safe to read from or write to the pool

    {
      auto g = allocator->lock(true); // true = write lock
      // Now the pool cannot be read or written by any other thread (even in
      // other processes) and it's safe to call allocator->allocate(),
      // allocator->free(), etc.
    }
    // Now it's no longer safe to read from or write to the pool

HashTable, PrefixTree, Queue, and PriorityQueue use this global lock when they call the allocator or read from the tree, and are therefore thread-safe by default. AtomicVector (and IntVector) do not use an allocator and have no need for these locks.

sharedstructures objects are not necessarily thread-safe within a process because one thread may remap the view of the shared memory segment while another thread attempts to access it. It's recommended for each thread that needs access to a Pool to have its own Pool object for this reason.

## Reliability

Operations on shared data structures use a global lock over the entire structure. Since operations generally involve only a few memory accesses, the critical sections should be quite short. However, processes can still crash or be killed during these critical sections, which leads to the lock being "held" by a dead process.

Currently, the lock wait algorithm will check periodically if the process holding the lock is still alive. If the holding process has died, the waiting process will "steal" the lock from that process and repair the allocator's internal data structures. This may be slow for large data structures, since it involves walking the entire list of allocated regions.

HashTable and PriorityQueue are not necessarily consistent in case of a crash, though this will be fixed in the future. For now, be wary of using a HashTable or PriorityQueue if a process crashed while operating on it.

PrefixTree, Queue, AtomicVector, and IntVector are always consistent and don't need any extra repairs after a crash. However, some memory in the pool may be leaked if a process crashes while operating on the structure, and there may be some extra (empty) allocated objects left over. These objects won't be visible to reads, and in the case of PrefixTree, the empty nodes will be deleted or reused when a write operation next touches them.

## Time complexity

Since most data structures call the allocator during mutations, the allocator's performance affects the structure's overall performance. The allocators currently have the following time complexities:

    SimpleAllocator::allocate:       O(# allocated blocks)
    SimpleAllocator::free:           O(1)
    LogarithmicAllocator::allocate:  O(log(memory size))
    LogarithmicAllocator::free:      O(log(memory size))

The performance of the structures' operations can then be expressed in reference to the above (use the appropriate figures depending on your choice of allocator).

Hash table performance is generally constant amortized, but can be up to linear in the number if keys in the worst case.

    HashTable::insert:  O(1) amortized + O(key+value length) + 1-3 allocations + 0-1 frees
    HashTable::incr:    O(1) amortized + 1-3 allocations
    HashTable::erase:   O(1) amortized + 0-3 frees
    HashTable::clear:   O(# items in table) + up to (2 * # items) frees
    HashTable::exists:  O(1) amortized
    HashTable::at:      O(1) amortized + O(key+value length)
    HashTable::size:    O(1)
    HashTable::bits:    O(1)

Prefix tree performance is dominated by the key length. Keep the key length as short as possible to reduce the tree's node count, which reduces both memory and time requirements.

    PrefixTree::insert:            O(key length) + up to (key length + 1) allocations + 0-1 frees
    PrefixTree::incr:              O(key length) + up to (key length + 1) allocations + 0-1 frees
    PrefixTree::erase:             O(key length) + up to (key length + 1) frees
    PrefixTree::clear:             O(node count) + (node count) frees + up to (key count) frees
    PrefixTree::exists:            O(key length)
    PrefixTree::type:              O(key length)
    PrefixTree::at:                O(key length) + O(value length)
    PrefixTree::next_key:          O(key length)
    PrefixTree::next_key_value:    O(key length) + O(returned value length)
    PrefixTree::find:              O(key length) + O(found value length)
    PrefixTree::lower_bound:       O(key length) + O(found value length)
    PrefixTree::upper_bound:       O(key length) + O(found value length)
    PrefixTree::size:              O(1)
    PrefixTree::node_size:         O(1)
    PrefixTree::bytes_for_prefix:  O(prefix length) + O(nodes in subtree)
    PrefixTree::nodes_for_prefix:  O(prefix length) + O(nodes in subtree)

Queue performance is mostly affected by the cost of copying the value into and out of the shared memory pool.

    Queue::push_back:   1 allocation + O(value length)
    Queue::push_front:  1 allocation + O(value length)
    Queue::push:        calls either push_back or push_front exactly once
    Queue::pop_back:    O(value length) + 1 free
    Queue::pop_front:   O(value length) + 1 free
    Queue::pop:         calls either pop_back or pop_front exactly once
    Queue::size:        O(1)
    Queue::bytes:       O(1)

Priority queues are binary heaps, which have logarithmic complexity for most operations. For Queue::push, the array is reallocated when it expands beyond a power of two, but is never shrunk. When an expand occurs, the array must be copied into a new memory location, incurring a linear time penalty.

    PriorityQueue::push (no expand):  O(log(queue length)) + 1 allocation + O(value length)
    PriorityQueue::push (expand):     O(queue length) + 2 allocations + 1 free + O(value length)
    PriorityQueue::pop:               O(log(queue length)) + 1 free + O(value length)
    PriorityQueue::clear:             O(queue length) + (queue length) frees
    PriorityQueue::size:              O(1)

Atomic vector (IntVector in Python) performance can't be improved much from its current state.

    AtomicVector::expand:            O(1) + several system calls
    AtomicVector::size:              O(1)
    AtomicVector::load:              O(1)
    AtomicVector::store:             O(1)
    AtomicVector::exchange:          O(1)
    AtomicVector::compare_exchange:  O(1)
    AtomicVector::fetch_add:         O(1)
    AtomicVector::fetch_sub:         O(1)
    AtomicVector::fetch_and:         O(1)
    AtomicVector::fetch_or:          O(1)
    AtomicVector::fetch_xor:         O(1)

## Future work

There's a lot to do here.
- Use a more efficient locking strategy. Currently we use spinlocks with some heuristics.
- Make hash tables and priority queues always consistent for crash recovery.
- Make hash tables support more hash functions.
- Make hash tables support dynamic expansion (rehashing).
- Make hash tables support atomic increments in Python.
- Support shrinking the priority queue heap array.
- Return immutable objects for complex types in Python to make in-place modification not fail silently.
- Add more data structures to the library.
- Make pool creation race-free. Currently processes can get weird errors if they both attempt to create the same pool at the same time.
- Make sharedstructures objects thread-safe within a process.
- Fix spurious failures in QueueTest.
