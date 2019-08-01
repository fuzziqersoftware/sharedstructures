# sharedstructures

sharedstructures is a C++ and Python 2/3 library for storing data structures in automatically-created, dynamically-sized shared memory objects. This library can be used to share complex data between processes in a performant way.

This library currently supports these data structures:
- Hash tables with binary string keys and values
- Prefix trees with binary string keys and typed values (supported types are null, bool, int, float, and binary string)
- Queues (doubly-linked lists) with binary string values
- Atomic 64-bit integer arrays

## Building

- Build and install phosg (https://github.com/fuzziqersoftware/phosg)
- Build the C++ and Python libraries and test them by running `make`. You can build specific libraries by running `make cpp_only`, `make py_only`, or `make py3_only`.
- Run `sudo make install`.

If it doesn't work on your system, let me know. I've built and tested it on Mac OS X 10.12 and Ubuntu 14.04 and 16.04.

## Data structures

Data structure objects can be used on top of an Allocator or Pool object. (See examples below for usage information.) Currently there are three data structures.

**HashTable** implements a binary-safe map of strings to strings. HashTables have a fixed bucket count at creation time and cannot be resized dynamically. This issue will be fixed in the future.

**PrefixTree** implements a binary-safe map of strings to values of any of the following types:
- Strings
- Integers
- Floating-point numbers
- Boolean values
- Null (this is not the same as the key not existing - a key can exist and have a Null value)

Both HashTable and PrefixTree support getting and setting individual keys, iteration over all or part of the map, conditional writes (check-and-set, check-and-delete), and atomic increments. Note that atomic increments on HashTables are supported only in C++ and not in Python, but all other operations are supported in both languages.

**Queue** implements a doubly-linked list of binary strings. Items may only be accessed at the ends of the list, not at arbitrary positions within the list.

**IntVector** implements an array of 64-bit signed integers, supporting various atomic operations on them. Unlike the other data structures, IntVector operates directly on top of a Pool object and does not have an Allocator. This is necessary because it implements only lock-free operations, and Allocators require using locks. This also means that a Pool containing an IntVector may not contain any other data structures.

The header files (HashTable.hh, PrefixTree.hh, Queue.hh, and IntVector.hh) document how to use these objects. Take a look at the test source (HashTableTest.cc, PrefixTreeTest.cc, QueueTest.cc, and IntVectorTest.cc) for usage examples.

## Basic usage

### Mapping types

Hash tables have a number of unimplemented convenience features; for example, the bucket count cannot be changed without rebuilding the entire table manually. For sharing mappings/dictionaries, use a prefix tree instead.

The following C++ code opens a prefix tree object, creating it if it doesn't exist:

    #include <memory>
    #include <sharedstructures/Pool.hh>
    #include <sharedstructures/LogarithmicAllocator.hh>
    #include <sharedstructures/PrefixTree.hh>

    using namespace sharedstructures;

    std::shared_ptr<Pool> pool(new Pool(filename));
    std::shared_ptr<Allocator> alloc(new LogarithmicAllocator(pool));
    std::shared_ptr<PrefixTree> tree(new PrefixTree(alloc, 0));

    // note: k1, k2, etc. are all std::string
    tree->insert(k1, v1);  // insert or overwrite a value
    PrefixTree::LookupResult v2 = tree->at(k2);  // retrieve a value
    bool was_erased = tree->erase(k3);  // delete a value
    bool key_exists = tree->exists(k4);  // check if a key exists
    size_t count = tree->size();  // get the number of key-value pairs

See PrefixTree.hh for more details on how to use the tree object. The HashTable interface is similar.

Python usage is a bit more intuitive - `sharedstructures.PrefixTree` objects behave mostly like dicts (but read the "Python wrapper" section below):

    import sharedstructures

    tree = sharedstructures.PrefixTree(filename, 'logarithmic', 0)

    tree[k1] = v1   # insert or overwrite a value
    v2 = tree[k2]   # retrieve a value
    del tree[k3]    # delete a value
    if k4 in tree:  # check if a key exists

### Queues

Queue usage is pretty straightforward.

In C++, you can add std::string objects and pointer/size pairs to the queue, but it only returns std::string objects. Usage:

    #include <memory>
    #include <sharedstructures/Pool.hh>
    #include <sharedstructures/LogarithmicAllocator.hh>
    #include <sharedstructures/Queue.hh>

    using namespace sharedstructures;

    std::shared_ptr<Pool> pool(new Pool(filename));
    std::shared_ptr<Allocator> alloc(new LogarithmicAllocator(pool));
    std::shared_ptr<Queue> q(new Queue(alloc, 0));

    // add items to the queue
    q->push_back(item1);  // std::string
    q->push_back(item1_data, item1_size);  // (void*, size_t)
    q->push_front(item2);  // std::string
    q->push_front(item2_data, item2_size);  // (void*, size_t)

    // remove items from the queue (throws std::out_of_range if empty)
    std::string item3 = q->pop_back();
    std::string item4 = q->pop_front();

    // get the number of items in the queue
    size_t count = q->size();

In Python, all queue items are bytes objects. Usage:

    import sharedstructures

    q = sharedstructures.Queue(filename, 'logarithmic', 0)

    # add items to the queue
    q.push_back(item1)
    q.push_front(item2)

    # remove items from the queue (raises IndexError if empty)
    item3 = q.pop_back()
    item4 = q.pop_front()

    # get the number of items in the queue
    num_items = len(q)

### Atomic integer vectors

Unlike the other data structures, atomic integer vectors do not use an underlying allocator; instead, they manage memory in the pool internally. This means that an AtomicIntegerVector cannot share a pool with any other data structures.

The following C++ code opens an atomic integer vector object, creating it if it doesn't exist:

    #include <memory>
    #include <sharedstructures/Pool.hh>
    #include <sharedstructures/IntVector.hh>

    using namespace sharedstructures;

    std::shared_ptr<Pool> pool(new Pool(filename));
    std::shared_ptr<IntVector> v(new IntVector(pool));

    int64_t value = v.load(index);  // retrieve a value
    v.store(index, value);  // overwrite a value
    int64_t old_value = v.exchange(index, new_value);  // swap a value
    int64_t old_value = v.compare_exchange(index, expected_value, new_value);  // compare and swap a value
    int64_t old_value = v.fetch_add(index, delta);  // add to a value
    int64_t old_value = v.fetch_sub(index, delta);  // subtract from a value
    int64_t old_value = v.fetch_and(index, mask);  // bitwise-and a value
    int64_t old_value = v.fetch_or(index, mask);  // bitwise-or a value
    int64_t old_value = v.fetch_xor(index, mask);  // bitwise-xor a value

The Python interface for IntVector is similar to the C++ interface:

    import sharedstructures

    v = sharedstructures.IntVector(filename)

    value = v.load(index);  # retrieve a value
    v.store(index, value);  # overwrite a value
    old_value = v.exchange(index, new_value);  # swap a value
    old_value = v.compare_exchange(index, expected_value, new_value);  # compare and swap a value
    old_value = v.add(index, delta);  # add to a value
    old_value = v.subtract(index, delta);  # subtract from a value
    old_value = v.bitwise_and(index, mask);  # bitwise-and a value
    old_value = v.bitwise_or(index, mask);  # bitwise-or a value
    old_value = v.bitwise_xor(index, mask);  # bitwise-xor a value

## Interfaces and objects

The Pool object (Pool.hh) implements a raw expandable memory pool. Unlike standard memory semantics, it deals with relative pointers ("offsets") since the pool base address can move in the process' address space. Offsets can be converted to usable pointers with the `Pool::PoolPointer` member class, which handles the offset logic internally and behaves like a normal pointer externally. Performance-sensitive callers can use `Pool::at<T>` instead, but its return values can be invalidated by pool expansion.

You'll usually want to use some kind of allocator on top of the Pool object. The Allocator object manages pool expansion and assignment of regions for the application's needs. There are currently two allocators implemented:
- SimpleAllocator achieves high space efficiency and constant-time frees, but allocations take up to linear time in the number of existing blocks.
- LogarithmicAllocator compromises space efficiency for speed; it wastes more memory, but both allocations and frees take logarithmic time in the size of the pool.

The allocator type of a pool can't be changed after creating it, so choose the allocator type based on what the access patterns will be. Use SimpleAllocator if you need high space efficiency or are sharing read-only data (at the cost of slow writes), or use LogarithmicAllocator if you need both reads and writes to be fast and can sacrifice some space for speed.

### Mapping iteration semantics

Iteration over either of the mapping structures only locks the structure while advancing the iterator, so it's possible for an iteration to see an inconsistent view of the data structure due to concurrent modifications by other processes.

Iterating a HashTable produces items in pseudorandom order. If an item exists in the table for the duration of the iteration, then it will be returned; if it's created or deleted during the iteration, then it may or may not be returned. Similarly, if a key's value is changed during the iteration, then either its new or old value may be returned.

Iterating a PrefixTree produces items in lexicographic order. Concurrent changes after the current key (lexicographically) will be visible, changes at or before the current key will not. Additionally, PrefixTree supports `lower_bound` and `upper_bound` in C++, and has `keys_from`, `values_from`, and `items_from` in Python to support starting iteration at places within the tree.

For both structures, the iterator objects cache one or more results on the iterator object itself, so values can't be modified through the iterator object.

Queues and IntVectors are not iterable.

## Python wrapper semantics

HashTable, PrefixTree, Queue, and IntVector can also be used from Python with the included module. For HashTable and PrefixTree, keys can be accessed directly with the subscript operator (`t[k] = value`; `value = t[k]`; `del t[k]`). Keys must be strings (bytes in Python 3); TypeError is raised if some other type is given for a key. For Queue and IntVector, items cannot be accessed using the subscript operator; you have to call the appropriate functions on the object instead.

For mapping types, the Python wrapper transparently marshals objects that aren't basic types - which means you can store tuples, dicts, lists, etc. in HashTables and PrefixTrees, though this will be inefficient for large objects. Storing numeric values and True/False/None in a PrefixTree will use the tree's corresponding native types, so they can be easily accessed from non-Python programs.

There are a few quirks to watch out for:
- With HashTable and PrefixTree, modifying complex values in-place will silently fail because `t[k]` returns a copy of the value at `k`, since it's generally not safe to directly modify values without holding the pool lock. Statements like `t[k1] = {}; t[k1][k2] = 17` won't work - after doing this, `t[k1]` will still be an empty dictionary.
- With HashTable and PrefixTree, strings and numeric values *can* be modified "in-place" because Python implements this using separate load and store operations - so `t[k] += 1` works, but is vulnerable to data races when multiple processes are accessing the structure. PrefixTree supports atomic increments on numeric keys by using `t.incr(k, delta)`.
- With HashTable and PrefixTree, `t.items` is an alias for `t.iteritems` (and similarly for `.keys` -> `.iterkeys` and `.values` -> `.itervalues`). For example, in both Python 2 and 3, `t.items()` returns an iterator instead of a list.
- HashTable and PrefixTree aren't subclasses of dict. They can be converted to (non-shared) dicts by doing `dict(t.iteritems())`. This may not produce a consistent snapshot though; see "iteration semantics" above.
- Queue and IntVector aren't subclasses of list. Unlike the others, there's no easy way to convert them to non-shared lists.

## Thread safety

The allocators are not thread-safe by default, but they include a global read-write lock that the caller can access to achieve thread-safety. To use it, do the following:

    {
      auto g = allocator->lock(false); // false = read lock
      // now the pool cannot be expanded or remapped within this process, and
      // it's safe to read from the pool
    }
    // now it's no longer safe to read from or write to the pool

    {
      auto g = allocator->lock(true); // true = write lock
      // now the pool cannot be read or written by any other thread (even in
      // other processes) and it's safe to call allocator->allocate,
      // allocator->free, etc.
    }
    // now it's no longer safe to read from or write to the pool

HashTable, PrefixTree, and Queue use this global lock when they call the allocator or read from the tree, and are therefore thread-safe by default.

sharedstructures objects are not necessarily thread-safe within a process because one thread may remap the view of the shared memory segment while another thread attempts to access it. It's recommended for each thread that needs access to a Pool to have its own Pool object for this reason.

## Reliability

Operations on shared data structures use a global lock over the entire structure. Since operations generally involve only a few memory accesses, the critical sections should be quite short. However, processes can still crash or be killed during these critical sections, which leads to the lock being "held" by a dead process.

Currently, the lock wait algorithm will check periodically if the process holding the lock is still alive. If the holding process has died, the waiting process will "steal" the lock from that process and repair the allocator's internal data structures. This may be slow for large data structures, since it involves walking the entire list of allocated regions.

HashTable is not necessarily consistent in case of a crash, though this will be fixed in the future. For now, be wary of using a HashTable if a process crashed while operating on it.

PrefixTree and Queue are always consistent and doesn't need any extra repairs after a crash. However, some memory in the pool may be leaked if a process crashes while operating on the structure, and there may be some extra (empty) nodes left over. These nodes won't be visible to reads, and in the case of PrefixTree, the empty nodes will be deleted or reused when a write operation next touches them.

## Future work

There's a lot to do here.
- Use a more efficient locking strategy. Currently we use spinlocks.
- Make hash tables always consistent for crash recovery.
- Make hash tables support more hash functions.
- Make hash tables support dynamic expansion (rehashing).
- Make hash tables support atomic increments in Python.
- Return immutable objects for complex types in Python to make in-place modification not fail silently.
- Add more data structures to the library.
- Make pool creation race-free. Currently processes can get weird errors if they both attempt to create the same pool at the same time.
- Make sharedstructures objects thread-safe within a process.
