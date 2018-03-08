# sharedstructures

sharedstructures is a C++ and Python 2/3 library for storing data structures in automatically-created, dynamically-sized shared memory objects. This library can be used to share complex data between processes in a performant way. Currently hash tables and prefix trees are implemented, though only prefix trees are currently recommended for production use.

## Building

- Build and install phosg (https://github.com/fuzziqersoftware/phosg)
- Build the C++ and Python libraries and test them by running `make`. You can build specific libraries by running `make cpp_only`, `make py_only`, or `make py3_only`.
- Run `sudo make install`.

If it doesn't work on your system, let me know. I've built and tested it on Mac OS X 10.12 and Ubuntu 14.04 and 16.04.

## Basic usage

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

See PrefixTree.hh for more details on how to use the tree object.

Python usage is a bit more intuitive - sharedstructures objects behave mostly like dicts (but read the "Python wrapper" section below):

    import sharedstructures

    tree = sharedstructures.PrefixTree(filename, 'logarithmic', 0)

    tree[k1] = v1   # insert or overwrite a value
    v2 = tree[k2]   # retrieve a value
    del tree[k3]    # delete a value
    if k4 in tree:  # check if a key exists

## Interfaces and objects

The Pool object (Pool.hh) implements a raw expandable memory pool. Unlike standard memory semantics, it deals with relative pointers ("offsets") since the pool base address can move in the process' address space. Offsets can be converted to usable pointers with the `Pool::PoolPointer` member class, which handles the offset logic internally and behaves like a normal pointer externally. Performance-sensitive callers can use `Pool::at<T>` instead, but its return values can be invalidated by pool expansion.

Generally you'll want to use some kind of allocator on top of the Pool object. The Allocator object manages pool expansion and assignment of regions for the application's needs. There are currently two allocators implemented:
- SimpleAllocator achieves high space efficiency and constant-time frees, but allocations take up to linear time in the number of existing blocks.
- LogarithmicAllocator compromises space efficiency for speed; it wastes more memory, but both allocations and frees take logarithmic time in the size of the pool.

The allocator type of a pool can't be changed after creating it. Choose the allocator type based on what the access patterns will be - use SimpleAllocator if you have memory size concerns, use LogarithmicAllocator if you have speed concerns.

## Data structures

Data structure objects can be used on top of an Allocator object. Currently there are two data structures.

HashTable implements a binary-safe map of strings to strings. HashTables have a fixed bucket count at creation time and cannot be resized dynamically. This issue will be fixed in the future.

PrefixTree implements a binary-safe map of strings to values of any of the following types:
- Strings
- Integers
- Floating-point numbers
- Boolean values
- Null (this is not the same as the key not existing - a key can exist and have a Null value)

Both structures support getting and setting individual keys, iteration over all or part of the map, conditional writes (check-and-set, check-and-delete), and atomic increments. All of these operations are supported in both C++ and Python, except atomic increments on HashTables (these are supported only in C++).

The header files (HashTable.hh and PrefixTree.hh) document how to use these objects. Take a look at the test source (HashTableTest.cc and PrefixTreeTest.cc) for usage examples.

### Iteration semantics

Iteration over either of these structures only locks the structure while advancing the iterator, so it's possible for an iteration to see an inconsistent view of the data structure due to concurrent modifications by other processes.

Iterating a HashTable produces items in pseudorandom order. If an item exists in the table for the duration of the iteration, then it will be returned; if it's created or deleted during the iteration, then it may or may not be returned. Similarly, if a key's value is changed during the iteration, then either its new or old value may be returned.

Iterating a PrefixTree produces items in lexicographic order. Concurrent changes after the current key (lexicographically) will be visible, changes at or before the current key will not. Additionally, PrefixTree supports `lower_bound` and `upper_bound` in C++, and has `keys_from`, `values_from`, and `items_from` in Python to support starting iteration at places within the tree.

For both structures, the iterator objects cache one or more results on the iterator object itself, so values can't be modified through the iterator object.

## Python wrapper

HashTable and PrefixTree can also be used from Python with the included module. Keys can be accessed directly with the subscript operator (`t[k] = value`; `value = t[k]`; `del t[k]`). Keys must be strings (bytes in Python 3); TypeError is raised if some other type is given for a key.

The Python wrapper transparently marshals objects that aren't basic types - which means you can store tuples, dicts, lists, etc. in HashTables and PrefixTrees, though this will be inefficient for large objects. Storing numeric values and True/False/None in a PrefixTree will use the tree's corresponding native types, so they can be easily accessed from non-Python programs.

There are a few things to watch out for:
- Modifying complex values in-place will silently fail because `t[k]` returns a copy of the value at `k`, since it's generally not safe to directly modify values without holding the pool lock. Statements like `t[k1] = {}; t[k1][k2] = 17` won't work - after doing this, `t[k1]` will still be an empty dictionary.
- Strings and numeric values *can* be modified "in-place" because Python implements this using separate load and store operations - so `t[k] += 1` works, but is vulnerable to data races when multiple processes are accessing the structure. PrefixTree supports atomic increments on numeric keys by using `t.incr(k, delta)`.
- `t.items` is an alias for `t.iteritems` (and similarly for `.keys` -> `.iterkeys` and `.values` -> `.itervalues`). For example, in both Python 2 and 3, `t.items()` returns an iterator instead of a list.
- HashTable and PrefixTree aren't subclasses of dict. They can be converted to (non-shared) dicts by doing `dict(t.iteritems())`. This may not produce a consistent snapshot though; see "iteration semantics" above.

## Reliability

Operations on shared data structures use a global lock over the entire structure. Since operations generally involve only a few memory accesses, the critical sections should be quite short. However, processes can still crash or be killed during these critical sections, which leads to the lock being "held" by a dead process.

Currently, the lock wait algorithm will check periodically if the process holding the lock is still alive. If the holding process has died, the waiting process will "steal" the lock from that process and repair the allocator's internal data structures. This may be slow for large data structures, since it involves walking the entire list of allocated regions.

HashTable is not necessarily consistent in case of a crash, though this will be fixed in the future. For now, be wary of using a HashTable if a process crashed while operating on it.

PrefixTree is always consistent and doesn't need any extra repairs after a crash. However, some memory in the pool may be leaked, and there may be some extra (empty) nodes left over. These nodes won't be visible to gets or iterations, and will be deleted or reused when a write operation next touches them.

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

## Companies using sharedstructures in production

- [Quora](http://www.quora.com/)
