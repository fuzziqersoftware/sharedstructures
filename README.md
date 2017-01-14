# sharedstructures

sharedstructures is a library for storing data structures in automatically-created, dynamically-sized shared memory objects. This library can be used to share complex data between processes in a performant way. Currently hash tables and prefix trees are implemented.

## Building

- Build and install phosg (https://github.com/fuzziqersoftware/phosg)
- Build the C++ and Python libraries and test them by running `make`. If you don't have Python headers installed, you can build the C++ libraries only by running `make cpp_only`.
- If you're running on Mac OS X, run `make osx_cpp32_test osx_py32_test` to run the tests in 32-bit mode.
- Run `sudo make install`.

If it doesn't work on your system, let me know. I've built and tested it on Mac OS X 10.11 and Ubuntu 16.04.

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

The header files (HashTable.hh and PrefixTree.hh) document how to use these objects. Take a look at the test source (HashTableTest.cc and PrefixTreeTest.cc) for usage examples.

### Iteration semantics

Iteration over either of these structures doesn't lock the structure, so it's possible for an iteration to see an inconsistent view of the data structure due to concurrent modifications by other processes.

Iterating a HashTable produces items in pseudorandom order. If an item exists in the table for the duration of the iteration, then it will be returned; if it's created or deleted during the iteration, then it may or may not be returned. Similarly, if its value is changed during the iteration, then either its new or old value may be returned.

Iterating a PrefixTree produces items in lexicographic order. This ordering makes its behavior with concurrent modifications easier to predict: concurrent changes after the current key (lexicographically) will be visible, changes at or before the current key will not.

For both structures, the iterator objects cache one or more results on the iterator object itself, so values can't be modified through the iterator object.

## Python wrapper

HashTable and PrefixTree can also be used from Python with the included module. Keys can be accessed directly with the subscript operator (`t[k] = value`; `value = t[k]`).

The Python wrapper transparently marshals objects that aren't basic types - which means you can store tuples, dicts, lists, etc. in HashTables and PrefixTrees, though this will be inefficient for large objects. Storing numeric values and True/False/None in a PrefixTree will use the tree's corresponding native types, so they can be easily accessed from non-Python programs.

There are a few things to watch out for:
- In Python, modifying complex values in-place will silently fail because `t[k]` returns a copy of the value at `k`, since it's generally not safe to directly modify values without holding the pool lock. Statements like `t[k1] = {}; t[k1][k2] = 17` won't work - after doing this, `t[k1]` will still be an empty dictionary.
- Strings and numeric values *can* be modified "in-place" because Python implements this using separate load and store operations - so `t[k] += 1` works, but is vulnerable to data races when multiple processes are accessing the structure. PrefixTree supports atomic increments on numeric keys by using `t.incr(k, delta)`.
- HashTable and PrefixTree aren't subclasses of dict. They can be converted to (non-shared) dicts by doing `dict(t.iteritems())`.

## Future work

There's a lot to do here.
- Use a more efficient locking strategy. Currently we use spinlocks.
- Make hash tables support more hash functions.
- Make hash tables support dynamic expansion (rehashing).
- Write `HashTable::incr`.
- Return immutable objects for complex types in Python to make in-place modification not fail silently.
- Add more data structures to the library.
