# sharedstructures

sharedstructures is a library for storing data structures in automatically-created, dynamically-sized shared memory objects. This library can be used to share complex data between processes in a performant way. Currently only hash tables and prefix trees are implemented.

## Building

- Build and install phosg (https://github.com/fuzziqersoftware/phosg)
- Build the C++ and Python libraries and test them by running `make`. If you don't have Python headers installed, you can build the C++ libraries only by running `make cpp_only`.
- If you're running on Mac OS X, run `make osx_cpp32_test osx_py32_test` to run the tests in 32-bit mode.
- Run `sudo make install`.

If it doesn't work on your system, let me know. I've built and tested it on Mac OS X 10.11 and Ubuntu 16.04.

## Interfaces and objects

The Pool object (Pool.hh) implements a raw expandable memory pool. Unlike standard memory semantics, it deals with relative pointers ("offsets") since the pool base address can move in the process' address space. Offsets can be converted to usable pointers with the `Pool::PoolPointer` member class, which handles the offset logic internally and behaves like a normal pointer externally. Performance-sensitive callers can use `Pool::at<T>` instead, but its return values can be invalidated by pool expansion.

Generally you'll want to use some kind of allocator on top of the Pool object. The Allocator object manages pool expansion and assignment of regions for the application's needs. There are currently two allocators implemented:
- SimpleAllocator achieves high space efficiency and constant-time frees, but allocations are up to linear in the number of existing blocks.
- LogarithmicAllocator compromises space efficiency for speed; it wastes more memory, but both allocations and frees are logarithmic in the size of the pool.

## Data structures

Data structure objects can be used on top of an Allocator object. Currently there are two data structures.

HashTable implements a binary-safe map of strings to strings. The header file (HashTable.hh) documents its usage. Currently HashTable instances have a fixed bucket count at creation time and cannot be resized dynamically.

PrefixTree implements a binary-safe map of strings to values of any of the following types:
- Strings
- Integers
- Floating-point numbers
- Boolean values
- Null (this is not the same as the key not existing - a key can exist and have a Null value)

The header file (PrefixTree.hh) documents its usage. A PrefixTree can also be used to implement a set of strings by simply only using Null values.

Take a look at the test source (HashTableTest.cc and PrefixTreeTest.cc) for usage examples.

### Python wrapper

HashTable and PrefixTree can also be used from Python with the included module. After construction, these objects behave similarly to dicts.

The Python wrapper transparently marshals objects that aren't basic types - which means you can store tuples, dicts, lists, etc. in HashTables and PrefixTrees, though this will be inefficient for large objects. Storing numeric values and True/False/None in a PrefixTree will use the tree's corresponding types.

### Caveats

Some things to be aware of:
- You can't change the allocator type of a data structure after creating it. Choose your allocator based on what your access patterns will be - use SimpleAllocator if you have memory size concerns, use LogarithmicAllocator if you have speed concerns.
- HashTables aren't iterable yet. This will be implemented in the future.
- Most values can't be modified in-place. This is by design; we don't return references to objects stored in the pool since most complex structures can't be safely accessed without a lock.
- In Python, modifying complex values in-place will silently fail because `t[k]` returns a copy of the value at k (for the same reason as above). Statements like `t[k] = {}; t[k][v] = 17` won't work - after doing this, `t[k]` will still be an empty dictionary. In the future, we can make this kind of error noisy by returning immutable objects that behave like dicts/sets/etc. instead.
- Simple types *can* be modified "in-place" because Python implements this using separate load and store operations - so `t['object'] += 1` works, but is vulnerable to data races when multiple processes are accessing the structure. PrefixTree supports atomic increments on numeric keys with the `.incr()` function.
- In Python, HashTable and PrefixTree aren't subclasses of dict. PrefixTrees can be converted to (non-shared) dicts though by doing `dict(t.iteritems())`.

## Future work

There's a lot to do here.
- Use a more efficient locking strategy. Currently we use spinlocks.
- Make hash tables support more hash functions.
- Make hash tables support dynamic expansion (rehashing).
- Make prefix trees support complex subtypes; e.g. keys with hash tables or lists as values.
- Return immutable objects for complex types in Python.
- Add more data structures to the library.