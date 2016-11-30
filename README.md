# sharedstructures

sharedstructures is a library for storing data structures in automatically-created, dynamically-sized shared memory objects. This library can be used to share complex data between processes in a performant way. Currently only hash tables and prefix trees are implemented, and hash tables aren't close to feature-completeness.

## Building

- Build and install phosg (https://github.com/fuzziqersoftware/phosg)
- Build the C++ and Python libraries and test them by running `make`. If you don't have Python headers installed, you can build the C++ libraries only by running `make cpp_only`.
- If you're running on Mac OS X, run `make osx_cpp32_test osx_py32_test` to run the tests in 32-bit mode.
- Run `sudo make install`.

If it doesn't work on your system, let me know. I've built and tested it on Mac OS X 10.11 and Ubuntu 16.04.

## Interface

Pool (Pool.hh) implements a raw expandable memory pool and simple allocator. Unlike standard allocators, it deals with relative pointers ("offsets") since the pool base address can move in the process' address space. Offsets can be converted to usable pointers with the `Pool::PoolPointer` member class, which handles the offset logic internally and behaves like a normal pointer externally.

HashTable implements a binary-safe map of strings to strings. The header file (HashTable.hh) documents its usage. Currently HashTable instances have a fixed bucket count at creation time and cannot be resized dynamically.

PrefixTree implements a binary-safe map of strings to values of any of the following types:
- Strings
- Integers (these are inlined in the tree structure if the top 4 bits have the same value, requiring fewer memory accesses)
- Floating-point numbers
- Boolean values (these are inlined in the tree structure)
- Null (this is different from the key not existing - a key can exist and have a Null value)

The header file (PrefixTree.hh) documents its usage. A PrefixTree can also be used to implement a set of strings by simply only using Null values.

The Python wrapper supports directly passing through these basic types to the underlying tree, and transparently marshals objects that aren't basic types - which means you can store tuples, dicts, lists, etc. in HashTables and PrefixTrees, though this will be inefficient for large objects.

Take a look at the tests (e.g. PrefixTreeTest.cc or PrefixTreeTest.py) for usage examples.

### Caveats

A few things to be aware of:
- HashTables aren't iterable. This is not by design; it's just not implemented yet.
- Most values can't be modified in-place. This is by design; we don't return references to objects stored in the pool since most complex structures can't be safely accessed without a lock.
- In Python, modifying complex values in-place will silently fail because `t[k]` returns a copy of the value at k (for the same reason as above). Things like `t['dict_key'] = {}; t['dict_key']['value'] = 17` won't work - after doing this, `t['dict_key']` will still be an empty dictionary. In the future, we can make this kind of error noisy by returning immutable objects that look like dicts/sets/etc. instead.
- Simple types *can* be modified "in-place" because Python implements that using separate load and store operations - so `t['object'] += 1` works, but is vulnerable to data races when multiple processes are accessing the structure. PrefixTree supports atomic increments on numeric keys with the `.incr()` function.
- In Python, HashTable and PrefixTree aren't subclasses of dict. PrefixTrees can be converted to (non-shared) dicts though by doing `dict(t.iteritems())`.

## Future work

There's a lot to do here.
- Use a more efficient allocator algorithm. Currently `Pool::allocate` is up to linear in the number of allocated blocks.
- Use a more efficient locking strategy. Currently we use spinlocks.
- Make hash tables support more hash functions.
- Make hash tables support dynamic expansion (rehashing).
- Make prefix trees support complex subtypes; e.g. keys with hash tables or lists as values.
- Return immutable objects for complex types in Python.
- Add more data structures to the library.