#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <marshal.h>

#if PY_MAJOR_VERSION >= 3
#define IS_PY3K
#endif

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <utility>

#include "Pool.hh"
#include "Allocator.hh"
#include "SimpleAllocator.hh"
#include "LogarithmicAllocator.hh"
#include "HashTable.hh"
#include "PrefixTree.hh"

using namespace std;

using ResultValueType = sharedstructures::PrefixTree::ResultValueType;
using LookupResult = sharedstructures::PrefixTree::LookupResult;



// TODO: make sure long is supported everywhere int is supported


static const char* sharedstructures_doc =
"Dynamically-sized shared-memory data structure module.\n\
\n\
This module provides the HashTable and PrefixTree classes.";



// helper functions

static shared_ptr<sharedstructures::Allocator> sharedstructures_internal_get_allocator(
    const char* pool_name, const char* allocator_type) {
  shared_ptr<sharedstructures::Pool> pool(new sharedstructures::Pool(pool_name));
  shared_ptr<sharedstructures::Allocator> allocator;
  if (!allocator_type || !strcmp(allocator_type, "simple")) {
    allocator.reset(new sharedstructures::SimpleAllocator(pool));
  } else if (!strcmp(allocator_type, "logarithmic")) {
    allocator.reset(new sharedstructures::LogarithmicAllocator(pool));
  } else {
    throw out_of_range("unknown allocator type");
  }
  return allocator;
}

static pair<const char*, size_t> sharedstructures_internal_get_key(
    PyObject* key) {
  // key must be a bytes (not unicode!)
  if (!PyBytes_Check(key)) {
    PyErr_SetString(PyExc_TypeError, "sharedstructures keys must be strings");
    return make_pair(nullptr, 0);
  }

  ssize_t key_size;
  char* key_data;
  if (PyBytes_AsStringAndSize(key, &key_data, &key_size) == -1) {
    return make_pair(nullptr, 0);
  }

  return make_pair(key_data, key_size);
}

static PyObject* sharedstructures_internal_get_python_object_for_result(
    const sharedstructures::PrefixTree::LookupResult& res) {
  switch (res.type) {
    case ResultValueType::Missing:
      // this can't happen
      PyErr_SetString(PyExc_NotImplementedError, "missing result returned");
      return NULL;

    case ResultValueType::String:
      if (res.as_string.empty()) {
        return PyBytes_FromStringAndSize(NULL, 0);
      }
      switch (res.as_string[0]) {
        // the first byte tells what the format is
        case 0: // byte string
          return PyBytes_FromStringAndSize(res.as_string.data() + 1, res.as_string.size() - 1);
        case 1: // unicode string
          return PyUnicode_FromUnicode((const Py_UNICODE*)(res.as_string.data() + 1),
              (res.as_string.size() - 1) / sizeof(Py_UNICODE));
        case 2: // marshalled object
          return PyMarshal_ReadObjectFromString(
              const_cast<char*>(res.as_string.data()) + 1,
              res.as_string.size() - 1);
        default:
          PyErr_SetString(PyExc_TypeError, "unknown string format");
          return NULL;
      }

    case ResultValueType::Int:
#ifdef IS_PY3K
      return PyLong_FromLongLong(res.as_int);
#else
      if (res.as_int > PyInt_GetMax()) {
        return PyLong_FromLongLong(res.as_int);
      } else {
        return PyInt_FromLong(res.as_int);
      }
#endif

    case ResultValueType::Double:
      return PyFloat_FromDouble(res.as_double);

    case ResultValueType::Bool: {
      PyObject* ret = res.as_bool ? Py_True : Py_False;
      Py_INCREF(ret);
      return ret;
    }

    case ResultValueType::Null:
      Py_INCREF(Py_None);
      return Py_None;
  }

  PyErr_SetString(PyExc_NotImplementedError, "result has unknown type");
  return NULL;
}

static LookupResult sharedstructures_internal_get_result_for_python_object(
    PyObject* o) {
  if (o == Py_None) {
    return LookupResult();

  } else if (o == Py_True) {
    return LookupResult(true);

  } else if (o == Py_False) {
    return LookupResult(false);

  } else if (PyFloat_Check(o)) {
    double v = PyFloat_AsDouble(o);
    if (v == -1.0 && PyErr_Occurred()) {
      throw runtime_error("failed to convert python object to LookupResult");
    }
    return LookupResult(v);

#ifndef IS_PY3K
  } else if (PyInt_Check(o)) {
    int64_t v = PyInt_AsLong(o);
    if (v == -1 && PyErr_Occurred()) {
      throw runtime_error("failed to convert python object to LookupResult");
    }
    return LookupResult(v);
#endif

  } else if (PyLong_Check(o)) {
    int64_t v = PyLong_AsLongLong(o);
    if (v == -1 && PyErr_Occurred()) {
      throw runtime_error("failed to convert python object to LookupResult");
    }
    return LookupResult(v);

  } else if (PyBytes_Check(o)) {
    LookupResult res("\x00", 1);
    char* data;
    Py_ssize_t size;
    if (PyBytes_AsStringAndSize(o, &data, &size) == -1) {
      throw runtime_error("failed to convert python object to LookupResult");
    }
    res.as_string.append(data, size);
    return res;

  } else if (PyUnicode_Check(o)) {
    LookupResult res("\x01", 1);
#ifdef IS_PY3K
    Py_ssize_t count = PyUnicode_GetLength(o);
#else
    Py_ssize_t count = PyUnicode_GetSize(o);
#endif
    const Py_UNICODE* data = PyUnicode_AsUnicode(o);
    if (!data) {
      throw runtime_error("failed to convert python object to LookupResult");
    }
    res.as_string.append((const char*)data, sizeof(Py_UNICODE) * count);
    return res;

  } else {
    PyObject* marshalled_obj = PyMarshal_WriteObjectToString(o,
        Py_MARSHAL_VERSION);
    if (!marshalled_obj) {
      // TODO: does PyMarshal_WriteObjectToString set an exception on failure?
      // here we assume it does
      throw runtime_error("failed to convert python object to LookupResult");
    }

    LookupResult res("\x02", 1);
    char* data;
    Py_ssize_t size;
    if (PyBytes_AsStringAndSize(marshalled_obj, &data, &size) == -1) {
      Py_DECREF(marshalled_obj);
      throw runtime_error("failed to convert python object to LookupResult");
    }
    res.as_string.append(data, size);
    Py_DECREF(marshalled_obj);
    return res;
  }
}




// HashTable, HashTableIterator, PrefixTree and PrefixTreeIterator definitions

static const char* sharedstructures_HashTable_doc =
"Shared-memory hash table object.\n\
\n\
sharedstructures.HashTable(pool_name[, allocator_type[, base_offset[, bits]]])\n\
\n\
Arguments:\n\
- pool_name: the name of the shared-memory pool to operate on.\n\
- allocator_type: 'simple' (default) or 'logarithmic' (see README.md).\n\
- base_offset: if given, opens a HashTable at this offset within the pool. If\n\
  not given, opens a HashTable at the pool's base offset. If the pool's base\n\
  offset is 0, creates a new HashTable and sets the pool's base offset to the\n\
  new HashTable's offset.\n\
- bits: if a new HashTable is created, it will bave 2^bits buckets (default 8\n\
  bits).";

typedef struct {
  PyObject_HEAD
  shared_ptr<sharedstructures::HashTable> table;
} sharedstructures_HashTable;

static const char* sharedstructures_HashTableIterator_doc =
"Shared-memory hash table iterator.";

typedef struct {
  PyObject_HEAD
  sharedstructures_HashTable* table_obj;
  sharedstructures::HashTableIterator it;
  bool return_keys;
  bool return_values;
} sharedstructures_HashTableIterator;

static const char* sharedstructures_PrefixTree_doc =
"Shared-memory prefix tree object.\n\
\n\
sharedstructures.PrefixTree(pool_name[, allocator_type[, base_offset]])\n\
\n\
Arguments:\n\
- pool_name: the name of the shared-memory pool to operate on.\n\
- allocator_type: 'simple' (default) or 'logarithmic' (see README.md).\n\
- base_offset: if given, opens a PrefixTree at this offset within the pool. If\n\
  not given, opens a PrefixTree at the pool's base offset. If the pool's base\n\
  offset is 0, creates a new PrefixTree and sets the pool's base offset to the\n\
  new PrefixTree's offset.";

typedef struct {
  PyObject_HEAD
  shared_ptr<sharedstructures::PrefixTree> table;
} sharedstructures_PrefixTree;

static const char* sharedstructures_PrefixTreeIterator_doc =
"Shared-memory prefix tree iterator.";

typedef struct {
  PyObject_HEAD
  sharedstructures_PrefixTree* tree_obj;
  sharedstructures::PrefixTreeIterator it;
  bool return_keys;
  bool return_values;
} sharedstructures_PrefixTreeIterator;





// HashTableIterator object method definitions

static PyObject* sharedstructures_HashTableIterator_New(PyTypeObject* type,
    PyObject* args, PyObject* kwargs) {

  sharedstructures_HashTableIterator* self = (sharedstructures_HashTableIterator*)PyType_GenericNew(
      type, args, kwargs);
  if (!self) {
    return NULL;
  }

  // PyArg_ParseTupleAndKeywords takes a char** where it should take a const
  // char** (this argument is never modified), so we have to const_cast it, sigh
  static const char* kwarg_names[] = {"table_obj", "return_keys", "return_values", NULL};
  static char** kwarg_names_arg = const_cast<char**>(kwarg_names);
  PyObject* return_keys_obj;
  PyObject* return_values_obj;
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OOO", kwarg_names_arg,
      &self->table_obj, &return_keys_obj, &return_values_obj)) {
    Py_DECREF(self);
    return NULL;
  }
  if (return_keys_obj == Py_True) {
    self->return_keys = true;
  } else if (return_keys_obj == Py_False) {
    self->return_keys = false;
  } else {
    PyErr_SetString(PyExc_NotImplementedError, "iter() got non-bool return_keys");
    Py_DECREF(self);
    return NULL;
  }
  if (return_values_obj == Py_True) {
    self->return_values = true;
  } else if (return_values_obj == Py_False) {
    self->return_values = false;
  } else {
    PyErr_SetString(PyExc_NotImplementedError, "iter() got non-bool return_values");
    Py_DECREF(self);
    return NULL;
  }

  if (!self->return_keys && !self->return_values) {
    PyErr_SetString(PyExc_NotImplementedError, "iterators must return keys or values or both, not neither");
    return NULL;
  }

  Py_INCREF(self->table_obj);

  new (&self->it) sharedstructures::HashTableIterator(self->table_obj->table->begin());

  return (PyObject*)self;
}

static void sharedstructures_HashTableIterator_Dealloc(PyObject* py_self) {
  sharedstructures_HashTableIterator* self = (sharedstructures_HashTableIterator*)py_self;

  self->it.sharedstructures::HashTableIterator::~HashTableIterator();

  Py_DECREF(self->table_obj);
  Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* sharedstructures_HashTableIterator_Iter(PyObject* py_self) {
  Py_INCREF(py_self);
  return py_self;
}

static PyObject* sharedstructures_HashTableIterator_Next(PyObject* py_self) {
  sharedstructures_HashTableIterator* self = (sharedstructures_HashTableIterator*)py_self;
  sharedstructures_HashTable* table = (sharedstructures_HashTable*)self->table_obj;

  if (self->it == table->table->end()) {
    PyErr_SetNone(PyExc_StopIteration);
    return NULL;
  }

  auto res = *self->it;
  self->it++;

  // TODO: factor this out with PrefixTreeIterator
  if (self->return_keys && self->return_values) {
    // if both, return a tuple of the two items
    PyObject* ret_key = PyBytes_FromStringAndSize(res.first.data(), res.first.size());
    if (!ret_key) {
      return NULL;
    }
    PyObject* ret_value = PyMarshal_ReadObjectFromString(
        const_cast<char*>(res.second.data()), res.second.size());
    if (!ret_value) {
      Py_DECREF(ret_key);
      return NULL;
    }
    PyObject* ret = PyTuple_Pack(2, ret_key, ret_value);
    if (!ret) {
      Py_DECREF(ret_key);
      Py_DECREF(ret_value);
    }
    return ret;
  }

  if (self->return_keys) {
    return PyBytes_FromStringAndSize(res.first.data(), res.first.size());
  }

  if (self->return_values) {
    return PyMarshal_ReadObjectFromString(const_cast<char*>(res.second.data()),
        res.second.size());
  }

  PyErr_SetString(PyExc_NotImplementedError, "iterators must return keys or values or both, not neither");
  return NULL;
}

static PyObject* sharedstructures_HashTableIterator_Repr(PyObject* py_self) {
#ifdef IS_PY3K
  return PyUnicode_FromFormat(
      "<sharedstructures.HashTable.iterator at %p>", py_self);
#else
  return PyBytes_FromFormat(
      "<sharedstructures.HashTable.iterator at %p>", py_self);
#endif
}

static PyTypeObject sharedstructures_HashTableIteratorType = {
   PyVarObject_HEAD_INIT(NULL, 0)
   "sharedstructures.HashTableIterator",                   // tp_name
   sizeof(sharedstructures_HashTableIterator),             // tp_basicsize
   0,                                                      // tp_itemsize
   (destructor)sharedstructures_HashTableIterator_Dealloc, // tp_dealloc
   0,                                                      // tp_print
   0,                                                      // tp_getattr
   0,                                                      // tp_setattr
   0,                                                      // tp_compare
   sharedstructures_HashTableIterator_Repr,                // tp_repr
   0,                                                      // tp_as_number
   0,                                                      // tp_as_sequence
   0,                                                      // tp_as_mapping
   0,                                                      // tp_hash
   0,                                                      // tp_call
   0,                                                      // tp_str
   0,                                                      // tp_getattro
   0,                                                      // tp_setattro
   0,                                                      // tp_as_buffer
   Py_TPFLAGS_DEFAULT,                                     // tp_flag
   sharedstructures_HashTableIterator_doc,                 // tp_doc
   0,                                                      // tp_traverse
   0,                                                      // tp_clear
   0,                                                      // tp_richcompare
   0,                                                      // tp_weaklistoffset
   sharedstructures_HashTableIterator_Iter,                // tp_iter
   sharedstructures_HashTableIterator_Next,                // tp_iternext
   0,                                                      // tp_methods
   0,                                                      // tp_members
   0,                                                      // tp_getset
   0,                                                      // tp_base
   0,                                                      // tp_dict
   0,                                                      // tp_descr_get
   0,                                                      // tp_descr_set
   0,                                                      // tp_dictoffset
   0,                                                      // tp_init
   0,                                                      // tp_alloc
   sharedstructures_HashTableIterator_New,                 // tp_new
};





// HashTable object method definitions

static PyObject* sharedstructures_HashTable_New(PyTypeObject* type,
    PyObject* args, PyObject* kwargs) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)PyType_GenericNew(
      type, args, kwargs);
  if (!self) {
    return NULL;
  }

  // see comment in sharedstructures_HashTableIterator_New about const_cast
  static const char* kwarg_names[] = {"pool_name", "allocator_type",
      "base_offset", "bits", NULL};
  static char** kwarg_names_arg = const_cast<char**>(kwarg_names);
  const char* pool_name;
  Py_ssize_t base_offset = 0;
  uint8_t bits = 8;
  const char* allocator_type = NULL;
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|snb", kwarg_names_arg,
      &pool_name, &allocator_type, &base_offset, &bits)) {
    Py_DECREF(self);
    return NULL;
  }

  // try to construct the pool before filling in the python object
  try {
    auto allocator = sharedstructures_internal_get_allocator(pool_name,
        allocator_type);
    new (&self->table) shared_ptr<sharedstructures::HashTable>(
        new sharedstructures::HashTable(allocator, base_offset, bits));

  } catch (const exception& e) {
    PyErr_Format(PyExc_RuntimeError, "failed to initialize hash table: %s", e.what());
    Py_DECREF(self);
    return NULL;
  }

  return (PyObject*)self;
}

static void sharedstructures_HashTable_Dealloc(PyObject* obj) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)obj;
  self->table.~shared_ptr();
  Py_TYPE(self)->tp_free((PyObject*)self);
}

static const char* sharedstructures_HashTable_verify_doc =
"Checks the internal state of the shared allocator.\n\
\n\
On success, returns None. Otherwise, returns a bytes object with a description\n\
of the error.";

static PyObject* sharedstructures_HashTable_verify(PyObject* py_self) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)py_self;
  try {
    self->table->get_allocator()->verify();
  } catch (const exception& e) {
    return PyBytes_FromStringAndSize(e.what(), strlen(e.what()));
  }
  Py_INCREF(Py_None);
  return Py_None;
}

static Py_ssize_t sharedstructures_HashTable_Len(PyObject* py_self) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)py_self;
  return self->table->size();
}

static int sharedstructures_HashTable_In(PyObject* py_self, PyObject* key) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)py_self;

  auto k = sharedstructures_internal_get_key(key);
  if (!k.first) {
    return -1;
  }

  return self->table->exists(k.first, k.second);
}

static PyObject* sharedstructures_HashTable_GetItem(PyObject* py_self,
    PyObject* key) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)py_self;

  auto k = sharedstructures_internal_get_key(key);
  if (!k.first) {
    return NULL;
  }

  try {
    string res = self->table->at(k.first, k.second);
    return PyMarshal_ReadObjectFromString(const_cast<char*>(res.data()),
        res.size());

  } catch (const out_of_range& e) {
    PyErr_SetObject(PyExc_KeyError, key);
    return NULL;
  }
}

static int sharedstructures_HashTable_SetItem(PyObject* py_self, PyObject* key,
    PyObject* value) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)py_self;

  auto k = sharedstructures_internal_get_key(key);
  if (!k.first) {
    return -1;
  }

  if (!value) {
    if (!self->table->erase(k.first, k.second)) {
      PyErr_SetObject(PyExc_KeyError, key);
      return -1;
    }

  } else {
    PyObject* marshalled_obj = PyMarshal_WriteObjectToString(value,
        Py_MARSHAL_VERSION);
    if (!marshalled_obj) {
      // TODO: does PyMarshal_WriteObjectToString set an exception on failure?
      // here we assume it does
      return -1;
    }

    char* data;
    Py_ssize_t size;
    if (PyBytes_AsStringAndSize(marshalled_obj, &data, &size) == -1) {
      Py_DECREF(marshalled_obj);
      return -1;
    }
    self->table->insert(k.first, k.second, data, size);
    Py_DECREF(marshalled_obj);
  }

  return 0;
}

static PyObject* sharedstructures_HashTable_Repr(PyObject* py_self) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)py_self;
  // PyBytes_FromFormat doesn't support e.g. PRIx64 so we pretend base_offset is
  // a pointer instead

#ifdef IS_PY3K
  return PyUnicode_FromFormat(
      "<sharedstructures.HashTable on %s:%p at %p>",
      self->table->get_allocator()->get_pool()->get_name().c_str(),
      (const void*)self->table->base(), py_self);
#else
  return PyBytes_FromFormat(
      "<sharedstructures.HashTable on %s:%p at %p>",
      self->table->get_allocator()->get_pool()->get_name().c_str(),
      (const void*)self->table->base(), py_self);
#endif
}

static const char* sharedstructures_HashTable_clear_doc =
"Deletes all entries in the table.";

static PyObject* sharedstructures_HashTable_clear(PyObject* py_self) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)py_self;

  self->table->clear();

  Py_INCREF(Py_None);
  return Py_None;
}

static const char* sharedstructures_HashTable_check_and_set_doc =
"Conditionally sets a value if the check key\'s value matches the check value.\n\
\n\
HashTable.check_and_set(check_key, check_value, target_key[, target_value])\n\
  -> bool\n\
\n\
Atomically checks if check_key exists and if its value matches check_value. If\n\
so, sets target_key to target_value. If target_value is not given, deletes\n\
target_key instead.\n\
\n\
Returns True if a set was performed or a key was deleted. Returns False if the\n\
check fails, or if target_key was going to be deleted but it already didn't\n\
exist.";

static PyObject* sharedstructures_HashTable_check_and_set(PyObject* py_self,
    PyObject* args) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)py_self;

  char* check_key;
  Py_ssize_t check_key_size;
  PyObject* check_value_object;
  char* target_key;
  Py_ssize_t target_key_size;
  PyObject* target_value_object = NULL;
  if (!PyArg_ParseTuple(args, "s#Os#|O", &check_key, &check_key_size,
      &check_value_object, &target_key, &target_key_size,
      &target_value_object)) {
    return NULL;
  }

  PyObject* marshalled_check_value_obj = PyMarshal_WriteObjectToString(
      check_value_object, Py_MARSHAL_VERSION);
  if (!marshalled_check_value_obj) {
    // TODO: does PyMarshal_WriteObjectToString set an exception on failure?
    // here we assume it does
    return NULL;
  }
  char* marshalled_check_value_data;
  Py_ssize_t marshalled_check_value_size;
  if (PyBytes_AsStringAndSize(marshalled_check_value_obj,
      &marshalled_check_value_data, &marshalled_check_value_size) == -1) {
    Py_DECREF(marshalled_check_value_obj);
    return NULL;
  }

  sharedstructures::HashTable::CheckRequest check(check_key, check_key_size,
      marshalled_check_value_data, marshalled_check_value_size);

  bool written;
  if (target_value_object) {

    PyObject* marshalled_target_value_obj = PyMarshal_WriteObjectToString(
        target_value_object, Py_MARSHAL_VERSION);
    if (!marshalled_target_value_obj) {
      // TODO: does PyMarshal_WriteObjectToString set an exception on failure?
      // here we assume it does
      Py_DECREF(marshalled_check_value_obj);
      return NULL;
    }
    char* marshalled_target_value_data;
    Py_ssize_t marshalled_target_value_size;
    if (PyBytes_AsStringAndSize(marshalled_target_value_obj,
        &marshalled_target_value_data, &marshalled_target_value_size) == -1) {
      Py_DECREF(marshalled_check_value_obj);
      Py_DECREF(marshalled_target_value_obj);
      return NULL;
    }

    written = self->table->insert(target_key, target_key_size,
        marshalled_target_value_data, marshalled_target_value_size, &check);
    Py_DECREF(marshalled_target_value_obj);

  } else {
    written = self->table->erase(target_key, target_key_size, &check);
  }

  Py_DECREF(marshalled_check_value_obj);

  PyObject* ret = written ? Py_True : Py_False;
  Py_INCREF(ret);
  return ret;
}

static const char* sharedstructures_HashTable_check_missing_and_set_doc =
"Conditionally sets a value if the check key does not exist.\n\
\n\
HashTable.check_missing_and_set(check_key, target_key[, target_value]) -> bool\n\
\n\
Atomically checks if check_key exists. If it doesn't exist, sets target_key to\n\
target_value. If target_value is not given, deletes target_key instead.\n\
\n\
Returns True if a set was performed or a key was deleted. Returns False if the\n\
check_key exists, or if target_key was going to be deleted but it already\n\
didn't exist.";

static PyObject* sharedstructures_HashTable_check_missing_and_set(
    PyObject* py_self, PyObject* args) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)py_self;

  char* check_key;
  Py_ssize_t check_key_size;
  char* target_key;
  Py_ssize_t target_key_size;
  PyObject* target_value_object = NULL;
  if (!PyArg_ParseTuple(args, "s#s#|O", &check_key, &check_key_size,
      &target_key, &target_key_size, &target_value_object)) {
    return NULL;
  }

  sharedstructures::HashTable::CheckRequest check(check_key, check_key_size);

  bool written;
  if (target_value_object) {

    PyObject* marshalled_target_value_obj = PyMarshal_WriteObjectToString(
        target_value_object, Py_MARSHAL_VERSION);
    if (!marshalled_target_value_obj) {
      // TODO: does PyMarshal_WriteObjectToString set an exception on failure?
      // here we assume it does
      return NULL;
    }
    char* marshalled_target_value_data;
    Py_ssize_t marshalled_target_value_size;
    if (PyBytes_AsStringAndSize(marshalled_target_value_obj,
        &marshalled_target_value_data, &marshalled_target_value_size) == -1) {
      Py_DECREF(marshalled_target_value_obj);
      return NULL;
    }

    written = self->table->insert(target_key, target_key_size,
        marshalled_target_value_data, marshalled_target_value_size, &check);
    Py_DECREF(marshalled_target_value_obj);

  } else {
    written = self->table->erase(target_key, target_key_size, &check);
  }

  PyObject* ret = written ? Py_True : Py_False;
  Py_INCREF(ret);
  return ret;
}

static PyObject* sharedstructures_HashTable_iter_generic(PyObject* py_self,
    bool return_keys, bool return_values) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)py_self;

  // args: table, return_keys, return_values
  PyObject* args = Py_BuildValue("OOO", self, return_keys ? Py_True : Py_False,
      return_values ? Py_True : Py_False);
  if (!args) {
    return NULL;
  }

  PyObject* it = PyObject_CallObject(
      (PyObject*)&sharedstructures_HashTableIteratorType, args);
  Py_DECREF(args);

  return it;
}

static const char* sharedstructures_HashTable_iterkeys_doc =
"Returns an iterator over all keys in the table.";

static PyObject* sharedstructures_HashTable_iterkeys(PyObject* py_self) {
  return sharedstructures_HashTable_iter_generic(py_self, true, false);
}

static const char* sharedstructures_HashTable_itervalues_doc =
"Returns an iterator over all values in the table.";

static PyObject* sharedstructures_HashTable_itervalues(PyObject* py_self) {
  return sharedstructures_HashTable_iter_generic(py_self, false, true);
}

static const char* sharedstructures_HashTable_iteritems_doc =
"Returns an iterator over all key/value pairs in the table.";

static PyObject* sharedstructures_HashTable_iteritems(PyObject* py_self) {
  return sharedstructures_HashTable_iter_generic(py_self, true, true);
}

static PyObject* sharedstructures_HashTable_Iter(PyObject* py_self) {
  return sharedstructures_HashTable_iterkeys(py_self);
}

static const char* sharedstructures_HashTable_bits_doc =
"Returns the hash bucket count factor.";

static PyObject* sharedstructures_HashTable_bits(PyObject* py_self) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)py_self;
#ifdef IS_PY3K
  return PyLong_FromLong(self->table->bits());
#else
  return PyInt_FromLong(self->table->bits());
#endif
}

static const char* sharedstructures_HashTable_pool_bytes_doc =
"Returns the size of the underlying shared memory pool.";

static PyObject* sharedstructures_HashTable_pool_bytes(PyObject* py_self) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)py_self;
#ifdef IS_PY3K
  return PyLong_FromSize_t(self->table->get_allocator()->get_pool()->size());
#else
  return PyInt_FromSize_t(self->table->get_allocator()->get_pool()->size());
#endif
}

static const char* sharedstructures_HashTable_pool_free_bytes_doc =
"Returns the amount of free space in the underlying shared memory pool.";

static PyObject* sharedstructures_HashTable_pool_free_bytes(PyObject* py_self) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)py_self;
#ifdef IS_PY3K
  return PyLong_FromSize_t(self->table->get_allocator()->bytes_free());
#else
  return PyInt_FromSize_t(self->table->get_allocator()->bytes_free());
#endif
}

static const char* sharedstructures_HashTable_pool_allocated_bytes_doc =
"Returns the amount of allocated space in the underlying shared memory pool.";

static PyObject* sharedstructures_HashTable_pool_allocated_bytes(PyObject* py_self) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)py_self;
#ifdef IS_PY3K
  return PyLong_FromSize_t(self->table->get_allocator()->bytes_allocated());
#else
  return PyInt_FromSize_t(self->table->get_allocator()->bytes_allocated());
#endif
}

static PyMethodDef sharedstructures_HashTable_methods[] = {
  {"pool_bytes", (PyCFunction)sharedstructures_HashTable_pool_bytes, METH_NOARGS,
      sharedstructures_HashTable_pool_bytes_doc},
  {"pool_free_bytes", (PyCFunction)sharedstructures_HashTable_pool_free_bytes, METH_NOARGS,
      sharedstructures_HashTable_pool_free_bytes_doc},
  {"pool_allocated_bytes", (PyCFunction)sharedstructures_HashTable_pool_allocated_bytes, METH_NOARGS,
      sharedstructures_HashTable_pool_allocated_bytes_doc},
  {"check_and_set", (PyCFunction)sharedstructures_HashTable_check_and_set, METH_VARARGS,
      sharedstructures_HashTable_check_and_set_doc},
  {"check_missing_and_set", (PyCFunction)sharedstructures_HashTable_check_missing_and_set, METH_VARARGS,
      sharedstructures_HashTable_check_missing_and_set_doc},
  {"clear", (PyCFunction)sharedstructures_HashTable_clear, METH_NOARGS,
      sharedstructures_HashTable_clear_doc},
  {"bits", (PyCFunction)sharedstructures_HashTable_bits, METH_NOARGS,
      sharedstructures_HashTable_bits_doc},
  {"iterkeys", (PyCFunction)sharedstructures_HashTable_iterkeys, METH_NOARGS,
      sharedstructures_HashTable_iterkeys_doc},
  {"keys", (PyCFunction)sharedstructures_HashTable_iterkeys, METH_NOARGS,
      sharedstructures_HashTable_iterkeys_doc},
  {"itervalues", (PyCFunction)sharedstructures_HashTable_itervalues, METH_NOARGS,
      sharedstructures_HashTable_itervalues_doc},
  {"values", (PyCFunction)sharedstructures_HashTable_itervalues, METH_NOARGS,
      sharedstructures_HashTable_itervalues_doc},
  {"iteritems", (PyCFunction)sharedstructures_HashTable_iteritems, METH_NOARGS,
      sharedstructures_HashTable_iteritems_doc},
  {"items", (PyCFunction)sharedstructures_HashTable_iteritems, METH_NOARGS,
      sharedstructures_HashTable_iteritems_doc},
  {"verify", (PyCFunction)sharedstructures_HashTable_verify, METH_NOARGS,
      sharedstructures_HashTable_verify_doc},
  {NULL},
};

static PySequenceMethods sharedstructures_HashTable_sequencemethods = {
  sharedstructures_HashTable_Len, // sq_length
  0, // sq_concat
  0, // sq_repeat
  0, // sq_item (we implement GetItem via the mapping protocol instead)
  0, // sq_slice
  0, // sq_ass_item (we implement GetItem via the mapping protocol instead)
  0, // sq_ass_slice
  sharedstructures_HashTable_In, // sq_contains
  0, // sq_inplace_concat
  0, // sq_inplace_repeat
};

static PyMappingMethods sharedstructures_HashTable_mappingmethods = {
  sharedstructures_HashTable_Len,
  sharedstructures_HashTable_GetItem,
  sharedstructures_HashTable_SetItem,
};

static PyTypeObject sharedstructures_HashTableType = {
   PyVarObject_HEAD_INIT(NULL, 0)
   "sharedstructures.HashTable",                    // tp_name
   sizeof(sharedstructures_HashTable),              // tp_basicsize
   0,                                               // tp_itemsize
   (destructor)sharedstructures_HashTable_Dealloc,  // tp_dealloc
   0,                                               // tp_print
   0,                                               // tp_getattr
   0,                                               // tp_setattr
   0,                                               // tp_compare
   sharedstructures_HashTable_Repr,                 // tp_repr
   0,                                               // tp_as_number
   &sharedstructures_HashTable_sequencemethods,     // tp_as_sequence
   &sharedstructures_HashTable_mappingmethods,      // tp_as_mapping
   0,                                               // tp_hash
   0,                                               // tp_call
   0,                                               // tp_str
   0,                                               // tp_getattro
   0,                                               // tp_setattro
   0,                                               // tp_as_buffer
   Py_TPFLAGS_DEFAULT,                              // tp_flag
   sharedstructures_HashTable_doc,                  // tp_doc
   0,                                               // tp_traverse
   0,                                               // tp_clear
   0,                                               // tp_richcompare
   0,                                               // tp_weaklistoffset
   sharedstructures_HashTable_Iter,                 // tp_iter
   0,                                               // tp_iternext
   sharedstructures_HashTable_methods,              // tp_methods
   0,                                               // tp_members
   0,                                               // tp_getset
   0,                                               // tp_base
   0,                                               // tp_dict
   0,                                               // tp_descr_get
   0,                                               // tp_descr_set
   0,                                               // tp_dictoffset
   0,                                               // tp_init
   0,                                               // tp_alloc
   sharedstructures_HashTable_New,                  // tp_new
};




// PrefixTreeIterator object method definitions

static PyObject* sharedstructures_PrefixTreeIterator_New(PyTypeObject* type,
    PyObject* args, PyObject* kwargs) {

  sharedstructures_PrefixTreeIterator* self = (sharedstructures_PrefixTreeIterator*)PyType_GenericNew(
      type, args, kwargs);
  if (!self) {
    return NULL;
  }

  // see comment in sharedstructures_HashTableIterator_New about const_cast
  static const char* kwarg_names[] = {"tree_obj", "return_keys", "return_values",
      "prefix", NULL};
  static char** kwarg_names_arg = const_cast<char**>(kwarg_names);
  PyObject* return_keys_obj = Py_True;
  PyObject* return_values_obj = Py_True;
  PyObject* prefix_obj = NULL;
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|OOO", kwarg_names_arg,
      &self->tree_obj, &return_keys_obj, &return_values_obj, &prefix_obj)) {
    Py_DECREF(self);
    return NULL;
  }
  if (return_keys_obj == Py_True) {
    self->return_keys = true;
  } else if (return_keys_obj == Py_False) {
    self->return_keys = false;
  } else {
    PyErr_SetString(PyExc_NotImplementedError, "iter() got non-bool return_keys");
    Py_DECREF(self);
    return NULL;
  }
  if (return_values_obj == Py_True) {
    self->return_values = true;
  } else if (return_values_obj == Py_False) {
    self->return_values = false;
  } else {
    PyErr_SetString(PyExc_NotImplementedError, "iter() got non-bool return_values");
    Py_DECREF(self);
    return NULL;
  }

  if (!self->return_keys && !self->return_values) {
    PyErr_SetString(PyExc_NotImplementedError, "iterators must return keys or values or both, not neither");
    return NULL;
  }

  Py_INCREF(self->tree_obj);

  if (prefix_obj) {
    char* key = NULL;
    Py_ssize_t key_length = 0;
    if (PyBytes_AsStringAndSize(prefix_obj, &key, &key_length) == -1) {
      return NULL;
    }
    new (&self->it) sharedstructures::PrefixTreeIterator(self->tree_obj->table->lower_bound(key, key_length));
  } else {
    new (&self->it) sharedstructures::PrefixTreeIterator(self->tree_obj->table->begin());
  }

  return (PyObject*)self;
}

static void sharedstructures_PrefixTreeIterator_Dealloc(PyObject* py_self) {
  sharedstructures_PrefixTreeIterator* self = (sharedstructures_PrefixTreeIterator*)py_self;

  self->it.sharedstructures::PrefixTreeIterator::~PrefixTreeIterator();

  Py_DECREF(self->tree_obj);
  Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject* sharedstructures_PrefixTreeIterator_Iter(PyObject* py_self) {
  Py_INCREF(py_self);
  return py_self;
}

static PyObject* sharedstructures_PrefixTreeIterator_Next(PyObject* py_self) {
  sharedstructures_PrefixTreeIterator* self = (sharedstructures_PrefixTreeIterator*)py_self;
  sharedstructures_PrefixTree* tree = (sharedstructures_PrefixTree*)self->tree_obj;

  if (self->it == tree->table->end()) {
    PyErr_SetNone(PyExc_StopIteration);
    return NULL;
  }

  auto res = *self->it;
  self->it++;

  if (self->return_keys && self->return_values) {
    // if both, return a tuple of the two items
    PyObject* ret_key = PyBytes_FromStringAndSize(res.first.data(), res.first.size());
    if (!ret_key) {
      return NULL;
    }
    PyObject* ret_value = sharedstructures_internal_get_python_object_for_result(res.second);
    if (!ret_value) {
      Py_DECREF(ret_key);
      return NULL;
    }
    PyObject* ret = PyTuple_Pack(2, ret_key, ret_value);
    if (!ret) {
      Py_DECREF(ret_key);
      Py_DECREF(ret_value);
    }
    return ret;
  }

  if (self->return_keys) {
    return PyBytes_FromStringAndSize(res.first.data(), res.first.size());
  }

  if (self->return_values) {
    return sharedstructures_internal_get_python_object_for_result(res.second);
  }

  PyErr_SetString(PyExc_NotImplementedError, "iterators must return keys or values or both, not neither");
  return NULL;
}

static PyObject* sharedstructures_PrefixTreeIterator_Repr(PyObject* py_self) {
#ifdef IS_PY3K
  return PyUnicode_FromFormat(
      "<sharedstructures.PrefixTree.iterator at %p>", py_self);
#else
  return PyBytes_FromFormat(
      "<sharedstructures.PrefixTree.iterator at %p>", py_self);
#endif
}

static PyTypeObject sharedstructures_PrefixTreeIteratorType = {
   PyVarObject_HEAD_INIT(NULL, 0)
   "sharedstructures.PrefixTreeIterator",                   // tp_name
   sizeof(sharedstructures_PrefixTreeIterator),             // tp_basicsize
   0,                                                       // tp_itemsize
   (destructor)sharedstructures_PrefixTreeIterator_Dealloc, // tp_dealloc
   0,                                                       // tp_print
   0,                                                       // tp_getattr
   0,                                                       // tp_setattr
   0,                                                       // tp_compare
   sharedstructures_PrefixTreeIterator_Repr,                // tp_repr
   0,                                                       // tp_as_number
   0,                                                       // tp_as_sequence
   0,                                                       // tp_as_mapping
   0,                                                       // tp_hash
   0,                                                       // tp_call
   0,                                                       // tp_str
   0,                                                       // tp_getattro
   0,                                                       // tp_setattro
   0,                                                       // tp_as_buffer
   Py_TPFLAGS_DEFAULT,                                      // tp_flag
   sharedstructures_PrefixTreeIterator_doc,                 // tp_doc
   0,                                                       // tp_traverse
   0,                                                       // tp_clear
   0,                                                       // tp_richcompare
   0,                                                       // tp_weaklistoffset
   sharedstructures_PrefixTreeIterator_Iter,                // tp_iter
   sharedstructures_PrefixTreeIterator_Next,                // tp_iternext
   0,                                                       // tp_methods
   0,                                                       // tp_members
   0,                                                       // tp_getset
   0,                                                       // tp_base
   0,                                                       // tp_dict
   0,                                                       // tp_descr_get
   0,                                                       // tp_descr_set
   0,                                                       // tp_dictoffset
   0,                                                       // tp_init
   0,                                                       // tp_alloc
   sharedstructures_PrefixTreeIterator_New,                 // tp_new
};




// PrefixTree object method definitions

static PyObject* sharedstructures_PrefixTree_New(PyTypeObject* type,
    PyObject* args, PyObject* kwargs) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)PyType_GenericNew(
      type, args, kwargs);
  if (!self) {
    return NULL;
  }

  // see comment in sharedstructures_HashTableIterator_New about const_cast
  static const char* kwarg_names[] = {"pool_name", "allocator_type", "base_offset", NULL};
  static char** kwarg_names_arg = const_cast<char**>(kwarg_names);
  const char* pool_name;
  Py_ssize_t base_offset = 0;
  const char* allocator_type = NULL;
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|sn", kwarg_names_arg,
      &pool_name, &allocator_type, &base_offset)) {
    Py_DECREF(self);
    return NULL;
  }

  try {
    auto allocator = sharedstructures_internal_get_allocator(pool_name,
        allocator_type);
    new (&self->table) shared_ptr<sharedstructures::PrefixTree>(
        new sharedstructures::PrefixTree(allocator, base_offset));

  } catch (const exception& e) {
    PyErr_Format(PyExc_RuntimeError, "failed to initialize prefix tree: %s", e.what());
    Py_DECREF(self);
    return NULL;
  }

  return (PyObject*)self;
}

static void sharedstructures_PrefixTree_Dealloc(PyObject* obj) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)obj;
  self->table.~shared_ptr();
  Py_TYPE(self)->tp_free((PyObject*)self);
}

static const char* sharedstructures_PrefixTree_verify_doc =
    sharedstructures_HashTable_verify_doc;

static PyObject* sharedstructures_PrefixTree_verify(PyObject* py_self) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)py_self;
  try {
    self->table->get_allocator()->verify();
  } catch (const exception& e) {
    return PyBytes_FromStringAndSize(e.what(), strlen(e.what()));
  }
  Py_INCREF(Py_None);
  return Py_None;
}

static Py_ssize_t sharedstructures_PrefixTree_Len(PyObject* py_self) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)py_self;
  return self->table->size();
}

static int sharedstructures_PrefixTree_In(PyObject* py_self, PyObject* key) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)py_self;

  auto k = sharedstructures_internal_get_key(key);
  if (!k.first) {
    return -1;
  }

  return self->table->exists(k.first, k.second);
}

static PyObject* sharedstructures_PrefixTree_GetItem(PyObject* py_self,
    PyObject* key) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)py_self;

  auto k = sharedstructures_internal_get_key(key);
  if (!k.first) {
    return NULL;
  }

  try {
    auto res = self->table->at(k.first, k.second);
    return sharedstructures_internal_get_python_object_for_result(res);

  } catch (const out_of_range& e) {
    PyErr_SetObject(PyExc_KeyError, key);
    return NULL;
  }
}

static int sharedstructures_PrefixTree_SetItem(PyObject* py_self, PyObject* key,
    PyObject* value) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)py_self;

  auto k = sharedstructures_internal_get_key(key);
  if (!k.first) {
    return -1;
  }

  if (!value) {
    if (!self->table->erase(k.first, k.second)) {
      PyErr_SetObject(PyExc_KeyError, key);
      return -1;
    }
    return 0;
  }

  if (value == Py_None) {
    self->table->insert(k.first, k.second);
    return 0;
  }

  if (value == Py_True) {
    self->table->insert(k.first, k.second, true);
    return 0;
  }

  if (value == Py_False) {
    self->table->insert(k.first, k.second, false);
    return 0;
  }

#ifndef IS_PY3K
  if (PyInt_Check(value)) {
    int64_t raw_value = PyInt_AsLong(value);
    if ((raw_value == -1) && PyErr_Occurred()) {
      return -1;
    }
    self->table->insert(k.first, k.second, raw_value);
    return 0;
  }
#endif

  if (PyLong_Check(value)) {
    int64_t raw_value = PyLong_AsLongLong(value);
    if ((raw_value == -1) && PyErr_Occurred()) {
      PyErr_Clear(); // we'll insert it as a marshalled string instead
    } else {
      self->table->insert(k.first, k.second, raw_value);
      return 0;
    }
  }

  if (PyFloat_Check(value)) {
    double raw_value = PyFloat_AsDouble(value);
    if ((raw_value == -1.0) && PyErr_Occurred()) {
      return -1;
    }
    self->table->insert(k.first, k.second, raw_value);
    return 0;
  }

  if (PyUnicode_Check(value)) {
#ifdef IS_PY3K
    Py_ssize_t size = PyUnicode_GetLength(value);
#else
    Py_ssize_t size = PyUnicode_GetSize(value);
#endif
    if (size < 0) {
      return -1;
    }
    Py_UNICODE* data = PyUnicode_AsUnicode(value);

    string insert_data;
    insert_data += '\x01';
    insert_data.append((const char*)data, size * sizeof(Py_UNICODE));
    self->table->insert(k.first, k.second, insert_data.data(), insert_data.size());
    return 0;
  }

  if (PyBytes_Check(value)) {
    char* data;
    Py_ssize_t size;
    if (PyBytes_AsStringAndSize(value, &data, &size) == -1) {
      return -1;
    }
    if (size == 0) {
      self->table->insert(k.first, k.second, "", 0);
    } else {
      // prepend the type byte
      string insert_data;
      insert_data += '\x00';
      insert_data.append(data, size);
      self->table->insert(k.first, k.second, insert_data.data(), insert_data.size());
    }
    return 0;
  }

  // no types matches, so we'll marshal instead
  PyObject* marshalled_obj = PyMarshal_WriteObjectToString(value,
      Py_MARSHAL_VERSION);
  if (!marshalled_obj) {
    // TODO: does PyMarshal_WriteObjectToString set an exception on failure?
    // here we assume it does
    return -1;
  }

  char format = 2;
  struct iovec iov[2];
  iov[0].iov_base = &format;
  iov[0].iov_len = 1;
  if (PyBytes_AsStringAndSize(marshalled_obj, (char**)(&iov[1].iov_base),
      (Py_ssize_t*)&iov[1].iov_len) == -1) {
    Py_DECREF(marshalled_obj);
    return -1;
  }
  self->table->insert(k.first, k.second, iov, 2);
  Py_DECREF(marshalled_obj);

  return 0;
}

static PyObject* sharedstructures_PrefixTree_Repr(PyObject* py_self) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)py_self;
  // PyBytes_FromFormat doesn't support e.g. PRIx64 so we pretend base_offset is
  // a pointer instead

#ifdef IS_PY3K
  return PyUnicode_FromFormat(
      "<sharedstructures.PrefixTree on %s:%p at %p>",
      self->table->get_allocator()->get_pool()->get_name().c_str(),
      (const void*)self->table->base(), py_self);
#else
  return PyBytes_FromFormat(
      "<sharedstructures.PrefixTree on %s:%p at %p>",
      self->table->get_allocator()->get_pool()->get_name().c_str(),
      (const void*)self->table->base(), py_self);
#endif
}

static const char* sharedstructures_PrefixTree_clear_doc =
"Deletes all entries in the table.";

static PyObject* sharedstructures_PrefixTree_clear(PyObject* py_self) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)py_self;

  self->table->clear();

  Py_INCREF(Py_None);
  return Py_None;
}

static const char* sharedstructures_PrefixTree_incr_doc =
"Atomically increments a numeric key's value.\n\
\n\
PrefixTree.incr(key, delta) -> int or float\n\
\n\
delta must match the type of key's value (incr can't increment an int by a float\n\
or vice-versa). If the key doesn't exist, creates it with the value of delta.\n\
\n\
Returns the value of the key after the operation.";

static PyObject* sharedstructures_PrefixTree_incr(PyObject* py_self,
    PyObject* args) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)py_self;

  char* k;
  Py_ssize_t k_size;
  PyObject* delta_obj;
  if (!PyArg_ParseTuple(args, "s#O", &k, &k_size, &delta_obj)) {
    return NULL;
  }

  if (PyLong_Check(delta_obj)) {
    int64_t delta = PyLong_AsLongLong(delta_obj);
    if ((delta == -1) && PyErr_Occurred()) {
      return NULL;
    }
    int64_t ret;
    try {
      ret = self->table->incr(k, k_size, delta);
    } catch (const out_of_range& e) {
      PyErr_SetString(PyExc_ValueError, "incr (int) against key of different type");
      return NULL;
    }

#ifdef IS_PY3K
    return PyLong_FromLongLong(ret);
#else
    if (ret > PyInt_GetMax()) {
      return PyLong_FromLongLong(ret);
    } else {
      return PyInt_FromLong(ret);
    }
#endif
  }

#ifndef IS_PY3K
  if (PyInt_Check(delta_obj)) {
    int64_t delta = PyInt_AsLong(delta_obj);
    if ((delta == -1) && PyErr_Occurred()) {
      return NULL;
    }
    int64_t ret;
    try {
      ret = self->table->incr(k, k_size, delta);
    } catch (const out_of_range& e) {
      PyErr_SetString(PyExc_ValueError, "incr (int) against key of different type");
      return NULL;
    }

    if (ret > PyInt_GetMax()) {
      return PyLong_FromLongLong(ret);
    } else {
      return PyInt_FromLong(ret);
    }
  }
#endif

  if (PyFloat_Check(delta_obj)) {
    double delta = PyFloat_AsDouble(delta_obj);
    if ((delta == -1.0) && PyErr_Occurred()) {
      return NULL;
    }
    double ret;
    try {
      ret = self->table->incr(k, k_size, delta);
    } catch (const out_of_range& e) {
      PyErr_SetString(PyExc_ValueError, "incr (float) against key of different type");
      return NULL;
    }

    return PyFloat_FromDouble(ret);
  }

  PyErr_SetString(PyExc_TypeError, "incr delta must be numeric");
  return NULL;
}

static const char* sharedstructures_PrefixTree_check_and_set_doc =
"Conditionally sets a value if the check key\'s value matches the check value.\n\
\n\
PrefixTree.check_and_set(check_key, check_value, target_key[, target_value])\n\
  -> bool\n\
\n\
Atomically checks if check_key exists and if its value matches check_value. If\n\
so, sets target_key to target_value. If target_value is not given, deletes\n\
target_key instead.\n\
\n\
Returns True if a set was performed or a key was deleted. Returns False if the\n\
check fails, or if target_key was going to be deleted but it already didn't\n\
exist.";

static PyObject* sharedstructures_PrefixTree_check_and_set(PyObject* py_self,
    PyObject* args) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)py_self;

  char* check_key;
  Py_ssize_t check_key_size;
  PyObject* check_value_object;
  char* target_key;
  Py_ssize_t target_key_size;
  PyObject* target_value_object = NULL;
  if (!PyArg_ParseTuple(args, "s#Os#|O", &check_key, &check_key_size,
      &check_value_object, &target_key, &target_key_size,
      &target_value_object)) {
    return NULL;
  }

  bool written;
  try {
    sharedstructures::PrefixTree::CheckRequest check(check_key, check_key_size);
    check.value = sharedstructures_internal_get_result_for_python_object(check_value_object);
    if (target_value_object) {
      auto target_value = sharedstructures_internal_get_result_for_python_object(target_value_object);
      written = self->table->insert(target_key, target_key_size, target_value, &check);
    } else {
      written = self->table->erase(target_key, target_key_size, &check);
    }
  } catch (const runtime_error& e) {
    return NULL;
  }

  PyObject* ret = written ? Py_True : Py_False;
  Py_INCREF(ret);
  return ret;
}

static const char* sharedstructures_PrefixTree_check_missing_and_set_doc =
"Conditionally sets a value if the check key does not exist.\n\
\n\
PrefixTree.check_missing_and_set(check_key, target_key[, target_value]) -> bool\n\
\n\
Atomically checks if check_key exists. If it doesn't exist, sets target_key to\n\
target_value. If target_value is not given, deletes target_key instead.\n\
\n\
Returns True if a set was performed or a key was deleted. Returns False if the\n\
check_key exists, or if target_key was going to be deleted but it already\n\
didn't exist.";

static PyObject* sharedstructures_PrefixTree_check_missing_and_set(
    PyObject* py_self, PyObject* args) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)py_self;

  char* check_key;
  Py_ssize_t check_key_size;
  char* target_key;
  Py_ssize_t target_key_size;
  PyObject* target_value_object = NULL;
  if (!PyArg_ParseTuple(args, "s#s#|O", &check_key, &check_key_size,
      &target_key, &target_key_size, &target_value_object)) {
    return NULL;
  }

  bool written;
  try {
    sharedstructures::PrefixTree::CheckRequest check(check_key, check_key_size,
        ResultValueType::Missing);
    if (target_value_object) {
      auto target_value = sharedstructures_internal_get_result_for_python_object(target_value_object);
      written = self->table->insert(target_key, target_key_size, target_value, &check);
    } else {
      written = self->table->erase(target_key, target_key_size, &check);
    }
  } catch (const runtime_error& e) {
    return NULL;
  }

  PyObject* ret = written ? Py_True : Py_False;
  Py_INCREF(ret);
  return ret;
}

static PyObject* sharedstructures_PrefixTree_iter_generic(PyObject* py_self,
    bool return_keys, bool return_values, PyObject* prefix = NULL) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)py_self;

  // args: table, return_keys, return_values
  PyObject* args = NULL;
  if (prefix) {
    args = Py_BuildValue("OOOO", self, return_keys ? Py_True : Py_False,
      return_values ? Py_True : Py_False, prefix);
  } else {
    args = Py_BuildValue("OOO", self, return_keys ? Py_True : Py_False,
      return_values ? Py_True : Py_False);
  }
  if (!args) {
    return NULL;
  }

  PyObject* it = PyObject_CallObject(
      (PyObject*)&sharedstructures_PrefixTreeIteratorType, args);
  Py_DECREF(args);

  return it;
}

static const char* sharedstructures_PrefixTree_iterkeys_doc =
"Returns an iterator over all keys in the table.";

static PyObject* sharedstructures_PrefixTree_iterkeys(PyObject* py_self) {
  return sharedstructures_PrefixTree_iter_generic(py_self, true, false);
}

static const char* sharedstructures_PrefixTree_keys_from_doc =
"Returns an iterator over all keys in the table, starting at the given key or prefix.";

static PyObject* sharedstructures_PrefixTree_keys_from(PyObject* py_self,
    PyObject* prefix) {
  return sharedstructures_PrefixTree_iter_generic(py_self, true, false, prefix);
}

static const char* sharedstructures_PrefixTree_itervalues_doc =
"Returns an iterator over all values in the table.";

static PyObject* sharedstructures_PrefixTree_itervalues(PyObject* py_self) {
  return sharedstructures_PrefixTree_iter_generic(py_self, false, true);
}

static const char* sharedstructures_PrefixTree_values_from_doc =
"Returns an iterator over all values in the table, starting at the given key or prefix.";

static PyObject* sharedstructures_PrefixTree_values_from(PyObject* py_self,
    PyObject* prefix) {
  return sharedstructures_PrefixTree_iter_generic(py_self, false, true, prefix);
}

static const char* sharedstructures_PrefixTree_iteritems_doc =
"Returns an iterator over all key/value pairs in the table.";

static PyObject* sharedstructures_PrefixTree_iteritems(PyObject* py_self) {
  return sharedstructures_PrefixTree_iter_generic(py_self, true, true);
}

static const char* sharedstructures_PrefixTree_items_from_doc =
"Returns an iterator over all items in the table, starting at the given key or prefix.";

static PyObject* sharedstructures_PrefixTree_items_from(PyObject* py_self,
    PyObject* prefix) {
  return sharedstructures_PrefixTree_iter_generic(py_self, true, true, prefix);
}

static PyObject* sharedstructures_PrefixTree_Iter(PyObject* py_self) {
  return sharedstructures_PrefixTree_iterkeys(py_self);
}

static const char* sharedstructures_PrefixTree_bytes_for_prefix_doc =
"Returns the size of the subtree rooted at the given prefix.\n\
\n\
This includes tree node overhead, but does not include allocator overhead.";

static PyObject* sharedstructures_PrefixTree_bytes_for_prefix(
    PyObject* py_self, PyObject* args) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)py_self;

  char* p;
  Py_ssize_t p_size;
  if (!PyArg_ParseTuple(args, "s#", &p, &p_size)) {
    return NULL;
  }

#ifdef IS_PY3K
  return PyLong_FromSize_t(self->table->bytes_for_prefix(p, p_size));
#else
  return PyInt_FromSize_t(self->table->bytes_for_prefix(p, p_size));
#endif
}

static const char* sharedstructures_PrefixTree_nodes_for_prefix_doc =
"Returns the number of nodes in the subtree rooted at the given prefix.";

static PyObject* sharedstructures_PrefixTree_nodes_for_prefix(
    PyObject* py_self, PyObject* args) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)py_self;

  char* p;
  Py_ssize_t p_size;
  if (!PyArg_ParseTuple(args, "s#", &p, &p_size)) {
    return NULL;
  }

#ifdef IS_PY3K
  return PyLong_FromSize_t(self->table->nodes_for_prefix(p, p_size));
#else
  return PyInt_FromSize_t(self->table->nodes_for_prefix(p, p_size));
#endif
}

static const char* sharedstructures_PrefixTree_pool_bytes_doc =
"Returns the size of the underlying shared memory pool.";

static PyObject* sharedstructures_PrefixTree_pool_bytes(PyObject* py_self) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)py_self;
#ifdef IS_PY3K
  return PyLong_FromSize_t(self->table->get_allocator()->get_pool()->size());
#else
  return PyInt_FromSize_t(self->table->get_allocator()->get_pool()->size());
#endif
}

static const char* sharedstructures_PrefixTree_pool_free_bytes_doc =
"Returns the amount of free space in the underlying shared memory pool.";

static PyObject* sharedstructures_PrefixTree_pool_free_bytes(PyObject* py_self) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)py_self;
#ifdef IS_PY3K
  return PyLong_FromSize_t(self->table->get_allocator()->bytes_free());
#else
  return PyInt_FromSize_t(self->table->get_allocator()->bytes_free());
#endif
}

static const char* sharedstructures_PrefixTree_pool_allocated_bytes_doc =
"Returns the amount of allocated space in the underlying shared memory pool.";

static PyObject* sharedstructures_PrefixTree_pool_allocated_bytes(PyObject* py_self) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)py_self;
#ifdef IS_PY3K
  return PyLong_FromSize_t(self->table->get_allocator()->bytes_allocated());
#else
  return PyInt_FromSize_t(self->table->get_allocator()->bytes_allocated());
#endif
}

static PyMethodDef sharedstructures_PrefixTree_methods[] = {
  {"bytes_for_prefix", (PyCFunction)sharedstructures_PrefixTree_bytes_for_prefix, METH_VARARGS,
      sharedstructures_PrefixTree_bytes_for_prefix_doc},
  {"nodes_for_prefix", (PyCFunction)sharedstructures_PrefixTree_nodes_for_prefix, METH_VARARGS,
      sharedstructures_PrefixTree_nodes_for_prefix_doc},
  {"pool_bytes", (PyCFunction)sharedstructures_PrefixTree_pool_bytes, METH_NOARGS,
      sharedstructures_PrefixTree_pool_bytes_doc},
  {"pool_free_bytes", (PyCFunction)sharedstructures_PrefixTree_pool_free_bytes, METH_NOARGS,
      sharedstructures_PrefixTree_pool_free_bytes_doc},
  {"pool_allocated_bytes", (PyCFunction)sharedstructures_PrefixTree_pool_allocated_bytes, METH_NOARGS,
      sharedstructures_PrefixTree_pool_allocated_bytes_doc},
  {"incr", (PyCFunction)sharedstructures_PrefixTree_incr, METH_VARARGS,
      sharedstructures_PrefixTree_incr_doc},
  {"check_and_set", (PyCFunction)sharedstructures_PrefixTree_check_and_set, METH_VARARGS,
      sharedstructures_PrefixTree_check_and_set_doc},
  {"check_missing_and_set", (PyCFunction)sharedstructures_PrefixTree_check_missing_and_set, METH_VARARGS,
      sharedstructures_PrefixTree_check_missing_and_set_doc},
  {"clear", (PyCFunction)sharedstructures_PrefixTree_clear, METH_NOARGS,
      sharedstructures_PrefixTree_clear_doc},
  {"iterkeys", (PyCFunction)sharedstructures_PrefixTree_iterkeys, METH_NOARGS,
      sharedstructures_PrefixTree_iterkeys_doc},
  {"keys", (PyCFunction)sharedstructures_PrefixTree_iterkeys, METH_NOARGS,
      sharedstructures_PrefixTree_iterkeys_doc},
  {"keys_from", (PyCFunction)sharedstructures_PrefixTree_keys_from, METH_O,
      sharedstructures_PrefixTree_keys_from_doc},
  {"itervalues", (PyCFunction)sharedstructures_PrefixTree_itervalues, METH_NOARGS,
      sharedstructures_PrefixTree_itervalues_doc},
  {"values", (PyCFunction)sharedstructures_PrefixTree_itervalues, METH_NOARGS,
      sharedstructures_PrefixTree_itervalues_doc},
  {"values_from", (PyCFunction)sharedstructures_PrefixTree_values_from, METH_O,
      sharedstructures_PrefixTree_values_from_doc},
  {"iteritems", (PyCFunction)sharedstructures_PrefixTree_iteritems, METH_NOARGS,
      sharedstructures_PrefixTree_iteritems_doc},
  {"items", (PyCFunction)sharedstructures_PrefixTree_iteritems, METH_NOARGS,
      sharedstructures_PrefixTree_iteritems_doc},
  {"items_from", (PyCFunction)sharedstructures_PrefixTree_items_from, METH_O,
      sharedstructures_PrefixTree_items_from_doc},
  {"verify", (PyCFunction)sharedstructures_PrefixTree_verify, METH_NOARGS,
      sharedstructures_PrefixTree_verify_doc},
  {NULL},
};

static PySequenceMethods sharedstructures_PrefixTree_sequencemethods = {
  sharedstructures_PrefixTree_Len, // sq_length
  0, // sq_concat
  0, // sq_repeat
  0, // sq_item (we implement GetItem via the mapping protocol instead)
  0, // sq_slice
  0, // sq_ass_item (we implement GetItem via the mapping protocol instead)
  0, // sq_ass_slice
  sharedstructures_PrefixTree_In, // sq_contains
  0, // sq_inplace_concat
  0, // sq_inplace_repeat
};

static PyMappingMethods sharedstructures_PrefixTree_mappingmethods = {
  sharedstructures_PrefixTree_Len,
  sharedstructures_PrefixTree_GetItem,
  sharedstructures_PrefixTree_SetItem,
};

static PyTypeObject sharedstructures_PrefixTreeType = {
   PyVarObject_HEAD_INIT(NULL, 0)
   "sharedstructures.PrefixTree",                   // tp_name
   sizeof(sharedstructures_PrefixTree),             // tp_basicsize
   0,                                               // tp_itemsize
   (destructor)sharedstructures_PrefixTree_Dealloc, // tp_dealloc
   0,                                               // tp_print
   0,                                               // tp_getattr
   0,                                               // tp_setattr
   0,                                               // tp_compare
   sharedstructures_PrefixTree_Repr,                // tp_repr
   0,                                               // tp_as_number
   &sharedstructures_PrefixTree_sequencemethods,    // tp_as_sequence
   &sharedstructures_PrefixTree_mappingmethods,     // tp_as_mapping
   0,                                               // tp_hash
   0,                                               // tp_call
   0,                                               // tp_str
   0,                                               // tp_getattro
   0,                                               // tp_setattro
   0,                                               // tp_as_buffer
   Py_TPFLAGS_DEFAULT,                              // tp_flag
   sharedstructures_PrefixTree_doc,                 // tp_doc
   0,                                               // tp_traverse
   0,                                               // tp_clear
   0,                                               // tp_richcompare
   0,                                               // tp_weaklistoffset
   sharedstructures_PrefixTree_Iter,                // tp_iter
   0,                                               // tp_iternext
   sharedstructures_PrefixTree_methods,             // tp_methods
   0,                                               // tp_members
   0,                                               // tp_getset
   0,                                               // tp_base
   0,                                               // tp_dict
   0,                                               // tp_descr_get
   0,                                               // tp_descr_set
   0,                                               // tp_dictoffset
   0,                                               // tp_init
   0,                                               // tp_alloc
   sharedstructures_PrefixTree_New,                 // tp_new
};




// module-level names

static PyObject* sharedstructures_delete_pool(PyObject* self, PyObject* args) {
  const char* pool_name;
  if (!PyArg_ParseTuple(args, "s", &pool_name)) {
    return NULL;
  }

  bool deleted;
  try {
    deleted = sharedstructures::Pool::delete_pool(pool_name);
  } catch (const exception& e) {
    PyErr_SetString(PyExc_RuntimeError, e.what());
    return NULL;
  }

  PyObject* ret = deleted ? Py_True : Py_False;
  Py_INCREF(ret);
  return ret;
}

static PyMethodDef sharedstructures_methods[] = {
  {"delete_pool", sharedstructures_delete_pool, METH_VARARGS,
      "Delete a shared pool (of any type)."},
  {NULL},
};




// initialization

#ifdef IS_PY3K
static struct PyModuleDef sharedstructures_module_def = {
  PyModuleDef_HEAD_INIT,
  "sharedstructures",       // m_name
  sharedstructures_doc,     // m_doc
  -1,                       // m_size
  sharedstructures_methods, // m_methods
  NULL,                     // m_reload
  NULL,                     // m_traverse
  NULL,                     // m_clear
  NULL,                     // m_free
};
#endif

static PyObject* sharedstructures_module_init() {
  if (PyType_Ready(&sharedstructures_HashTableType) < 0) {
    return NULL;
  }
  if (PyType_Ready(&sharedstructures_HashTableIteratorType) < 0) {
    return NULL;
  }
  if (PyType_Ready(&sharedstructures_PrefixTreeType) < 0) {
    return NULL;
  }
  if (PyType_Ready(&sharedstructures_PrefixTreeIteratorType) < 0) {
    return NULL;
  }

#if PY_MAJOR_VERSION >= 3
  PyObject* m = PyModule_Create(&sharedstructures_module_def);
#else
  PyObject* m = Py_InitModule3("sharedstructures", sharedstructures_methods,
      sharedstructures_doc);
#endif

  Py_INCREF(&sharedstructures_HashTableType);
  PyModule_AddObject(m, "HashTable", (PyObject*)&sharedstructures_HashTableType);
  Py_INCREF(&sharedstructures_HashTableIteratorType);
  PyModule_AddObject(m, "HashTableIterator", (PyObject*)&sharedstructures_HashTableIteratorType);
  Py_INCREF(&sharedstructures_PrefixTreeType);
  PyModule_AddObject(m, "PrefixTree", (PyObject*)&sharedstructures_PrefixTreeType);
  Py_INCREF(&sharedstructures_PrefixTreeIteratorType);
  PyModule_AddObject(m, "PrefixTreeIterator", (PyObject*)&sharedstructures_PrefixTreeIteratorType);

  return m;
}

#ifdef IS_PY3K

PyMODINIT_FUNC PyInit_sharedstructures(void) {
  return sharedstructures_module_init();
}

#else

#ifndef PyMODINIT_FUNC
#define PyMODINIT_FUNC void
#endif

PyMODINIT_FUNC initsharedstructures() {
  sharedstructures_module_init();
}

#endif
