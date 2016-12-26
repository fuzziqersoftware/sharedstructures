#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <marshal.h>

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
  if (PyString_AsStringAndSize(key, &key_data, &key_size) == -1) {
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
        return PyString_FromStringAndSize(NULL, 0);
      }
      switch (res.as_string[0]) {
        // the first byte tells what the format is
        case 0: // byte string
          return PyString_FromStringAndSize(res.as_string.data() + 1, res.as_string.size() - 1);
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
      if (res.as_int > PyInt_GetMax()) {
        return PyLong_FromLongLong(res.as_int);
      } else {
        return PyInt_FromLong(res.as_int);
      }

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

  } else if (PyInt_Check(o)) {
    int64_t v = PyInt_AsLong(o);
    if (v == -1 && PyErr_Occurred()) {
      throw runtime_error("failed to convert python object to LookupResult");
    }
    return LookupResult(v);

  } else if (PyString_Check(o)) {
    LookupResult res("\x00", 1);
    char* data;
    Py_ssize_t size;
    if (PyString_AsStringAndSize(o, &data, &size) == -1) {
      throw runtime_error("failed to convert python object to LookupResult");
    }
    res.as_string.append(data, size);
    return res;

  } else if (PyUnicode_Check(o)) {
    LookupResult res("\x01", 1);
    Py_ssize_t count = PyUnicode_GetSize(o);
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
    if (PyString_AsStringAndSize(marshalled_obj, &data, &size) == -1) {
      throw runtime_error("failed to convert python object to LookupResult");
    }
    res.as_string.append(data, size);
    Py_DECREF(marshalled_obj);
    return res;
  }
}




// HashTable, PrefixTree and PrefixTreeIterator definitions

typedef struct {
  PyObject_HEAD
  shared_ptr<sharedstructures::HashTable> table;
} sharedstructures_HashTable;

typedef struct {
  PyObject_HEAD
  shared_ptr<sharedstructures::PrefixTree> table;
} sharedstructures_PrefixTree;

typedef struct {
  PyObject_HEAD
  sharedstructures_PrefixTree* tree_obj;
  sharedstructures::PrefixTreeIterator it;
  bool return_keys;
  bool return_values;
} sharedstructures_PrefixTreeIterator;




// HashTable object method definitions

static PyObject* sharedstructures_HashTable_New(PyTypeObject* type,
    PyObject* args, PyObject* kwargs) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)PyType_GenericNew(
      type, args, kwargs);
  if (!self) {
    return NULL;
  }

  const char* pool_name;
  Py_ssize_t base_offset = 0;
  uint8_t bits = 8;
  const char* allocator_type = NULL;
  if (!PyArg_ParseTuple(args, "s|snbs", &pool_name, &allocator_type,
      &base_offset, &bits)) {
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
    PyErr_SetString(PyExc_RuntimeError, "failed to initialize table");
    Py_DECREF(self);
    return NULL;
  }

  return (PyObject*)self;
}

static void sharedstructures_HashTable_Dealloc(PyObject* obj) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)obj;
  self->table.~shared_ptr();
  self->ob_type->tp_free((PyObject*)self);
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
    if (PyString_AsStringAndSize(marshalled_obj, &data, &size) == -1) {
      return -1;
    }
    self->table->insert(k.first, k.second, data, size);
    Py_DECREF(marshalled_obj);
  }

  return 0;
}

static PyObject* sharedstructures_HashTable_Repr(PyObject* py_self) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)py_self;
  return PyString_FromFormat(
      "<sharedstructures.HashTable on %s:%" PRIu64 " at %p>",
      self->table->get_allocator()->get_pool()->get_name().c_str(),
      self->table->base(), py_self);
}

static PyObject* sharedstructures_HashTable_clear(PyObject* py_self) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)py_self;

  self->table->clear();

  Py_INCREF(Py_None);
  return Py_None;
}

static PyObject* sharedstructures_HashTable_bits(PyObject* py_self) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)py_self;
  return PyInt_FromLong(self->table->bits());
}

static PyObject* sharedstructures_HashTable_pool_bytes(PyObject* py_self) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)py_self;
  return PyInt_FromSize_t(self->table->get_allocator()->get_pool()->size());
}

static PyObject* sharedstructures_HashTable_pool_free_bytes(PyObject* py_self) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)py_self;
  return PyInt_FromSize_t(self->table->get_allocator()->bytes_free());
}

static PyObject* sharedstructures_HashTable_pool_allocated_bytes(PyObject* py_self) {
  sharedstructures_HashTable* self = (sharedstructures_HashTable*)py_self;
  return PyInt_FromSize_t(self->table->get_allocator()->bytes_allocated());
}

static PyMethodDef sharedstructures_HashTable_methods[] = {
  {"pool_bytes", (PyCFunction)sharedstructures_HashTable_pool_bytes, METH_NOARGS,
      "Returns the size of the underlying shared memory pool."},
  {"pool_free_bytes", (PyCFunction)sharedstructures_HashTable_pool_free_bytes, METH_NOARGS,
      "Returns the amount of free space in the underlying shared memory pool."},
  {"pool_allocated_bytes", (PyCFunction)sharedstructures_HashTable_pool_allocated_bytes, METH_NOARGS,
      "Returns the amount of allocated space (without overhead) in the underlying shared memory pool."},
  {"clear", (PyCFunction)sharedstructures_HashTable_clear, METH_NOARGS,
      "Deletes all entries in the table."},
  {"bits", (PyCFunction)sharedstructures_HashTable_bits, METH_NOARGS,
      "Returns the hash bucket count factor."},
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
   PyObject_HEAD_INIT(NULL)
   0,                                               // ob_size
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
   "TODO: write dox",                               // tp_doc
   0,                                               // tp_traverse
   0,                                               // tp_clear
   0,                                               // tp_richcompare
   0,                                               // tp_weaklistoffset
   0,                                               // tp_iter
   0,                                               // tp_iternext
   sharedstructures_HashTable_methods,             // tp_methods
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

  // args: (tree_obj, return_keys, return_values)
  PyObject* return_keys_obj;
  PyObject* return_values_obj;
  if (!PyArg_ParseTuple(args, "OOO", &self->tree_obj, &return_keys_obj, &return_values_obj)) {
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

  new (&self->it) sharedstructures::PrefixTreeIterator(self->tree_obj->table->begin());

  return (PyObject*)self;
}

static void sharedstructures_PrefixTreeIterator_Dealloc(PyObject* py_self) {
  sharedstructures_PrefixTreeIterator* self = (sharedstructures_PrefixTreeIterator*)py_self;

  self->it.sharedstructures::PrefixTreeIterator::~PrefixTreeIterator();

  Py_DECREF(self->tree_obj);
  self->ob_type->tp_free((PyObject*)self);
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
    PyObject* ret_key = PyString_FromStringAndSize(res.first.data(), res.first.size());
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
    return PyString_FromStringAndSize(res.first.data(), res.first.size());
  }

  if (self->return_values) {
    return sharedstructures_internal_get_python_object_for_result(res.second);
  }

  PyErr_SetString(PyExc_NotImplementedError, "iterators must return keys or values or both, not neither");
  return NULL;
}

static PyObject* sharedstructures_PrefixTreeIterator_Repr(PyObject* py_self) {
  sharedstructures_PrefixTreeIterator* self = (sharedstructures_PrefixTreeIterator*)py_self;
  PyObject* tree_obj_repr = PyObject_Repr((PyObject*)self->tree_obj);
  PyObject* ret = PyString_FromFormat(
      "<sharedstructures.PrefixTree.iterator on %s at %p>",
      PyString_AsString(tree_obj_repr), py_self);
  Py_DECREF(tree_obj_repr);
  return ret;
}

static PyTypeObject sharedstructures_PrefixTreeIteratorType = {
   PyObject_HEAD_INIT(NULL)
   0,                                                       // ob_size
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
   "TODO: write dox",                                       // tp_doc
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

  const char* pool_name;
  Py_ssize_t base_offset = 0;
  const char* allocator_type = NULL;
  if (!PyArg_ParseTuple(args, "s|sn", &pool_name, &allocator_type,
      &base_offset)) {
    Py_DECREF(self);
    return NULL;
  }

  try {
    auto allocator = sharedstructures_internal_get_allocator(pool_name,
        allocator_type);
    new (&self->table) shared_ptr<sharedstructures::PrefixTree>(
        new sharedstructures::PrefixTree(allocator, base_offset));

  } catch (const exception& e) {
    PyErr_SetString(PyExc_RuntimeError, "failed to initialize prefix tree");
    Py_DECREF(self);
    return NULL;
  }

  return (PyObject*)self;
}

static void sharedstructures_PrefixTree_Dealloc(PyObject* obj) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)obj;
  self->table.~shared_ptr();
  self->ob_type->tp_free((PyObject*)self);
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

  if (PyInt_Check(value)) {
    int64_t raw_value = PyInt_AsLong(value);
    if ((raw_value == -1) && PyErr_Occurred()) {
      return -1;
    }
    self->table->insert(k.first, k.second, raw_value);
    return 0;
  }

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
    Py_ssize_t size = PyUnicode_GetSize(value);
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

  if (PyString_Check(value)) {
    char* data;
    Py_ssize_t size;
    if (PyString_AsStringAndSize(value, &data, &size) == -1) {
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
  if (PyString_AsStringAndSize(marshalled_obj, (char**)(&iov[1].iov_base),
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
  return PyString_FromFormat(
      "<sharedstructures.PrefixTree on %s:%" PRIu64 " at %p>",
      self->table->get_allocator()->get_pool()->get_name().c_str(),
      self->table->base(), py_self);
}

static PyObject* sharedstructures_PrefixTree_clear(PyObject* py_self) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)py_self;

  self->table->clear();

  Py_INCREF(Py_None);
  return Py_None;
}

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

    if (ret > PyInt_GetMax()) {
      return PyLong_FromLongLong(ret);
    } else {
      return PyInt_FromLong(ret);
    }
  }

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

static PyObject* sharedstructures_PrefixTree_iter_generic(PyObject* py_self,
    bool return_keys, bool return_values) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)py_self;

  // args: table, return_keys, return_values
  PyObject* args = Py_BuildValue("OOO", self, return_keys ? Py_True : Py_False,
      return_values ? Py_True : Py_False);
  if (!args) {
    return NULL;
  }

  PyObject* it = PyObject_CallObject(
      (PyObject*)&sharedstructures_PrefixTreeIteratorType, args);
  Py_DECREF(args);

  return it;
}

static PyObject* sharedstructures_PrefixTree_iterkeys(PyObject* py_self) {
  return sharedstructures_PrefixTree_iter_generic(py_self, true, false);
}

static PyObject* sharedstructures_PrefixTree_itervalues(PyObject* py_self) {
  return sharedstructures_PrefixTree_iter_generic(py_self, false, true);
}

static PyObject* sharedstructures_PrefixTree_iteritems(PyObject* py_self) {
  return sharedstructures_PrefixTree_iter_generic(py_self, true, true);
}

static PyObject* sharedstructures_PrefixTree_Iter(PyObject* py_self) {
  return sharedstructures_PrefixTree_iterkeys(py_self);
}

static PyObject* sharedstructures_PrefixTree_pool_bytes(PyObject* py_self) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)py_self;
  return PyInt_FromSize_t(self->table->get_allocator()->get_pool()->size());
}

static PyObject* sharedstructures_PrefixTree_pool_free_bytes(PyObject* py_self) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)py_self;
  return PyInt_FromSize_t(self->table->get_allocator()->bytes_free());
}

static PyObject* sharedstructures_PrefixTree_pool_allocated_bytes(PyObject* py_self) {
  sharedstructures_PrefixTree* self = (sharedstructures_PrefixTree*)py_self;
  return PyInt_FromSize_t(self->table->get_allocator()->bytes_allocated());
}

static PyMethodDef sharedstructures_PrefixTree_methods[] = {
  {"pool_bytes", (PyCFunction)sharedstructures_PrefixTree_pool_bytes, METH_NOARGS,
      "Returns the size of the underlying shared memory pool."},
  {"pool_free_bytes", (PyCFunction)sharedstructures_PrefixTree_pool_free_bytes, METH_NOARGS,
      "Returns the amount of free space in the underlying shared memory pool."},
  {"pool_allocated_bytes", (PyCFunction)sharedstructures_PrefixTree_pool_allocated_bytes, METH_NOARGS,
      "Returns the amount of allocated space (without overhead) in the underlying shared memory pool."},
  {"incr", (PyCFunction)sharedstructures_PrefixTree_incr, METH_VARARGS,
      "Atomically increments an int or float value."},
  {"check_and_set", (PyCFunction)sharedstructures_PrefixTree_check_and_set, METH_VARARGS,
      "Conditionally sets a value if the check key\'s value matches the check value."},
  {"check_missing_and_set", (PyCFunction)sharedstructures_PrefixTree_check_missing_and_set, METH_VARARGS,
      "Conditionally sets a value if the check key doesn\'t exist."},
  {"clear", (PyCFunction)sharedstructures_PrefixTree_clear, METH_NOARGS,
      "Deletes all entries in the table."},
  {"iterkeys", (PyCFunction)sharedstructures_PrefixTree_iterkeys, METH_NOARGS,
      "Returns an iterator over all keys in the table."},
  {"itervalues", (PyCFunction)sharedstructures_PrefixTree_itervalues, METH_NOARGS,
      "Returns an iterator over all values in the table."},
  {"iteritems", (PyCFunction)sharedstructures_PrefixTree_iteritems, METH_NOARGS,
      "Returns an iterator over all key/value pairs in the table."},
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
   PyObject_HEAD_INIT(NULL)
   0,                                               // ob_size
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
   "TODO: write dox",                               // tp_doc
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

#ifndef PyMODINIT_FUNC
#define PyMODINIT_FUNC void
#endif

PyMODINIT_FUNC initsharedstructures() {
  if (PyType_Ready(&sharedstructures_HashTableType) < 0) {
    return;
  }
  if (PyType_Ready(&sharedstructures_PrefixTreeType) < 0) {
    return;
  }
  if (PyType_Ready(&sharedstructures_PrefixTreeIteratorType) < 0) {
    return;
  }

  PyObject* m = Py_InitModule3("sharedstructures", sharedstructures_methods,
      "TODO: write dox");

  Py_INCREF(&sharedstructures_HashTableType);
  PyModule_AddObject(m, "HashTable", (PyObject*)&sharedstructures_HashTableType);
  Py_INCREF(&sharedstructures_PrefixTreeType);
  PyModule_AddObject(m, "PrefixTree", (PyObject*)&sharedstructures_PrefixTreeType);
  Py_INCREF(&sharedstructures_PrefixTreeIteratorType);
  PyModule_AddObject(m, "PrefixTreeIterator", (PyObject*)&sharedstructures_PrefixTreeIteratorType);
}
