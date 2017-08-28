#define __STDC_FORMAT_MACROS
#include <errno.h>
#include <inttypes.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <phosg/Strings.hh>
#include <phosg/Time.hh>
#include <phosg/UnitTest.hh>
#include <string>

#include "Pool.hh"
#include "SimpleAllocator.hh"
#include "LogarithmicAllocator.hh"
#include "PrefixTree.hh"

using namespace std;

using namespace sharedstructures;
using LookupResult = PrefixTree::LookupResult;


shared_ptr<Allocator> create_allocator(shared_ptr<Pool> pool,
    const string& allocator_type) {
  if (allocator_type == "simple") {
    return shared_ptr<Allocator>(new SimpleAllocator(pool));
  }
  if (allocator_type == "logarithmic") {
    return shared_ptr<Allocator>(new LogarithmicAllocator(pool));
  }
  throw invalid_argument("unknown allocator type: " + allocator_type);
}


shared_ptr<PrefixTree> get_or_create_tree(const string& name,
    const string& allocator_type) {
  shared_ptr<Pool> pool(new Pool(name));
  shared_ptr<Allocator> alloc = create_allocator(pool, allocator_type);
  return shared_ptr<PrefixTree>(new PrefixTree(alloc, 0));
}


void expect_key_missing(const shared_ptr<PrefixTree> table, const void* k,
    size_t s) {
  try {
    table->at(k, s);
    expect(false);
  } catch (const out_of_range& e) { }
}


void verify_structure(const shared_ptr<PrefixTree> table,
    const char* expected_structure) {
  // remove whitespace from expected_structure
  string processed_expected_structure;
  for (const char* ch = expected_structure; *ch; ch++) {
    if (!isblank(*ch)) {
      processed_expected_structure += *ch;
    }
  }

  string actual_structure = table->get_structure();
  if (processed_expected_structure != actual_structure) {
    fprintf(stderr, "structures don\'t match\n  expected (orig): %s\n  expected: %s\n  actual  : %s\n",
        expected_structure, processed_expected_structure.c_str(),
        actual_structure.c_str());
  }
  expect_eq(processed_expected_structure, actual_structure);
}

void verify_state(
    const unordered_map<string, LookupResult>& expected,
    const shared_ptr<PrefixTree> table,
    size_t expected_node_size,
    const char* expected_structure = NULL) {
  expect_eq(expected.size(), table->size());
  expect_eq(expected_node_size, table->node_size());
  for (const auto& it : expected) {
    expect_eq(it.second, table->at(it.first.data(), it.first.size()));
  }

  auto missing_elements = expected;
  for (const auto& it : *table) {
    auto missing_it = missing_elements.find(it.first);
    expect_ne(missing_it, missing_elements.end());
    expect_eq(missing_it->second, it.second);
    missing_elements.erase(missing_it);
  }
  expect_eq(true, missing_elements.empty());

  if (expected_structure) {
    verify_structure(table, expected_structure);
  }
}


void run_basic_test(const string& allocator_type) {
  printf("-- [%s] basic\n", allocator_type.c_str());

  auto table = get_or_create_tree("test-table", allocator_type);

  size_t initial_pool_allocated = table->get_allocator()->bytes_allocated();
  expect_eq(0, table->size());

  expect_eq(true, table->insert("key1", 4, "value1", 6));
  expect_eq(1, table->size());
  expect_eq(4, table->node_size());
  expect_eq(true, table->insert("key2", 4, "value222", 8));
  expect_eq(2, table->size());
  expect_eq(4, table->node_size());
  expect_eq(true, table->insert("key3", 4, "value3", 6));
  expect_eq(3, table->size());
  expect_eq(4, table->node_size());

  expect_eq(3, table->nodes_for_prefix("k", 1));
  expect_eq(4, table->nodes_for_prefix("", 0));
  expect_eq(104, table->bytes_for_prefix("k", 1));
  expect_eq(2160, table->bytes_for_prefix("", 0)); // the root node has 00-FF

  LookupResult r;
  r.type = PrefixTree::ResultValueType::String;
  r.as_string = "value1";
  expect_eq(r, table->at("key1", 4));
  r.as_string = "value222";
  expect_eq(r, table->at("key2", 4));
  r.as_string = "value3";
  expect_eq(r, table->at("key3", 4));
  expect_eq(3, table->size());
  expect_eq(4, table->node_size());

  expect_eq(true, table->erase("key2", 4));
  expect_eq(2, table->size());
  expect_eq(4, table->node_size());
  expect_eq(false, table->erase("key2", 4));
  expect_eq(2, table->size());
  expect_eq(4, table->node_size());

  r.as_string = "value1";
  expect_eq(r, table->at("key1", 4));
  expect_key_missing(table, "key2", 4);
  r.as_string = "value3";
  expect_eq(r, table->at("key3", 4));
  expect_eq(2, table->size());
  expect_eq(4, table->node_size());

  expect_eq(true, table->insert("key1", 4, "value0", 6));
  expect_eq(2, table->size());
  expect_eq(4, table->node_size());

  r.as_string = "value0";
  expect_eq(r, table->at("key1", 4));
  expect_key_missing(table, "key2", 4);
  r.as_string = "value3";
  expect_eq(r, table->at("key3", 4));
  expect_eq(2, table->size());
  expect_eq(4, table->node_size());

  expect_eq(true, table->erase("key1", 4));
  expect_eq(1, table->size());
  expect_eq(4, table->node_size());
  expect_eq(true, table->erase("key3", 4));
  expect_eq(0, table->size());
  expect_eq(1, table->node_size());

  // the empty table should not leak any allocated memory
  expect_eq(initial_pool_allocated, table->get_allocator()->bytes_allocated());
}

static bool insert_vector(shared_ptr<PrefixTree> table, const string& key,
    const vector<string> value_parts) {
  vector<struct iovec> iov;
  for (const auto& part : value_parts) {
    iov.emplace_back();
    auto& this_iov = iov.back();
    this_iov.iov_base = const_cast<char*>(part.data());
    this_iov.iov_len = part.size();
  }
  return table->insert(key, iov.data(), iov.size());
}

void run_iovec_insert_test(const string& allocator_type) {
  printf("-- [%s] vector inserts\n", allocator_type.c_str());

  auto table = get_or_create_tree("test-table", allocator_type);

  size_t initial_pool_allocated = table->get_allocator()->bytes_allocated();
  expect_eq(0, table->size());

  expect_eq(true, insert_vector(table, "key1", {}));
  expect_eq(1, table->size());
  expect_eq(true, insert_vector(table, "key2", {""}));
  expect_eq(2, table->size());
  expect_eq(true, insert_vector(table, "key3", {"", "", ""}));
  expect_eq(3, table->size());
  expect_eq(true, insert_vector(table, "key4", {"value1"}));
  expect_eq(4, table->size());
  expect_eq(true, insert_vector(table, "key5", {"val", "ue1"}));
  expect_eq(5, table->size());
  expect_eq(true, insert_vector(table, "key6", {"value123456"}));
  expect_eq(6, table->size());
  expect_eq(true, insert_vector(table, "key7", {"val", "u", "e12345", "6"}));
  expect_eq(7, table->size());

  expect_eq(LookupResult(""), table->at("key1"));
  expect_eq(LookupResult(""), table->at("key2"));
  expect_eq(LookupResult(""), table->at("key3"));
  expect_eq(LookupResult("value1"), table->at("key4"));
  expect_eq(LookupResult("value1"), table->at("key5"));
  expect_eq(LookupResult("value123456"), table->at("key6"));
  expect_eq(LookupResult("value123456"), table->at("key7"));

  table->clear();

  // the empty table should not leak any allocated memory
  expect_eq(initial_pool_allocated, table->get_allocator()->bytes_allocated());
}

void run_conditional_writes_test(const string& allocator_type) {
  printf("-- [%s] conditional writes\n", allocator_type.c_str());

  auto table = get_or_create_tree("test-table", allocator_type);

  size_t initial_pool_allocated = table->get_allocator()->bytes_allocated();
  expect_eq(0, table->size());

  expect_eq(true, table->insert("key1", 4, "value1", 6));
  expect_eq(true, table->insert("key2", 4, 10.0));
  expect_eq(true, table->insert("key3", 4, true));

  // check that conditions on the same key work for various types
  {
    PrefixTree::CheckRequest check("key1", 4, "value2", 6);
    expect_eq(false, table->insert("key1", 4, "value1_1", 8, &check));
    expect_eq(LookupResult("value1", 6), table->at("key1", 4));

    check.value.as_string = "value1";
    expect_eq(true, table->insert("key1", 4, "value1_1", 8, &check));
    expect_eq(LookupResult("value1_1", 8), table->at("key1", 4));
  }
  {
    PrefixTree::CheckRequest check("key2", 4, 8.0);
    expect_eq(false, table->insert("key2", 4, 15.0, &check));
    expect_eq(LookupResult(10.0), table->at("key2", 4));

    check.value.as_double = 10.0;
    expect_eq(true, table->insert("key2", 4, 15.0, &check));
    expect_eq(LookupResult(15.0), table->at("key2", 4));
  }
  {
    PrefixTree::CheckRequest check("key3", 4, false);
    expect_eq(false, table->insert("key3", 4, false, &check));
    expect_eq(LookupResult(true), table->at("key3", 4));

    check.value.as_bool = true;
    expect_eq(true, table->insert("key3", 4, false, &check));
    expect_eq(LookupResult(false), table->at("key3", 4));
  }

  // now:
  // key1 = "value1_1"
  // key2 = 15.0
  // key3 = false

  // check that conditions on other keys work
  {
    PrefixTree::CheckRequest check("key3", 4, true);
    expect_eq(false, table->insert("key1", 4, "value1", 6, &check));
    expect_eq(LookupResult("value1_1", 8), table->at("key1", 4));

    check.value.as_bool = false;
    expect_eq(true, table->insert("key1", 4, "value1", 6, &check));
    expect_eq(LookupResult("value1", 6), table->at("key1", 4));
  }
  {
    PrefixTree::CheckRequest check("key1", 4, "value2", 6);
    expect_eq(false, table->insert("key2", 4, 10.0, &check));
    expect_eq(LookupResult(15.0), table->at("key2", 4));

    check.value.as_string = "value1";
    expect_eq(true, table->insert("key2", 4, 10.0, &check));
    expect_eq(LookupResult(10.0), table->at("key2", 4));
  }
  {
    PrefixTree::CheckRequest check("key2", 4, 20.0);
    expect_eq(false, table->insert("key3", 4, true, &check));
    expect_eq(LookupResult(false), table->at("key3", 4));

    check.value.as_double = 10.0;
    expect_eq(true, table->insert("key3", 4, true, &check));
    expect_eq(LookupResult(true), table->at("key3", 4));
  }

  // now:
  // key1 = "value1"
  // key2 = 10.0
  // key3 = true

  // check that Missing conditions work
  {
    PrefixTree::CheckRequest check("key4", 4);
    expect_eq(false, table->insert("key4", 4, &check));
    expect_eq(false, table->exists("key4", 4));
  }
  {
    PrefixTree::CheckRequest check("key2", 4,
        PrefixTree::ResultValueType::Missing);
    expect_eq(false, table->insert("key2", 4, &check));
    expect_eq(false, table->exists("key4", 4));
  }
  {
    PrefixTree::CheckRequest check("key4", 4,
        PrefixTree::ResultValueType::Missing);
    expect_eq(true, table->insert("key4", 4, &check));
    expect_eq(LookupResult(), table->at("key4", 4));
  }

  // now:
  // key1 = "value1"
  // key2 = 10.0
  // key3 = true
  // key4 = Null

  // check that conditional deletes work
  {
    PrefixTree::CheckRequest check("key1", 4, "value2", 6);
    expect_eq(false, table->erase("key1", 4, &check));
    expect_eq(LookupResult("value1", 6), table->at("key1", 4));

    check.value.as_string = "value1";
    expect_eq(true, table->erase("key1", 4, &check));
    expect_eq(false, table->exists("key1", 4));
  }
  {
    PrefixTree::CheckRequest check("key2", 4, 20.0);
    expect_eq(false, table->erase("key2", 4, &check));
    expect_eq(LookupResult(10.0), table->at("key2", 4));

    check.value.as_double = 10.0;
    expect_eq(true, table->erase("key2", 4, &check));
    expect_eq(false, table->exists("key2", 4));
  }
  {
    PrefixTree::CheckRequest check("key3", 4, false);
    expect_eq(false, table->erase("key3", 4, &check));
    expect_eq(LookupResult(true), table->at("key3", 4));

    check.value.as_bool = true;
    expect_eq(true, table->erase("key3", 4, &check));
    expect_eq(false, table->exists("key3", 4));
  }
  {
    // it doesn't make sense to do a Missing check on the same key for an erase,
    // but w/e - it's convenient for the test and it should work anyway
    PrefixTree::CheckRequest check("key4", 4,
        PrefixTree::ResultValueType::Missing);
    expect_eq(false, table->erase("key4", 4, &check));
    expect_eq(LookupResult(), table->at("key4", 4));
  }
  {
    PrefixTree::CheckRequest check("key4", 4);
    expect_eq(true, table->erase("key4", 4, &check));
    expect_eq(false, table->exists("key4", 4));
    expect_eq(false, table->erase("key4", 4, &check));
    expect_eq(false, table->exists("key4", 4));
  }

  // the empty table should not leak any allocated memory
  expect_eq(0, table->size());
  expect_eq(initial_pool_allocated, table->get_allocator()->bytes_allocated());
}

void run_reorganization_test(const string& allocator_type) {
  printf("-- [%s] reorganization\n", allocator_type.c_str());

  auto table = get_or_create_tree("test-table", allocator_type);

  size_t initial_pool_allocated = table->get_allocator()->bytes_allocated();

  // initial state: empty
  unordered_map<string, LookupResult> expected_state;
  verify_state(expected_state, table, 1, "([00,FF]@00+#)");

  // <> null
  //   a null
  //     b null
  //       (c) "abc"
  expect_eq(true, table->insert("abc", 3, "abc", 3));
  expected_state.emplace("abc", "abc");
  verify_state(expected_state, table, 3,
      "([00,FF]@00+#,"
      "61:("
      "  [62,62]@61+#,"
      "  62:("
      "    [63,63]@62+#,"
      "    63:s\"abc\")))");

  // <> null
  //   a null
  //     b "ab"
  //       (c) "abc"
  expect_eq(true, table->insert("ab", 2, "ab", 2));
  expected_state.emplace("ab", "ab");
  verify_state(expected_state, table, 3,
      "([00,FF]@00+#,"
      "61:("
      "  [62,62]@61+#,"
      "  62:("
      "    [63,63]@62+s\"ab\","
      "    63:s\"abc\")))");

  // <> null
  //   a null
  //     (b) "ab"
  table->erase("abc", 3);
  expected_state.erase("abc");
  verify_state(expected_state, table, 2,
      "([00,FF]@00+#,"
      "61:("
      "  [62,62]@61+#,"
      "  62:s\"ab\"))");

  // <> ""
  //   a null
  //     (b) "ab"
  expect_eq(true, table->insert("", 0, "", 0));
  expected_state.emplace("", "");
  verify_state(expected_state, table, 2,
      "([00,FF]@00+S\"\","
      "61:("
      "  [62,62]@61+#,"
      "  62:s\"ab\"))");

  // <> ""
  //   a null
  //     b "ab"
  //       c null
  //         (d) "abcd"
  expect_eq(true, table->insert("abcd", 4, "abcd", 4));
  expected_state.emplace("abcd", "abcd");
  verify_state(expected_state, table, 4,
      "([00,FF]@00+S\"\","
      "61:("
      "  [62,62]@61+#,"
      "  62:("
      "    [63,63]@62+s\"ab\","
      "    63:("
      "      [64,64]@63+#,"
      "      64:s\"abcd\"))))");

  // <> ""
  //   a null
  //     b null
  //       c null
  //         (d) "abcd"
  table->erase("ab", 2);
  expected_state.erase("ab");
  verify_state(expected_state, table, 4,
      "([00,FF]@00+S\"\","
      "61:("
      "  [62,62]@61+#,"
      "  62:("
      "    [63,63]@62+#,"
      "    63:("
      "      [64,64]@63+#,"
      "      64:s\"abcd\"))))");

  // <> ""
  //   a null
  //     b null
  //       c null
  //         d "abcd"
  //           (e) "abcde"
  expect_eq(true, table->insert("abcde", 5, "abcde", 5));
  expected_state.emplace("abcde", "abcde");
  verify_state(expected_state, table, 5,
      "([00,FF]@00+S\"\","
      "61:("
      "  [62,62]@61+#,"
      "  62:("
      "    [63,63]@62+#,"
      "    63:("
      "      [64,64]@63+#,"
      "      64:("
      "        [65,65]@64+s\"abcd\","
      "        65:s\"abcde\")))))");

  // <> ""
  //   a null
  //     b null
  //       c null
  //         d "abcd"
  //           (e) "abcde"
  //           (f) "abcdf"
  expect_eq(true, table->insert("abcdf", 5, "abcdf", 5));
  expected_state.emplace("abcdf", "abcdf");
  verify_state(expected_state, table, 5,
      "([00,FF]@00+S\"\","
      "61:("
      "  [62,62]@61+#,"
      "  62:("
      "    [63,63]@62+#,"
      "    63:("
      "      [64,64]@63+#,"
      "      64:("
      "        [65,66]@64+s\"abcd\","
      "        65:s\"abcde\","
      "        66:s\"abcdf\")))))");

  // <> ""
  //   a null
  //     b null
  //       c null
  //         d "abcd"
  //           (e) "abcde"
  //           (f) "abcdf"
  //         (e) "abce"
  expect_eq(true, table->insert("abce", 4, "abce", 4));
  expected_state.emplace("abce", "abce");
  verify_state(expected_state, table, 5,
      "([00,FF]@00+S\"\","
      "61:("
      "  [62,62]@61+#,"
      "  62:("
      "    [63,63]@62+#,"
      "    63:("
      "      [64,65]@63+#,"
      "      64:("
      "        [65,66]@64+s\"abcd\","
      "        65:s\"abcde\","
      "        66:s\"abcdf\"),"
      "      65:s\"abce\"))))");

  // <> ""
  //   a null
  //     b null
  //       c null
  //         d "abcd"
  //           (e) "abcde"
  //           (f) "abcdf"
  //         e "abce"
  //           (f) "abcef"
  expect_eq(true, table->insert("abcef", 5, "abcef", 5));
  expected_state.emplace("abcef", "abcef");
  verify_state(expected_state, table, 6,
      "([00,FF]@00+S\"\","
      "61:("
      "  [62,62]@61+#,"
      "  62:("
      "    [63,63]@62+#,"
      "    63:("
      "      [64,65]@63+#,"
      "      64:("
      "        [65,66]@64+s\"abcd\","
      "        65:s\"abcde\","
      "        66:s\"abcdf\"),"
      "      65:("
      "        [66,66]@65+s\"abce\","
      "        66:s\"abcef\")))))");

  // <> null
  table->clear();
  expected_state.clear();
  verify_state(expected_state, table, 1, "([00,FF]@00+#)");

  // the empty table should not leak any allocated memory
  expect_eq(initial_pool_allocated, table->get_allocator()->bytes_allocated());
}

void run_types_test(const string& allocator_type) {
  printf("-- [%s] types\n", allocator_type.c_str());

  // uncomment this to easily see the difference between result types
  // TODO: make this a helper function or something
  //LookupResult l(2.38);
  //auto r = table->at("key-double", 10);
  //string ls = l.str(), rs = r.str();
  //fprintf(stderr, "%s vs %s\n", ls.c_str(), rs.c_str());

  auto table = get_or_create_tree("test-table", allocator_type);

  size_t initial_pool_allocated = table->get_allocator()->bytes_allocated();

  expect_eq(0, table->size());
  expect_eq(1, table->node_size());

  // write a bunch of keys of different types
  expect_eq(true, table->insert("key-string", 10, "value-string", 12));
  expect_eq(true, table->insert("key-string-short", 16, "short", 5));
  expect_eq(true, table->insert("key-string-empty", 16, "", 0));
  expect_eq(true, table->insert("key-int", 7, (int64_t)(1024 * 1024 * -3)));
  expect_eq(true, table->insert("key-int-long", 12,
      (int64_t)0x9999999999999999));
  expect_eq(true, table->insert("key-double", 10, 2.38));
  expect_eq(true, table->insert("key-true", 8, true));
  expect_eq(true, table->insert("key-false", 9, false));
  expect_eq(true, table->insert("key-null", 8));

  expect_eq(9, table->size());
  expect_eq(42, table->node_size());

  // get their values again
  try {
    table->at("key-missing", 11);
    expect(false);
  } catch (const out_of_range& e) { }
  expect_eq(LookupResult("value-string"), table->at("key-string", 10));
  expect_eq(LookupResult("short"), table->at("key-string-short", 16));
  expect_eq(LookupResult(""), table->at("key-string-empty", 16));
  expect_eq(LookupResult((int64_t)1024 * 1024 * -3), table->at("key-int", 7));
  expect_eq(LookupResult((int64_t)0x9999999999999999),
      table->at("key-int-long", 12));
  expect_eq(LookupResult(2.38), table->at("key-double", 10));
  expect_eq(LookupResult(true), table->at("key-true", 8));
  expect_eq(LookupResult(false), table->at("key-false", 9));
  expect_eq(LookupResult(), table->at("key-null", 8));

  // make sure type() returns the same types as at()
  // note: type() doesn't throw for missing keys, but at() does
  expect_eq(PrefixTree::ResultValueType::Missing,
      table->type("key-missing", 11));
  expect_eq(PrefixTree::ResultValueType::String, table->type("key-string", 10));
  expect_eq(PrefixTree::ResultValueType::String,
      table->type("key-string-short", 16));
  expect_eq(PrefixTree::ResultValueType::String,
      table->type("key-string-empty", 16));
  expect_eq(PrefixTree::ResultValueType::Int, table->type("key-int", 7));
  expect_eq(PrefixTree::ResultValueType::Int, table->type("key-int-long", 12));
  expect_eq(PrefixTree::ResultValueType::Double,
      table->type("key-double", 10));
  expect_eq(PrefixTree::ResultValueType::Bool, table->type("key-true", 8));
  expect_eq(PrefixTree::ResultValueType::Bool, table->type("key-false", 9));
  expect_eq(PrefixTree::ResultValueType::Null, table->type("key-null", 8));

  // make sure exists() returns true for all the keys we expect
  expect_eq(false, table->exists("key-missing", 11));
  expect_eq(true, table->exists("key-string", 10));
  expect_eq(true, table->exists("key-string-short", 16));
  expect_eq(true, table->exists("key-string-empty", 16));
  expect_eq(true, table->exists("key-int", 7));
  expect_eq(true, table->exists("key-int-long", 12));
  expect_eq(true, table->exists("key-double", 10));
  expect_eq(true, table->exists("key-true", 8));
  expect_eq(true, table->exists("key-false", 9));
  expect_eq(true, table->exists("key-null", 8));

  // verify the tree's structure
  verify_structure(table,
      "([00,FF]@00+#,"
      "  6B:([65,65]@6B+#," // k
      "    65:([79,79]@65+#," // e
      "      79:([2D,2D]@79+#," // y
      "        2D:([64,74]@2D+#," // -
      "          64:([6F,6F]@64+#," // d
      "            6F:([75,75]@6F+#," // o
      "              75:([62,62]@75+#," // u
      "                62:([6C,6C]@62+#," // b
      "                  6C:([65,65]@6C+#," // l
      "                    65:D2.38)))))," // e (=2.38)
      "          66:([61,61]@66+#," // f
      "            61:([6C,6C]@61+#," // a
      "              6C:([73,73]@6C+#," // l
      "                73:([65,65]@73+#," // s
      "                  65:false))))," // e (=false)
      "          69:([6E,6E]@69+#," // i
      "            6E:([74,74]@6E+#," // n
      "              74:([2D,2D]@74+i-3145728," // t (=1024 * 1024 * -3)
      "                2D:([6C,6C]@2D+#," // -
      "                  6C:([6F,6F]@6C+#," // l
      "                    6F:([6E,6E]@6F+#," // o
      "                      6E:([67,67]@6E+#," // n
      "                        67:I-7378697629483820647)))))))," // g (=0x9999999999999999)
      "          6E:([75,75]@6E+#," // n
      "            75:([6C,6C]@75+#," // u
      "              6C:([6C,6C]@6C+#," // l
      "                6C:null)))," // l (=null)
      "          73:([74,74]@73+#," // s
      "            74:([72,72]@74+#," // t
      "              72:([69,69]@72+#," // r
      "                69:([6E,6E]@69+#," // i
      "                  6E:([67,67]@6E+#," // n
      "                    67:([2D,2D]@67+S\"value-string\"," // g (="value-string")
      "                      2D:([65,73]@2D+#," // -
      "                        65:([6D,6D]@65+#," // e
      "                          6D:([70,70]@6D+#," // m
      "                            70:([74,74]@70+#," // p
      "                              74:([79,79]@74+#," // t
      "                                79:S\"\"))))," // y (="")
      "                        73:([68,68]@73+#," // s
      "                          68:([6F,6F]@68+#," // h
      "                            6F:([72,72]@6F+#," // o
      "                              72:([74,74]@72+#," // r
      "                                74:s\"short\")))))))))))," // t (="short")
      "          74:([72,72]@74+#," // t
      "            72:([75,75]@72+#," // r
      "              75:([65,65]@75+#," // u
      "                65:true))))))))"); // e (=true)

  table->clear();
  expect_eq(0, table->size());
  expect_eq(1, table->node_size());

  // the empty table should not leak any allocated memory
  expect_eq(initial_pool_allocated, table->get_allocator()->bytes_allocated());
}

void run_incr_test(const string& allocator_type) {
  printf("-- [%s] incr\n", allocator_type.c_str());

  auto table = get_or_create_tree("test-table", allocator_type);

  size_t initial_pool_allocated = table->get_allocator()->bytes_allocated();

  expect_eq(0, table->size());
  expect_eq(true, table->insert("i", 1, (int64_t)10));
  expect_eq(true, table->insert("I", 1, (int64_t)0x3333333333333333));
  expect_eq(true, table->insert("d", 1, 1.0));
  expect_eq(3, table->size());
  verify_structure(table, "([00,FF]@00+#,49:I3689348814741910323,64:D1,69:i10)");

  // incr should create the key if it doesn't exist
  expect_eq(100, table->incr("i2", 2, (int64_t)100));
  expect_eq(0x5555555555555555, table->incr("I2", 2,
      (int64_t)0x5555555555555555));
  expect_eq(10.0, table->incr("d2", 2, 10.0));
  expect_eq(LookupResult((int64_t)100), table->at("i2", 2));
  expect_eq(LookupResult((int64_t)0x5555555555555555), table->at("I2", 2));
  expect_eq(LookupResult(10.0), table->at("d2", 2));
  expect_eq(6, table->size());
  verify_structure(table,
      "([00,FF]@00+#," // root
      "  49:([32,32]@49+I3689348814741910323," // I
      "    32:I6148914691236517205)," // I2
      "  64:([32,32]@64+D1," // d
      "    32:D10)," // d2
      "  69:([32,32]@69+i10," // i
      "    32:i100))"); // i2

  // incr should return the new value of the key
  expect_eq(99, table->incr("i2", 2, (int64_t)-1));
  expect_eq(0.0, table->incr("d2", 2, -10.0));
  expect_eq(LookupResult((int64_t)99), table->at("i2", 2));
  expect_eq(LookupResult(0.0), table->at("d2", 2));
  expect_eq(6, table->size());
  verify_structure(table,
      "([00,FF]@00+#," // root
      "  49:([32,32]@49+I3689348814741910323," // I
      "    32:I6148914691236517205)," // I2
      "  64:([32,32]@64+D1," // d
      "    32:d0)," // d2 (note this is d, not D)
      "  69:([32,32]@69+i10," // i
      "    32:i99))"); // i2

  expect_eq(3.0, table->incr("d2", 2, 3.0));
  expect_eq(LookupResult(3.0), table->at("d2", 2));
  expect_eq(6, table->size());
  verify_structure(table,
      "([00,FF]@00+#," // root
      "  49:([32,32]@49+I3689348814741910323," // I
      "    32:I6148914691236517205)," // I2
      "  64:([32,32]@64+D1," // d
      "    32:D3)," // d2
      "  69:([32,32]@69+i10," // i
      "    32:i99))"); // i2

  // test incr() on keys of the wrong type
  expect_eq(true, table->insert("n", 1));
  expect_eq(true, table->insert("s", 1, "value-string", 12));
  expect_eq(8, table->size());
  verify_structure(table,
      "([00,FF]@00+#," // root
      "  49:([32,32]@49+I3689348814741910323," // I
      "    32:I6148914691236517205)," // I2
      "  64:([32,32]@64+D1," // d
      "    32:D3)," // d2
      "  69:([32,32]@69+i10," // i
      "    32:i99)," // i2
      "  6E:null," // n
      "  73:S\"value-string\")"); // s

  try {
    table->incr("n", 1, 13.0);
    expect(false);
  } catch (const out_of_range& e) { }
  try {
    table->incr("n", 1, (int64_t)13);
    expect(false);
  } catch (const out_of_range& e) { }
  try {
    table->incr("s", 1, 13.0);
    expect(false);
  } catch (const out_of_range& e) { }
  try {
    table->incr("s", 1, (int64_t)13);
    expect(false);
  } catch (const out_of_range& e) { }
  try {
    table->incr("i", 1, 13.0);
    expect(false);
  } catch (const out_of_range& e) { }
  try {
    table->incr("I", 1, 13.0);
    expect(false);
  } catch (const out_of_range& e) { }
  try {
    table->incr("I2", 1, 13.0);
    expect(false);
  } catch (const out_of_range& e) { }
  try {
    table->incr("d", 1, (int64_t)13);
    expect(false);
  } catch (const out_of_range& e) { }
  try {
    table->incr("d2", 2, (int64_t)13);
    expect(false);
  } catch (const out_of_range& e) { }

  // the structure should not have changed at all
  verify_structure(table,
      "([00,FF]@00+#," // root
      "  49:([32,32]@49+I3689348814741910323," // I
      "    32:I6148914691236517205)," // I2
      "  64:([32,32]@64+D1," // d
      "    32:D3)," // d2
      "  69:([32,32]@69+i10," // i
      "    32:i99)," // i2
      "  6E:null," // n
      "  73:S\"value-string\")"); // s

  // test converting integers between Int and LongInt
  expect_eq(0xAAAAAAAAAAAAAAAA, table->incr("i", 1,
      (int64_t)0xAAAAAAAAAAAAAAA0));
  expect_eq(8, table->size());
  expect_eq(3, table->incr("I", 1, (int64_t)-0x3333333333333330));
  expect_eq(8, table->size());
  verify_structure(table,
      "([00,FF]@00+#," // root
      "  49:([32,32]@49+i3," // I
      "    32:I6148914691236517205)," // I2
      "  64:([32,32]@64+D1," // d
      "    32:D3)," // d2
      "  69:([32,32]@69+I-6148914691236517206," // i
      "    32:i99)," // i2
      "  6E:null," // n
      "  73:S\"value-string\")"); // s

  // we're done here
  table->clear();
  expect_eq(0, table->size());

  // the empty table should not leak any allocated memory
  expect_eq(initial_pool_allocated, table->get_allocator()->bytes_allocated());
}

void run_concurrent_readers_test(const string& allocator_type) {
  printf("-- [%s] concurrent readers\n", allocator_type.c_str());

  unordered_set<pid_t> child_pids;
  while ((child_pids.size() < 8) && !child_pids.count(0)) {
    pid_t pid = fork();
    if (pid == -1) {
      break;
    } else {
      child_pids.emplace(pid);
    }
  }

  if (child_pids.count(0)) {
    // child process: try up to 3 seconds to get the key
    auto table = get_or_create_tree("test-table", allocator_type);

    int64_t value = 100;
    uint64_t start_time = now();
    do {
      try {
        auto res = table->at("key1", 4);
        if (res == LookupResult((int64_t)value)) {
          value++;
        }
      } catch (const out_of_range& e) { }
      usleep(1); // yield to other processes
    } while ((value < 110) && (now() < (start_time + 1000000)));

    // we succeeded if we saw all the values from 100 to 110
    _exit(value != 110);

  } else {
    // parent process: write the key, then wait for children to terminate
    auto table = get_or_create_tree("test-table", allocator_type);

    for (int64_t value = 100; value < 110; value++) {
      usleep(50000);
      expect_eq(true, table->insert("key1", 4, (int64_t)value));
    }

    int num_failures = 0;
    int exit_status;
    pid_t exited_pid;
    while ((exited_pid = wait(&exit_status)) != -1) {
      child_pids.erase(exited_pid);
      if (WIFEXITED(exit_status) && (WEXITSTATUS(exit_status) == 0)) {
        printf("-- [%s]   child %d terminated successfully\n",
            allocator_type.c_str(), exited_pid);
      } else {
        printf("-- [%s]   child %d failed (%d)\n", allocator_type.c_str(),
            exited_pid, exit_status);
        num_failures++;
      }
    }

    expect_eq(true, child_pids.empty());
    expect_eq(0, num_failures);
  }
}


void run_concurrent_writers_test(const string& allocator_type) {
  printf("-- [%s] concurrent writers\n", allocator_type.c_str());

  unordered_set<pid_t> child_pids;
  while ((child_pids.size() < 10) && !child_pids.count(0)) {
    pid_t pid = fork();
    if (pid == -1) {
      break;
    } else {
      child_pids.emplace(pid);
    }
  }

  if (child_pids.count(0)) {
    // child process: try up to 3 seconds to get the key
    auto table = get_or_create_tree("test-table", allocator_type);

    uint64_t start_time = now();
    do {
      string key = string_printf("key%d", rand());
      string value = string_printf("value%d", rand());
      table->insert(key, value);
      usleep(1); // yield to other processes
    } while (now() < (start_time + 1000000));

    _exit(0);

  } else {
    // parent process: wait for children to terminate
    auto table = get_or_create_tree("test-table", allocator_type);

    int num_failures = 0;
    int exit_status;
    pid_t exited_pid;
    while ((exited_pid = wait(&exit_status)) != -1) {
      child_pids.erase(exited_pid);
      if (WIFEXITED(exit_status) && (WEXITSTATUS(exit_status) == 0)) {
        printf("-- [%s]   child %d terminated successfully\n",
            allocator_type.c_str(), exited_pid);
      } else {
        printf("-- [%s]   child %d failed (%d)\n", allocator_type.c_str(),
            exited_pid, exit_status);
        num_failures++;
      }
    }

    expect_eq(true, child_pids.empty());
    expect_eq(0, num_failures);

    table->get_allocator()->verify();
  }
}


int main(int argc, char* argv[]) {
  int retcode = 0;

  vector<string> allocator_types({"simple", "logarithmic"});
  try {
    for (const auto& allocator_type : allocator_types) {
      Pool::delete_pool("test-table");
      run_basic_test(allocator_type);
      run_iovec_insert_test(allocator_type);
      run_conditional_writes_test(allocator_type);
      run_reorganization_test(allocator_type);
      run_types_test(allocator_type);
      run_incr_test(allocator_type);
      run_concurrent_readers_test(allocator_type);
      run_concurrent_writers_test(allocator_type);
    }
    printf("all tests passed\n");

  } catch (const exception& e) {
    printf("failure: %s\n", e.what());
    retcode = 1;
  }
  Pool::delete_pool("test-table");

  return retcode;
}
