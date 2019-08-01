from __future__ import unicode_literals

import os
import subprocess
import sys
import time

import sharedstructures

POOL_NAME_PREFIX = "PrefixTreeTest-py-pool-"
ALLOCATOR_TYPES = ('simple', 'logarithmic')


def get_current_process_lsof():
  return subprocess.check_output(['lsof', '-p', str(os.getpid())])


def expect_key_missing(table, k):
  try:
    table[k]
    assert False, 'table[%r] didn\'t raise' % k
  except KeyError:
    pass


def verify_state(expected, table):
  assert len(expected) == len(table)
  for k, v in expected.items():
    assert table[k] == v, "%r (table) != %r (expected)" % (table[k], v)
  for k, v in table.items():
    assert expected[k] == v, "%r (expected) != %r (table)" % (expected[k], v)
  verify_allocator(table)


def verify_allocator(table):
  ret = table.verify()
  assert ret is None, ret.decode('utf-8')


def run_basic_test(allocator_type):
  print('-- [%s] basic' % allocator_type)
  before_lsof_count = len(get_current_process_lsof().splitlines())

  table = sharedstructures.PrefixTree(POOL_NAME_PREFIX + allocator_type, allocator_type)
  expected = {}

  def insert_both(e, t, k, v):
    t[k] = v
    e[k] = v

  def delete_both(e, t, k):
    del t[k]
    del e[k]

  verify_state(expected, table)
  insert_both(expected, table, b'key1', b'value1')
  verify_state(expected, table)
  insert_both(expected, table, b'key2', b'value2')
  verify_state(expected, table)
  insert_both(expected, table, b'key3', b'value3')
  verify_state(expected, table)

  assert [b'key2', b'key3'] == list(table.keys_from(b'key2'))
  assert [(b'key2', b'value2'), (b'key3', b'value3')] == list(table.items_from(b'key2'))

  assert [b'key3'] == list(table.keys_from(b'key3'))
  assert [(b'key3', b'value3')] == list(table.items_from(b'key3'))

  assert [] == list(table.keys_from(b'key4'))
  assert [] == list(table.items_from(b'key4'))

  delete_both(expected, table, b'key2')
  verify_state(expected, table)
  try:
    del table[b'key2']
    assert False, "del table[\'key2\'] did not raise KeyError"
  except KeyError:
    pass
  verify_state(expected, table)

  insert_both(expected, table, b'key1', b'value0')
  verify_state(expected, table)
  delete_both(expected, table, b'key1')
  verify_state(expected, table)
  delete_both(expected, table, b'key3')
  verify_state(expected, table)

  assert {} == expected

  del table  # this should unmap the shared memory pool and close the fd
  sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)

  # make sure we didn't leak an fd
  assert before_lsof_count == len(get_current_process_lsof().splitlines())


def run_conditional_writes_test(allocator_type):
  print("-- [%s] conditional writes" % allocator_type)

  table = sharedstructures.PrefixTree(POOL_NAME_PREFIX + allocator_type, allocator_type)
  expected = {}

  def insert_both(e, t, k, v):
    t[k] = v
    e[k] = v

  def delete_both(e, t, k):
    del t[k]
    del e[k]

  def conditional_insert_both(e, t, check_k, check_v, target_k, target_v,
      written):
    if t.check_and_set(check_k, check_v, target_k, target_v):
      e[target_k] = target_v
      assert written
    else:
      assert not written

  def conditional_missing_insert_both(e, t, check_k, target_k, target_v,
      written):
    if t.check_missing_and_set(check_k, target_k, target_v):
      e[target_k] = target_v
      assert written
    else:
      assert not written

  def conditional_delete_both(e, t, check_k, check_v, target_k, written):
    if t.check_and_set(check_k, check_v, target_k):
      del e[target_k]
      assert written
    else:
      assert not written

  def conditional_missing_delete_both(e, t, check_k, target_k, written):
    if t.check_missing_and_set(check_k, target_k):
      del e[target_k]
      assert written
    else:
      assert not written

  verify_state(expected, table)

  insert_both(expected, table, b"key1", b"value1")
  verify_state(expected, table)
  insert_both(expected, table, b"key2", 10.0)
  verify_state(expected, table)
  insert_both(expected, table, b"key3", True)
  verify_state(expected, table)

  # check that conditions on the same key work for various types
  conditional_insert_both(expected, table, b"key1", b"value2", b"key1",
      b"value1_1", False)
  verify_state(expected, table)
  conditional_insert_both(expected, table, b"key1", b"value1", b"key1",
      b"value1_1", True)
  verify_state(expected, table)

  conditional_insert_both(expected, table, b"key2", 8.0, b"key2", 15.0, False)
  verify_state(expected, table)
  conditional_insert_both(expected, table, b"key2", 10.0, b"key2", 15.0, True)
  verify_state(expected, table)

  conditional_insert_both(expected, table, b"key3", False, b"key3", False, False)
  verify_state(expected, table)
  conditional_insert_both(expected, table, b"key3", True, b"key3", False, True)
  verify_state(expected, table)

  # now:
  # key1 = "value1_1"
  # key2 = 15.0
  # key3 = False

  # check that conditions on other keys work
  conditional_insert_both(expected, table, b"key3", True, b"key1", b"value1",
      False)
  verify_state(expected, table)
  conditional_insert_both(expected, table, b"key3", False, b"key1", b"value1",
      True)
  verify_state(expected, table)

  conditional_insert_both(expected, table, b"key1", b"value2", b"key2", 10.0,
      False)
  verify_state(expected, table)
  conditional_insert_both(expected, table, b"key1", b"value1", b"key2", 10.0,
      True)
  verify_state(expected, table)

  conditional_insert_both(expected, table, b"key2", 20.0, b"key3", True, False)
  verify_state(expected, table)
  conditional_insert_both(expected, table, b"key2", 10.0, b"key3", True, True)
  verify_state(expected, table)

  # now:
  # key1 = "value1"
  # key2 = 10.0
  # key3 = True

  # check that Missing conditions work
  conditional_insert_both(expected, table, b"key4", None, b"key4", None, False)
  verify_state(expected, table)
  conditional_missing_insert_both(expected, table, b"key2", b"key4", None, False)
  verify_state(expected, table)
  conditional_missing_insert_both(expected, table, b"key4", b"key4", None, True)
  verify_state(expected, table)

  # now:
  # key1 = "value1"
  # key2 = 10.0
  # key3 = True
  # key4 = None

  # check that conditional deletes work
  conditional_delete_both(expected, table, b"key1", b"value2", b"key1", False)
  verify_state(expected, table)
  conditional_delete_both(expected, table, b"key1", b"value1", b"key1", True)
  verify_state(expected, table)

  conditional_delete_both(expected, table, b"key2", 20.0, b"key2", False)
  verify_state(expected, table)
  conditional_delete_both(expected, table, b"key2", 10.0, b"key2", True)
  verify_state(expected, table)

  conditional_delete_both(expected, table, b"key3", False, b"key3", False)
  verify_state(expected, table)
  conditional_delete_both(expected, table, b"key3", True, b"key3", True)
  verify_state(expected, table)

  conditional_missing_delete_both(expected, table, b"key4", b"key4", False)
  verify_state(expected, table)
  conditional_delete_both(expected, table, b"key4", None, b"key4", True)
  verify_state(expected, table)
  conditional_missing_delete_both(expected, table, b"key4", b"key4", False)
  verify_state(expected, table)

  assert expected == {}


def run_reorganization_test(allocator_type):
  print('-- [%s] reorganization' % allocator_type)
  table = sharedstructures.PrefixTree(POOL_NAME_PREFIX + allocator_type, allocator_type)
  expected = {}

  def insert_both(k):
    table[k] = k
    expected[k] = k

  def delete_both(k):
    del table[k]
    del expected[k]

  verify_state(expected, table)

  insert_both(b'abc')
  verify_state(expected, table)

  insert_both(b'ab')
  verify_state(expected, table)

  delete_both(b'abc')
  verify_state(expected, table)

  insert_both(b'')
  verify_state(expected, table)

  insert_both(b'abcd')
  verify_state(expected, table)

  delete_both(b'ab')
  verify_state(expected, table)

  insert_both(b'abcde')
  verify_state(expected, table)

  insert_both(b'abcdf')
  verify_state(expected, table)

  insert_both(b'abce')
  verify_state(expected, table)

  insert_both(b'abcef')
  verify_state(expected, table)

  table.clear()
  expected.clear()
  verify_state(expected, table)


def run_types_test(allocator_type):
  print('-- [%s] types' % allocator_type)
  table = sharedstructures.PrefixTree(POOL_NAME_PREFIX + allocator_type, allocator_type)
  expected = {}

  def insert_both(k, v):
    table[k] = v
    expected[k] = v

  def delete_both(k):
    del table[k]
    del expected[k]

  verify_state(expected, table)
  insert_both(b'key-string', b'value-string')
  verify_state(expected, table)
  insert_both(b'key-string-unicode', u'value-string-unicode')
  verify_state(expected, table)
  insert_both(b'key-int', 1024 * 1024 * -3)
  verify_state(expected, table)
  insert_both(b'key-int-long', 0x5555555555555555)
  verify_state(expected, table)
  insert_both(b'key-double', 2.38)
  verify_state(expected, table)
  insert_both(b'key-true', True)
  verify_state(expected, table)
  insert_both(b'key-false', False)
  verify_state(expected, table)
  insert_both(b'key-null', None)
  verify_state(expected, table)

  expect_key_missing(table, b'key-missing')

  # this calls exists() internally
  assert b'key-string' in table
  assert b'key-string-unicode' in table
  assert b'key-int' in table
  assert b'key-int-long' in table
  assert b'key-double' in table
  assert b'key-true' in table
  assert b'key-false' in table
  assert b'key-null' in table

  table.clear()
  expected.clear()
  verify_state(expected, table)


def run_complex_types_test(allocator_type):
  print('-- [%s] complex types' % allocator_type)
  table = sharedstructures.PrefixTree(POOL_NAME_PREFIX + allocator_type, allocator_type)
  expected = {}

  def insert_both(k, v):
    table[k] = v
    expected[k] = v

  def delete_both(k):
    del table[k]
    del expected[k]

  verify_state(expected, table)
  insert_both(b'key-list-empty', [])
  verify_state(expected, table)
  insert_both(b'key-list-ints', [1, 2, 3, 7])
  verify_state(expected, table)
  insert_both(b'key-tuple-empty', ())
  verify_state(expected, table)
  insert_both(b'key-tuple-ints', (1, 2, 3, 7))
  verify_state(expected, table)
  insert_both(b'key-set-empty', set())
  verify_state(expected, table)
  insert_both(b'key-set-ints', {1, 2, 3, 7})
  verify_state(expected, table)
  insert_both(b'key-dict-empty', {})
  verify_state(expected, table)
  insert_both(b'key-dict-ints', {1: 2, 3: 7})
  verify_state(expected, table)

  table.clear()
  expected.clear()
  verify_state(expected, table)


def run_incr_test(allocator_type):
  print('-- [%s] incr' % allocator_type)
  table = sharedstructures.PrefixTree(POOL_NAME_PREFIX + allocator_type, allocator_type)
  expected = {}

  def insert_both(k, v):
    table[k] = v
    expected[k] = v

  def delete_both(k):
    del table[k]
    del expected[k]

  assert 0 == len(table)
  table[b'key-int'] = 10
  table[b'key-int-long'] = 0x3333333333333333
  table[b'key-double'] = 1.0
  assert 3 == len(table)

  # giving garbage to incr() should cause a TypeError
  try:
    table.incr(b'key-missing', b'not a number, lolz')
    assert False
  except TypeError:
    pass
  try:
    table.incr(b'key-missing', {'still': 'not', 'a': 'number'})
    assert False
  except TypeError:
    pass

  # incr should create the key if it doesn't exist
  assert 100 == table.incr(b'key-int2', 100)
  assert 0x5555555555555555 == table.incr(b'key-int-long2', 0x5555555555555555)
  assert 10.0 == table.incr(b'key-double2', 10.0)
  assert 100 == table[b'key-int2']
  assert 0x5555555555555555 == table[b'key-int-long2']
  assert 10.0 == table[b'key-double2']
  assert 6 == len(table)

  # incr should return the new value of the key
  assert 99 == table.incr(b'key-int2', -1)
  assert 0.0 == table.incr(b'key-double2', -10.0)
  assert 99 == table[b'key-int2']
  assert 0.0 == table[b'key-double2']
  assert 6 == len(table)

  # test incr() on keys of the wrong type
  table[b'key-null'] = None
  table[b'key-string'] = b'value-string'
  assert 8 == len(table)
  try:
    table.incr(b'key-null', 13.0)
    assert False
  except ValueError:
    pass
  assert None == table[b'key-null']
  try:
    table.incr(b'key-null', 13)
    assert False
  except ValueError:
    pass
  assert None == table[b'key-null']
  try:
    table.incr(b'key-string', 13.0)
    assert False
  except ValueError:
    pass
  assert b'value-string' == table[b'key-string']
  try:
    table.incr(b'key-string', 13)
    assert False
  except ValueError:
    pass
  assert b'value-string' == table[b'key-string']
  try:
    table.incr(b'key-int', 13.0)
    assert False
  except ValueError:
    pass
  assert 10 == table[b'key-int']
  try:
    table.incr(b'key-int-long', 13.0)
    assert False
  except ValueError:
    pass
  assert 0x3333333333333333 == table[b'key-int-long']
  try:
    table.incr(b'key-int-long2', 13.0)
    assert False
  except ValueError:
    pass
  assert 0x5555555555555555 == table[b'key-int-long2']
  try:
    table.incr(b'key-double', 13)
    assert False
  except ValueError:
    pass
  assert 1.0 == table[b'key-double']

  # test converting integers between Int and Number
  assert 0x2AAAAAAAAAAAAAAA == table.incr(b'key-int', 0x2AAAAAAAAAAAAAA0)
  assert 8 == len(table)
  assert 3 == table.incr(b'key-int-long', -0x3333333333333330)
  assert 8 == len(table)

  # we're done here
  table.clear()
  assert len(table) == 0


def run_concurrent_readers_test(allocator_type):
  print('-- [%s] concurrent readers' % allocator_type)

  table = sharedstructures.PrefixTree(POOL_NAME_PREFIX + allocator_type, allocator_type)
  del table

  child_pids = set()
  while (len(child_pids) < 8) and (0 not in child_pids):
    child_pids.add(os.fork())

  if 0 in child_pids:
    # child process: try up to a second to get the key
    table = sharedstructures.PrefixTree(POOL_NAME_PREFIX + allocator_type, allocator_type)

    value = 100
    start_time = int(time.time() * 1000000)
    while (value < 110) and \
        (int(time.time() * 1000000) < (start_time + 1000000)):
      time.sleep(0.001)
      try:
        res = table[b'key1']
      except KeyError:
        pass
      else:
        if res == value:
          value += 1

    os._exit(int(value != 110))

  else:
    # parent process: write the key, then wait for children to terminate
    table = sharedstructures.PrefixTree(POOL_NAME_PREFIX + allocator_type, allocator_type)

    for value in range(100, 110):
      time.sleep(0.05)
      table[b'key1'] = value

    num_failures = 0
    while child_pids:
      pid, exit_status = os.wait()
      child_pids.remove(pid)
      if os.WIFEXITED(exit_status) and (os.WEXITSTATUS(exit_status) == 0):
        print('-- [%s]   child %d terminated successfully' % (
            allocator_type, pid))
      else:
        print('-- [%s]   child %d failed (%d)' % (
            allocator_type, pid, exit_status))
        num_failures += 1

    assert 0 == len(child_pids)
    assert 0 == num_failures


def main():
  try:
    for allocator_type in ALLOCATOR_TYPES:
      sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)
      run_basic_test(allocator_type)
      run_conditional_writes_test(allocator_type)
      run_reorganization_test(allocator_type)
      run_types_test(allocator_type)
      run_complex_types_test(allocator_type)
      run_incr_test(allocator_type)
      run_concurrent_readers_test(allocator_type)
    print('all tests passed')
    return 0

  finally:
    for allocator_type in ALLOCATOR_TYPES:
      sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)


if __name__ == '__main__':
  sys.exit(main())
