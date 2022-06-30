import os
import struct
import subprocess
import sys
import time

import sharedstructures

POOL_NAME_PREFIX = "HashTableTest-py-pool-"
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
    assert table[k] == v
  for k, v in table.items():
    assert expected[k] == v
  verify_allocator(table)


def verify_allocator(table):
  ret = table.verify()
  assert ret is None, ret.decode('utf-8')


def run_basic_test(allocator_type):
  print("-- [%s] basic" % allocator_type)
  before_lsof_count = len(get_current_process_lsof().splitlines())

  table = sharedstructures.HashTable(POOL_NAME_PREFIX + allocator_type, allocator_type)
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

  del table  # This should unmap the shared memory pool and close the fd
  sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)

  # This will fail if the test prints anything after before_lsof is taken since
  # the stdout/stderr offsets will be different
  assert before_lsof_count == len(get_current_process_lsof().splitlines())


def run_conditional_writes_test(allocator_type):
  print("-- [%s] conditional writes" % allocator_type)

  table = sharedstructures.HashTable(POOL_NAME_PREFIX + allocator_type, allocator_type)
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
  insert_both(expected, table, b"key2", b"value2")
  verify_state(expected, table)

  # Check that conditions on the same key work
  conditional_insert_both(expected, table, b"key1", b"value2", b"key1",
      b"value1_1", False)
  verify_state(expected, table)
  conditional_insert_both(expected, table, b"key1", b"value1", b"key1",
      b"value1_1", True)
  verify_state(expected, table)

  # Check that conditions on other keys work
  conditional_insert_both(expected, table, b"key2", b"value1", b"key1",
      b"value1", False)
  verify_state(expected, table)
  conditional_insert_both(expected, table, b"key2", b"value2", b"key1",
      b"value1", True)
  verify_state(expected, table)

  # Check that missing conditions work
  conditional_missing_insert_both(expected, table, b"key2", b"key3", b"value3",
      False)
  verify_state(expected, table)
  conditional_missing_insert_both(expected, table, b"key3", b"key3", b"value3",
      True)
  verify_state(expected, table)

  # Check that conditional deletes work
  conditional_delete_both(expected, table, b"key1", b"value2", b"key1", False)
  verify_state(expected, table)
  conditional_delete_both(expected, table, b"key1", b"value1", b"key1", True)
  verify_state(expected, table)

  conditional_missing_delete_both(expected, table, b"key3", b"key2", False)
  verify_state(expected, table)
  conditional_missing_delete_both(expected, table, b"key1", b"key2", True)
  verify_state(expected, table)

  delete_both(expected, table, b"key3")

  assert expected == {}


def run_collision_test(allocator_type):
  print("-- [%s] collision" % allocator_type)

  table = sharedstructures.HashTable(POOL_NAME_PREFIX + allocator_type, allocator_type, 0, 2)
  expected = {}

  def insert_both(e, t, k, v):
    t[k] = v
    e[k] = v

  def delete_both(e, t, k):
    del t[k]
    del e[k]

  # Writing 5 keys to a 4-slot hashtable forces a collision
  assert 2 == table.bits()
  assert 0 == len(table)

  insert_both(expected, table, b'key1', b'value1')
  insert_both(expected, table, b'key2', b'value2')
  insert_both(expected, table, b'key3', b'value3')
  insert_both(expected, table, b'key4', b'value4')
  insert_both(expected, table, b'key5', b'value5')
  verify_state(expected, table)

  while expected:
    k, _ = expected.popitem();
    del table[k]
    verify_state(expected, table)


def run_incr_test(allocator_type):
  print('-- [%s] incr' % allocator_type)
  table = sharedstructures.HashTable(POOL_NAME_PREFIX + allocator_type, allocator_type, 0, 6)
  expected = {}

  def insert_both(k, v):
    table[k] = v
    expected[k] = v

  def delete_both(k):
    del table[k]
    del expected[k]

  # Giving garbage to incr() should cause a TypeError
  try:
    table.incr(b'missing', b'not a number, lolz')
    assert False
  except TypeError:
    pass
  try:
    table.incr(b'missing', {'still': 'not', 'a': 'number'})
    assert False
  except TypeError:
    pass

  assert 0 == len(table)
  insert_both(b'int8', struct.pack(b'@b', 40))
  insert_both(b'int16', struct.pack(b'@h', 4000))
  insert_both(b'int32', struct.pack(b'@l', 60000000))
  insert_both(b'int64', struct.pack(b'@q', 800000000000000))
  insert_both(b'float', struct.pack(b'@f', 10.0))
  insert_both(b'double', struct.pack(b'@d', 15.5))
  insert_both(b'string', b'7 bytes')
  assert 7 == len(table)

  # incr should create the key if it doesn't exist
  assert -10 == table.incr(b"int8-2", -10)
  assert -4000 == table.incr(b"int16-2", -4000)
  assert -60000000 == table.incr(b"int32-2", -60000000)
  assert -800000000000000 == table.incr(b"int64-2", -800000000000000)
  assert -10.0 == table.incr(b"float-2", -10.0)
  assert -15.5 == table.incr(b"double-2", -15.5)
  assert 13 == len(table)

  # All the keys should have the values we set, but the keys created by incr
  # should all be 64 bits
  assert struct.pack(b'@b', 40) == table[b"int8"]
  assert struct.pack(b'@h', 4000) == table[b"int16"]
  assert struct.pack(b'@l', 60000000) == table[b"int32"]
  assert struct.pack(b'@q', 800000000000000) == table[b"int64"]
  assert struct.pack(b'@f', 10.0) == table[b"float"]
  assert struct.pack(b'@d', 15.5) == table[b"double"]
  assert struct.pack(b'@q', -10) == table[b"int8-2"]
  assert struct.pack(b'@q', -4000) == table[b"int16-2"]
  assert struct.pack(b'@q', -60000000) == table[b"int32-2"]
  assert struct.pack(b'@q', -800000000000000) == table[b"int64-2"]
  assert struct.pack(b'@d', -10.0) == table[b"float-2"]
  assert struct.pack(b'@d', -15.5) == table[b"double-2"]
  assert 13 == table.size()

  # incr should return the new value of the key
  assert struct.pack(b'@b', 44) == table.incr(b"int8", 4)
  assert struct.pack(b'@h', 4010) == table.incr(b"int16", 10)
  assert struct.pack(b'@l', 60000100) == table.incr(b"int32", 100)
  assert struct.pack(b'@q', 800000000001000) == table.incr(b"int64", 1000)
  assert struct.pack(b'@f', 30.0) == table.incr(b"float", 20.0)
  assert struct.pack(b'@d', 25.5) == table.incr(b"double", 10.0)
  assert struct.pack(b'@q', -14) == table.incr(b"int8-2", -4)
  assert struct.pack(b'@q', -4010) == table.incr(b"int16-2", -10)
  assert struct.pack(b'@q', -60000100) == table.incr(b"int32-2", -100)
  assert struct.pack(b'@q', -800000000001000) == table.incr(b"int64-2", -1000)
  assert struct.pack(b'@d', -30.0) == table.incr(b"float-2", -20.0)
  assert struct.pack(b'@d', -25.5) == table.incr(b"double-2", -10.0)
  assert 13 == table.size()

  # Test incr() on keys of the wrong size
  try:
    table.incr(b"string", 14)
    assert False
  except ValueError:
    pass
  try:
    table.incr(b"string", 15.0)
    assert False
  except ValueError:
    pass

  table.clear()
  assert len(table) == 0


# TODO: deduplicate this with PrefixTreeTest
def run_concurrent_readers_test(allocator_type):
  print('-- [%s] concurrent readers' % allocator_type)

  table = sharedstructures.HashTable(POOL_NAME_PREFIX + allocator_type, allocator_type)
  del table

  child_pids = set()
  while (len(child_pids) < 8) and (0 not in child_pids):
    child_pids.add(os.fork())

  if 0 in child_pids:
    # Child process: try up to a second to get the key
    table = sharedstructures.HashTable(POOL_NAME_PREFIX + allocator_type, allocator_type)

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
          print('-- [%s]   child %d saw value %d' % (allocator_type, os.getpid(), value))
          value += 1

    if int(time.time() * 1000000) >= (start_time + 1000000):
      print('-- [%s]   child %d timed out' % (allocator_type, os.getpid()))

    os._exit(int(value != 110))

  else:
    # Parent process: write the key, then wait for children to terminate
    table = sharedstructures.HashTable(POOL_NAME_PREFIX + allocator_type, allocator_type)

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
      sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)
      run_conditional_writes_test(allocator_type)
      sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)
      run_collision_test(allocator_type)
      # TODO: enable this test again when the python module supports incr
      # sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)
      # run_incr_test(allocator_type)
      sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)
      run_concurrent_readers_test(allocator_type)
    print('all tests passed')
    return 0

  finally:
    for allocator_type in ALLOCATOR_TYPES:
      sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)


if __name__ == '__main__':
  sys.exit(main())
