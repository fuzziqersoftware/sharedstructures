import os
import subprocess
import sys
import time

import sharedstructures


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
  for k, v in expected.iteritems():
    assert table[k] == v


def run_basic_test(allocator_type):
  print("[%s] -- basic" % allocator_type)
  before_lsof_count = len(get_current_process_lsof().splitlines())

  table = sharedstructures.HashTable("test-table", allocator_type)
  expected = {}

  def insert_both(e, t, k, v):
    t[k] = v
    e[k] = v

  def delete_both(e, t, k):
    del t[k]
    del e[k]

  verify_state(expected, table)
  insert_both(expected, table, 'key1', 'value1')
  verify_state(expected, table)
  insert_both(expected, table, 'key2', 'value2')
  verify_state(expected, table)
  insert_both(expected, table, 'key3', 'value3')
  verify_state(expected, table)

  delete_both(expected, table, 'key2')
  verify_state(expected, table)
  try:
    del table['key2']
    assert False, "del table[\'key2\'] did not raise KeyError"
  except KeyError:
    pass
  verify_state(expected, table)

  insert_both(expected, table, 'key1', 'value0')
  verify_state(expected, table)
  delete_both(expected, table, 'key1')
  verify_state(expected, table)
  delete_both(expected, table, 'key3')
  verify_state(expected, table)

  assert {} == expected

  del table  # this should unmap the shared memory pool and close the fd
  sharedstructures.delete_pool('test-table')

  # this will fail if the test prints anything after before_lsof is taken since
  # the stdout/stderr offsets will be different
  assert before_lsof_count == len(get_current_process_lsof().splitlines())


def run_collision_test(allocator_type):
  print("[%s] -- collision" % allocator_type)

  table = sharedstructures.HashTable("test-table", allocator_type, 0, 2)
  expected = {}

  def insert_both(e, t, k, v):
    t[k] = v
    e[k] = v

  def delete_both(e, t, k):
    del t[k]
    del e[k]

  # writing 5 keys to a 4-slot hashtable forces a collision
  assert 2 == table.bits()
  assert 0 == len(table)

  insert_both(expected, table, 'key1', 'value1')
  insert_both(expected, table, 'key2', 'value2')
  insert_both(expected, table, 'key3', 'value3')
  insert_both(expected, table, 'key4', 'value4')
  insert_both(expected, table, 'key5', 'value5')
  verify_state(expected, table)

  while expected:
    k, _ = expected.popitem();
    del table[k]
    verify_state(expected, table)


# TODO: deduplicate this with PrefixTreeTest
def run_concurrent_readers_test(allocator_type):
  print('[%s] -- concurrent readers' % allocator_type)

  table = sharedstructures.HashTable('test-table', allocator_type)
  del table

  child_pids = set()
  while (len(child_pids) < 8) and (0 not in child_pids):
    child_pids.add(os.fork())

  if 0 in child_pids:
    # child process: try up to a second to get the key
    table = sharedstructures.HashTable('test-table', allocator_type)

    value = 100
    start_time = int(time.time() * 1000000)
    while (value < 110) and (int(time.time() * 1000000) < (start_time + 1000000)):
      time.sleep(0.001)
      try:
        res = table['key1']
      except KeyError:
        pass
      else:
        if res == value:
          value += 1

    os._exit(int(value != 110))

  else:
    # parent process: write the key, then wait for children to terminate
    table = sharedstructures.HashTable('test-table', allocator_type)

    for value in xrange(100, 110):
      time.sleep(0.05)
      table['key1'] = value

    num_failures = 0
    while child_pids:
      pid, exit_status = os.wait()
      child_pids.remove(pid)
      if os.WIFEXITED(exit_status) and (os.WEXITSTATUS(exit_status) == 0):
        print('[%s] --   child %d terminated successfully' % (allocator_type, pid))
      else:
        print('[%s] --   child %d failed (%d)' % (allocator_type, pid, exit_status))
        num_failures += 1

    assert 0 == len(child_pids)
    assert 0 == num_failures


def main():
  try:
    for allocator_type in ('simple', 'logarithmic'):
      sharedstructures.delete_pool('test-table')
      run_basic_test(allocator_type)
      sharedstructures.delete_pool('test-table')
      run_collision_test(allocator_type)
      sharedstructures.delete_pool('test-table')
      run_concurrent_readers_test(allocator_type)
    print('all tests passed')
    return 0

  finally:
    sharedstructures.delete_pool('test-table')


if __name__ == '__main__':
  sys.exit(main())
