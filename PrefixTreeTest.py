import os
import subprocess
import sys
import time

import sharedstructures


def get_current_process_lsof():
  return subprocess.check_output(['lsof', '-p', str(os.getpid())])


def diff(a, b):
  import difflib
  return '\n'.join(x.rstrip() for x in difflib.unified_diff(a.splitlines(), b.splitlines()))


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
  for k, v in table.iteritems():
    assert expected[k] == v


def run_basic_test():
  print('-- basic')
  before_lsof = get_current_process_lsof()

  table = sharedstructures.PrefixTree('test-table')
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
  after_lsof = get_current_process_lsof()
  if before_lsof != after_lsof:
    print(diff(before_lsof, after_lsof))
    assert False


def run_reorganization_test():
  print('-- reorganization')
  table = sharedstructures.PrefixTree('test-table')
  expected = {}

  def insert_both(k):
    table[k] = k
    expected[k] = k

  def delete_both(k):
    del table[k]
    del expected[k]

  verify_state(expected, table)

  insert_both('abc')
  verify_state(expected, table)

  insert_both('ab')
  verify_state(expected, table)

  delete_both('abc')
  verify_state(expected, table)

  insert_both('')
  verify_state(expected, table)

  insert_both('abcd')
  verify_state(expected, table)

  delete_both('ab')
  verify_state(expected, table)

  insert_both('abcde')
  verify_state(expected, table)

  insert_both('abcdf')
  verify_state(expected, table)

  insert_both('abce')
  verify_state(expected, table)

  insert_both('abcef')
  verify_state(expected, table)

  table.clear()
  expected.clear()
  verify_state(expected, table)


def run_types_test():
  print('-- types')
  table = sharedstructures.PrefixTree('test-table')
  expected = {}

  def insert_both(k, v):
    table[k] = v
    expected[k] = v

  def delete_both(k):
    del table[k]
    del expected[k]

  verify_state(expected, table)
  insert_both('key-string', 'value-string')
  verify_state(expected, table)
  insert_both('key-string-unicode', u'value-string-unicode')
  verify_state(expected, table)
  insert_both('key-int', 1024 * 1024 * -3)
  verify_state(expected, table)
  insert_both('key-int-long', 0x5555555555555555)
  verify_state(expected, table)
  insert_both('key-double', 2.38)
  verify_state(expected, table)
  insert_both('key-true', True)
  verify_state(expected, table)
  insert_both('key-false', False)
  verify_state(expected, table)
  insert_both('key-null', None)
  verify_state(expected, table)

  expect_key_missing(table, 'key-missing')

  # this calls exists() internally
  assert 'key-string' in table
  assert 'key-string-unicode' in table
  assert 'key-int' in table
  assert 'key-int-long' in table
  assert 'key-double' in table
  assert 'key-true' in table
  assert 'key-false' in table
  assert 'key-null' in table

  table.clear()
  expected.clear()
  verify_state(expected, table)


def run_complex_types_test():
  print('-- complex types')
  table = sharedstructures.PrefixTree('test-table')
  expected = {}

  def insert_both(k, v):
    table[k] = v
    expected[k] = v

  def delete_both(k):
    del table[k]
    del expected[k]

  verify_state(expected, table)
  insert_both('key-list-empty', [])
  verify_state(expected, table)
  insert_both('key-list-ints', [1, 2, 3, 7])
  verify_state(expected, table)
  insert_both('key-tuple-empty', ())
  verify_state(expected, table)
  insert_both('key-tuple-ints', (1, 2, 3, 7))
  verify_state(expected, table)
  insert_both('key-set-empty', set())
  verify_state(expected, table)
  insert_both('key-set-ints', {1, 2, 3, 7})
  verify_state(expected, table)
  insert_both('key-dict-empty', {})
  verify_state(expected, table)
  insert_both('key-dict-ints', {1: 2, 3: 7})
  verify_state(expected, table)

  table.clear()
  expected.clear()
  verify_state(expected, table)


def run_concurrent_readers_test():
  print('-- concurrent readers')

  table = sharedstructures.PrefixTree('test-table')
  del table

  child_pids = set()
  while (len(child_pids) < 8) and (0 not in child_pids):
    child_pids.add(os.fork())

  if 0 in child_pids:
    # child process: try up to a second to get the key
    table = sharedstructures.PrefixTree('test-table')

    value = 100
    start_time = int(time.time() * 1000000)
    while (value < 110) and (int(time.time() * 1000000) < (start_time + 1000000)):
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
    table = sharedstructures.PrefixTree('test-table')

    for value in xrange(100, 110):
      time.sleep(0.05)
      table['key1'] = value

    num_failures = 0
    while child_pids:
      pid, exit_status = os.wait()
      child_pids.remove(pid)
      if os.WIFEXITED(exit_status) and (os.WEXITSTATUS(exit_status) == 0):
        print('--   child %d terminated successfully' % pid)
      else:
        print('--   child %d failed (%d)' % (pid, exit_status))
        num_failures += 1

    assert 0 == len(child_pids)
    assert 0 == num_failures


def main():
  try:
    sharedstructures.delete_pool('test-table')
    run_basic_test()
    run_reorganization_test()
    run_types_test()
    run_complex_types_test()
    run_concurrent_readers_test()
    print('all tests passed')
    return 0

  finally:
    sharedstructures.delete_pool('test-table')


if __name__ == '__main__':
  sys.exit(main())
