from __future__ import unicode_literals

import os
import subprocess
import sys
import time

import sharedstructures

POOL_NAME_PREFIX = "QueueTest-py-pool-"
ALLOCATOR_TYPES = ('simple', 'logarithmic')


def get_current_process_lsof():
  return subprocess.check_output(['lsof', '-p', str(os.getpid())])


def q_push(q, front, item):
  if front:
    q.push_front(item)
  else:
    q.push_back(item)


def q_pop(q, front):
  if front:
    return q.pop_front()
  else:
    return q.pop_back()


def run_basic_test(allocator_type):
  print('-- [%s] basic' % allocator_type)
  before_lsof_count = len(get_current_process_lsof().splitlines())

  q = sharedstructures.Queue(POOL_NAME_PREFIX + allocator_type, allocator_type)
  assert len(q) == 0
  assert q.bytes() == 0

  def test_queue(q, reverse):
    print('-- [%s]   %s queue operation' % (allocator_type, "reverse" if reverse else "forward"))

    q_push(q, reverse, b"v1");
    assert 1 == len(q)
    assert 2 == q.bytes()
    q_push(q, reverse, b"val2");
    assert 2 == len(q)
    assert 6 == q.bytes()
    q_push(q, reverse, b"value-3");
    assert 3 == len(q)
    assert 13 == q.bytes()

    assert b"v1" == q_pop(q, not reverse)
    assert 2 == len(q)
    assert 11 == q.bytes()
    assert b"val2" ==q_pop(q, not reverse)
    assert 1 == len(q)
    assert 7 == q.bytes()
    assert b"value-3" == q_pop(q, not reverse)
    assert 0 == len(q)
    assert 0 == q.bytes()
    try:
      q_pop(q, not reverse)
      assert False
    except IndexError:
      pass

  test_queue(q, False);
  test_queue(q, True);

  def test_stack(q, front):
    print("-- [%s]   %s stack operation" % (allocator_type, "front" if front else "back"))

    q_push(q, front, b"v1")
    assert 1 == len(q)
    assert 2 == q.bytes()
    q_push(q, front, b"val2");
    assert 2 == len(q)
    assert 6 == q.bytes()
    q_push(q, front, b"value-3");
    assert 3 == len(q)
    assert 13 == q.bytes()

    assert b"value-3" == q_pop(q, front)
    assert 2 == len(q)
    assert 6 == q.bytes()
    assert b"val2" == q_pop(q, front)
    assert 1 == len(q)
    assert 2 == q.bytes()
    assert b"v1" == q_pop(q, front)
    assert 0 == len(q)
    assert 0 == q.bytes()
    try:
      q_pop(q, front);
      assert False
    except IndexError:
      pass

  test_stack(q, False);
  test_stack(q, True);

  del q  # this should unmap the shared memory pool and close the fd
  sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)

  # make sure we didn't leak an fd
  assert before_lsof_count == len(get_current_process_lsof().splitlines())


def run_concurrent_producers_test(allocator_type):
  print('-- [%s] concurrent producers' % allocator_type)

  child_pids = set()
  while (len(child_pids) < 8) and (0 not in child_pids):
    child_pids.add(os.fork())

  if 0 in child_pids:
    # child process: generate the numbers [0, 1000) prefixed with our pid
    q = sharedstructures.Queue(POOL_NAME_PREFIX + allocator_type, allocator_type)

    pid = os.getpid()
    for x in range(1000):
      q.push_back(b'%d-%d' % (pid, x))

    os._exit(0)

  else:
    # parent process: write the key, then wait for children to terminate
    q = sharedstructures.Queue(POOL_NAME_PREFIX + allocator_type, allocator_type)

    latest_value_from_process = {pid: -1 for pid in child_pids}

    num_failures = 0
    while child_pids:
      try:
        data = q.pop_front()
        pid, value = data.split(b'-')
        pid = int(pid)
        value = int(value)
        assert pid in latest_value_from_process
        assert latest_value_from_process[pid] < value
        latest_value_from_process[pid] = value

      except IndexError:  # queue is empty
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


def run_concurrent_consumers_test(allocator_type):
  print('-- [%s] concurrent consumers' % allocator_type)

  # initialize the queue before forking children
  q = sharedstructures.Queue(POOL_NAME_PREFIX + allocator_type, allocator_type)
  del q

  child_pids = set()
  while (len(child_pids) < 8) and (0 not in child_pids):
    child_pids.add(os.fork())

  if 0 in child_pids:
    # child process: read numbers and expect them to be in increasing order
    # stop when -1 is received
    q = sharedstructures.Queue(POOL_NAME_PREFIX + allocator_type, allocator_type)

    prev_value = -1
    value = 0
    while True:
      try:
        item = q.pop_front()
        value = int(item)
        if value == -1:
          break
      except IndexError:
        time.sleep(0.00001)
        continue
      assert value > prev_value
      prev_value = value

    os._exit(0)

  else:
    # parent process: push numbers [0, 10000), then wait for children
    q = sharedstructures.Queue(POOL_NAME_PREFIX + allocator_type, allocator_type)

    for v in range(10000):
      q.push_back(b'%d' % v)

    for _ in child_pids:
      q.push_back(b'-1')

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
      run_concurrent_producers_test(allocator_type)
      sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)
      run_concurrent_consumers_test(allocator_type)
    print('all tests passed')
    return 0

  finally:
    for allocator_type in ALLOCATOR_TYPES:
      sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)


if __name__ == '__main__':
  sys.exit(main())