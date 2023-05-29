import os
import subprocess
import sys
import time

import sharedstructures

POOL_NAME_PREFIX = "QueueTest-py-pool-"
ALLOCATOR_TYPES = ("simple", "logarithmic")


def get_current_process_lsof():
    return subprocess.check_output(["lsof", "-p", str(os.getpid())])


def q_push(q, front, item, raw=False):
    if front:
        q.push_front(item, raw=raw)
    else:
        q.push_back(item, raw=raw)


def q_pop(q, front, raw=False):
    if front:
        return q.pop_front(raw=raw)
    else:
        return q.pop_back(raw=raw)


def run_basic_test(allocator_type):
    print("-- [%s] basic" % allocator_type)
    before_lsof_count = len(get_current_process_lsof().splitlines())

    q = sharedstructures.Queue(POOL_NAME_PREFIX + allocator_type, allocator_type)
    assert len(q) == 0
    assert q.bytes() == 0

    def test_queue(q, reverse):
        print(
            "-- [%s]   %s queue operation"
            % (allocator_type, "reverse" if reverse else "forward")
        )

        q_push(q, reverse, b"v1")
        assert 1 == len(q)
        q_push(q, reverse, "val2")
        assert 2 == len(q)
        q_push(q, reverse, 47)
        assert 3 == len(q)
        q_push(q, reverse, None)
        assert 4 == len(q)
        q_push(q, reverse, (None, False, True, 37, 2.0, ["lol", "hax"], {1: 2}))
        assert 5 == len(q)

        assert b"v1" == q_pop(q, not reverse)
        assert 4 == len(q)
        assert "val2" == q_pop(q, not reverse)
        assert 3 == len(q)
        assert 47 == q_pop(q, not reverse)
        assert 2 == len(q)
        assert None == q_pop(q, not reverse)
        assert 1 == len(q)
        assert (None, False, True, 37, 2.0, ["lol", "hax"], {1: 2}) == q_pop(
            q, not reverse
        )
        assert 0 == len(q)
        assert q.bytes() == 0
        try:
            q_pop(q, not reverse)
            assert False
        except IndexError:
            pass

    test_queue(q, False)
    test_queue(q, True)

    def test_stack(q, front):
        print(
            "-- [%s]   %s stack operation"
            % (allocator_type, "front" if front else "back")
        )

        q_push(q, front, b"v1")
        assert 1 == len(q)
        q_push(q, front, "val2")
        assert 2 == len(q)
        q_push(q, front, 47)
        assert 3 == len(q)
        q_push(q, front, None)
        assert 4 == len(q)
        q_push(q, front, (None, False, True, 37, 2.0, ["lol", "hax"], {1: 2}))
        assert 5 == len(q)

        assert (None, False, True, 37, 2.0, ["lol", "hax"], {1: 2}) == q_pop(q, front)
        assert 4 == len(q)
        assert None == q_pop(q, front)
        assert 3 == len(q)
        assert 47 == q_pop(q, front)
        assert 2 == len(q)
        assert "val2" == q_pop(q, front)
        assert 1 == len(q)
        assert b"v1" == q_pop(q, front)
        assert 0 == len(q)
        assert q.bytes() == 0
        try:
            q_pop(q, front)
            assert False
        except IndexError:
            pass

    test_stack(q, False)
    test_stack(q, True)

    del q  # this should unmap the shared memory pool and close the fd
    sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)

    # make sure we didn't leak an fd
    assert before_lsof_count == len(get_current_process_lsof().splitlines())


def run_raw_test(allocator_type):
    print("-- [%s] basic" % allocator_type)
    before_lsof_count = len(get_current_process_lsof().splitlines())

    q = sharedstructures.Queue(POOL_NAME_PREFIX + allocator_type, allocator_type)
    assert len(q) == 0
    assert q.bytes() == 0

    def test_queue(q, reverse):
        print(
            "-- [%s]   %s queue operation"
            % (allocator_type, "reverse" if reverse else "forward")
        )

        try:
            q_push(q, reverse, None, raw=True)
            assert False
        except TypeError:
            pass
        assert 0 == len(q)

        q_push(q, reverse, b"v1", raw=True)
        assert 1 == len(q)
        q_push(q, reverse, b"v2", raw=True)
        assert 2 == len(q)
        q_push(q, reverse, b"v3", raw=True)
        assert 3 == len(q)

        assert b"v1" == q_pop(q, not reverse, raw=True)
        assert 2 == len(q)
        assert b"v2" == q_pop(q, not reverse, raw=True)
        assert 1 == len(q)
        assert b"v3" == q_pop(q, not reverse, raw=True)
        assert 0 == len(q)
        assert q.bytes() == 0
        try:
            q_pop(q, not reverse, raw=True)
            assert False
        except IndexError:
            pass

    test_queue(q, False)
    test_queue(q, True)

    def test_stack(q, front):
        print(
            "-- [%s]   %s stack operation"
            % (allocator_type, "front" if front else "back")
        )

        try:
            q_push(q, front, None, raw=True)
            assert False
        except TypeError:
            pass
        assert 0 == len(q)

        q_push(q, front, b"v1", raw=True)
        assert 1 == len(q)
        q_push(q, front, b"v2", raw=True)
        assert 2 == len(q)
        q_push(q, front, b"v3", raw=True)
        assert 3 == len(q)

        assert b"v3" == q_pop(q, front, raw=True)
        assert 2 == len(q)
        assert b"v2" == q_pop(q, front, raw=True)
        assert 1 == len(q)
        assert b"v1" == q_pop(q, front, raw=True)
        assert 0 == len(q)
        assert q.bytes() == 0
        try:
            q_pop(q, front, raw=True)
            assert False
        except IndexError:
            pass

    test_stack(q, False)
    test_stack(q, True)

    del q  # this should unmap the shared memory pool and close the fd
    sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)

    # make sure we didn't leak an fd
    assert before_lsof_count == len(get_current_process_lsof().splitlines())


def run_concurrent_producers_test(allocator_type):
    print("-- [%s] concurrent producers" % allocator_type)

    # initialize the queue before forking children
    q = sharedstructures.Queue(POOL_NAME_PREFIX + allocator_type, allocator_type)
    del q

    child_pids = set()
    while (len(child_pids) < 8) and (0 not in child_pids):
        child_pids.add(os.fork())

    if 0 in child_pids:
        # child process: generate the numbers [0, 1000) prefixed with our pid
        q = sharedstructures.Queue(POOL_NAME_PREFIX + allocator_type, allocator_type)

        pid = os.getpid()
        for x in range(1000):
            q.push_back(b"%d-%d" % (pid, x))

        os._exit(0)

    else:
        # parent process: write the key, then wait for children to terminate
        q = sharedstructures.Queue(POOL_NAME_PREFIX + allocator_type, allocator_type)

        latest_value_from_process = {pid: -1 for pid in child_pids}

        num_failures = 0
        while child_pids:
            try:
                data = q.pop_front()
                pid, value = data.split(b"-")
                pid = int(pid)
                value = int(value)
                assert pid in latest_value_from_process
                assert latest_value_from_process[pid] < value
                latest_value_from_process[pid] = value

            except IndexError:  # queue is empty
                pid, exit_status = os.wait()
                child_pids.remove(pid)
                if os.WIFEXITED(exit_status) and (os.WEXITSTATUS(exit_status) == 0):
                    print(
                        "-- [%s]   child %d terminated successfully"
                        % (allocator_type, pid)
                    )
                else:
                    print(
                        "-- [%s]   child %d failed (%d)"
                        % (allocator_type, pid, exit_status)
                    )
                    num_failures += 1

        assert 0 == len(child_pids)
        assert 0 == num_failures


def run_concurrent_consumers_test(allocator_type):
    print("-- [%s] concurrent consumers" % allocator_type)

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
            q.push_back(b"%d" % v)

        for _ in child_pids:
            q.push_back(b"-1")

        num_failures = 0
        while child_pids:
            pid, exit_status = os.wait()
            child_pids.remove(pid)
            if os.WIFEXITED(exit_status) and (os.WEXITSTATUS(exit_status) == 0):
                print(
                    "-- [%s]   child %d terminated successfully" % (allocator_type, pid)
                )
            else:
                print(
                    "-- [%s]   child %d failed (%d)"
                    % (allocator_type, pid, exit_status)
                )
                num_failures += 1

        assert 0 == len(child_pids)
        assert 0 == num_failures


def main():
    try:
        for allocator_type in ALLOCATOR_TYPES:
            sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)
            run_basic_test(allocator_type)
            sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)
            run_raw_test(allocator_type)
            sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)
            run_concurrent_producers_test(allocator_type)
            sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)
            run_concurrent_consumers_test(allocator_type)
        print("all tests passed")
        return 0

    finally:
        for allocator_type in ALLOCATOR_TYPES:
            sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)


if __name__ == "__main__":
    sys.exit(main())
