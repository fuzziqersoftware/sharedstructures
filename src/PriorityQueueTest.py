import os
import subprocess
import sys
import time

import sharedstructures

POOL_NAME_PREFIX = "PriorityQueueTest-py-pool-"
ALLOCATOR_TYPES = ("simple", "logarithmic")


def get_current_process_lsof() -> bytes:
    return subprocess.check_output(["lsof", "-p", str(os.getpid())])


def run_basic_test(allocator_type: str) -> None:
    print("-- [%s] basic" % allocator_type)
    before_lsof_count = len(get_current_process_lsof().splitlines())

    q = sharedstructures.PriorityQueue(
        POOL_NAME_PREFIX + allocator_type, allocator_type
    )
    assert len(q) == 0

    q.push(b"v1")
    assert 1 == len(q)
    q.push(b"v3")
    assert 2 == len(q)
    q.push(b"v20")
    assert 3 == len(q)
    q.push(b"v2")
    assert 4 == len(q)

    assert b"v1" == q.pop()
    assert 3 == len(q)
    assert b"v2" == q.pop()
    assert 2 == len(q)
    assert b"v20" == q.pop()
    assert 1 == len(q)
    assert b"v3" == q.pop()
    assert 0 == len(q)
    try:
        q.pop()
        assert False
    except IndexError:
        pass

    del q  # This should unmap the shared memory pool and close the fd
    sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)

    # Make sure we didn't leak an fd
    assert before_lsof_count == len(get_current_process_lsof().splitlines())


def run_concurrent_producers_test(allocator_type: str) -> None:
    print("-- [%s] concurrent producers" % allocator_type)

    # Create the pool before forking to avoid creation races
    q = sharedstructures.PriorityQueue(
        POOL_NAME_PREFIX + allocator_type, allocator_type
    )
    del q

    child_pids: set[int] = set()
    while (len(child_pids) < 8) and (0 not in child_pids):
        child_pids.add(os.fork())

    if 0 in child_pids:
        # Child process: generate the numbers [0, 1000) prefixed with our pid
        q = sharedstructures.PriorityQueue(
            POOL_NAME_PREFIX + allocator_type, allocator_type
        )

        pid = os.getpid()
        for x in range(1000):
            q.push(b"%d-%04d" % (pid, x))

        os._exit(0)

    else:
        # Parent process: write the key, then wait for children to terminate
        q = sharedstructures.PriorityQueue(
            POOL_NAME_PREFIX + allocator_type, allocator_type
        )

        latest_value_from_process = {pid: -1 for pid in child_pids}

        num_failures = 0
        while child_pids:
            try:
                data = q.pop()
                pid, value = data.split(b"-")
                pid = int(pid)
                value = int(value)
                assert pid in latest_value_from_process
                assert latest_value_from_process[pid] < value
                latest_value_from_process[pid] = value

            except IndexError:  # Queue is empty
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


def run_concurrent_consumers_test(allocator_type: str) -> None:
    print("-- [%s] concurrent consumers" % allocator_type)

    # Initialize the queue before forking children
    q = sharedstructures.PriorityQueue(
        POOL_NAME_PREFIX + allocator_type, allocator_type
    )
    del q

    child_pids: set[int] = set()
    while (len(child_pids) < 8) and (0 not in child_pids):
        child_pids.add(os.fork())

    if 0 in child_pids:
        # Child process: read numbers and expect them to be in increasing order
        # stop when -1 is received
        q = sharedstructures.PriorityQueue(
            POOL_NAME_PREFIX + allocator_type, allocator_type
        )

        prev_value = -1
        value = 0
        while True:
            try:
                item = q.pop()
                if item == b"END":
                    break
                value = int(item)
            except IndexError:
                time.sleep(0.00001)
                continue
            assert value > prev_value
            prev_value = value

        os._exit(0)

    else:
        # Parent process: push numbers [0, 10000), then wait for children
        q = sharedstructures.PriorityQueue(
            POOL_NAME_PREFIX + allocator_type, allocator_type
        )

        for v in range(10000):
            q.push(b"%04d" % v)

        for _ in child_pids:
            q.push(b"END")

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


def main() -> int:
    try:
        for allocator_type in ALLOCATOR_TYPES:
            sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)
            run_basic_test(allocator_type)
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
