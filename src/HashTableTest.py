import os
import subprocess
import sys
import time
from typing import Any, Callable, Mapping

import sharedstructures

POOL_NAME_PREFIX = "HashTableTest-py-pool-"
ALLOCATOR_TYPES = ("simple", "logarithmic")


def get_current_process_lsof() -> bytes:
    return subprocess.check_output(["lsof", "-p", str(os.getpid())])


def expect_key_missing(table: sharedstructures.HashTable, k: bytes) -> None:
    try:
        table[k]
        assert False, "table[%r] didn't raise" % k
    except KeyError:
        pass


def verify_state(
    expected: Mapping[bytes, Any], table: sharedstructures.HashTable
) -> None:
    assert len(expected) == len(table)
    for k, v in expected.items():
        assert table[k] == v
    for k, v in table.items():
        assert expected[k] == v
    verify_allocator(table)


def verify_allocator(table: sharedstructures.HashTable) -> None:
    ret = table.verify()
    assert ret is None, ret.decode("utf-8")


def run_test_and_check_lsof(
    fn: Callable[[sharedstructures.HashTable, str], None],
    allocator_type: str,
) -> None:
    before_lsof_count = len(get_current_process_lsof().splitlines())
    table = sharedstructures.HashTable(
        POOL_NAME_PREFIX + allocator_type, allocator_type
    )

    fn(table, allocator_type)

    del table  # This should unmap the shared memory pool and close the fd
    sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)
    # Make sure we didn't leak an fd
    assert before_lsof_count == len(get_current_process_lsof().splitlines())


def run_basic_test(table: sharedstructures.HashTable, allocator_type: str) -> None:
    print("-- [%s] basic" % allocator_type)
    expected: dict[bytes, Any] = {}

    def insert_both(k: bytes, v: Any):
        table[k] = v
        expected[k] = v

    def delete_both(k: bytes):
        del table[k]
        del expected[k]

    verify_state(expected, table)
    insert_both(b"key1", b"value1")
    verify_state(expected, table)
    insert_both(b"key2", b"value2")
    verify_state(expected, table)
    insert_both(b"key3", b"value3")
    verify_state(expected, table)

    delete_both(b"key2")
    verify_state(expected, table)
    try:
        del table[b"key2"]
        assert False, "del table['key2'] did not raise KeyError"
    except KeyError:
        pass
    verify_state(expected, table)

    insert_both(b"key1", b"value0")
    verify_state(expected, table)
    delete_both(b"key1")
    verify_state(expected, table)
    delete_both(b"key3")
    verify_state(expected, table)

    assert {} == expected


def run_conditional_writes_test(allocator_type: str) -> None:
    print("-- [%s] conditional writes" % allocator_type)

    table = sharedstructures.HashTable(
        POOL_NAME_PREFIX + allocator_type, allocator_type
    )
    expected: dict[bytes, Any] = {}

    def insert_both(k: bytes, v: Any) -> None:
        table[k] = v
        expected[k] = v

    def delete_both(k: bytes) -> None:
        del table[k]
        del expected[k]

    def conditional_insert_both(
        check_k: bytes, check_v: Any, target_k: bytes, target_v: Any, written: bool
    ) -> None:
        if table.check_and_set(check_k, check_v, target_k, target_v):
            expected[target_k] = target_v
            assert written
        else:
            assert not written

    def conditional_missing_insert_both(
        check_k: bytes, target_k: bytes, target_v: Any, written: bool
    ) -> None:
        if table.check_missing_and_set(check_k, target_k, target_v):
            expected[target_k] = target_v
            assert written
        else:
            assert not written

    def conditional_delete_both(
        check_k: bytes, check_v: Any, target_k: bytes, written: bool
    ) -> None:
        if table.check_and_set(check_k, check_v, target_k):
            del expected[target_k]
            assert written
        else:
            assert not written

    def conditional_missing_delete_both(
        check_k: bytes, target_k: bytes, written: bool
    ) -> None:
        if table.check_missing_and_set(check_k, target_k):
            del expected[target_k]
            assert written
        else:
            assert not written

    verify_state(expected, table)

    insert_both(b"key1", b"value1")
    verify_state(expected, table)
    insert_both(b"key2", b"value2")
    verify_state(expected, table)

    # Check that conditions on the same key work
    conditional_insert_both(b"key1", b"value2", b"key1", b"value1_1", False)
    verify_state(expected, table)
    conditional_insert_both(b"key1", b"value1", b"key1", b"value1_1", True)
    verify_state(expected, table)

    # Check that conditions on other keys work
    conditional_insert_both(b"key2", b"value1", b"key1", b"value1", False)
    verify_state(expected, table)
    conditional_insert_both(b"key2", b"value2", b"key1", b"value1", True)
    verify_state(expected, table)

    # Check that missing conditions work
    conditional_missing_insert_both(b"key2", b"key3", b"value3", False)
    verify_state(expected, table)
    conditional_missing_insert_both(b"key3", b"key3", b"value3", True)
    verify_state(expected, table)

    # Check that conditional deletes work
    conditional_delete_both(b"key1", b"value2", b"key1", False)
    verify_state(expected, table)
    conditional_delete_both(b"key1", b"value1", b"key1", True)
    verify_state(expected, table)

    conditional_missing_delete_both(b"key3", b"key2", False)
    verify_state(expected, table)
    conditional_missing_delete_both(b"key1", b"key2", True)
    verify_state(expected, table)

    delete_both(b"key3")

    assert expected == {}


def run_collision_test(allocator_type: str) -> None:
    print("-- [%s] collision" % allocator_type)

    table = sharedstructures.HashTable(
        POOL_NAME_PREFIX + allocator_type, allocator_type, 0, 2
    )
    expected: dict[bytes, Any] = {}

    def insert_both(k: bytes, v: Any) -> None:
        table[k] = v
        expected[k] = v

    # Writing 5 keys to a 4-slot hashtable forces a collision
    assert 2 == table.bits()
    assert 0 == len(table)

    insert_both(b"key1", b"value1")
    insert_both(b"key2", b"value2")
    insert_both(b"key3", b"value3")
    insert_both(b"key4", b"value4")
    insert_both(b"key5", b"value5")
    verify_state(expected, table)

    while expected:
        k, _ = expected.popitem()
        del table[k]
        verify_state(expected, table)


# TODO: deduplicate this with PrefixTreeTest
def run_concurrent_readers_test(allocator_type: str) -> None:
    print("-- [%s] concurrent readers" % allocator_type)

    table = sharedstructures.HashTable(
        POOL_NAME_PREFIX + allocator_type, allocator_type
    )
    del table

    child_pids: set[int] = set()
    while (len(child_pids) < 8) and (0 not in child_pids):
        child_pids.add(os.fork())

    if 0 in child_pids:
        # Child process: try up to a second to get the key
        table = sharedstructures.HashTable(
            POOL_NAME_PREFIX + allocator_type, allocator_type
        )

        value = 100
        start_time = int(time.time() * 1000000)
        while (value < 110) and (int(time.time() * 1000000) < (start_time + 1000000)):
            time.sleep(0.001)
            try:
                res = table[b"key1"]
            except KeyError:
                pass
            else:
                if res == value:
                    print(
                        "-- [%s]   child %d saw value %d"
                        % (allocator_type, os.getpid(), value)
                    )
                    value += 1

        if int(time.time() * 1000000) >= (start_time + 1000000):
            print("-- [%s]   child %d timed out" % (allocator_type, os.getpid()))

        os._exit(int(value != 110))

    else:
        # Parent process: write the key, then wait for children to terminate
        table = sharedstructures.HashTable(
            POOL_NAME_PREFIX + allocator_type, allocator_type
        )

        for value in range(100, 110):
            time.sleep(0.05)
            table[b"key1"] = value

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
            run_test_and_check_lsof(run_basic_test, allocator_type)
            sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)
            run_conditional_writes_test(allocator_type)
            sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)
            run_collision_test(allocator_type)
            sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)
            run_concurrent_readers_test(allocator_type)
        print("all tests passed")
        return 0

    finally:
        for allocator_type in ALLOCATOR_TYPES:
            sharedstructures.delete_pool(POOL_NAME_PREFIX + allocator_type)


if __name__ == "__main__":
    sys.exit(main())
