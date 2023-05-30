import os
import subprocess
import sys

import sharedstructures

POOL_NAME = "IntVectorTest-py-pool"


def get_current_process_lsof() -> bytes:
    return subprocess.check_output(["lsof", "-p", str(os.getpid())])


def run_basic_test() -> None:
    print("-- basic")
    before_lsof_count = len(get_current_process_lsof().splitlines())

    v = sharedstructures.IntVector(POOL_NAME)

    limit = 1024

    assert len(v) == 0
    v.expand(10)
    assert len(v) == 10
    v.expand(5)
    assert len(v) == 10
    v.expand(limit)
    assert len(v) == limit

    # Test load/store
    for x in range(limit):
        assert v.load(x) == 0
    for x in range(limit):
        v.store(x, x)
    for x in range(limit):
        assert v.load(x) == x

    # Test exchange
    for x in range(limit):
        assert v.exchange(x, x + 10) == x
    for x in range(limit):
        assert v.load(x) == x + 10
    for x in range(limit):
        assert v.exchange(x, x) == x + 10
    for x in range(limit):
        assert v.load(x) == x

    # Test compare_exchange
    for x in range(limit):
        assert v.compare_exchange(x, 10, 15) == x
    for x in range(limit):
        assert v.load(x) == (15 if (x == 10) else x)
    v.store(10, 10)

    # Test add/subtract
    for x in range(limit):
        assert v.add(x, 30) == x
    for x in range(limit):
        assert v.load(x) == x + 30
    for x in range(limit):
        assert v.subtract(x, 30) == x + 30
    for x in range(limit):
        assert v.load(x) == x

    # Test bitwise_and/bitwise_or
    for x in range(limit):
        assert v.bitwise_or(x, 0x7F) == x
    for x in range(limit):
        assert v.load(x) == x | 0x7F
    for x in range(limit):
        assert v.bitwise_and(x, ~0x7F) == x | 0x7F
    for x in range(limit):
        assert v.load(x) == x & ~0x7F

    # Reset for xor test
    for x in range(limit):
        v.store(x, x)

    # Test bitwise_xor
    for x in range(limit):
        assert v.bitwise_xor(x, 0x7F) == x
    for x in range(limit):
        assert v.load(x) == x ^ 0x7F
    for x in range(limit):
        assert v.bitwise_xor(x, 0x7F) == x ^ 0x7F
    for x in range(limit):
        assert v.load(x) == x

    for x in range(10):
        v.store(x, 0)
    for x in range(320):
        v.store_bit(x * 2, True)
    for x in range(640):
        assert v.load_bit(x) == (not (x & 1))
    for x in range(10):
        assert v.load(x) == -0x5555555555555556
    for x in range(160):
        v.store_bit(x * 4, False)
    for x in range(10):
        assert v.load(x) == 0x2222222222222222
    for x in range(320):
        assert v.xor_bit(x * 2) == (not (x & 1))
    for x in range(10):
        assert v.load(x) == -0x7777777777777778

    del v  # This should unmap the shared memory pool and close the fd
    sharedstructures.delete_pool(POOL_NAME)

    # Make sure we didn't leak an fd
    assert before_lsof_count == len(get_current_process_lsof().splitlines())


def main() -> int:
    try:
        sharedstructures.delete_pool(POOL_NAME)
        run_basic_test()
        print("all tests passed")
        return 0

    finally:
        sharedstructures.delete_pool(POOL_NAME)


if __name__ == "__main__":
    sys.exit(main())
