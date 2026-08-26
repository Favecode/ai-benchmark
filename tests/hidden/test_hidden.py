"""
Hidden test suite. Not shown to the agent during development.
Run via tests/test.sh, which also performs the store.py integrity check
before invoking this file.
"""
import sys
import os
import threading
import random

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
for candidate in (
    os.path.join(ROOT, "src"),
    os.path.join(ROOT, "environment", "src"),
    "/workspace/src",
):
    if os.path.isdir(candidate):
        sys.path.insert(0, candidate)
        break

from store import MockDistributedStore, StorePartitionedError
from rate_limiter import RateLimiter


# ---------------------------------------------------------------------------
# 1. Sliding-window precision (rejects the "fixed window" trap)
# ---------------------------------------------------------------------------
def test_sliding_window_not_fixed_window():
    store = MockDistributedStore()
    rl = RateLimiter(store, limit=10, window_seconds=10, node_id="n1")
    t0 = 5_000_000.0

    # Fill the window fully at t0.
    for _ in range(10):
        assert rl.allow("c1", now=t0) is True
    assert rl.allow("c1", now=t0) is False

    # At t0 + 5 (halfway into the *next* bucket boundary for a 10s window
    # starting a new bucket at t0+10), a correct sliding-window
    # implementation weights the previous bucket's count. Just past the
    # boundary (t0 + 10.5s) the effective count should still be close to
    # the full 10 (heavily weighted by the previous bucket), so a burst of
    # 10 *more* requests immediately after the boundary must NOT all be
    # allowed. A naive fixed-window implementation resets to 0 exactly at
    # the boundary and would wrongly allow a full new burst instantly.
    t1 = t0 + 10.5
    allowed_immediately_after_boundary = sum(
        1 for _ in range(10) if rl.allow("c1", now=t1)
    )
    assert allowed_immediately_after_boundary <= 2, (
        f"expected sliding window to still be ~heavily weighted by previous "
        f"bucket just after a boundary, but {allowed_immediately_after_boundary} "
        f"requests were allowed (looks like a fixed-window reset)"
    )

    # Fully past two windows -> should be freely allowed again.
    t2 = t0 + 25
    assert rl.allow("c1", now=t2) is True


def test_sliding_window_boundary_recovery_is_gradual():
    store = MockDistributedStore()
    rl = RateLimiter(store, limit=10, window_seconds=10, node_id="n1")
    t0 = 6_000_000.0
    for _ in range(10):
        assert rl.allow("c2", now=t0) is True

    # Deep into the next bucket (90% through it), the previous bucket's
    # weight should have decayed a lot, so most of a fresh 10-request burst
    # should now be allowed.
    t_late = t0 + 19.0
    allowed = sum(1 for _ in range(10) if rl.allow("c2", now=t_late))
    assert allowed >= 7, f"expected window to have substantially recovered, got {allowed}/10"


# ---------------------------------------------------------------------------
# 2. Partition handling + degrade policy
# ---------------------------------------------------------------------------
def test_fail_closed_denies_during_partition():
    store = MockDistributedStore()
    rl = RateLimiter(store, limit=5, window_seconds=10, node_id="nA", degrade_policy="fail_closed")
    t0 = 7_000_000.0
    store._admin_set_partition("nA", True)
    assert rl.allow("client", now=t0) is False, "fail_closed must deny when store unreachable"


def test_fail_open_allows_during_partition():
    store = MockDistributedStore()
    rl = RateLimiter(store, limit=5, window_seconds=10, node_id="nB", degrade_policy="fail_open")
    t0 = 7_100_000.0
    store._admin_set_partition("nB", True)
    assert rl.allow("client", now=t0) is True, "fail_open must allow when store unreachable"


def test_partition_heal_reconciles_without_double_counting():
    """
    The hardest hidden test: a node goes fail_open during a partition,
    serves 3 requests locally (each of which must eventually count against
    the shared budget), then the partition heals. The implementation must
    sync those 3 requests to the shared store *exactly once* — not zero
    times (silently losing the accounting) and not more than once
    (double-charging the client).
    """
    store = MockDistributedStore()
    rl = RateLimiter(store, limit=5, window_seconds=10, node_id="nC", degrade_policy="fail_open")
    t0 = 7_200_000.0

    store._admin_set_partition("nC", True)
    for _ in range(3):
        assert rl.allow("recon_client", now=t0) is True
    store._admin_set_partition("nC", False)

    # Trigger reconciliation: whatever the implementation's mechanism is
    # (e.g. lazily on next allow() call), the *next* call must observe the
    # store now reflecting the 3 partitioned requests, so only 2 more
    # should fit in a limit of 5.
    assert rl.allow("recon_client", now=t0) is True   # 4th of 5
    assert rl.allow("recon_client", now=t0) is True   # 5th of 5
    assert rl.allow("recon_client", now=t0) is False  # 6th must be denied

    # Simulate a second node also seeing the reconciled state.
    rl2 = RateLimiter(store, limit=5, window_seconds=10, node_id="nD", degrade_policy="fail_open")
    assert rl2.allow("recon_client", now=t0) is False, (
        "shared budget was exceeded — reconciliation likely double-counted "
        "or never actually wrote through to the shared store"
    )


def test_partition_heal_does_not_replay_on_every_call():
    """
    Guards against a naive "replay the pending buffer every time" bug that
    would double- or triple-count on subsequent calls after healing.
    """
    store = MockDistributedStore()
    rl = RateLimiter(store, limit=100, window_seconds=10, node_id="nE", degrade_policy="fail_open")
    t0 = 7_300_000.0

    store._admin_set_partition("nE", True)
    for _ in range(4):
        rl.allow("replay_client", now=t0)
    store._admin_set_partition("nE", False)

    rl.allow("replay_client", now=t0)  # triggers reconciliation once
    rl.allow("replay_client", now=t0)
    rl.allow("replay_client", now=t0)

    total = store.get_bucket("rl:replay_client", int(t0 // 10), node_id="nE")
    # 4 partitioned + 3 online = 7. If the buffer replayed each call, this
    # would balloon well past 7.
    assert total == 7, f"expected exactly 7 counted requests, got {total} (replay bug likely)"


# ---------------------------------------------------------------------------
# 3. Clock skew between nodes
# ---------------------------------------------------------------------------
def test_clock_skew_uses_passed_in_now_not_wall_clock():
    store = MockDistributedStore()
    rl = RateLimiter(store, limit=3, window_seconds=10, node_id="nF")
    skewed_past = 1.0  # far outside any real wall-clock bucket
    for _ in range(3):
        assert rl.allow("skew_client", now=skewed_past) is True
    assert rl.allow("skew_client", now=skewed_past) is False, (
        "implementation appears to ignore the provided `now` and read the "
        "real wall clock instead"
    )


# ---------------------------------------------------------------------------
# 4. Concurrency correctness
# ---------------------------------------------------------------------------
def test_concurrent_requests_never_exceed_limit():
    store = MockDistributedStore()
    limit = 50
    rl = RateLimiter(store, limit=limit, window_seconds=10, node_id="nG")
    t0 = 8_000_000.0
    results = []
    lock = threading.Lock()

    def worker():
        r = rl.allow("concurrent_client", now=t0)
        with lock:
            results.append(r)

    threads = [threading.Thread(target=worker) for _ in range(200)]
    random.shuffle(threads)
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    allowed_count = sum(1 for r in results if r)
    assert allowed_count == limit, (
        f"expected exactly {limit} allowed under concurrent load, got {allowed_count} "
        f"(race condition in shared counter update)"
    )


def test_concurrent_multi_node_never_exceed_limit():
    store = MockDistributedStore()
    limit = 30
    t0 = 8_100_000.0
    results = []
    lock = threading.Lock()

    def worker(node_id):
        rl = RateLimiter(store, limit=limit, window_seconds=10, node_id=node_id)
        r = rl.allow("multi_node_client", now=t0)
        with lock:
            results.append(r)

    threads = [threading.Thread(target=worker, args=(f"node{i%5}",)) for i in range(120)]
    random.shuffle(threads)
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    allowed_count = sum(1 for r in results if r)
    assert allowed_count == limit, f"expected exactly {limit}, got {allowed_count}"


# ---------------------------------------------------------------------------
# 5. Anti-cheat: verify the store is actually being used, not bypassed
# ---------------------------------------------------------------------------
def test_implementation_actually_calls_the_store():
    store = MockDistributedStore()
    rl = RateLimiter(store, limit=5, window_seconds=10, node_id="nH")
    t0 = 9_000_000.0
    for _ in range(3):
        rl.allow("audited_client", now=t0)
    total_calls = store._admin_call_count(node_id="nH")
    assert total_calls >= 3, (
        f"expected the RateLimiter to invoke store methods (with node_id='nH') "
        f"at least once per allow() call, saw {total_calls} calls — implementation "
        f"may be tracking state locally instead of coordinating through the store"
    )


def test_store_file_not_modified():
    store_path = None
    for candidate in (
        os.path.join(os.path.dirname(__file__), "..", "..", "src", "store.py"),
        os.path.join(os.path.dirname(__file__), "..", "..", "environment", "src", "store.py"),
        "/workspace/src/store.py",
    ):
        if os.path.exists(candidate):
            store_path = candidate
            break
    assert store_path is not None, "could not resolve store.py path"

    import hashlib
    expected_hash_path = None
    for candidate in (
        os.path.join(os.path.dirname(__file__), "..", "..", ".store_hash_expected"),
        os.path.join(os.path.dirname(__file__), "..", "..", "environment", ".store_hash_expected"),
        "/workspace/.store_hash_expected",
    ):
        if os.path.exists(candidate):
            expected_hash_path = candidate
            break
    if expected_hash_path is not None:
        with open(store_path, "rb") as f:
            actual = hashlib.sha256(f.read()).hexdigest()
        with open(expected_hash_path) as f:
            expected = f.read().strip()
        assert actual == expected, "store.py has been modified — this file must not be edited"


if __name__ == "__main__":
    import traceback
    tests = [v for k, v in list(globals().items()) if k.startswith("test_")]
    passed, failed = 0, 0
    for t in tests:
        try:
            t()
            print(f"PASS  {t.__name__}")
            passed += 1
        except Exception:
            print(f"FAIL  {t.__name__}")
            traceback.print_exc()
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
