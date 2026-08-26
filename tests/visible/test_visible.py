"""
Visible tests. You should get all of these passing before you consider
your implementation done — but passing these alone is NOT sufficient to
pass the benchmark. Hidden tests cover partition/reconciliation,
concurrency, clock skew, and cost-weighted edge cases not exercised here.
"""
import sys
import os

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
for candidate in (
    os.path.join(ROOT, "src"),
    os.path.join(ROOT, "environment", "src"),
    "/workspace/src",
):
    if os.path.isdir(candidate):
        sys.path.insert(0, candidate)
        break

from store import MockDistributedStore
from rate_limiter import RateLimiter


def test_basic_allow_under_limit():
    store = MockDistributedStore()
    rl = RateLimiter(store, limit=5, window_seconds=10, node_id="n1")
    t0 = 1_000_000.0
    for _ in range(5):
        assert rl.allow("clientA", now=t0) is True
    assert rl.allow("clientA", now=t0) is False, "6th request in same window should be denied"


def test_window_slides_and_recovers():
    store = MockDistributedStore()
    rl = RateLimiter(store, limit=4, window_seconds=10, node_id="n1")
    t0 = 1_000_000.0
    for _ in range(4):
        assert rl.allow("clientB", now=t0) is True
    assert rl.allow("clientB", now=t0) is False

    # Fully outside the window (2x window size later) -> should be fully allowed again
    t1 = t0 + 25
    for _ in range(4):
        assert rl.allow("clientB", now=t1) is True


def test_independent_clients_dont_share_budget():
    store = MockDistributedStore()
    rl = RateLimiter(store, limit=2, window_seconds=10, node_id="n1")
    t0 = 2_000_000.0
    assert rl.allow("alice", now=t0) is True
    assert rl.allow("alice", now=t0) is True
    assert rl.allow("alice", now=t0) is False
    # bob has his own budget
    assert rl.allow("bob", now=t0) is True
    assert rl.allow("bob", now=t0) is True
    assert rl.allow("bob", now=t0) is False


def test_cost_weighted_requests():
    store = MockDistributedStore()
    rl = RateLimiter(store, limit=10, window_seconds=10, node_id="n1")
    t0 = 3_000_000.0
    assert rl.allow("heavy", cost=7, now=t0) is True
    assert rl.allow("heavy", cost=3, now=t0) is True
    assert rl.allow("heavy", cost=1, now=t0) is False, "budget exhausted, even cost-1 should fail"


def test_multi_node_shares_state_via_store():
    store = MockDistributedStore()
    rl_node1 = RateLimiter(store, limit=3, window_seconds=10, node_id="n1")
    rl_node2 = RateLimiter(store, limit=3, window_seconds=10, node_id="n2")
    t0 = 4_000_000.0
    assert rl_node1.allow("shared_client", now=t0) is True
    assert rl_node2.allow("shared_client", now=t0) is True
    assert rl_node1.allow("shared_client", now=t0) is True
    # 4th request across both nodes combined should be denied
    assert rl_node2.allow("shared_client", now=t0) is False


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
