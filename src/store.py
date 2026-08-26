"""
MockDistributedStore
=====================
A thread-safe, in-process stand-in for a shared external store (e.g. Redis)
used to simulate a multi-node deployment without requiring real network
infrastructure. This file is part of the fixed benchmark harness.

DO NOT MODIFY THIS FILE. Its SHA-256 hash is checked by the grading script
before hidden tests run; any change causes an automatic 0 score.

Semantics this store provides (and that a real distributed KV store would
plausibly provide too):

  - increment(key, bucket, amount, idempotency_key=None) -> int
        Atomically adds `amount` to counts[key][bucket] and returns the new
        total for that bucket. If `idempotency_key` is provided and has been
        seen before for this (key, bucket) pair, the call is a no-op and the
        *current* value is returned instead (exactly-once semantics, as a
        real production store with dedup would offer).

  - get_bucket(key, bucket) -> int
        Read-only fetch of a bucket's current count. Never raises.

  - is_partitioned(node_id) -> bool
        Whether the given node currently cannot reach the store. Calling any
        mutating method while partitioned raises StorePartitionedError.
        Calling get_bucket while partitioned also raises, to accurately model
        "no connectivity" rather than "connectivity but read-only".

Test harnesses (visible/hidden) call `_admin_set_partition` and
`_admin_set_latency_ms` to control the simulated environment. These
admin methods are NOT part of the interface a rate limiter implementation
should call.
"""

import hashlib
import threading
import time


class StorePartitionedError(Exception):
    """Raised when a node attempts to reach the store while partitioned."""
    pass


class MockDistributedStore:
    def __init__(self):
        self._lock = threading.RLock()
        self._counts = {}          # (key, bucket) -> int
        self._seen_idempotency = {}  # (key, bucket) -> set of idempotency keys
        self._partitioned_nodes = set()
        self._latency_ms = 0
        self._call_log = []        # list of (node_id, method, ts) for anti-cheat auditing

    # ---- public interface (what a RateLimiter implementation may call) ----

    def increment(self, key, bucket, amount, node_id=None, idempotency_key=None):
        self._maybe_raise_if_partitioned(node_id)
        self._simulate_latency()
        with self._lock:
            self._call_log.append((node_id, "increment", time.time()))
            dedup_set = self._seen_idempotency.setdefault((key, bucket), set())
            if idempotency_key is not None:
                if idempotency_key in dedup_set:
                    return self._counts.get((key, bucket), 0)
                dedup_set.add(idempotency_key)
            new_val = self._counts.get((key, bucket), 0) + amount
            self._counts[(key, bucket)] = new_val
            return new_val

    def get_bucket(self, key, bucket, node_id=None):
        self._maybe_raise_if_partitioned(node_id)
        self._simulate_latency()
        with self._lock:
            self._call_log.append((node_id, "get_bucket", time.time()))
            return self._counts.get((key, bucket), 0)

    def is_partitioned(self, node_id):
        with self._lock:
            return node_id in self._partitioned_nodes

    # ---- admin / test-harness-only methods ----

    def _admin_set_partition(self, node_id, partitioned):
        with self._lock:
            if partitioned:
                self._partitioned_nodes.add(node_id)
            else:
                self._partitioned_nodes.discard(node_id)

    def _admin_set_latency_ms(self, ms):
        with self._lock:
            self._latency_ms = ms

    def _admin_call_count(self, node_id=None, method=None):
        with self._lock:
            return sum(
                1 for (n, m, _) in self._call_log
                if (node_id is None or n == node_id) and (method is None or m == method)
            )

    def _admin_snapshot(self):
        with self._lock:
            return dict(self._counts)

    # ---- internals ----

    def _maybe_raise_if_partitioned(self, node_id):
        if node_id is not None and self.is_partitioned(node_id):
            raise StorePartitionedError(f"node {node_id!r} is partitioned from the store")

    def _simulate_latency(self):
        if self._latency_ms:
            time.sleep(self._latency_ms / 1000.0)


def file_sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        h.update(f.read())
    return h.hexdigest()
