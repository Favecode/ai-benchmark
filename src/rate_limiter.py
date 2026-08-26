"""Reference implementation of RateLimiter (canonical solution)."""
import time
import threading
import uuid

from store import StorePartitionedError


class RateLimiter:
    def __init__(self, store, limit, window_seconds, node_id, degrade_policy="fail_open"):
        if degrade_policy not in ("fail_open", "fail_closed"):
            raise ValueError("degrade_policy must be 'fail_open' or 'fail_closed'")
        self._store = store
        self._limit = limit
        self._window = window_seconds
        self._node_id = node_id
        self._degrade_policy = degrade_policy
        self._lock = threading.Lock()
        # buffered (key, bucket, cost, idempotency_key) tuples accrued while
        # partitioned under fail_open, awaiting reconciliation to the store.
        self._pending = []

    def allow(self, client_id, cost=1, now=None):
        if now is None:
            now = time.time()
        key = f"rl:{client_id}"
        bucket = int(now // self._window)

        with self._lock:
            # Reconcile any pending offline increments first, if we can
            # currently reach the store.
            if not self._store.is_partitioned(self._node_id):
                self._flush_pending()

            try:
                return self._try_allow(key, bucket, cost, now)
            except StorePartitionedError:
                if self._degrade_policy == "fail_open":
                    idempotency_key = str(uuid.uuid4())
                    self._pending.append((key, bucket, cost, idempotency_key))
                    return True
                else:
                    return False

    # ---- internals ----

    def _flush_pending(self):
        if not self._pending:
            return
        still_pending = []
        for (key, bucket, cost, idem) in self._pending:
            try:
                self._store.increment(key, bucket, cost, node_id=self._node_id,
                                       idempotency_key=idem)
            except StorePartitionedError:
                still_pending.append((key, bucket, cost, idem))
        self._pending = still_pending

    def _try_allow(self, key, bucket, cost, now):
        prev_bucket = bucket - 1
        elapsed = now - bucket * self._window
        weight_prev = max(0.0, (self._window - elapsed) / self._window)

        # Optimistic increment-then-check-then-rollback: atomic at the
        # store level, so concurrent callers (same or different nodes)
        # never overshoot the limit even under heavy contention.
        idempotency_key = str(uuid.uuid4())
        new_curr = self._store.increment(key, bucket, cost, node_id=self._node_id,
                                          idempotency_key=idempotency_key)
        prev_count = self._store.get_bucket(key, prev_bucket, node_id=self._node_id)

        estimated_total = prev_count * weight_prev + new_curr

        if estimated_total <= self._limit:
            return True
        else:
            # Roll back: this request doesn't fit, undo the speculative add.
            self._store.increment(key, bucket, -cost, node_id=self._node_id,
                                   idempotency_key=str(uuid.uuid4()))
            return False
