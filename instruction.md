# Sliding-Window Rate Limiter Under Network Partition

## Background

You're working on the API gateway team at a payments company. The current
rate limiter is single-node only — it worked fine when the service ran on
one box, but the service now runs as multiple stateless instances behind a
load balancer, and the naive per-process counter no longer enforces limits
correctly (a client can get `N × num_instances` requests through instead of
`N`).

You've been given a coordination layer — `src/store.py` — that models a
shared external key-value store (think Redis) that all instances can read
and write to. It also simulates realistic failure modes: nodes can become
temporarily "partitioned" from the store (unable to reach it), and calls
can carry injected network latency. **You must not modify `src/store.py`.**
Its contents are fixed and hashed; any change will cause grading to abort
before your solution is even scored.

Your job is to implement `src/rate_limiter.py`, specifically the
`RateLimiter` class, so that it correctly enforces a shared rate limit
across any number of coordinating nodes, using only the `store` object
passed into its constructor.

## What you must implement

Fill in `RateLimiter.__init__` and `RateLimiter.allow` in
`src/rate_limiter.py` according to the docstring contract already present
in that file. In summary:

- **Algorithm**: implement a *sliding window* rate limiter (not a fixed/
  tumbling window — a fixed window has a well-known bug where a client can
  burst up to `2×limit` requests around a window boundary; your
  implementation must not have this bug).
- **Multi-node coordination**: all state that determines whether a request
  is allowed must be coordinated through the provided `store`, not kept
  only in local process memory. Multiple `RateLimiter` instances (with
  different `node_id`s) sharing the same `store` object must together
  enforce a single combined budget per `client_id`.
- **Cost-weighted requests**: the `allow(client_id, cost=1, now=None)`
  method must support a `cost` parameter — a single call can consume more
  than 1 unit of the client's budget.
- **Deterministic time**: honor the `now` parameter when provided rather
  than always reading the real wall clock. Tests will pass explicit
  timestamps (including deliberately "skewed" ones) to verify your
  window-bucketing logic directly.
- **Partition tolerance**: `store` calls will raise
  `store.StorePartitionedError` when the calling node is currently
  partitioned from the store (see `store.py` for the exact exception type
  and when it's raised). Your `RateLimiter` must respect the
  `degrade_policy` passed to its constructor:
  - `"fail_open"`: when the store is unreachable, allow the request
    locally rather than blocking the client on an infrastructure issue.
  - `"fail_closed"`: when the store is unreachable, deny the request
    rather than risk exceeding the limit.
- **Partition healing / reconciliation**: this is the crux of the task.
  Any requests allowed locally by a `fail_open` node during a partition
  still consume real budget — once the node can reach the store again,
  it must reconcile those requests into the shared state **exactly once**.
  Losing them (silent under-counting) and re-applying them more than once
  (double-counting, which could wrongly deny a client who never actually
  exceeded their limit) are both incorrect.
- **Concurrency**: a single `RateLimiter` instance may be called from
  multiple threads concurrently, and multiple `RateLimiter` instances
  (representing different nodes) may call the shared store concurrently.
  In both cases, the number of requests ultimately allowed for a given
  client within a window must never exceed the configured limit, even
  under heavy concurrent contention.

## Constraints

- Standard library only — no network access is available inside the
  grading sandbox, and no third-party packages will be installed.
- Do not modify `src/store.py`. You may read it freely to understand the
  interface it exposes (that's expected and necessary).
- Do not change the `RateLimiter` class name or the signatures of
  `__init__` / `allow` — the grading harness instantiates and calls this
  exact interface.
- Do not add sleep-based timing hacks to "win races" against the test
  harness; your correctness must hold structurally, not by chance timing.

## Validating your work

Run the visible tests as you go:

```
python3 tests/visible/test_visible.py
```

Passing all visible tests is necessary but **not sufficient** — final
grading runs an additional hidden test suite covering sliding-window
precision at bucket boundaries, partition/reconciliation edge cases,
clock skew, and concurrency correctness, none of which are shown to you.
A correct, general implementation of the contract above will pass all of
it; solutions that special-case the visible test scenarios will not.

## Acceptance criteria

Your solution passes grading when `bash tests/test.sh` exits with status 0.
