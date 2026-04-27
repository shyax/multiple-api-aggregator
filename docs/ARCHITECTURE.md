# Architecture

## 1. Why a central scheduler?

Naive multi-API codebases end up with one rate limiter per API, each running on its own clock, each unaware of the others. Two failure modes follow:

1. **Global budget overrun.** The sum of per-API limits exceeds the host's outbound capacity, network egress, or upstream contractual ceiling.
2. **Capacity misallocation.** A high-volume low-priority API can starve a low-volume high-priority one because the limiters don't see each other's traffic.

This system fixes both by routing every outbound call through a single scheduler that consults *one* shared rate-limit pool and *one* shared priority queue.

## 2. Component map

```
        ┌────────────────────────────┐
        │       Aggregator           │   public façade
        │   (orchestrator/orchestrator.py)
        └─────────┬──────────────────┘
                  │ submit(Request)
                  ▼
        ┌────────────────────────────┐
        │   AgingPriorityQueue       │   priority + aging + backpressure
        │  (priority_queue/queue.py) │
        └─────────┬──────────────────┘
                  │ peek_pending(exclude=...)
                  ▼
        ┌────────────────────────────┐
        │       Scheduler            │   dispatch loop
        │   (scheduler/scheduler.py) │
        └─┬────────┬─────────┬───────┘
          │        │         │
          ▼        ▼         ▼
   SharedRateLimiter  CircuitBreaker  AdapterPool
   (global+per-API    (per API)       (BaseAdapter
    token buckets)                     instances)
                              │
                              ▼ outcome
                  ┌────────────────────────┐
                  │  AdaptiveThrottler     │   AIMD on rolling error rate
                  │  (throttler/adaptive)  │
                  └────────────────────────┘
```

## 3. Scheduling algorithm

The dispatch loop runs one coroutine. Each iteration:

1. **Acquire a worker slot.** A semaphore caps concurrent in-flight dispatches at `worker_count`. This bounds the resource footprint independent of queue depth.
2. **Pick the next dispatchable request.** Compute the set of *unservable APIs*:
   - circuit breaker is `OPEN`
   - rate limiter `time_until(api) > 0.005s`
   Then `peek_pending(exclude_apis=...)` scans the heap and returns the request with the lowest **effective priority**.
3. **Pop that specific request** from the queue (`pop_specific`).
4. **Launch dispatch as a task.** The semaphore is held until the task completes, then released via callback. The scheduler immediately loops back to pick the next request — pipelining as deep as `worker_count`.
5. **In the dispatch task:**
   - re-check circuit breaker (`allow()`)
   - atomically acquire one global + one per-API token (`SharedRateLimiter.try_acquire`)
   - call `adapter.execute(request)`
   - record outcome (success / 4xx / 5xx / 429 / timeout) into circuit breaker, throttler, and metrics
   - on retryable failure: consult `RetryPolicy`, schedule re-enqueue with backoff and priority decay
   - on success: complete the request's pending future

### Why peek-then-pop instead of just `get()`?

A blocking `get()` returns whatever sits at the heap root, even if its API is unservable right now. The scheduler would then have to put it back, having wasted a worker slot.  Peek-then-pop lets us **skip past unservable items** — so a stuck API never blocks sibling APIs.

### Effective priority and aging

```
effective_priority = base_priority - wait_time * aging_factor
```

Lower = better (min-heap semantics).

- A fresh `HIGH` (priority 0) starts at `0`.
- A `LOW` (priority 100) waiting 50 seconds at `aging_factor=2` has effective `100 - 100 = 0` — it now ties HIGH.

This guarantees **bounded fairness** (PRD FR4): a LOW request can wait at most `(LOW - HIGH) / aging_factor` seconds in the worst case before it beats fresh HIGHs.

## 4. Rate allocation logic

### Two-level token bucket

Every API is constrained by **two** independent token buckets:

| Bucket | Refill rate | Capacity | Purpose |
|---|---|---|---|
| Global | `AggregatorConfig.global_rate` tokens/sec | `global_capacity` | Hard cap on total outbound traffic |
| Per-API | `APIConfig.rate` | `APIConfig.capacity` | Honour upstream's per-API contract |

A request is dispatched only when **both** buckets have ≥ 1 token. The acquire is **atomic** — if the global bucket pays but the per-API bucket can't, we **refund the global** so we never under-count global capacity.

### Why fractional tokens?

The bucket tracks `tokens` as a float. At low refill rates (e.g. 0.5/sec), an integer-only bucket exhibits step-function behaviour and bursts unpredictably across the second boundary. Fractional refill smooths this out and matches the analytical token-bucket model.

### Adaptive adjustment

`AdaptiveThrottler` watches per-API outcomes (rolling window of `error_window` events; 429s count double) and updates the **per-API bucket's `rate`** in place:

- error rate ≥ `high_error_rate`: `rate *= step_down` (default 0.5 — aggressive cut)
- error rate ≤ `low_error_rate`: `rate *= step_up` (default 1.10 — gentle recovery)
- bounded to `[base_rate * min_rate_factor, base_rate * max_rate_factor]`

The operator-declared base rate is a **ceiling**: adaptive control can lower it but never raise it past `max_rate_factor` (default 1.0).

## 5. Failure scenarios and how the system responds

| Scenario | What the system does |
|---|---|
| Upstream returns `429` | Adapter raises `RateLimitedError`. Scheduler records it, retries with `Retry-After` honoured if present, else exponential backoff with jitter. AdaptiveThrottler records 429 with **2× weight**, accelerating rate cut. |
| Upstream returns `5xx` | `UpstreamError`. Circuit breaker `on_failure()`. After `failure_threshold` consecutive failures, breaker opens. Scheduler skips that API entirely until `recovery_timeout` elapses. |
| Upstream returns `4xx` | `NonRetryableError`. Future fails immediately, no retry. |
| Upstream times out | Adapter wraps `TimeoutError` as `UpstreamError`. Same path as 5xx. |
| Local rate limiter says wait | Scheduler peeks past this API, dispatches sibling APIs in the meantime. |
| Circuit breaker is OPEN | Scheduler peeks past this API entirely. Background re-enqueue with priority decay so the request retries when the breaker probes (HALF_OPEN). |
| Circuit breaker is HALF_OPEN | Exactly `half_open_max_probes` requests get through. Each success counts toward `success_threshold`; one failure re-OPENs. |
| Queue near capacity | Submissions past `drop_low_priority_threshold` reject `LOW`-priority work synchronously with `NonRetryableError`. HIGH/MEDIUM still accepted up to `queue_max_size`. |
| Identical request already in flight | Dedup layer returns the existing future. Useful when many agents query the same endpoint with the same params. |
| Process restart | Adapters and pending futures are torn down cleanly via `Aggregator.stop()`. Any in-flight futures resolve with `SchedulerStoppedError` so callers don't hang. |

## 6. Observability

`Aggregator.stats()` emits a JSON-serializable snapshot:

```python
{
  "queue_depth": 12,
  "queue_per_api": {"api_a": 3, "api_b": 9},
  "queue_per_priority": {0: 1, 50: 7, 100: 4},
  "rate_limiter": {"global": {"tokens": 8.2, "rate": 100.0}, "apis": {...}},
  "circuit_breakers": {"api_a": "closed", "api_b": "open"},
  "throttler": {"api_a": {"factor": 1.0, "error_rate": 0.0}, ...},
  "metrics": {
    "counters": {"requests_submitted": 200, "requests_succeeded": 189, ...},
    "latency": {"queue_wait_seconds": {"p50": 0.07, "p95": 0.42, ...}},
    "per_api": {"api_b": {"events": {"upstream_error": 36}, "latency": {...}}}
  }
}
```

Drop this into structured logs, ship to a metrics backend (Prometheus, Datadog), or render as a dashboard.

## 7. Tuning guide

| If you observe... | Tune... |
|---|---|
| API getting 429s despite our rate limit | Lower `APIConfig.rate` (or `capacity` for burstiness). The mock test `test_per_api_rate_limit_respected` shows how. |
| HIGH requests delayed behind LOW | Lower `aging_factor` (less promotion of LOWs) or increase `worker_count` (more parallelism). |
| LOW requests starving | Raise `aging_factor`. The starvation bound is `(LOW - HIGH) / aging_factor` seconds. |
| Circuit breaker flapping (open/close cycles) | Raise `failure_threshold` (less sensitive) or `recovery_timeout` (longer cool-down). |
| Adaptive throttler over-cutting | Raise `high_error_rate` (less sensitive trigger) or `min_rate_factor` (floor for the cut). |
| Queue grows unboundedly | Raise `worker_count` if CPU-bound; lower `global_rate` if upstream-bound; tune `drop_low_priority_threshold` to shed LOW work earlier. |
| p95 queue wait too high for HIGH | Raise `worker_count`, lower `global_rate` (counterintuitive — fewer items queued means less wait), or split workload to a dedicated `Aggregator` instance for HIGH-only work. |
| Memory growth | Lower `queue_max_size`, ensure consumers aren't leaking by not awaiting `submit()`. |

## 8. Determinism and testing

- All clocks injectable via `clock=` parameter on `TokenBucket`, `SharedRateLimiter`, `AgingPriorityQueue`, `CircuitBreaker`. Tests use a `FakeClock` (see `tests/conftest.py`) to advance time without sleeping.
- Mock APIs (`mock_apis/mocks.py`) accept seeded RNGs so failure-injection tests are reproducible.
- Integration tests use `CallableAdapter` with in-process handlers — no network, no flakiness.

## 9. Extension paths (PRD §18)

The current design is a single-process aggregator. The natural distributed variant:

- **Distributed scheduler:** replace the in-memory queue with Redis Streams or a Postgres-backed work queue; replace the lock-based `SharedRateLimiter` with a Lua script on Redis (`INCR` + `EXPIRE`) for atomic global token accounting across nodes.
- **SLA-based scheduling:** extend `Priority` to carry a deadline; `peek_pending` becomes earliest-deadline-first with overdue requests promoted to HIGH automatically.
- **Dynamic priority policies:** make `aging_factor` and `priority_decay` per-API so different APIs have different fairness regimes.
- **Persistent retry queue:** for crash recovery, persist enqueued requests to disk (SQLite, RocksDB) and replay on restart — required for the launchd-managed deployment in the SOW.
