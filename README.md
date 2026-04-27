# Multi-API Aggregator

A distributed-ready async aggregation layer for orchestrating calls across many external APIs under a **shared global rate budget**, with **priority-aware scheduling**, **per-API circuit breakers**, **adaptive throttling**, and **bounded-fairness queue aging**.

Built to spec against the PRD in this repo. All seven functional requirements (FR1–FR7) are implemented and covered by tests.

```
                          submit()
                              │
                              ▼
                ┌──────────────────────────┐
                │  Aging Priority Queue    │  <─── aging factor
                │  HIGH | MED | LOW        │       (no starvation)
                └────────────┬─────────────┘
                             │ peek best non-excluded
                             ▼
              ┌──────────────────────────────┐
              │           Scheduler          │
              │  ┌─────────────────────────┐ │
              │  │  Shared Rate Limit Pool │ │  global + per-API token buckets
              │  │  Circuit Breakers (×n)  │ │  CLOSED / HALF_OPEN / OPEN
              │  │  Worker Semaphore       │ │  bounded concurrency
              │  └─────────────────────────┘ │
              └────────────┬─────────────────┘
                           │ dispatch
              ┌────────────┴────────────────┐
              ▼            ▼                ▼
          Adapter A    Adapter B        Adapter C
              │            │                │
              ▼            ▼                ▼
        ┌─── outcome feedback ───────────────┐
        │                                    │
        ▼                                    ▼
   Adaptive Throttler                   Metrics
   (adjusts bucket rate                 (counters,
    on rolling error rate)               latency,
                                         throughput)
```

## Quick start

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest                    # 42 tests, ~15s, 86% coverage
python examples/demo.py   # mixed-priority workload against 3 mock APIs
```

## Public API in 30 seconds

```python
from aggregator.common import AggregatorConfig, APIConfig, Priority, Request
from aggregator.adapters.base import HTTPAdapter
from aggregator.orchestrator.orchestrator import Aggregator

cfg = AggregatorConfig(
    global_rate=100,
    apis=[
        APIConfig(name="sam_gov", rate=10),
        APIConfig(name="usaspending", rate=20),
    ],
)
adapters = [
    HTTPAdapter("sam_gov", base_url="https://api.sam.gov"),
    HTTPAdapter("usaspending", base_url="https://api.usaspending.gov"),
]

async with Aggregator(cfg, adapters) as agg:
    response = await agg.submit(Request(
        api="sam_gov",
        method="GET",
        url="/opportunities/v2/search",
        params={"limit": 10},
        priority=Priority.HIGH,
    ))
```

## Layout

```
aggregator/
├── orchestrator/      Aggregator façade — submit(), stats(), lifecycle
├── scheduler/         Central dispatch loop (no per-API isolated schedulers)
├── rate_limiter/      Token bucket + atomic global+per-API SharedRateLimiter
├── priority_queue/    Heap-based queue with aging
├── circuit_breaker/   Per-API CLOSED/HALF_OPEN/OPEN state machine
├── retry/             Exponential backoff + jitter, priority decay
├── throttler/         Adaptive AIMD-style rate adjustment
├── adapters/          BaseAdapter, HTTPAdapter (aiohttp), CallableAdapter
├── mock_apis/         Three failure profiles for tests/demo
├── metrics/           Counters, gauges, percentile histograms
└── common/            Shared types, config dataclasses, exception hierarchy
```

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the scheduling algorithm, rate allocation logic, failure scenarios, and tuning guide.

## What this demonstrates

| Capability | Implementation | Test |
|---|---|---|
| Centralized scheduling | `scheduler/scheduler.py` is the single dispatch loop | `tests/test_integration.py::test_basic_submit_returns_response` |
| Shared global rate budget | `SharedRateLimiter` atomically acquires from global + per-API | `tests/test_shared_pool.py` |
| Priority execution | `AgingPriorityQueue.peek_pending` picks min effective priority | `tests/test_integration.py::test_high_priority_preempts_low` |
| Bounded fairness | Aging factor promotes waiting requests | `tests/test_integration.py::test_starvation_bound_via_aging` |
| Adaptive behavior | `AdaptiveThrottler` AIMD on rolling error rate | `tests/test_adaptive_throttler.py` |
| API isolation | Per-API circuit breakers + per-API buckets | `tests/test_integration.py::test_failure_isolation_across_apis` |
| Retry integration | `RetryPolicy` re-enqueues with priority decay | `tests/test_integration.py::test_retry_promotes_through_priority_decay` |

Plus: in-flight **deduplication** (identical pending requests share one dispatch), **backpressure** (LOW priority dropped past threshold), **observability** (latency p50/p95/p99 per API, queue depth gauges, throughput counters).

## Design constraints

- No blocking calls — fully `asyncio` + `aiohttp`
- Single central scheduler — no per-API isolated schedulers
- No hardcoded limits — every threshold lives in `common/config.py`
- Config-driven — `AggregatorConfig` + per-API `APIConfig`
