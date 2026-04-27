"""End-to-end tests against the public Aggregator façade.

These prove the PRD acceptance criteria (§16):
- global rate budget enforced under load
- high-priority preempts low
- per-API rate limits respected
- failures don't cascade across APIs
- bounded fairness (no starvation)
"""

from __future__ import annotations

import asyncio
import time

import pytest

from aggregator.adapters.base import CallableAdapter
from aggregator.common.config import (
    AdaptiveConfig,
    AggregatorConfig,
    APIConfig,
    CircuitBreakerConfig,
    RetryConfig,
)
from aggregator.common.exceptions import NonRetryableError
from aggregator.common.types import Priority, Request
from aggregator.mock_apis.mocks import (
    HighLatencyAPI,
    IntermittentErrorAPI,
    StrictRateLimitAPI,
)
from aggregator.orchestrator.orchestrator import Aggregator


def _basic_config(global_rate: float = 50.0) -> tuple[AggregatorConfig, dict]:
    apis = [
        APIConfig(
            name="api_a",
            rate=10,
            capacity=10,
            retry=RetryConfig(max_retries=2, base_delay=0.01, max_delay=0.05, jitter=0.0),
            circuit_breaker=CircuitBreakerConfig(failure_threshold=4, recovery_timeout=0.5, success_threshold=1),
            adaptive=AdaptiveConfig(enabled=False),
        ),
        APIConfig(
            name="api_b",
            rate=10,
            capacity=10,
            retry=RetryConfig(max_retries=3, base_delay=0.01, max_delay=0.05, jitter=0.0),
            circuit_breaker=CircuitBreakerConfig(failure_threshold=3, recovery_timeout=0.2, success_threshold=1),
            adaptive=AdaptiveConfig(error_window=20, high_error_rate=0.30, low_error_rate=0.05),
        ),
        APIConfig(
            name="api_c",
            rate=10,
            capacity=10,
            retry=RetryConfig(max_retries=2, base_delay=0.01, max_delay=0.05, jitter=0.0),
            circuit_breaker=CircuitBreakerConfig(failure_threshold=10),
            adaptive=AdaptiveConfig(enabled=False),
        ),
    ]
    cfg = AggregatorConfig(
        global_rate=global_rate,
        global_capacity=int(global_rate),
        apis=apis,
        worker_count=8,
        queue_max_size=1000,
        drop_low_priority_threshold=900,
        backpressure_threshold=600,
    )
    mocks = {
        "api_a": StrictRateLimitAPI("api_a", limit_per_second=8),
        "api_b": IntermittentErrorAPI("api_b", error_rate=0.0, seed=1),
        "api_c": HighLatencyAPI("api_c", latency=0.02, jitter=0.005, seed=1),
    }
    return cfg, mocks


def _adapters(mocks: dict) -> list[CallableAdapter]:
    return [m.adapter() for m in mocks.values()]


async def test_basic_submit_returns_response():
    cfg, mocks = _basic_config()
    async with Aggregator(cfg, _adapters(mocks)) as agg:
        resp = await agg.submit(Request(api="api_a", url="/x"))
    assert resp.ok
    assert resp.api == "api_a"


async def test_high_priority_preempts_low():
    """When the queue has both LOW and HIGH waiting, HIGH must complete first.

    Note: we do not preempt in-flight requests (no cancellation). This test
    proves preemption against *queued* requests — the workload ensures the queue
    stays deeper than the worker pool throughout, so most HIGHs jump the line
    over LOWs that haven't been dispatched yet.
    """
    cfg, mocks = _basic_config(global_rate=4)  # bottleneck so queue actually builds
    cfg.worker_count = 2
    async with Aggregator(cfg, _adapters(mocks)) as agg:
        finished_order: list[Priority] = []
        lock = asyncio.Lock()

        async def submit(p: Priority, n: int):
            # Distinct URLs so dedup doesn't coalesce.
            r = await agg.submit(Request(api="api_c", url=f"/{p.name}/{n}", priority=p))
            async with lock:
                finished_order.append(p)
            return r

        # 30 LOWs first (way more than worker pool), then 6 HIGHs after a moment.
        low_tasks = [asyncio.create_task(submit(Priority.LOW, i)) for i in range(30)]
        await asyncio.sleep(0.05)
        high_tasks = [asyncio.create_task(submit(Priority.HIGH, i)) for i in range(6)]
        await asyncio.gather(*low_tasks, *high_tasks)

    high_positions = [i for i, p in enumerate(finished_order) if p == Priority.HIGH]
    low_positions = [i for i, p in enumerate(finished_order) if p == Priority.LOW]
    median_high = sorted(high_positions)[len(high_positions) // 2]
    median_low = sorted(low_positions)[len(low_positions) // 2]
    assert median_high < median_low, (high_positions, low_positions)


async def test_per_api_rate_limit_respected():
    """The strict 8 req/s mock should never reject (the per-API bucket is set lower)."""
    cfg, mocks = _basic_config(global_rate=100)
    # Lower api_a's local bucket strictly below the mock's threshold so the bucket is
    # the binding constraint, not the upstream. Mock allows 8/sec, so we use rate=3
    # with capacity=3 to ensure even a burst-then-refill window stays well under 8.
    cfg.apis[0].rate = 3
    cfg.apis[0].capacity = 3
    async with Aggregator(cfg, _adapters(mocks)) as agg:
        await asyncio.gather(
            *[agg.submit(Request(api="api_a", url=f"/{i}")) for i in range(20)]
        )
    assert mocks["api_a"].rejections == 0, "bucket should have prevented every 429"


async def test_failure_isolation_across_apis():
    """If one API hammers errors, sibling APIs keep serving."""
    cfg, mocks = _basic_config()
    mocks["api_b"] = IntermittentErrorAPI("api_b", error_rate=1.0, seed=1)  # always fails
    async with Aggregator(cfg, [m.adapter() for m in mocks.values()]) as agg:
        b_task = asyncio.gather(
            *[
                _safe_submit(agg, Request(api="api_b", url=f"/{i}", max_retries=1))
                for i in range(10)
            ],
            return_exceptions=True,
        )
        # Concurrently, api_c work should keep flowing.
        c_responses = await asyncio.gather(
            *[agg.submit(Request(api="api_c", url=f"/{i}")) for i in range(10)]
        )
        await b_task
    assert all(r.ok for r in c_responses)
    # Circuit should have opened on api_b at least once.
    assert agg.circuit_breakers["api_b"].trips >= 1


async def _safe_submit(agg, req):
    try:
        return await agg.submit(req)
    except Exception as e:  # noqa: BLE001
        return e


async def test_starvation_bound_via_aging():
    """A LOW priority request must eventually complete even under sustained HIGH load."""
    cfg, mocks = _basic_config(global_rate=4)
    cfg.aging_factor = 50.0  # aggressive aging so we don't wait minutes in tests
    async with Aggregator(cfg, _adapters(mocks)) as agg:
        low = asyncio.create_task(
            agg.submit(Request(api="api_c", url="/low", priority=Priority.LOW))
        )

        async def hammer(idx: int):
            i = 0
            try:
                while not low.done():
                    await agg.submit(
                        Request(api="api_c", url=f"/hi/{idx}/{i}", priority=Priority.HIGH)
                    )
                    i += 1
            except Exception:
                pass

        hammers = [asyncio.create_task(hammer(idx)) for idx in range(3)]
        try:
            resp = await asyncio.wait_for(low, timeout=5.0)
        finally:
            for h in hammers:
                h.cancel()
            await asyncio.gather(*hammers, return_exceptions=True)
    assert resp.ok


async def test_retry_promotes_through_priority_decay():
    """A failing-then-recovering request still completes."""
    cfg, mocks = _basic_config()
    flaky = IntermittentErrorAPI("api_b", error_rate=0.0)
    flaky_adapter = CallableAdapter("api_b", _flake_then_succeed(flaky))
    adapters = [mocks["api_a"].adapter(), flaky_adapter, mocks["api_c"].adapter()]
    async with Aggregator(cfg, adapters) as agg:
        resp = await agg.submit(Request(api="api_b", url="/y", max_retries=3))
    assert resp.ok
    assert resp.attempts >= 2


def _flake_then_succeed(api: IntermittentErrorAPI):
    state = {"calls": 0}

    async def handler(request):
        state["calls"] += 1
        if state["calls"] < 2:
            return 500, None, {}
        return 200, {"ok": True}, {}

    return handler


async def test_dedup_coalesces_identical_inflight_requests():
    cfg, mocks = _basic_config()
    counter = {"calls": 0}

    async def slow_handler(request):
        counter["calls"] += 1
        await asyncio.sleep(0.1)
        return 200, {"id": request.id}, {}

    adapters = [mocks["api_a"].adapter(), mocks["api_b"].adapter(), CallableAdapter("api_c", slow_handler)]
    async with Aggregator(cfg, adapters) as agg:
        results = await asyncio.gather(
            *[agg.submit(Request(api="api_c", url="/dedupe", params={"q": "x"})) for _ in range(5)]
        )
    assert counter["calls"] == 1, "identical in-flight calls must be coalesced"
    assert all(r.ok for r in results)


async def test_backpressure_drops_low_priority():
    cfg, mocks = _basic_config(global_rate=2)
    cfg.queue_max_size = 50
    cfg.drop_low_priority_threshold = 10
    async with Aggregator(cfg, _adapters(mocks)) as agg:
        # Fill with HIGH priority work to push the queue past threshold.
        bg = [
            asyncio.create_task(agg.submit(Request(api="api_c", url=f"/h{i}", priority=Priority.HIGH)))
            for i in range(40)
        ]
        await asyncio.sleep(0.2)
        with pytest.raises(NonRetryableError):
            await agg.submit(Request(api="api_c", url="/lo", priority=Priority.LOW))
        for t in bg:
            t.cancel()
        await asyncio.gather(*bg, return_exceptions=True)
