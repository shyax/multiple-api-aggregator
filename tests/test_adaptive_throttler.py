import asyncio

from aggregator.adapters.base import CallableAdapter
from aggregator.common.config import (
    AdaptiveConfig,
    AggregatorConfig,
    APIConfig,
    CircuitBreakerConfig,
    RetryConfig,
)
from aggregator.common.types import Request
from aggregator.orchestrator.orchestrator import Aggregator
from aggregator.rate_limiter.shared_pool import SharedRateLimiter
from aggregator.throttler.adaptive import AdaptiveThrottler


def test_throttles_down_on_high_error_rate():
    pool = SharedRateLimiter(
        global_rate=100,
        global_capacity=100,
        api_configs=[APIConfig(name="x", rate=20, capacity=20)],
    )
    cfg = AdaptiveConfig(
        enabled=True,
        error_window=20,
        high_error_rate=0.30,
        low_error_rate=0.05,
        step_down=0.5,
        step_up=1.10,
    )
    t = AdaptiveThrottler(pool, {"x": 20}, {"x": cfg})
    # Pump in lots of failures
    for _ in range(20):
        t.record("x", success=False)
    assert t.factor("x") < 1.0
    assert pool.api_buckets["x"].rate < 20.0


def test_throttles_back_up_after_recovery():
    pool = SharedRateLimiter(
        global_rate=100,
        global_capacity=100,
        api_configs=[APIConfig(name="x", rate=20, capacity=20)],
    )
    cfg = AdaptiveConfig(
        enabled=True,
        error_window=20,
        high_error_rate=0.30,
        low_error_rate=0.05,
        step_down=0.5,
        step_up=1.10,
    )
    t = AdaptiveThrottler(pool, {"x": 20}, {"x": cfg})
    for _ in range(20):
        t.record("x", success=False)
    pulled_down = t.factor("x")
    # Recovery: many successes
    for _ in range(40):
        t.record("x", success=True)
    assert t.factor("x") > pulled_down


def test_disabled_config_does_nothing():
    pool = SharedRateLimiter(
        global_rate=100,
        global_capacity=100,
        api_configs=[APIConfig(name="x", rate=20, capacity=20)],
    )
    t = AdaptiveThrottler(
        pool, {"x": 20}, {"x": AdaptiveConfig(enabled=False)}
    )
    for _ in range(50):
        t.record("x", success=False)
    assert t.factor("x") == 1.0
    assert pool.api_buckets["x"].rate == 20.0


async def test_throttler_lowers_rate_under_real_load():
    """End-to-end: a flaky upstream causes the live bucket rate to drop."""
    state = {"calls": 0}

    async def flaky(req):
        state["calls"] += 1
        # 60% of calls fail
        if state["calls"] % 5 < 3:
            return 500, None, {}
        await asyncio.sleep(0.005)
        return 200, {"ok": True}, {}

    cfg = AggregatorConfig(
        global_rate=100, global_capacity=100,
        apis=[APIConfig(
            name="flake", rate=50, capacity=50,
            retry=RetryConfig(max_retries=0, base_delay=0.01, jitter=0),
            circuit_breaker=CircuitBreakerConfig(failure_threshold=1000),
            adaptive=AdaptiveConfig(
                enabled=True, error_window=20,
                high_error_rate=0.30, low_error_rate=0.05,
                step_down=0.5, min_rate_factor=0.10,
            ),
        )],
        worker_count=8,
    )
    async with Aggregator(cfg, [CallableAdapter("flake", flaky)]) as agg:
        await asyncio.gather(
            *[
                _safe(agg.submit(Request(api="flake", url=f"/{i}", max_retries=0)))
                for i in range(30)
            ],
            return_exceptions=True,
        )
        snap = agg.throttler.snapshot()["flake"]
    assert snap["factor"] < 1.0, snap


async def _safe(coro):
    try:
        return await coro
    except Exception as e:  # noqa: BLE001
        return e
