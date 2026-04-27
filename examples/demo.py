"""Runnable demo of the multi-API aggregator.

Spins up three mock APIs with the failure profiles described in the PRD,
submits a mixed-priority workload, and prints a structured snapshot showing:

- request throughput per API
- queue depth by priority over time
- circuit breaker trips
- adaptive throttler factor evolution
- p50/p95/p99 latency

Run:

    python examples/demo.py
"""

from __future__ import annotations

import asyncio
import json
import random
from typing import Iterable

from aggregator.common.config import (
    AdaptiveConfig,
    AggregatorConfig,
    APIConfig,
    CircuitBreakerConfig,
    RetryConfig,
)
from aggregator.common.exceptions import AggregatorError
from aggregator.common.types import Priority, Request
from aggregator.mock_apis.mocks import build_default_mocks
from aggregator.orchestrator.orchestrator import Aggregator


def build_config() -> AggregatorConfig:
    return AggregatorConfig(
        global_rate=20,        # 20 req/sec across the whole platform
        global_capacity=20,
        worker_count=8,
        aging_factor=2.0,
        queue_max_size=2000,
        backpressure_threshold=1500,
        drop_low_priority_threshold=1800,
        apis=[
            APIConfig(
                name="api_a",          # strict rate-limited upstream
                rate=8, capacity=8,
                retry=RetryConfig(max_retries=3, base_delay=0.1, max_delay=1.0, jitter=0.25),
                circuit_breaker=CircuitBreakerConfig(failure_threshold=10),
                adaptive=AdaptiveConfig(enabled=True, error_window=30),
            ),
            APIConfig(
                name="api_b",          # intermittent 5xx
                rate=10, capacity=10,
                retry=RetryConfig(max_retries=4, base_delay=0.05, max_delay=0.8),
                circuit_breaker=CircuitBreakerConfig(failure_threshold=6, recovery_timeout=2.0),
                adaptive=AdaptiveConfig(enabled=True, error_window=30, high_error_rate=0.30, step_down=0.5),
            ),
            APIConfig(
                name="api_c",          # high-latency
                rate=15, capacity=15,
                retry=RetryConfig(max_retries=2, base_delay=0.1, max_delay=0.5),
                circuit_breaker=CircuitBreakerConfig(failure_threshold=20),
                adaptive=AdaptiveConfig(enabled=False),
            ),
        ],
    )


def workload(rng: random.Random, n: int = 200) -> Iterable[Request]:
    apis = ["api_a", "api_b", "api_c"]
    priorities = [Priority.HIGH, Priority.MEDIUM, Priority.LOW]
    weights = [0.15, 0.55, 0.30]  # most traffic is MEDIUM
    for i in range(n):
        api = rng.choice(apis)
        priority = rng.choices(priorities, weights=weights, k=1)[0]
        yield Request(
            api=api,
            method="GET",
            url=f"/items/{i}",
            params={"q": rng.randint(0, 9999)},
            priority=priority,
            max_retries=3,
        )


async def main() -> None:
    rng = random.Random(42)
    cfg = build_config()
    mocks = build_default_mocks(seed=42)
    adapters = [mocks[name].adapter() for name in ("api_a", "api_b", "api_c")]

    async with Aggregator(cfg, adapters) as agg:
        # Periodic snapshot logger so you can watch the system breathe.
        async def snapshot_loop() -> None:
            while True:
                await asyncio.sleep(0.5)
                snap = agg.stats()
                print(
                    f"queue={snap['queue_depth']:>4} "
                    f"per_priority={snap['queue_per_priority']} "
                    f"breakers={snap['circuit_breakers']} "
                    f"throttle={ {k: v['factor'] for k, v in snap['throttler'].items()} }"
                )

        snap_task = asyncio.create_task(snapshot_loop())

        outcomes = {"ok": 0, "err": 0}
        async def submit(req: Request) -> None:
            try:
                await agg.submit(req)
                outcomes["ok"] += 1
            except AggregatorError:
                outcomes["err"] += 1

        # Fire the whole workload concurrently so the scheduler sees real backpressure.
        await asyncio.gather(*[submit(r) for r in workload(rng, n=200)])

        snap_task.cancel()
        try:
            await snap_task
        except asyncio.CancelledError:
            pass

        print("\n=== final snapshot ===")
        print(json.dumps(agg.stats(), indent=2, default=str))
        print(f"\noutcomes: {outcomes}")
        print(f"mock api_a: calls={mocks['api_a'].calls} rejections={mocks['api_a'].rejections}")
        print(f"mock api_b: calls={mocks['api_b'].calls} errors={mocks['api_b'].errors}")
        print(f"mock api_c: calls={mocks['api_c'].calls}")


if __name__ == "__main__":
    asyncio.run(main())
