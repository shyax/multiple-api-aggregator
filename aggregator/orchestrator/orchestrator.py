"""Aggregator — public façade.

Wires together queue, rate limiter, circuit breakers, retry policies, adaptive
throttler, scheduler, and metrics. Callers get a single async API:

    async with Aggregator(config, adapters) as agg:
        response = await agg.submit(Request(api="api_a", url="/foo"))

Submission supports inline deduplication: identical in-flight requests share
a single dispatch and resolve to the same response.
"""

from __future__ import annotations

import asyncio
from typing import Iterable

from aggregator.adapters.base import BaseAdapter
from aggregator.circuit_breaker.breaker import CircuitBreaker
from aggregator.common.config import AggregatorConfig
from aggregator.common.exceptions import NonRetryableError
from aggregator.common.types import Priority, Request, Response
from aggregator.metrics.collector import MetricsCollector
from aggregator.priority_queue.queue import AgingPriorityQueue
from aggregator.rate_limiter.shared_pool import SharedRateLimiter
from aggregator.retry.policy import RetryPolicy
from aggregator.scheduler.scheduler import Scheduler
from aggregator.throttler.adaptive import AdaptiveThrottler


class Aggregator:
    def __init__(
        self,
        config: AggregatorConfig,
        adapters: Iterable[BaseAdapter],
    ) -> None:
        self._config = config
        self._adapters: dict[str, BaseAdapter] = {a.name: a for a in adapters}
        self._validate_config()

        self.metrics = MetricsCollector()
        self.queue = AgingPriorityQueue(
            aging_factor=config.aging_factor,
            max_size=config.queue_max_size,
        )
        self.rate_limiter = SharedRateLimiter(
            global_rate=config.global_rate,
            global_capacity=config.global_capacity,
            api_configs=config.apis,
        )
        self.circuit_breakers: dict[str, CircuitBreaker] = {
            cfg.name: CircuitBreaker(cfg.name, cfg.circuit_breaker) for cfg in config.apis
        }
        self.retry_policies: dict[str, RetryPolicy] = {
            cfg.name: RetryPolicy(cfg.retry) for cfg in config.apis
        }
        self.throttler = AdaptiveThrottler(
            rate_limiter=self.rate_limiter,
            api_base_rates={cfg.name: cfg.rate for cfg in config.apis},
            api_configs={cfg.name: cfg.adaptive for cfg in config.apis},
        )

        self._pending: dict[str, asyncio.Future[Response]] = {}
        self._dedup: dict[str, asyncio.Future[Response]] = {}
        self.scheduler = Scheduler(
            config=config,
            queue=self.queue,
            rate_limiter=self.rate_limiter,
            circuit_breakers=self.circuit_breakers,
            adapters=self._adapters,
            retry_policies=self.retry_policies,
            throttler=self.throttler,
            metrics=self.metrics,
            pending=self._pending,
        )

    def _validate_config(self) -> None:
        if not self._config.apis:
            raise ValueError("AggregatorConfig.apis must list at least one APIConfig")
        configured = {cfg.name for cfg in self._config.apis}
        missing = configured - set(self._adapters.keys())
        if missing:
            raise ValueError(f"missing adapters for configured APIs: {sorted(missing)}")
        unknown = set(self._adapters.keys()) - configured
        if unknown:
            raise ValueError(f"adapters supplied without APIConfig: {sorted(unknown)}")

    async def __aenter__(self) -> "Aggregator":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.stop()

    async def start(self) -> None:
        await self.scheduler.start()

    async def stop(self) -> None:
        await self.scheduler.stop()
        await asyncio.gather(*(a.aclose() for a in self._adapters.values()), return_exceptions=True)

    async def submit(self, request: Request) -> Response:
        """Submit a request and await its response.

        Backpressure: when the queue is past `drop_low_priority_threshold`, LOW
        priority requests are rejected synchronously with NonRetryableError.
        """
        if request.api not in self._adapters:
            raise NonRetryableError(f"unknown api: {request.api}")
        depth = len(self.queue)
        if depth >= self._config.drop_low_priority_threshold and request.priority >= Priority.LOW:
            self.metrics.incr("requests_rejected_backpressure")
            raise NonRetryableError("queue at backpressure threshold for low-priority work")

        # In-flight dedup: identical pending requests get the same future.
        key = request.dedup_key()
        existing = self._dedup.get(key)
        if existing is not None and not existing.done():
            self.metrics.incr("requests_deduped")
            return await asyncio.shield(existing)

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Response] = loop.create_future()
        self._pending[request.id] = fut
        self._dedup[key] = fut

        def _cleanup_dedup(_):
            # Only remove if we're still the active dedup entry.
            if self._dedup.get(key) is fut:
                self._dedup.pop(key, None)

        fut.add_done_callback(_cleanup_dedup)

        await self.queue.put(request)
        self.scheduler.notify()
        self.metrics.incr("requests_submitted")
        self.metrics.gauge("queue_depth", float(len(self.queue)))
        return await fut

    def stats(self) -> dict:
        """Snapshot of current state — useful for logs/dashboards."""
        return {
            "queue_depth": len(self.queue),
            "queue_per_api": self.queue.per_api_depths(),
            "queue_per_priority": self.queue.per_priority_depths(),
            "rate_limiter": self.rate_limiter.snapshot(),
            "circuit_breakers": {
                api: breaker.state.value for api, breaker in self.circuit_breakers.items()
            },
            "throttler": self.throttler.snapshot(),
            "metrics": self.metrics.snapshot(),
        }
