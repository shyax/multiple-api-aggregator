"""Shared rate limit pool: one global bucket + one per-API bucket.

The acquire path is atomic: if the per-API bucket cannot satisfy the request
after the global bucket already paid, the global tokens are refunded so we
never under-count global capacity.
"""

from __future__ import annotations

import asyncio
import time
from typing import Callable

from aggregator.common.config import APIConfig
from aggregator.rate_limiter.token_bucket import TokenBucket


class SharedRateLimiter:
    def __init__(
        self,
        global_rate: float,
        global_capacity: int,
        api_configs: list[APIConfig],
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._clock = clock
        self.global_bucket = TokenBucket(global_rate, global_capacity, clock=clock)
        self.api_buckets: dict[str, TokenBucket] = {
            cfg.name: TokenBucket(cfg.rate, cfg.capacity, clock=clock) for cfg in api_configs
        }
        self._weights: dict[str, int] = {cfg.name: cfg.weight for cfg in api_configs}
        self._lock = asyncio.Lock()

    def has_api(self, api: str) -> bool:
        return api in self.api_buckets

    def add_api(self, cfg: APIConfig) -> None:
        self.api_buckets[cfg.name] = TokenBucket(cfg.rate, cfg.capacity, clock=self._clock)
        self._weights[cfg.name] = cfg.weight

    def weight(self, api: str) -> int:
        return self._weights.get(api, 1)

    async def try_acquire(self, api: str, n: int = 1) -> bool:
        """Atomic: take from global+per-API. On partial failure, refund and report False."""
        if api not in self.api_buckets:
            raise KeyError(f"unknown api: {api}")
        async with self._lock:
            api_bucket = self.api_buckets[api]
            if not self.global_bucket.try_acquire(n):
                return False
            if not api_bucket.try_acquire(n):
                self.global_bucket.refund(n)
                return False
            return True

    async def acquire(self, api: str, n: int = 1, *, timeout: float | None = None) -> None:
        """Block until both buckets satisfy the request. Cooperative."""
        deadline = None if timeout is None else self._clock() + timeout
        while True:
            if await self.try_acquire(api, n):
                return
            wait_global = self.global_bucket.time_until(n)
            wait_api = self.api_buckets[api].time_until(n)
            wait = max(wait_global, wait_api)
            if deadline is not None:
                remaining = deadline - self._clock()
                if remaining <= 0:
                    raise asyncio.TimeoutError(f"rate limiter timeout for {api}")
                wait = min(wait, remaining)
            await asyncio.sleep(max(wait, 0.001))

    def time_until(self, api: str, n: int = 1) -> float:
        """Estimated seconds until both buckets can satisfy `n` tokens."""
        return max(
            self.global_bucket.time_until(n),
            self.api_buckets[api].time_until(n),
        )

    def snapshot(self) -> dict:
        return {
            "global": {
                "tokens": round(self.global_bucket.tokens, 3),
                "capacity": self.global_bucket.capacity,
                "rate": self.global_bucket.rate,
            },
            "apis": {
                name: {
                    "tokens": round(bucket.tokens, 3),
                    "capacity": bucket.capacity,
                    "rate": bucket.rate,
                }
                for name, bucket in self.api_buckets.items()
            },
        }
