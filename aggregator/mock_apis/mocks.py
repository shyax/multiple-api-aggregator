"""Mock APIs used to validate scheduler behaviour without touching the network.

Each mock simulates one of the failure modes called out in the PRD:

- `StrictRateLimitAPI` — rejects (429) any call that breaches its own internal
  per-second budget. Useful for proving the per-API token bucket works.
- `IntermittentErrorAPI` — randomly returns 500 with configurable error rate.
  Drives the circuit breaker and adaptive throttler.
- `HighLatencyAPI` — sleeps a configurable amount before returning. Drives
  concurrency cap behaviour.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections import deque

from aggregator.adapters.base import CallableAdapter
from aggregator.common.types import Request


class _MockBase:
    name: str
    calls: int = 0


class StrictRateLimitAPI(_MockBase):
    def __init__(self, name: str = "api_a", limit_per_second: int = 5) -> None:
        self.name = name
        self.limit_per_second = limit_per_second
        self._timestamps: deque[float] = deque()
        self.calls = 0
        self.rejections = 0

    async def __call__(self, request: Request) -> tuple[int, object, dict[str, str]]:
        self.calls += 1
        now = time.monotonic()
        cutoff = now - 1.0
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()
        if len(self._timestamps) >= self.limit_per_second:
            self.rejections += 1
            return 429, None, {"Retry-After": "1"}
        self._timestamps.append(now)
        await asyncio.sleep(0.005)
        return 200, {"api": self.name, "id": request.id}, {}

    def adapter(self) -> CallableAdapter:
        return CallableAdapter(self.name, self.__call__)


class IntermittentErrorAPI(_MockBase):
    def __init__(self, name: str = "api_b", error_rate: float = 0.3, seed: int | None = None) -> None:
        self.name = name
        self.error_rate = error_rate
        self.calls = 0
        self.errors = 0
        self._rng = random.Random(seed)

    async def __call__(self, request: Request) -> tuple[int, object, dict[str, str]]:
        self.calls += 1
        await asyncio.sleep(0.01)
        if self._rng.random() < self.error_rate:
            self.errors += 1
            return 500, None, {}
        return 200, {"api": self.name, "id": request.id}, {}

    def set_error_rate(self, rate: float) -> None:
        self.error_rate = max(0.0, min(1.0, rate))

    def adapter(self) -> CallableAdapter:
        return CallableAdapter(self.name, self.__call__)


class HighLatencyAPI(_MockBase):
    def __init__(self, name: str = "api_c", latency: float = 0.2, jitter: float = 0.1, seed: int | None = None) -> None:
        self.name = name
        self.latency = latency
        self.jitter = jitter
        self.calls = 0
        self._rng = random.Random(seed)

    async def __call__(self, request: Request) -> tuple[int, object, dict[str, str]]:
        self.calls += 1
        delay = max(0.0, self.latency + self._rng.uniform(-self.jitter, self.jitter))
        await asyncio.sleep(delay)
        return 200, {"api": self.name, "id": request.id, "delay": round(delay, 3)}, {}

    def adapter(self) -> CallableAdapter:
        return CallableAdapter(self.name, self.__call__)


def build_default_mocks(seed: int | None = 7) -> dict[str, _MockBase]:
    """Construct the canonical demo mock set referenced in the PRD."""
    return {
        "api_a": StrictRateLimitAPI("api_a", limit_per_second=5),
        "api_b": IntermittentErrorAPI("api_b", error_rate=0.35, seed=seed),
        "api_c": HighLatencyAPI("api_c", latency=0.15, jitter=0.05, seed=seed),
    }
