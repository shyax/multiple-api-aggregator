"""Async token bucket with fractional refill.

The bucket tracks a real-valued token count to avoid the discretization bias
that integer-only buckets show at low refill rates (e.g. 0.5/sec).
A monotonic clock injection makes the bucket testable without sleeping.
"""

from __future__ import annotations

import asyncio
import time
from typing import Callable


class TokenBucket:
    def __init__(
        self,
        rate: float,
        capacity: int,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if rate < 0:
            raise ValueError("rate must be non-negative")
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self._rate = float(rate)
        self._capacity = int(capacity)
        self._tokens: float = float(capacity)
        self._clock = clock
        self._last_refill: float = clock()

    @property
    def rate(self) -> float:
        return self._rate

    @rate.setter
    def rate(self, value: float) -> None:
        if value < 0:
            raise ValueError("rate must be non-negative")
        self._refill()
        self._rate = float(value)

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def tokens(self) -> float:
        self._refill()
        return self._tokens

    def _refill(self) -> None:
        now = self._clock()
        elapsed = now - self._last_refill
        if elapsed <= 0:
            return
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
        self._last_refill = now

    def try_acquire(self, n: int = 1) -> bool:
        """Non-blocking acquire. Returns True iff `n` tokens were taken."""
        if n <= 0:
            return True
        self._refill()
        if self._tokens >= n:
            self._tokens -= n
            return True
        return False

    def refund(self, n: int = 1) -> None:
        """Return previously-taken tokens (used when an atomic multi-bucket acquire half-fails)."""
        if n <= 0:
            return
        self._tokens = min(self._capacity, self._tokens + n)

    def time_until(self, n: int = 1) -> float:
        """Seconds until `n` tokens will be available, given current state and rate."""
        self._refill()
        if self._tokens >= n:
            return 0.0
        if self._rate == 0:
            return float("inf")
        return (n - self._tokens) / self._rate

    async def acquire(self, n: int = 1, *, timeout: float | None = None) -> None:
        """Block until `n` tokens are available. Cooperative — yields via asyncio.sleep."""
        deadline = None if timeout is None else self._clock() + timeout
        while True:
            if self.try_acquire(n):
                return
            wait = self.time_until(n)
            if deadline is not None:
                remaining = deadline - self._clock()
                if remaining <= 0:
                    raise asyncio.TimeoutError(
                        f"timed out waiting for {n} tokens"
                    )
                wait = min(wait, remaining)
            await asyncio.sleep(max(wait, 0.001))
