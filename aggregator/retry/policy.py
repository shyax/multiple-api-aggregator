"""Retry policy: classification, exponential backoff with jitter, priority decay."""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Any

from aggregator.common.config import RetryConfig
from aggregator.common.exceptions import (
    CircuitOpenError,
    NonRetryableError,
    RateLimitedError,
    RetryableError,
    UpstreamError,
)
from aggregator.common.types import Priority, Request


@dataclass
class RetryDecision:
    should_retry: bool
    delay: float = 0.0
    new_priority: Priority | None = None
    reason: str = ""


def classify_exception(exc: BaseException) -> type[Exception] | None:
    """Map an exception to one of our retry classes; return None for non-retryable."""
    if isinstance(exc, NonRetryableError):
        return None
    if isinstance(exc, (RateLimitedError, UpstreamError, CircuitOpenError, RetryableError)):
        return type(exc)
    if isinstance(exc, (asyncio.TimeoutError, ConnectionError)):
        return UpstreamError
    return None


def compute_backoff(
    attempt: int,
    config: RetryConfig,
    *,
    rng: random.Random | None = None,
) -> float:
    """Exponential backoff with full-jitter envelope.

    delay = min(base * 2^attempt, max) * (1 + jitter * U(-1, 1))
    """
    rng = rng or random
    raw = config.base_delay * (2 ** max(0, attempt))
    capped = min(raw, config.max_delay)
    if config.jitter <= 0:
        return capped
    multiplier = 1.0 + config.jitter * (2 * rng.random() - 1)
    return max(0.0, capped * multiplier)


class RetryPolicy:
    def __init__(self, config: RetryConfig, *, rng: random.Random | None = None) -> None:
        self._config = config
        self._rng = rng or random.Random()

    def decide(self, request: Request, exc: BaseException | None, status: int | None = None) -> RetryDecision:
        max_retries = min(request.max_retries, self._config.max_retries)
        if request.retry_count >= max_retries:
            return RetryDecision(should_retry=False, reason="max_retries_exhausted")

        # If we got an explicit Retry-After from a 429, honour it
        if isinstance(exc, RateLimitedError) and exc.retry_after is not None:
            delay = float(exc.retry_after)
        else:
            delay = compute_backoff(request.retry_count, self._config, rng=self._rng)

        category = classify_exception(exc) if exc else None
        if exc is not None and category is None:
            return RetryDecision(should_retry=False, reason=f"non_retryable:{type(exc).__name__}")

        # Status-based classification (for adapter responses returning structured errors)
        if status is not None and status not in (None,):
            if 500 <= status < 600 or status == 429:
                pass  # retryable
            elif 400 <= status < 500:
                return RetryDecision(should_retry=False, reason=f"client_error:{status}")

        new_priority = self._decay(request.priority)
        return RetryDecision(
            should_retry=True,
            delay=delay,
            new_priority=new_priority,
            reason=category.__name__ if category else "transient",
        )

    def _decay(self, priority: Priority) -> Priority:
        new_value = int(priority) + self._config.priority_decay
        # Snap to nearest defined Priority bucket so we keep using IntEnum semantics
        if new_value <= Priority.HIGH:
            return Priority.HIGH
        if new_value >= Priority.LOW:
            return Priority.LOW
        return Priority.MEDIUM
