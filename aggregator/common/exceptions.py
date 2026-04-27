"""Exception hierarchy used across the aggregator."""

from __future__ import annotations


class AggregatorError(Exception):
    """Base class for everything raised inside the aggregator."""


class RetryableError(AggregatorError):
    """Transient failure — the request should be re-enqueued."""


class NonRetryableError(AggregatorError):
    """Permanent failure — the request must not be retried."""


class RateLimitedError(RetryableError):
    """Upstream signalled a 429 (or analogous rate-limit) response."""

    def __init__(self, message: str = "rate limited", retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class UpstreamError(RetryableError):
    """Upstream returned a 5xx or other transient transport error."""


class CircuitOpenError(RetryableError):
    """Circuit breaker rejected the call; the API is currently OPEN."""


class SchedulerStoppedError(AggregatorError):
    """Scheduler stopped before the request could complete."""
