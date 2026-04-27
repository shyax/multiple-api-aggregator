"""Multi-API aggregator with shared rate-limit pool, priority scheduling, and adaptive throttling."""

from aggregator.common import (
    AdaptiveConfig,
    AggregatorConfig,
    AggregatorError,
    APIConfig,
    CircuitBreakerConfig,
    CircuitOpenError,
    NonRetryableError,
    Priority,
    RateLimitedError,
    Request,
    Response,
    RetryableError,
    RetryConfig,
    SchedulerStoppedError,
    UpstreamError,
)

__all__ = [
    "AdaptiveConfig",
    "AggregatorConfig",
    "AggregatorError",
    "APIConfig",
    "CircuitBreakerConfig",
    "CircuitOpenError",
    "NonRetryableError",
    "Priority",
    "RateLimitedError",
    "Request",
    "Response",
    "RetryableError",
    "RetryConfig",
    "SchedulerStoppedError",
    "UpstreamError",
]
