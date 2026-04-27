"""Configuration dataclasses. Everything is config-driven (no hardcoded limits)."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    """Consecutive failures (rolling) before opening."""

    recovery_timeout: float = 5.0
    """Seconds to stay OPEN before transitioning to HALF_OPEN."""

    half_open_max_probes: int = 1
    """How many probe requests are allowed while HALF_OPEN."""

    success_threshold: int = 2
    """Consecutive HALF_OPEN successes required to close again."""


@dataclass
class RetryConfig:
    max_retries: int = 3
    base_delay: float = 0.2
    max_delay: float = 5.0
    jitter: float = 0.25
    """Multiplicative jitter envelope (delay * (1 +/- jitter * U(0,1)))."""

    priority_decay: int = 5
    """Each retry adds this to base priority (lower = higher priority, so retries demote slightly)."""


@dataclass
class AdaptiveConfig:
    enabled: bool = True
    error_window: int = 50
    """Sample size used for rolling error rate."""

    high_error_rate: float = 0.30
    """If rolling error rate >= this, throttle down."""

    low_error_rate: float = 0.05
    """If rolling error rate <= this for a while, throttle back up."""

    min_rate_factor: float = 0.10
    """Floor for adaptive multiplier on configured rate."""

    max_rate_factor: float = 1.0
    """Ceiling — adaptive throttler never exceeds the configured rate."""

    step_down: float = 0.5
    """Multiplier applied when throttling down."""

    step_up: float = 1.10
    """Multiplier applied when relaxing."""


@dataclass
class APIConfig:
    name: str
    rate: float
    """Tokens/sec replenishment for this API's local bucket."""

    capacity: int | None = None
    """Bucket capacity. Defaults to ceil(rate)."""

    max_concurrency: int = 10
    weight: int = 1
    """Used for weighted-fair allocation when multiple APIs compete for the global pool."""

    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    adaptive: AdaptiveConfig = field(default_factory=AdaptiveConfig)

    def __post_init__(self) -> None:
        if self.capacity is None:
            self.capacity = max(1, int(round(self.rate)))


@dataclass
class AggregatorConfig:
    global_rate: float = 100.0
    """Tokens/sec for the shared global pool."""

    global_capacity: int | None = None
    apis: list[APIConfig] = field(default_factory=list)

    queue_max_size: int = 10_000
    aging_factor: float = 1.0
    """Effective priority = base_priority - wait_time * aging_factor (per second)."""

    backpressure_threshold: int = 5_000
    """Queue depth where intake slows."""

    drop_low_priority_threshold: int = 9_000
    """Queue depth where LOW-priority enqueues are rejected outright."""

    worker_count: int = 16
    """Concurrency cap for the dispatch executor."""

    def __post_init__(self) -> None:
        if self.global_capacity is None:
            self.global_capacity = max(1, int(round(self.global_rate)))
