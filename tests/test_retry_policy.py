import random

from aggregator.common.config import RetryConfig
from aggregator.common.exceptions import (
    NonRetryableError,
    RateLimitedError,
    UpstreamError,
)
from aggregator.common.types import Priority, Request
from aggregator.retry.policy import RetryPolicy, compute_backoff


def test_backoff_increases_geometrically():
    cfg = RetryConfig(base_delay=0.1, max_delay=10.0, jitter=0.0)
    delays = [compute_backoff(i, cfg) for i in range(4)]
    assert delays == [0.1, 0.2, 0.4, 0.8]


def test_backoff_capped_at_max():
    cfg = RetryConfig(base_delay=1.0, max_delay=2.0, jitter=0.0)
    assert compute_backoff(10, cfg) == 2.0


def test_jitter_within_envelope():
    cfg = RetryConfig(base_delay=1.0, max_delay=10.0, jitter=0.5)
    rng = random.Random(0)
    for _ in range(100):
        d = compute_backoff(2, cfg, rng=rng)  # raw = 4.0
        assert 4.0 * 0.5 <= d <= 4.0 * 1.5


def test_nonretryable_returns_no_retry():
    cfg = RetryConfig()
    policy = RetryPolicy(cfg)
    req = Request(api="a")
    decision = policy.decide(req, NonRetryableError("boom"))
    assert not decision.should_retry


def test_retryable_returns_retry_with_decay():
    cfg = RetryConfig(priority_decay=10, jitter=0.0)
    policy = RetryPolicy(cfg)
    req = Request(api="a", priority=Priority.HIGH)
    decision = policy.decide(req, UpstreamError("502"))
    assert decision.should_retry
    assert decision.new_priority is not None
    assert int(decision.new_priority) >= int(Priority.HIGH)


def test_retry_after_honoured_for_429():
    cfg = RetryConfig()
    policy = RetryPolicy(cfg)
    req = Request(api="a")
    decision = policy.decide(req, RateLimitedError("429", retry_after=2.5))
    assert decision.should_retry
    assert decision.delay == 2.5


def test_max_retries_exhausted():
    cfg = RetryConfig(max_retries=2)
    policy = RetryPolicy(cfg)
    req = Request(api="a", retry_count=2)
    decision = policy.decide(req, UpstreamError("502"))
    assert not decision.should_retry
