from aggregator.circuit_breaker.breaker import CircuitBreaker, CircuitState
from aggregator.common.config import CircuitBreakerConfig


def test_starts_closed(fake_clock):
    cb = CircuitBreaker("a", CircuitBreakerConfig(), clock=fake_clock)
    assert cb.state is CircuitState.CLOSED
    assert cb.allow()


def test_opens_after_threshold(fake_clock):
    cfg = CircuitBreakerConfig(failure_threshold=3)
    cb = CircuitBreaker("a", cfg, clock=fake_clock)
    for _ in range(3):
        cb.on_failure()
    assert cb.state is CircuitState.OPEN
    assert not cb.allow()
    assert cb.trips == 1


def test_recovery_to_half_open(fake_clock):
    cfg = CircuitBreakerConfig(failure_threshold=2, recovery_timeout=5)
    cb = CircuitBreaker("a", cfg, clock=fake_clock)
    cb.on_failure(); cb.on_failure()
    assert cb.state is CircuitState.OPEN
    fake_clock.advance(5)
    assert cb.state is CircuitState.HALF_OPEN
    assert cb.allow()


def test_half_open_failure_re_opens(fake_clock):
    cfg = CircuitBreakerConfig(failure_threshold=1, recovery_timeout=1)
    cb = CircuitBreaker("a", cfg, clock=fake_clock)
    cb.on_failure()
    fake_clock.advance(1)
    assert cb.allow()  # consume probe slot
    cb.on_failure()
    assert cb.state is CircuitState.OPEN
    assert cb.trips == 2


def test_half_open_success_threshold_closes(fake_clock):
    cfg = CircuitBreakerConfig(failure_threshold=1, recovery_timeout=1, success_threshold=2, half_open_max_probes=2)
    cb = CircuitBreaker("a", cfg, clock=fake_clock)
    cb.on_failure()
    fake_clock.advance(1)
    assert cb.allow()
    cb.on_success()
    assert cb.state is CircuitState.HALF_OPEN
    assert cb.allow()
    cb.on_success()
    assert cb.state is CircuitState.CLOSED


def test_half_open_probes_capped(fake_clock):
    cfg = CircuitBreakerConfig(failure_threshold=1, recovery_timeout=1, half_open_max_probes=1)
    cb = CircuitBreaker("a", cfg, clock=fake_clock)
    cb.on_failure()
    fake_clock.advance(1)
    assert cb.allow()
    assert not cb.allow()  # second probe blocked
