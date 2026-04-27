from aggregator.rate_limiter.token_bucket import TokenBucket


def test_starts_full(fake_clock):
    b = TokenBucket(rate=10, capacity=5, clock=fake_clock)
    assert b.tokens == 5.0


def test_try_acquire_consumes(fake_clock):
    b = TokenBucket(rate=10, capacity=5, clock=fake_clock)
    assert b.try_acquire(3)
    assert b.tokens == 2.0
    assert not b.try_acquire(3)
    assert b.tokens == 2.0


def test_refill_is_fractional(fake_clock):
    b = TokenBucket(rate=10, capacity=5, clock=fake_clock)
    assert b.try_acquire(5)
    fake_clock.advance(0.1)  # 1 token
    assert abs(b.tokens - 1.0) < 1e-9


def test_refill_caps_at_capacity(fake_clock):
    b = TokenBucket(rate=100, capacity=5, clock=fake_clock)
    fake_clock.advance(10)
    assert b.tokens == 5.0


def test_time_until(fake_clock):
    b = TokenBucket(rate=2, capacity=2, clock=fake_clock)
    b.try_acquire(2)
    assert b.time_until(2) == 1.0


def test_refund(fake_clock):
    b = TokenBucket(rate=1, capacity=5, clock=fake_clock)
    b.try_acquire(3)
    b.refund(2)
    assert b.tokens == 4.0
    # Refund cannot exceed capacity
    b.refund(100)
    assert b.tokens == 5.0


def test_zero_rate_never_refills(fake_clock):
    b = TokenBucket(rate=0, capacity=2, clock=fake_clock)
    b.try_acquire(2)
    fake_clock.advance(100)
    assert b.tokens == 0.0
    assert b.time_until(1) == float("inf")
