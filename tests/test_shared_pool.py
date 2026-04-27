import pytest

from aggregator.common.config import APIConfig
from aggregator.rate_limiter.shared_pool import SharedRateLimiter


@pytest.fixture
def pool(fake_clock):
    apis = [APIConfig(name="a", rate=2, capacity=2), APIConfig(name="b", rate=10, capacity=10)]
    return SharedRateLimiter(global_rate=5, global_capacity=5, api_configs=apis, clock=fake_clock)


async def test_acquire_takes_from_both_buckets(pool):
    assert await pool.try_acquire("a")
    snapshot = pool.snapshot()
    assert snapshot["global"]["tokens"] == 4
    assert snapshot["apis"]["a"]["tokens"] == 1


async def test_per_api_limit_blocks_with_global_capacity(pool):
    assert await pool.try_acquire("a")
    assert await pool.try_acquire("a")
    # API a is now empty; another acquire must fail without consuming global tokens.
    snap_before = pool.snapshot()["global"]["tokens"]
    assert not await pool.try_acquire("a")
    snap_after = pool.snapshot()["global"]["tokens"]
    assert snap_before == snap_after, "global tokens must be refunded on partial failure"


async def test_global_limit_blocks_even_if_api_has_capacity(pool, fake_clock):
    # Drain global pool through API b which has plenty of local capacity.
    for _ in range(5):
        assert await pool.try_acquire("b")
    assert not await pool.try_acquire("b")
    # Advancing the clock refills both buckets.
    fake_clock.advance(1.0)
    assert await pool.try_acquire("b")
