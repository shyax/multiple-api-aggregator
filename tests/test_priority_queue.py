import asyncio

import pytest

from aggregator.common.types import Priority, Request
from aggregator.priority_queue.queue import AgingPriorityQueue


def make_request(api: str = "a", priority: Priority = Priority.MEDIUM) -> Request:
    return Request(api=api, url="/x", priority=priority)


async def test_high_priority_dequeued_first(fake_clock):
    q = AgingPriorityQueue(aging_factor=0.0, clock=fake_clock)
    low = make_request(priority=Priority.LOW)
    high = make_request(priority=Priority.HIGH)
    await q.put(low)
    await q.put(high)
    first = await q.get()
    second = await q.get()
    assert first is high
    assert second is low


async def test_aging_promotes_low_priority(fake_clock):
    # Aging factor strong enough that 100 seconds wait beats LOW->HIGH gap (=100)
    q = AgingPriorityQueue(aging_factor=2.0, clock=fake_clock)
    old_low = make_request(priority=Priority.LOW)
    await q.put(old_low)
    fake_clock.advance(60.0)  # 60 sec * 2.0 = 120 priority units of aging
    fresh_high = make_request(priority=Priority.HIGH)
    await q.put(fresh_high)
    first = await q.get()
    assert first is old_low, "the older LOW should now beat the fresh HIGH"


async def test_fifo_within_same_priority(fake_clock):
    q = AgingPriorityQueue(aging_factor=0.0, clock=fake_clock)
    rs = [make_request(priority=Priority.MEDIUM) for _ in range(5)]
    for r in rs:
        await q.put(r)
    out = [await q.get() for _ in rs]
    assert out == rs


async def test_queue_full_raises(fake_clock):
    q = AgingPriorityQueue(aging_factor=0.0, max_size=2, clock=fake_clock)
    await q.put(make_request())
    await q.put(make_request())
    with pytest.raises(asyncio.QueueFull):
        await q.put(make_request())


async def test_per_api_depth_tracking(fake_clock):
    q = AgingPriorityQueue(aging_factor=0.0, clock=fake_clock)
    await q.put(make_request("a"))
    await q.put(make_request("a"))
    await q.put(make_request("b"))
    assert q.per_api_depths() == {"a": 2, "b": 1}
    await q.get()
    depths = q.per_api_depths()
    assert depths["a"] + depths["b"] == 2


async def test_peek_pending_excludes_apis(fake_clock):
    q = AgingPriorityQueue(aging_factor=0.0, clock=fake_clock)
    a_req = make_request("a", Priority.HIGH)
    b_req = make_request("b", Priority.LOW)
    await q.put(a_req)
    await q.put(b_req)
    # If we exclude api a, even though it's higher priority, b should be next.
    assert q.peek_pending(exclude_apis={"a"}) is b_req
    assert q.peek_pending() is a_req


async def test_pop_specific_removes_arbitrary_request(fake_clock):
    q = AgingPriorityQueue(aging_factor=0.0, clock=fake_clock)
    requests = [make_request() for _ in range(5)]
    for r in requests:
        await q.put(r)
    target = requests[2]
    popped = await q.pop_specific(target.id)
    assert popped is target
    remaining = []
    while len(q):
        remaining.append(await q.get())
    assert target not in remaining
    assert len(remaining) == 4
