"""Priority queue with aging.

Aging guarantees bounded fairness (no starvation): a request's effective
priority decreases (= becomes more important under min-heap semantics)
linearly with its wait time. This means a LOW-priority request that has been
waiting `wait` seconds beats a fresh HIGH-priority one once
`wait >= (HIGH - LOW) / aging_factor`.

We can't mutate heap entries efficiently, so the queue re-heaps when items
are inserted; selection re-evaluates effective priority at pop time using a
linear scan over the heap. With per-API queue depth in the thousands this is
still negligible compared to network IO.
"""

from __future__ import annotations

import asyncio
import heapq
import itertools
import time
from dataclasses import dataclass, field
from typing import Callable, Iterable

from aggregator.common.types import Priority, Request


@dataclass(order=True)
class QueueItem:
    sort_key: tuple = field(compare=True)
    seq: int = field(compare=False, default=0)
    request: Request = field(compare=False, default=None)  # type: ignore[assignment]
    enqueued_at: float = field(compare=False, default=0.0)


class AgingPriorityQueue:
    def __init__(
        self,
        *,
        aging_factor: float = 1.0,
        max_size: int = 10_000,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._heap: list[QueueItem] = []
        self._aging_factor = aging_factor
        self._max_size = max_size
        self._clock = clock
        self._counter = itertools.count()
        self._not_empty = asyncio.Event()
        self._lock = asyncio.Lock()
        self._per_api: dict[str, int] = {}
        self._per_priority: dict[int, int] = {p: 0 for p in Priority}

    def __len__(self) -> int:
        return len(self._heap)

    @property
    def max_size(self) -> int:
        return self._max_size

    @property
    def aging_factor(self) -> float:
        return self._aging_factor

    def is_full(self) -> bool:
        return len(self._heap) >= self._max_size

    def per_api_depths(self) -> dict[str, int]:
        return dict(self._per_api)

    def per_priority_depths(self) -> dict[int, int]:
        return dict(self._per_priority)

    def _effective_priority(self, item: QueueItem, now: float) -> float:
        wait = max(0.0, now - item.enqueued_at)
        return item.request.priority - wait * self._aging_factor

    async def put(self, request: Request) -> None:
        async with self._lock:
            if len(self._heap) >= self._max_size:
                raise asyncio.QueueFull(f"priority queue full ({self._max_size})")
            now = self._clock()
            request.enqueued_at = now
            seq = next(self._counter)
            item = QueueItem(
                sort_key=(request.priority, seq),
                seq=seq,
                request=request,
                enqueued_at=now,
            )
            heapq.heappush(self._heap, item)
            self._per_api[request.api] = self._per_api.get(request.api, 0) + 1
            self._per_priority[int(request.priority)] = (
                self._per_priority.get(int(request.priority), 0) + 1
            )
            self._not_empty.set()

    async def get(self) -> Request:
        """Pop highest-priority request after applying aging. Blocks if empty."""
        while True:
            await self._not_empty.wait()
            async with self._lock:
                if not self._heap:
                    self._not_empty.clear()
                    continue
                request = self._pop_best_locked()
                if not self._heap:
                    self._not_empty.clear()
                return request

    def _pop_best_locked(self) -> Request:
        now = self._clock()
        best_idx = 0
        best_eff = self._effective_priority(self._heap[0], now)
        for i in range(1, len(self._heap)):
            eff = self._effective_priority(self._heap[i], now)
            if eff < best_eff or (eff == best_eff and self._heap[i].seq < self._heap[best_idx].seq):
                best_eff = eff
                best_idx = i
        if best_idx == 0:
            item = heapq.heappop(self._heap)
        else:
            item = self._heap[best_idx]
            last = self._heap.pop()
            if best_idx < len(self._heap):
                self._heap[best_idx] = last
                heapq.heapify(self._heap)
        self._per_api[item.request.api] = max(0, self._per_api.get(item.request.api, 1) - 1)
        self._per_priority[int(item.request.priority)] = max(
            0, self._per_priority.get(int(item.request.priority), 1) - 1
        )
        return item.request

    def peek_pending(self, *, exclude_apis: Iterable[str] = ()) -> Request | None:
        """Non-blocking peek at the request that *would* be popped next.

        Used by the scheduler to avoid blocking on a queue head it can't dispatch
        yet (e.g. when the head's API is rate-limited or circuit-open). The
        scheduler can ask for the best candidate excluding APIs it can't service.
        """
        if not self._heap:
            return None
        now = self._clock()
        excluded = set(exclude_apis)
        best: tuple[float, int, QueueItem] | None = None
        for item in self._heap:
            if item.request.api in excluded:
                continue
            eff = self._effective_priority(item, now)
            key = (eff, item.seq)
            if best is None or key < (best[0], best[1]):
                best = (eff, item.seq, item)
        return best[2].request if best else None

    async def pop_specific(self, request_id: str) -> Request | None:
        async with self._lock:
            for i, item in enumerate(self._heap):
                if item.request.id == request_id:
                    if i == len(self._heap) - 1:
                        self._heap.pop()
                    else:
                        last = self._heap.pop()
                        self._heap[i] = last
                        heapq.heapify(self._heap)
                    self._per_api[item.request.api] = max(
                        0, self._per_api.get(item.request.api, 1) - 1
                    )
                    self._per_priority[int(item.request.priority)] = max(
                        0, self._per_priority.get(int(item.request.priority), 1) - 1
                    )
                    if not self._heap:
                        self._not_empty.clear()
                    return item.request
            return None

    def shutdown(self) -> None:
        """Wake any waiter so they can observe shutdown state."""
        self._not_empty.set()
