"""Scheduler — central dispatch loop.

Responsibilities (PRD §5.1, FR1–FR7):

1. Pull the best dispatchable request from the priority queue.
2. Skip requests whose API is OPEN or rate-limited; pick the next best.
3. Acquire global+per-API tokens atomically.
4. Hand off to a worker pool bounded by `worker_count`.
5. On completion: record metrics, feed the adaptive throttler, and re-enqueue
   on retryable failures with priority decay.

Determinism: the loop never blocks on a single request — if the queue head
isn't dispatchable, it picks the next-best candidate. This is the property
that prevents a stalled API from starving other APIs.
"""

from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable

from aggregator.adapters.base import BaseAdapter
from aggregator.circuit_breaker.breaker import CircuitBreaker, CircuitState
from aggregator.common.config import AggregatorConfig
from aggregator.common.exceptions import (
    AggregatorError,
    CircuitOpenError,
    NonRetryableError,
    RateLimitedError,
    SchedulerStoppedError,
    UpstreamError,
)
from aggregator.common.types import Request, Response
from aggregator.metrics.collector import MetricsCollector
from aggregator.priority_queue.queue import AgingPriorityQueue
from aggregator.rate_limiter.shared_pool import SharedRateLimiter
from aggregator.retry.policy import RetryPolicy
from aggregator.throttler.adaptive import AdaptiveThrottler

PendingMap = dict[str, "asyncio.Future[Response]"]


class Scheduler:
    def __init__(
        self,
        config: AggregatorConfig,
        queue: AgingPriorityQueue,
        rate_limiter: SharedRateLimiter,
        circuit_breakers: dict[str, CircuitBreaker],
        adapters: dict[str, BaseAdapter],
        retry_policies: dict[str, RetryPolicy],
        throttler: AdaptiveThrottler,
        metrics: MetricsCollector,
        pending: PendingMap,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._config = config
        self._queue = queue
        self._rate_limiter = rate_limiter
        self._circuit_breakers = circuit_breakers
        self._adapters = adapters
        self._retry_policies = retry_policies
        self._throttler = throttler
        self._metrics = metrics
        self._pending = pending
        self._clock = clock

        self._worker_sem = asyncio.Semaphore(config.worker_count)
        self._inflight: set[asyncio.Task] = set()
        self._running = False
        self._loop_task: asyncio.Task | None = None
        self._wake = asyncio.Event()

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._loop_task = asyncio.create_task(self._run(), name="scheduler-loop")

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        self._wake.set()
        self._queue.shutdown()
        if self._loop_task is not None:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except (asyncio.CancelledError, Exception):
                pass
        # Drain in-flight workers
        if self._inflight:
            await asyncio.gather(*self._inflight, return_exceptions=True)
        # Resolve any still-pending futures so callers don't hang forever
        for fut in list(self._pending.values()):
            if not fut.done():
                fut.set_exception(SchedulerStoppedError("scheduler stopped"))
        self._pending.clear()

    def notify(self) -> None:
        """Wake the scheduler — e.g. when a token bucket should now have capacity."""
        self._wake.set()

    async def _run(self) -> None:
        try:
            while self._running:
                await self._worker_sem.acquire()
                try:
                    request = await self._select_dispatchable()
                except SchedulerStoppedError:
                    self._worker_sem.release()
                    break
                if request is None:
                    self._worker_sem.release()
                    # Nothing dispatchable right now — wait briefly or until notified
                    try:
                        await asyncio.wait_for(self._wake.wait(), timeout=0.05)
                    except asyncio.TimeoutError:
                        pass
                    self._wake.clear()
                    continue
                task = asyncio.create_task(self._dispatch(request), name=f"dispatch-{request.id}")
                self._inflight.add(task)
                task.add_done_callback(self._on_dispatch_done)
        except asyncio.CancelledError:
            return

    def _on_dispatch_done(self, task: asyncio.Task) -> None:
        self._inflight.discard(task)
        self._worker_sem.release()
        self._wake.set()

    async def _select_dispatchable(self) -> Request | None:
        """Block until *some* request is dispatchable, but never on a head we can't serve.

        Strategy: peek at the best candidate excluding APIs that are currently
        unservable (circuit OPEN, or rate limiter says wait). If we find one,
        pop it. Otherwise return None and let the loop sleep briefly.
        """
        if not self._running:
            raise SchedulerStoppedError()
        # Quick path: queue empty.
        if len(self._queue) == 0:
            try:
                # Short wait to give producers a chance.
                request = await asyncio.wait_for(self._queue.get(), timeout=0.1)
                return request if self._is_dispatchable(request) else self._reject_or_replan(request)
            except asyncio.TimeoutError:
                return None
        excluded: set[str] = set()
        for api, breaker in self._circuit_breakers.items():
            if breaker.state is CircuitState.OPEN:
                excluded.add(api)
        # Also exclude APIs whose buckets can't satisfy 1 token right now.
        for api in list(self._rate_limiter.api_buckets.keys()):
            if self._rate_limiter.time_until(api) > 0.005:
                excluded.add(api)
        candidate = self._queue.peek_pending(exclude_apis=excluded)
        if candidate is None:
            return None
        # Pop *that* specific request.
        popped = await self._queue.pop_specific(candidate.id)
        return popped if popped is not None else None

    def _is_dispatchable(self, request: Request) -> bool:
        breaker = self._circuit_breakers.get(request.api)
        if breaker is not None and breaker.state is CircuitState.OPEN:
            return False
        return self._rate_limiter.time_until(request.api) <= 0.005

    def _reject_or_replan(self, request: Request) -> Request | None:
        # If the popped request can't be dispatched right now, requeue and let
        # the loop come back. We don't synchronously block on it.
        asyncio.create_task(self._queue.put(request))
        return None

    async def _dispatch(self, request: Request) -> None:
        api = request.api
        breaker = self._circuit_breakers.get(api)
        if breaker is not None and not breaker.allow():
            await self._handle_circuit_open(request)
            return

        acquired = await self._rate_limiter.try_acquire(api)
        if not acquired:
            # Rate limiter changed state since we picked — return the request and try again later.
            await self._queue.put(request)
            return

        adapter = self._adapters.get(api)
        if adapter is None:
            self._fail_request(request, NonRetryableError(f"no adapter for {api}"))
            return

        wait_time = self._clock() - (request.enqueued_at or request.created_at)
        self._metrics.observe("queue_wait_seconds", wait_time)
        self._metrics.api_event(api, "dispatch")
        start = self._clock()
        try:
            response = await adapter.execute(request)
        except RateLimitedError as exc:
            self._metrics.api_event(api, "rate_limited")
            self._throttler.record(api, success=False, rate_limited=True)
            await self._maybe_retry(request, exc)
            return
        except UpstreamError as exc:
            self._metrics.api_event(api, "upstream_error")
            if breaker is not None:
                breaker.on_failure()
            self._throttler.record(api, success=False)
            await self._maybe_retry(request, exc)
            return
        except NonRetryableError as exc:
            self._metrics.api_event(api, "client_error")
            self._fail_request(request, exc)
            return
        except Exception as exc:  # noqa: BLE001 — adapter contract leak; classify as upstream
            self._metrics.api_event(api, "unexpected_error")
            if breaker is not None:
                breaker.on_failure()
            self._throttler.record(api, success=False)
            await self._maybe_retry(request, UpstreamError(str(exc)))
            return

        latency = self._clock() - start
        self._metrics.api_event(api, "success", latency=latency)
        self._metrics.incr("requests_succeeded")
        if breaker is not None:
            breaker.on_success()
        self._throttler.record(api, success=True)
        self._complete_request(request, response)

    async def _handle_circuit_open(self, request: Request) -> None:
        self._metrics.api_event(request.api, "circuit_open_block")
        await self._maybe_retry(request, CircuitOpenError(f"{request.api} circuit open"))

    async def _maybe_retry(self, request: Request, exc: AggregatorError) -> None:
        policy = self._retry_policies.get(request.api)
        if policy is None:
            self._fail_request(request, exc)
            return
        decision = policy.decide(request, exc)
        if not decision.should_retry:
            self._metrics.incr("requests_failed")
            self._fail_request(request, exc)
            return
        request.retry_count += 1
        if decision.new_priority is not None:
            request.priority = decision.new_priority
        self._metrics.incr("requests_retried")
        self._metrics.api_event(request.api, "retry")

        async def reenqueue() -> None:
            if decision.delay > 0:
                await asyncio.sleep(decision.delay)
            try:
                await self._queue.put(request)
                self._wake.set()
            except asyncio.QueueFull:
                self._fail_request(request, exc)

        asyncio.create_task(reenqueue())

    def _complete_request(self, request: Request, response: Response) -> None:
        fut = self._pending.pop(request.id, None)
        if fut is not None and not fut.done():
            fut.set_result(response)

    def _fail_request(self, request: Request, exc: BaseException) -> None:
        fut = self._pending.pop(request.id, None)
        if fut is not None and not fut.done():
            fut.set_exception(exc)
