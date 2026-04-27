"""Per-API circuit breaker.

State machine:

    CLOSED  --(failure_threshold consecutive failures)-->  OPEN
    OPEN    --(recovery_timeout elapsed)-->                HALF_OPEN
    HALF_OPEN --(success_threshold successes)-->          CLOSED
    HALF_OPEN --(any failure)-->                          OPEN

While HALF_OPEN, only `half_open_max_probes` calls are allowed in flight; the
scheduler is responsible for picking which request becomes the probe.
"""

from __future__ import annotations

import time
from enum import Enum
from typing import Callable

from aggregator.common.config import CircuitBreakerConfig


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(
        self,
        api: str,
        config: CircuitBreakerConfig,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.api = api
        self._config = config
        self._clock = clock
        self._state = CircuitState.CLOSED
        self._consec_failures = 0
        self._consec_successes = 0
        self._opened_at: float | None = None
        self._probes_in_flight = 0
        self._total_failures = 0
        self._total_trips = 0

    @property
    def state(self) -> CircuitState:
        self._maybe_transition_to_half_open()
        return self._state

    @property
    def trips(self) -> int:
        return self._total_trips

    @property
    def failures(self) -> int:
        return self._total_failures

    def _maybe_transition_to_half_open(self) -> None:
        if self._state is CircuitState.OPEN and self._opened_at is not None:
            if self._clock() - self._opened_at >= self._config.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                self._probes_in_flight = 0
                self._consec_successes = 0

    def allow(self) -> bool:
        """Check whether a call is allowed right now. If allowed in HALF_OPEN, reserves a probe slot."""
        self._maybe_transition_to_half_open()
        if self._state is CircuitState.CLOSED:
            return True
        if self._state is CircuitState.OPEN:
            return False
        # HALF_OPEN
        if self._probes_in_flight < self._config.half_open_max_probes:
            self._probes_in_flight += 1
            return True
        return False

    def on_success(self) -> None:
        if self._state is CircuitState.HALF_OPEN:
            self._probes_in_flight = max(0, self._probes_in_flight - 1)
            self._consec_successes += 1
            if self._consec_successes >= self._config.success_threshold:
                self._reset_to_closed()
        else:
            self._consec_failures = 0

    def on_failure(self) -> None:
        self._total_failures += 1
        if self._state is CircuitState.HALF_OPEN:
            self._probes_in_flight = max(0, self._probes_in_flight - 1)
            self._trip()
            return
        self._consec_failures += 1
        self._consec_successes = 0
        if self._consec_failures >= self._config.failure_threshold:
            self._trip()

    def _trip(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = self._clock()
        self._total_trips += 1
        self._consec_failures = 0
        self._consec_successes = 0
        self._probes_in_flight = 0

    def _reset_to_closed(self) -> None:
        self._state = CircuitState.CLOSED
        self._opened_at = None
        self._consec_failures = 0
        self._consec_successes = 0
        self._probes_in_flight = 0

    def force_open(self) -> None:
        self._trip()

    def force_close(self) -> None:
        self._reset_to_closed()
