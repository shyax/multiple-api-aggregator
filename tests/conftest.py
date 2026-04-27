"""Test fixtures.

A `FakeClock` wraps `time.monotonic`-style usage so token-bucket and aging
behaviour can be exercised without real sleeping.
"""

from __future__ import annotations

import pytest


class FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self._t = start

    def __call__(self) -> float:
        return self._t

    def advance(self, seconds: float) -> None:
        self._t += seconds


@pytest.fixture
def fake_clock() -> FakeClock:
    return FakeClock()
