"""In-process metrics collector.

Designed to be embedded — no Prometheus dependency. Counters and gauges are
plain dicts; histograms are bounded reservoirs that emit p50/p95/p99 on
snapshot. Snapshot output is JSON-serialisable so it drops into structured
logs or any sink the operator wires up.
"""

from __future__ import annotations

import bisect
import time
from collections import defaultdict, deque
from typing import Any


class _Histogram:
    __slots__ = ("samples", "_max")

    def __init__(self, max_samples: int = 1000) -> None:
        self._max = max_samples
        self.samples: deque[float] = deque(maxlen=max_samples)

    def observe(self, value: float) -> None:
        self.samples.append(value)

    def percentiles(self) -> dict[str, float]:
        if not self.samples:
            return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "count": 0}
        ordered = sorted(self.samples)
        n = len(ordered)

        def pct(p: float) -> float:
            idx = min(n - 1, max(0, int(round(p * (n - 1)))))
            return round(ordered[idx], 4)

        return {
            "p50": pct(0.50),
            "p95": pct(0.95),
            "p99": pct(0.99),
            "count": n,
        }


class MetricsCollector:
    def __init__(self) -> None:
        self.started_at = time.monotonic()
        self.counters: dict[str, int] = defaultdict(int)
        self.gauges: dict[str, float] = {}
        self._histograms: dict[str, _Histogram] = defaultdict(_Histogram)
        self._per_api_counters: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._per_api_latency: dict[str, _Histogram] = defaultdict(_Histogram)

    def incr(self, key: str, value: int = 1) -> None:
        self.counters[key] += value

    def gauge(self, key: str, value: float) -> None:
        self.gauges[key] = value

    def observe(self, key: str, value: float) -> None:
        self._histograms[key].observe(value)

    def api_event(self, api: str, event: str, *, latency: float | None = None) -> None:
        self._per_api_counters[api][event] += 1
        if latency is not None:
            self._per_api_latency[api].observe(latency)

    def snapshot(self) -> dict[str, Any]:
        uptime = time.monotonic() - self.started_at
        return {
            "uptime_seconds": round(uptime, 3),
            "counters": dict(self.counters),
            "gauges": dict(self.gauges),
            "latency": {k: h.percentiles() for k, h in self._histograms.items()},
            "per_api": {
                api: {
                    "events": dict(events),
                    "latency": self._per_api_latency[api].percentiles()
                    if api in self._per_api_latency
                    else {"p50": 0.0, "p95": 0.0, "p99": 0.0, "count": 0},
                }
                for api, events in self._per_api_counters.items()
            },
        }
