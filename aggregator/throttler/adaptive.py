"""Adaptive throttler.

Watches per-API outcomes via a rolling window and adjusts each API's bucket
refill rate (multiplicative AIMD-style: aggressive cut on stress, gentle
recovery on calm). Adjustments are bounded to [min_rate_factor, max_rate_factor]
of the configured base rate so we never exceed the operator-declared budget.

Each API gets its own AdaptiveConfig (see APIConfig.adaptive) so APIs with
different reliability profiles can have different thresholds.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from aggregator.common.config import AdaptiveConfig
from aggregator.rate_limiter.shared_pool import SharedRateLimiter


@dataclass
class _APIState:
    base_rate: float
    config: AdaptiveConfig
    factor: float = 1.0
    outcomes: deque = field(default_factory=lambda: deque(maxlen=50))


class AdaptiveThrottler:
    def __init__(
        self,
        rate_limiter: SharedRateLimiter,
        api_base_rates: dict[str, float],
        api_configs: dict[str, AdaptiveConfig],
    ) -> None:
        self._rate_limiter = rate_limiter
        self._states: dict[str, _APIState] = {}
        for api, rate in api_base_rates.items():
            cfg = api_configs.get(api, AdaptiveConfig())
            self._states[api] = _APIState(
                base_rate=rate,
                config=cfg,
                outcomes=deque(maxlen=cfg.error_window),
            )

    def factor(self, api: str) -> float:
        st = self._states.get(api)
        return st.factor if st else 1.0

    def record(self, api: str, *, success: bool, rate_limited: bool = False) -> None:
        st = self._states.get(api)
        if st is None or not st.config.enabled:
            return
        # 429s count double — upstream is explicitly telling us we're going too fast.
        weight = 2 if rate_limited else 1
        for _ in range(weight):
            st.outcomes.append(0 if success else 1)
        self._maybe_adjust(api, st)

    def _maybe_adjust(self, api: str, st: _APIState) -> None:
        cfg = st.config
        warmup = max(5, (st.outcomes.maxlen or cfg.error_window) // 4)
        if len(st.outcomes) < warmup:
            return
        error_rate = sum(st.outcomes) / len(st.outcomes)
        new_factor = st.factor
        if error_rate >= cfg.high_error_rate:
            new_factor = max(cfg.min_rate_factor, st.factor * cfg.step_down)
        elif error_rate <= cfg.low_error_rate:
            new_factor = min(cfg.max_rate_factor, st.factor * cfg.step_up)
        if abs(new_factor - st.factor) < 1e-6:
            return
        st.factor = new_factor
        bucket = self._rate_limiter.api_buckets.get(api)
        if bucket is not None:
            bucket.rate = st.base_rate * new_factor

    def snapshot(self) -> dict[str, dict]:
        return {
            api: {
                "factor": round(st.factor, 3),
                "effective_rate": round(st.base_rate * st.factor, 3),
                "samples": len(st.outcomes),
                "error_rate": (
                    round(sum(st.outcomes) / len(st.outcomes), 3) if st.outcomes else 0.0
                ),
            }
            for api, st in self._states.items()
        }
