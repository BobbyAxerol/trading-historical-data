from __future__ import annotations

import random
import time
from dataclasses import dataclass
from email.utils import parsedate_to_datetime


@dataclass(frozen=True)
class RateProbeResult:
    requested_rps: float
    requests: int
    successes: int
    rate_limited: int
    retry_after_seen: bool
    latency_p95_ms: float | None


def parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return max(0.0, float(text))
    except ValueError:
        pass
    try:
        dt = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    return max(0.0, dt.timestamp() - time.time())


class SyncRateLimiter:
    """Small synchronous limiter for probe/client code.

    Phase 3 may replace this with an async limiter. Keeping Phase 1 sync avoids
    adding async dependencies before API behavior is known.
    """

    def __init__(self, requests_per_second: float):
        self.requests_per_second = max(float(requests_per_second), 0.001)
        self._next_at = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        if self._next_at > now:
            time.sleep(self._next_at - now)
        interval = 1.0 / self.requests_per_second
        self._next_at = max(self._next_at, time.monotonic()) + interval


def backoff_sleep(attempt: int, *, min_delay: float, max_delay: float, jitter: bool = True) -> float:
    delay = min(max_delay, max(min_delay, min_delay * (2 ** max(0, attempt - 1))))
    if jitter:
        delay += random.uniform(0.0, min(1.0, delay * 0.25))
    time.sleep(delay)
    return delay
