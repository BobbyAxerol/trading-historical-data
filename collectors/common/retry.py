from __future__ import annotations

import random
import time
from collections import deque
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def retry_sync(
    fn: Callable[[], T],
    *,
    attempts: int = 5,
    base_sleep: float = 2.0,
    max_sleep: float = 300.0,
    retryable: tuple[type[BaseException], ...] = (Exception,),
) -> T:
    last_error: BaseException | None = None
    for attempt in range(attempts):
        try:
            return fn()
        except retryable as exc:
            last_error = exc
            if attempt == attempts - 1:
                break
            sleep_for = min(base_sleep**attempt + random.uniform(0.2, 1.5), max_sleep)
            time.sleep(sleep_for)
    assert last_error is not None
    raise last_error


class SlidingWindowRateLimiter:
    def __init__(self, max_calls: int, period_seconds: int):
        self.max_calls = max_calls
        self.period_seconds = period_seconds
        self.calls: deque[float] = deque()

    def wait(self) -> None:
        now = time.time()
        cutoff = now - self.period_seconds
        while self.calls and self.calls[0] < cutoff:
            self.calls.popleft()

        if len(self.calls) >= self.max_calls:
            sleep_for = self.calls[0] + self.period_seconds - now + 0.5
            if sleep_for > 0:
                time.sleep(sleep_for)

        self.calls.append(time.time())

