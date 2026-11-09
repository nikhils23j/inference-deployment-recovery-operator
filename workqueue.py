"""A rate-limited work queue modelled on client-go's workqueue.

Guarantees (same as client-go):
  * a key sitting in the queue is stored once, however many events arrive (dedup);
  * a key is never handed to two workers at once; if it is re-added while being
    processed it is marked dirty and re-queued when ``done`` is called;
  * ``add_rate_limited`` delays by per-key exponential backoff (+ jitter here) and a
    global token bucket, whichever is longer; ``forget`` resets the per-key counter.

The clock is injected so the same class runs under the simulator and real time.
It is not thread-safe by itself; the live runtime wraps it in a lock.
"""
from __future__ import annotations

import heapq
import random
from typing import Callable, Hashable


class RateLimiter:
    def __init__(self, base: float = 0.005, cap: float = 30.0, qps: float = 10.0, burst: int = 100,
                 jitter: float = 0.5, rng: random.Random | None = None):
        self.base, self.cap, self.qps, self.burst, self.jitter = base, cap, qps, burst, jitter
        self.failures: dict[Hashable, int] = {}
        self.tokens = float(burst)
        self.last: float | None = None
        self.rng = rng or random.Random(0)

    def when(self, key: Hashable, now: float) -> float:
        n = self.failures.get(key, 0)
        self.failures[key] = n + 1
        expo = min(self.cap, self.base * (2 ** n))
        # "equal jitter": keep at least (1 - jitter) of the delay so backoff still grows
        expo *= (1 - self.jitter) + self.jitter * self.rng.random()
        if self.last is not None:
            self.tokens = min(self.burst, self.tokens + (now - self.last) * self.qps)
        self.last = now
        self.tokens -= 1
        bucket = 0.0 if self.tokens >= 0 else -self.tokens / self.qps
        return max(expo, bucket)

    def forget(self, key: Hashable) -> None:
        self.failures.pop(key, None)

    def retries(self, key: Hashable) -> int:
        return self.failures.get(key, 0)


class WorkQueue:
    def __init__(self, clock: Callable[[], float], limiter: RateLimiter | None = None):
        self.clock = clock
        self.limiter = limiter or RateLimiter()
        self.queue: list[Hashable] = []
        self.dirty: set[Hashable] = set()
        self.processing: set[Hashable] = set()
        self.waiting: list[tuple[float, int, Hashable]] = []
        self._seq = 0
        self.adds = 0

    def add(self, key: Hashable) -> None:
        self.adds += 1
        if key in self.dirty:
            return
        self.dirty.add(key)
        if key not in self.processing:
            self.queue.append(key)

    def add_after(self, key: Hashable, delay: float) -> None:
        if delay <= 0:
            self.add(key)
            return
        self._seq += 1
        heapq.heappush(self.waiting, (self.clock() + delay, self._seq, key))

    def add_rate_limited(self, key: Hashable) -> float:
        delay = self.limiter.when(key, self.clock())
        self.add_after(key, delay)
        return delay

    def forget(self, key: Hashable) -> None:
        self.limiter.forget(key)

    def _promote(self) -> None:
        now = self.clock()
        while self.waiting and self.waiting[0][0] <= now:
            _, _, key = heapq.heappop(self.waiting)
            self.add(key)

    def get(self) -> Hashable | None:
        """Non-blocking: return the next ready key, or None."""
        self._promote()
        if not self.queue:
            return None
        key = self.queue.pop(0)
        self.processing.add(key)
        self.dirty.discard(key)
        return key

    def done(self, key: Hashable) -> None:
        self.processing.discard(key)
        if key in self.dirty:
            self.queue.append(key)

    def next_ready_time(self) -> float | None:
        self._promote()
        if self.queue:
            return self.clock()
        return self.waiting[0][0] if self.waiting else None

    def __len__(self) -> int:
        return len(self.queue)
