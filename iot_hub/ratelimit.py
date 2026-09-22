"""按设备与全局两个维度的限速、以及全局有界积压控制。

- 限速：令牌桶，突发量 ``burst`` 不超过每秒补充上限的累积；每设备桶懒创建，
  设备桶表本身也有硬上限（避免海量未知设备 ID 撑爆内存）。
- 积压：全局有界计数信号量。进入处理前 acquire，结束（成功/失败/异常）必须
  release，保证失败操作不残留占用，任何时刻在途量 <= 上限。
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from typing import Callable

from .errors import BacklogFullError, RateLimitedError


class TokenBucket:
    """标准令牌桶。容量 burst，每秒补充 rate 个。"""

    def __init__(self, rate: float, burst: float, clock: Callable[[], float] | None = None) -> None:
        if rate < 0 or burst < 0:
            raise ValueError("rate and burst must be non-negative")
        self.rate = rate
        self.burst = burst
        self.tokens = float(burst)
        self.updated = (clock or time.time)()
        self._clock = clock or time.time
        self._lock = threading.Lock()

    def allow(self, cost: float = 1.0) -> bool:
        with self._lock:
            now = self._clock()
            elapsed = max(0.0, now - self.updated)
            self.updated = now
            self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
            if self.tokens >= cost:
                self.tokens -= cost
                return True
            return False


class RateLimiter:
    """全局桶 + 每设备桶（设备桶表 LRU 有界）。"""

    def __init__(
        self,
        *,
        global_rate: float = 10_000.0,
        global_burst: float = 20_000.0,
        device_rate: float = 10.0,
        device_burst: float = 20.0,
        max_device_buckets: int = 100_000,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._clock = clock or time.time
        self.global_bucket = TokenBucket(global_rate, global_burst, self._clock)
        self.device_rate = device_rate
        self.device_burst = device_burst
        self.max_device_buckets = max_device_buckets
        self._device_buckets: OrderedDict[str, TokenBucket] = OrderedDict()
        self._guard = threading.Lock()

    def _get_device_bucket(self, device_id: str) -> TokenBucket:
        with self._guard:
            b = self._device_buckets.get(device_id)
            if b is None:
                b = TokenBucket(self.device_rate, self.device_burst, self._clock)
                self._device_buckets[device_id] = b
            else:
                self._device_buckets.move_to_end(device_id)
            while len(self._device_buckets) > self.max_device_buckets:
                self._device_buckets.popitem(last=False)
            return b

    def check(self, device_id: str, cost: float = 1.0) -> None:
        """先设备维度后全局维度；超限抛 RateLimitedError，拒绝原因可区分。"""
        db = self._get_device_bucket(device_id)
        if not db.allow(cost):
            raise RateLimitedError(
                "per-device rate limit exceeded",
                details={"dimension": "device", "device_id": device_id},
            )
        if not self.global_bucket.allow(cost):
            # 设备桶已消耗一个令牌而全局拒绝：归还，避免设备维度被多扣
            with db._lock:
                db.tokens = min(db.burst, db.tokens + cost)
            raise RateLimitedError(
                "global rate limit exceeded",
                details={"dimension": "global"},
            )


class BoundedBacklog:
    """全局有界在途计数，上下文管理器语义，保证异常路径也释放。"""

    def __init__(self, limit: int) -> None:
        if limit <= 0:
            raise ValueError("backlog limit must be positive")
        self.limit = limit
        self._sem = threading.BoundedSemaphore(limit)
        self._active_lock = threading.Lock()
        self._active = 0

    @property
    def active(self) -> int:
        with self._active_lock:
            return self._active

    def acquire(self) -> None:
        if not self._sem.acquire(blocking=False):
            raise BacklogFullError(details={"limit": self.limit})
        with self._active_lock:
            self._active += 1

    def release(self) -> None:
        with self._active_lock:
            if self._active <= 0:
                # 防御性：绝不允许多释放导致计数为负/信号量越界
                return
            self._active -= 1
        self._sem.release()

    def __enter__(self) -> "BoundedBacklog":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()
