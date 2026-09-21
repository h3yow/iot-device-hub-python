"""按设备与全局维度的限速（令牌桶）与有界积压队列。

策略：任一维度超限即拒绝（RATE_LIMITED / BACKLOG_FULL），
不降级为静默丢弃；所有状态有界，杜绝无界内存占用。
"""
from __future__ import annotations

import threading
from collections import deque

from .errors import ErrorCode, HubError


class _Bucket:
    __slots__ = ("capacity", "tokens", "rate", "updated_at")

    def __init__(self, rate: float, burst: int) -> None:
        self.capacity = float(burst)
        self.tokens = float(burst)
        self.rate = rate
        self.updated_at = 0.0

    def try_take(self, now: float) -> bool:
        elapsed = max(0.0, now - self.updated_at)
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.updated_at = now
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


class RateLimiter:
    """令牌桶：每设备 + 全局双维度，任一超限即拒绝。"""

    def __init__(self, per_device_rate: float, per_device_burst: int,
                 global_rate: float, global_burst: int,
                 max_device_buckets: int = 100000) -> None:
        self._lock = threading.Lock()
        self._per_device_rate = per_device_rate
        self._per_device_burst = per_device_burst
        self._global = _Bucket(global_rate, global_burst)
        self._devices: dict[str, _Bucket] = {}
        self._max_device_buckets = max_device_buckets

    def acquire(self, device_id: str, now: float) -> None:
        with self._lock:
            bucket = self._devices.get(device_id)
            if bucket is None:
                if len(self._devices) >= self._max_device_buckets:
                    raise HubError(ErrorCode.RATE_LIMITED, "限速表已满")
                bucket = _Bucket(self._per_device_rate, self._per_device_burst)
                bucket.updated_at = now
                self._devices[device_id] = bucket
            # 先取全局再取设备；全局失败不消耗设备令牌
            if not self._global.try_take(now):
                raise HubError(ErrorCode.RATE_LIMITED, "超出全局限速")
            if not bucket.try_take(now):
                raise HubError(ErrorCode.RATE_LIMITED, "超出设备限速")


class Backlog:
    """有界积压队列：满时抛 BACKLOG_FULL，保证内存有界。"""

    def __init__(self, capacity: int) -> None:
        self._lock = threading.Lock()
        self._capacity = capacity
        self._items: deque = deque()

    def push(self, item: object) -> None:
        with self._lock:
            if len(self._items) >= self._capacity:
                raise HubError(ErrorCode.BACKLOG_FULL, "积压队列已满")
            self._items.append(item)

    def pop(self) -> object | None:
        with self._lock:
            return self._items.popleft() if self._items else None

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)
