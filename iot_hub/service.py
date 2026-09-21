"""门面：组合各子系统，统一鉴权、限流、统计与错误归一化。

一致性约定：
- 每个对外操作先限流再鉴权再执行，失败路径不写任何业务状态；
- 统计计数仅在操作最终生效/拒绝时各累加一次，持锁更新，绝不重复累加；
- OTA 确认成功才推进设备固件版本；失败/超时设备保持原可用版本。
"""
from __future__ import annotations

import threading
import time

from .auth import DeviceRegistry
from .errors import ErrorCode, HubError
from .models import OtaJob, Scope, ShadowState, TelemetryMessage
from .ota import OtaManager
from .ratelimit import Backlog, RateLimiter
from .shadow import ShadowStore
from .telemetry import TelemetryIngestor


class Stats:
    """线程安全计数器：键为 (维度, 结论)，如 ('telemetry', 'applied')。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: dict[tuple[str, str], int] = {}

    def incr(self, dimension: str, outcome: str) -> None:
        with self._lock:
            key = (dimension, outcome)
            self._counts[key] = self._counts.get(key, 0) + 1

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {f"{d}/{o}": n for (d, o), n in sorted(self._counts.items())}


class DeviceHubService:
    def __init__(self, now_fn=time.time) -> None:
        self._now = now_fn
        self.registry = DeviceRegistry()
        self.telemetry = TelemetryIngestor()
        self.shadow = ShadowStore()
        self.ota = OtaManager()
        self.rate_limiter = RateLimiter(10.0, 20, 1000.0, 2000)
        self.backlog = Backlog(10000)
        self.stats = Stats()

    # ---- 连接生命周期 ----

    def connect(self, token: str) -> str:
        device = self.registry.authenticate(token, Scope.SHADOW_READ, self._now())
        self.registry.set_online(device.device_id, True, self._now())
        self.stats.incr("connection", "accepted")
        return device.device_id

    def disconnect(self, device_id: str) -> None:
        self.registry.set_online(device_id, False, self._now())
        self.shadow.mark_offline(device_id)
        self.stats.incr("connection", "closed")

    # ---- 遥测 ----

    def ingest_telemetry(self, token: str, msg: TelemetryMessage) -> str:
        now = self._now()
        self.rate_limiter.acquire(msg.device_id, now)
        device = self.registry.authenticate(token, Scope.TELEMETRY_WRITE, now)
        if device.device_id != msg.device_id:
            self.stats.incr("telemetry", "scope_denied")
            raise HubError(ErrorCode.SCOPE_DENIED, "不得代其他设备上报")
        try:
            result = self.telemetry.ingest(msg, now)
        except HubError as exc:
            self.stats.incr("telemetry", exc.code.value.lower())
            raise
        self.stats.incr("telemetry", "applied")
        return result

    # ---- 影子 ----

    def update_reported(self, token: str, device_id: str, patch: dict,
                        base_version: int | None) -> ShadowState:
        now = self._now()
        self.rate_limiter.acquire(device_id, now)
        device = self.registry.authenticate(token, Scope.SHADOW_WRITE, now)
        if device.device_id != device_id:
            self.stats.incr("shadow", "scope_denied")
            raise HubError(ErrorCode.SCOPE_DENIED, "不得修改其他设备影子")
        try:
            state = self.shadow.update_reported(
                device_id, patch, base_version, online=device.online, now=now)
        except HubError as exc:
            self.stats.incr("shadow", exc.code.value.lower())
            raise
        self.stats.incr("shadow", "applied")
        return state

    def update_desired(self, device_id: str, patch: dict,
                       base_version: int | None) -> ShadowState:
        now = self._now()
        self.rate_limiter.acquire(device_id, now)
        try:
            state = self.shadow.update_desired(device_id, patch, base_version, now)
        except HubError as exc:
            self.stats.incr("shadow", exc.code.value.lower())
            raise
        self.stats.incr("shadow", "applied")
        return state

    def get_shadow(self, device_id: str) -> ShadowState:
        return self.shadow.get(device_id)

    # ---- OTA ----

    def dispatch_ota(self, campaign_id: str, device_id: str) -> OtaJob:
        device = self.registry.get_device(device_id)
        try:
            job = self.ota.dispatch(campaign_id, device, self._now())
        except HubError as exc:
            self.stats.incr("ota", exc.code.value.lower())
            raise
        self.stats.incr("ota", job.status.value.lower())
        return job

    def ack_ota(self, token: str, device_id: str, job_id: str, success: bool) -> OtaJob:
        now = self._now()
        device = self.registry.authenticate(token, Scope.OTA_ACK, now)
        if device.device_id != device_id:
            self.stats.incr("ota", "scope_denied")
            raise HubError(ErrorCode.SCOPE_DENIED, "不得代其他设备确认升级")
        try:
            job = self.ota.ack(device_id, job_id, success, now)
        except HubError as exc:
            self.stats.incr("ota", exc.code.value.lower())
            raise
        if success:
            # 只有确认成功才推进固件版本；失败/超时保持原可用版本
            device.firmware_version = job.target_version
        self.stats.incr("ota", job.status.value.lower())
        return job

    def sweep_ota_timeouts(self) -> list[OtaJob]:
        jobs = self.ota.sweep_timeouts(self._now())
        for _ in jobs:
            self.stats.incr("ota", "rolled_back")
        return jobs

    def promote_ota(self, campaign_id: str) -> list[OtaJob]:
        def is_online(device_id: str) -> bool:
            try:
                return self.registry.get_device(device_id).online
            except HubError:
                return False
        return self.ota.promote_queued(campaign_id, is_online, self._now())
