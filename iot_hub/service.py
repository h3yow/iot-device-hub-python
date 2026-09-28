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
from .command import CommandQueue
from .errors import ErrorCode, HubError
from .models import Command, ExpiryConclusion, OtaJob, Scope, ShadowState, TelemetryMessage
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
        self.commands = CommandQueue()
        self.rate_limiter = RateLimiter(10.0, 20, 1000.0, 2000)
        self.backlog = Backlog(10000)
        self.stats = Stats()

    # ---- 连接生命周期 ----

    def connect(self, token: str) -> str:
        device_id, _, _ = self.connect_with_report(token)
        return device_id

    def connect_with_report(self, token: str
                            ) -> tuple[str, list[Command], list[ExpiryConclusion]]:
        """与 connect 相同，但把本次重连补投与过期剔除结论一并带回。

        返回 (device_id, delivered, expired_conclusions)：业务侧可直接拿到
        被剔除的具体指令与原因，而不只是一个汇总计数。统计仍只各计一次。
        """
        now = self._now()
        device = self.registry.authenticate(token, Scope.SHADOW_READ, now)
        self.registry.set_online(device.device_id, True, now)
        # 重连即按发送先后补投该设备待投递指令；过期者在投递前剔除并落可解释结论
        delivered, expired = self.commands.drain_report(device.device_id, now)
        self.stats.incr("connection", "accepted")
        for _ in delivered:
            self.stats.incr("command", "delivered")
        for _ in expired:
            self.stats.incr("command", "expired")
        return device.device_id, delivered, expired

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

    # ---- 离线指令/配置下发 ----

    def send_command(self, device_id: str, kind: str, payload: dict,
                     idempotency_key: str, ttl: float | None = None) -> Command:
        """业务侧下发：设备不存在明确拒绝；离线排队、在线直接投递。

        任何拒绝（设备不存在/参数非法/队列超限/重复幂等键）都在状态提交前
        返回，队列里不留半条记录，也不消耗序号与后续额度。
        """
        now = self._now()
        try:
            device = self.registry.get_device(device_id)
        except HubError as exc:
            self.stats.incr("command", exc.code.value.lower())
            raise
        try:
            command, created = self.commands.enqueue(
                device_id, kind, payload, idempotency_key, now,
                ttl=ttl, online=device.online)
        except HubError as exc:
            self.stats.incr("command", exc.code.value.lower())
            raise
        if created:
            # 在线即投递计 delivered，离线排队计 queued；幂等命中不计数
            outcome = "delivered" if command.delivery_count > 0 else "queued"
            self.stats.incr("command", outcome)
        return command

    def ack_command(self, token: str, device_id: str, command_id: str,
                    success: bool) -> Command:
        """设备确认：先鉴权（凭据不可用与队列结论可区分）再校验归属，终态只生效一次。"""
        now = self._now()
        self.rate_limiter.acquire(device_id, now)
        device = self.registry.authenticate(token, Scope.COMMAND_ACK, now)
        if device.device_id != device_id:
            self.stats.incr("command", "scope_denied")
            raise HubError(ErrorCode.SCOPE_DENIED, "不得代其他设备确认指令")
        try:
            command = self.commands.ack(device_id, command_id, success, now)
        except HubError as exc:
            self.stats.incr("command", exc.code.value.lower())
            raise
        self.stats.incr("command", command.status.value.lower())
        return command

    def sweep_expired_commands(self) -> list[Command]:
        """清扫所有已过有效期仍未走完的指令，结论可解释。"""
        expired = self.commands.sweep_expired(self._now())
        for _ in expired:
            self.stats.incr("command", "expired")
        return expired

    def pending_commands(self, device_id: str) -> list[Command]:
        return self.commands.pending_for(device_id)

    def expired_command_conclusions(self, device_id: str,
                                    limit: int | None = None
                                    ) -> list[ExpiryConclusion]:
        """查询一台设备被剔除的过期指令结论（哪一条、原因、时刻），按发生先后返回。

        设备不存在明确拒绝（UNKNOWN_DEVICE）；结论与活动队列回收解耦，
        历史指令确认/回收后仍可查。
        """
        self.registry.get_device(device_id)
        return self.commands.expired_for(device_id, limit=limit)
