"""OTA 更新：分批下发、确认、失败/超时回退与离线排队。

确定语义：
- 兼容性：型号必须匹配；当前版本低于固件要求的 ``min_from_version`` 时拒绝，
  错误码 OTA_INCOMPATIBLE。
- 分批：按 device_id 稳定哈希分桶 + percent 百分比；同型号可同时有多个 rollout，
  高版本号 rollout 优先。未在批次内/不兼容 -> 明确拒绝，不静默排队。
- 生命周期：QUEUED(离线) -> DELIVERED(已下发, 带确认截止) -> CONFIRMED；
  设备报失败或 ``confirm_timeout_s`` 到期 -> FAILED -> ROLLED_BACK，并把设备
  当前版本恢复为最近的 CONFIRMED 版本（last_known_good_version）。
- 未 CONFIRMED 的更新绝不改变 last_known_good_version，也不被视为已生效。
- 离线队列按 rollout 有界，超限 -> BACKLOG_FULL，不留半状态（入队与状态切换
  在同一设备锁内）。
"""

from __future__ import annotations

from typing import Any

from .errors import (
    BacklogFullError,
    ConflictError,
    DeviceNotRegisteredError,
    MalformedPayloadError,
    NotFoundError,
    OtaIncompatibleError,
    OtaNoAvailableVersionError,
    OtaStateInvalidError,
)
from .models import Firmware, OtaDelivery, OtaStatus, Rollout
from .storage import HubStore


def version_tuple(v: str) -> tuple[int, ...]:
    parts: list[int] = []
    for p in str(v).split("."):
        if not p.isdigit():
            raise MalformedPayloadError(f"invalid version segment '{p}'")
        parts.append(int(p))
    return tuple(parts)


def version_lt(a: str | None, b: str) -> bool:
    if a is None:
        return True
    return version_tuple(a) < version_tuple(b)


class OtaManager:
    def __init__(self, store: HubStore, *, queue_capacity_per_rollout: int = 10_000) -> None:
        self.store = store
        self.queue_capacity = queue_capacity_per_rollout

    # ---------- 固件 / rollout 管理 ----------

    def register_firmware(
        self,
        *,
        model: str,
        version: str,
        min_from_version: str | None = None,
    ) -> Firmware:
        version_tuple(version)  # 提前归一化格式错误
        key = (model, version)
        if key in self.store.firmware:
            raise ConflictError(f"firmware {model}@{version} already exists")
        fw = Firmware(
            model=model,
            version=version,
            min_from_version=min_from_version,
            created_at=self.store.clock(),
        )
        self.store.firmware[key] = fw
        return fw

    def create_rollout(
        self,
        *,
        firmware_version: str,
        model: str,
        percent: int = 100,
        confirm_timeout_s: float = 3600.0,
        rollout_id: str | None = None,
        min_from_version: str | None = None,
    ) -> Rollout:
        fw = self.store.firmware.get((model, firmware_version))
        if fw is None:
            raise NotFoundError(
                f"firmware {model}@{firmware_version} is not registered"
            )
        rid = rollout_id or f"ro-{model}-{firmware_version}-{len(self.store.rollouts) + 1}"
        if rid in self.store.rollouts:
            raise ConflictError(f"rollout '{rid}' already exists")
        if not 1 <= percent <= 100:
            raise MalformedPayloadError("percent must be within [1,100]")
        ro = Rollout(
            rollout_id=rid,
            firmware_version=firmware_version,
            model=model,
            min_from_version=min_from_version if min_from_version is not None else fw.min_from_version,
            confirm_timeout_s=confirm_timeout_s,
            percent=percent,
            created_at=self.store.clock(),
        )
        self.store.rollouts[rid] = ro
        self.store.ota_queues.setdefault(rid, [])
        return ro

    # ---------- 兼容性 ----------

    def _check_compatible(self, device_model: str, device_version: str | None, ro: Rollout) -> None:
        if device_model != ro.model:
            raise OtaIncompatibleError(
                f"firmware model '{ro.model}' does not match device model '{device_model}'",
                details={"device_model": device_model, "firmware_model": ro.model, "reason": "MODEL_MISMATCH"},
            )
        if ro.min_from_version is not None and version_lt(device_version, ro.min_from_version):
            raise OtaIncompatibleError(
                f"device version {device_version} is below required min_from_version "
                f"{ro.min_from_version}",
                details={
                    "device_version": device_version,
                    "min_from_version": ro.min_from_version,
                    "reason": "VERSION_TOO_OLD",
                },
            )

    def _candidate_rollouts(self, device_id: str) -> list[Rollout]:
        """该设备型号下，按固件版本从高到低的 rollout。"""
        dev = self.store.devices[device_id]
        ros = [ro for ro in self.store.rollouts.values() if ro.model == dev.model]
        return sorted(ros, key=lambda r: version_tuple(r.firmware_version), reverse=True)

    # ---------- 下发 / 拉取 ----------

    def deliver_to_device(self, device_id: str, rollout_id: str | None = None) -> OtaDelivery:
        """把一个 rollout 分配给设备；离线则入队（QUEUED），在线则直接下发（DELIVERED）。"""
        if device_id not in self.store.devices:
            raise DeviceNotRegisteredError()
        with self.store.lock_for(device_id):
            dev = self.store.devices[device_id]
            st = self.store.ota_state_for(device_id)
            now = self.store.clock()

            self._sweep_locked(device_id, now)
            # sweep 可能把状态改成 ROLLED_BACK；终态之外不允许重复分配
            if st.status in (OtaStatus.DELIVERED, OtaStatus.QUEUED):
                raise OtaStateInvalidError(
                    f"device already has an in-progress update in status {st.status.value}",
                    details={"status": st.status.value, "target_version": st.target_version},
                )
            if st.status == OtaStatus.CONFIRMED and st.target_version == dev.current_fw_version:
                same = self.store.rollouts.get(st.rollout_id or "")
                if rollout_id is None or (same and same.rollout_id == rollout_id):
                    raise OtaStateInvalidError(
                        "device is already on the target firmware version",
                        details={"version": dev.current_fw_version},
                    )

            if rollout_id is not None:
                ro = self.store.rollouts.get(rollout_id)
                if ro is None:
                    raise NotFoundError(f"rollout '{rollout_id}' does not exist")
                if not ro.in_batch(device_id):
                    raise OtaIncompatibleError(
                        "device is not selected by this rollout's batch",
                        details={"rollout_id": rollout_id, "reason": "NOT_IN_BATCH"},
                    )
            else:
                ro = self._select_rollout_locked(device_id)
                if ro is None:
                    raise OtaNoAvailableVersionError(
                        details={"device_id": device_id}
                    )

            self._check_compatible(dev.model, dev.current_fw_version, ro)
            if not version_lt(dev.current_fw_version, ro.firmware_version):
                raise OtaStateInvalidError(
                    "device firmware is already at or above the rollout version",
                    details={
                        "device_version": dev.current_fw_version,
                        "target_version": ro.firmware_version,
                    },
                )

            from_version = dev.current_fw_version
            if dev.online:
                return self._mark_delivered_locked(device_id, ro, from_version, now)
            # 离线：入队 + 状态原子切换
            queue = self.store.ota_queues.setdefault(ro.rollout_id, [])
            if len(queue) >= self.queue_capacity:
                self.store.metrics.incr("ota.queue_rejected")
                raise BacklogFullError(
                    "OTA queue for this rollout is full",
                    details={"rollout_id": ro.rollout_id, "limit": self.queue_capacity},
                )
            queue.append(device_id)
            st.rollout_id = ro.rollout_id
            st.from_version = from_version
            st.target_version = ro.firmware_version
            st.status = OtaStatus.QUEUED
            st.delivered_at = None
            st.deadline = None
            st.failure_reason = None
            self.store.metrics.incr("ota.queued")
            return OtaDelivery(
                device_id=device_id,
                rollout_id=ro.rollout_id,
                version=ro.firmware_version,
                from_version=from_version,
                deadline=0.0,
                status=OtaStatus.QUEUED.value,
            )

    def _select_rollout_locked(self, device_id: str) -> Rollout | None:
        st = self.store.ota_state_for(device_id)
        dev = self.store.devices[device_id]
        for ro in self._candidate_rollouts(device_id):
            if not ro.in_batch(device_id):
                continue
            try:
                self._check_compatible(dev.model, dev.current_fw_version, ro)
            except OtaIncompatibleError:
                continue
            if not version_lt(dev.current_fw_version, ro.firmware_version):
                continue
            # 已经在该 rollout 上失败/回退过的，不重复自动选择
            if st.rollout_id == ro.rollout_id and st.status in (
                OtaStatus.FAILED,
                OtaStatus.ROLLED_BACK,
            ):
                continue
            return ro
        return None

    def pull(self, device_id: str) -> OtaDelivery | None:
        """设备上线/轮询时拉取：先扫超时，再下发队列项或新批次。"""
        if device_id not in self.store.devices:
            raise DeviceNotRegisteredError()
        with self.store.lock_for(device_id):
            now = self.store.clock()
            dev = self.store.devices[device_id]
            dev.online = True
            self._sweep_locked(device_id, now)
            st = self.store.ota_state_for(device_id)

            if st.status == OtaStatus.QUEUED and st.rollout_id:
                ro = self.store.rollouts[st.rollout_id]
                queue = self.store.ota_queues.get(ro.rollout_id, [])
                if device_id in queue:
                    queue.remove(device_id)
                return self._mark_delivered_locked(device_id, ro, st.from_version, now)

            if st.status in (OtaStatus.NONE, OtaStatus.ROLLED_BACK, OtaStatus.CONFIRMED):
                ro = self._select_rollout_locked(device_id)
                if ro is None:
                    return None
                self._check_compatible(dev.model, dev.current_fw_version, ro)
                return self._mark_delivered_locked(device_id, ro, dev.current_fw_version, now)
            return None  # DELIVERED 进行中：不重复下发

    def _mark_delivered_locked(
        self,
        device_id: str,
        ro: Rollout,
        from_version: str | None,
        now: float,
    ) -> OtaDelivery:
        st = self.store.ota_state_for(device_id)
        st.rollout_id = ro.rollout_id
        st.from_version = from_version
        st.target_version = ro.firmware_version
        st.status = OtaStatus.DELIVERED
        st.delivered_at = now
        st.deadline = now + ro.confirm_timeout_s
        st.confirmed_at = None
        st.failure_reason = None
        st.attempts += 1
        self.store.metrics.incr("ota.delivered")
        return OtaDelivery(
            device_id=device_id,
            rollout_id=ro.rollout_id,
            version=ro.firmware_version,
            from_version=from_version,
            deadline=st.deadline,
            status=OtaStatus.DELIVERED.value,
        )

    # ---------- 确认 / 失败 / 回退 ----------

    def confirm(self, device_id: str, version: str) -> dict[str, Any]:
        if device_id not in self.store.devices:
            raise DeviceNotRegisteredError()
        with self.store.lock_for(device_id):
            now = self.store.clock()
            self._sweep_locked(device_id, now)
            dev = self.store.devices[device_id]
            st = self.store.ota_state_for(device_id)
            if st.status != OtaStatus.DELIVERED:
                raise OtaStateInvalidError(
                    "no delivered update awaiting confirmation",
                    details={"status": st.status.value},
                )
            if st.target_version != version:
                raise OtaIncompatibleError(
                    f"confirmed version {version} does not match delivered target "
                    f"{st.target_version}",
                    details={
                        "confirmed": version,
                        "target": st.target_version,
                        "reason": "VERSION_MISMATCH",
                    },
                )
            st.status = OtaStatus.CONFIRMED
            st.confirmed_at = now
            st.failure_reason = None
            # 只有确认成功才推进"当前版本"与"已知良好版本"
            dev.current_fw_version = version
            dev.last_known_good_version = version
            self.store.metrics.incr("ota.confirmed")
            return st.to_dict()

    def report_failure(self, device_id: str, reason: str) -> dict[str, Any]:
        if device_id not in self.store.devices:
            raise DeviceNotRegisteredError()
        with self.store.lock_for(device_id):
            st = self.store.ota_state_for(device_id)
            if st.status not in (OtaStatus.DELIVERED, OtaStatus.QUEUED):
                raise OtaStateInvalidError(
                    "no in-progress update to fail",
                    details={"status": st.status.value},
                )
            self._rollback_locked(device_id, "DEVICE_REPORTED:" + reason[:120])
            return self.state_view(device_id)

    def _rollback_locked(self, device_id: str, reason: str) -> None:
        dev = self.store.devices[device_id]
        st = self.store.ota_state_for(device_id)
        now = self.store.clock()
        # 从排队/下发队列移除，避免后续被重复投递
        if st.rollout_id:
            queue = self.store.ota_queues.get(st.rollout_id)
            if queue and device_id in queue:
                queue.remove(device_id)
        attempted = st.target_version
        st.deadline = None
        if dev.last_known_good_version is None:
            # 没有任何已确认版本：保持 FAILED 终态（不会再被重复扫描），
            # 但向调用方返回确定结论——没有可回退的版本。
            st.status = OtaStatus.FAILED
            st.failure_reason = reason
            st.target_version = attempted
            self.store.metrics.incr("ota.failed_no_lkg")
            raise OtaNoAvailableVersionError(
                details={"device_id": device_id, "attempted_version": attempted}
            )
        dev.current_fw_version = dev.last_known_good_version
        st.status = OtaStatus.ROLLED_BACK
        st.failure_reason = reason
        st.target_version = dev.last_known_good_version
        self.store.metrics.incr("ota.rolled_back")

    def _sweep_locked(self, device_id: str, now: float) -> None:
        """检查确认截止：超时即失败并回退（幂等，可重复调用）。"""
        st = self.store.ota_state_for(device_id)
        if st.status == OtaStatus.DELIVERED and st.deadline is not None and now > st.deadline:
            self._rollback_locked(device_id, "CONFIRM_TIMEOUT")

    def sweep_timeouts(self) -> int:
        """全局扫描所有进行中设备的超时（可由后台线程或请求前触发）。

        单台设备回退时若得到"无可用版本"等确定结论，不影响其余设备的扫描。
        """
        now = self.store.clock()
        n = 0
        for device_id, st in list(self.store.ota_states.items()):
            with self.store.lock_for(device_id):
                before = st.status
                try:
                    self._sweep_locked(device_id, now)
                except OtaNoAvailableVersionError:
                    # 状态已落为 FAILED 终态（deadline 已清空），下次不会重复处理
                    self.store.metrics.incr("ota.sweep_no_lkg")
                if before == OtaStatus.DELIVERED and st.status in (
                    OtaStatus.ROLLED_BACK,
                    OtaStatus.FAILED,
                ):
                    n += 1
        return n

    def mark_online(self, device_id: str, online: bool) -> None:
        with self.store.lock_for(device_id):
            dev = self.store.devices.get(device_id)
            if dev is not None:
                dev.online = online
                if online:
                    dev.last_seen_at = self.store.clock()

    def state_view(self, device_id: str) -> dict[str, Any]:
        if device_id not in self.store.devices:
            raise DeviceNotRegisteredError()
        with self.store.lock_for(device_id):
            self._sweep_locked(device_id, self.store.clock())
            st = self.store.ota_state_for(device_id)
            dev = self.store.devices[device_id]
            return {
                **st.to_dict(),
                "current_fw_version": dev.current_fw_version,
                "last_known_good_version": dev.last_known_good_version,
                "online": dev.online,
            }
