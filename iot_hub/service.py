"""平台门面：把鉴权、限速/积压、遥测、影子、OTA 编排成一组确定的用例。

所有设备相关用例都遵循相同顺序：
1. 入口信号量（BoundedBacklog）——任何异常路径都释放，绝不残留占用；
2. 鉴权（未注册/吊销/宽限过期/scope 不足分别给出可区分错误）；
3. 限速（设备维度 -> 全局维度）；
4. 业务处理（设备锁内原子推进）。
"""

from __future__ import annotations

import threading
from typing import Any, Callable

from .auth import AuthManager
from .models import Scope
from .ota import OtaManager
from .ratelimit import BoundedBacklog, RateLimiter
from .shadow import ShadowManager
from .storage import HubStore
from .telemetry import TelemetryManager


class HubService:
    def __init__(
        self,
        store: HubStore | None = None,
        *,
        rate_limiter: RateLimiter | None = None,
        backlog: BoundedBacklog | None = None,
        max_inflight: int = 10_000,
        clock: Callable[[], float] | None = None,
        auto_sweep: bool = True,
        sweep_interval_s: float = 1.0,
    ) -> None:
        self.store = store or HubStore(clock=clock)
        self.auth = AuthManager(self.store)
        self.telemetry = TelemetryManager(self.store)
        self.shadow = ShadowManager(self.store)
        self.ota = OtaManager(self.store)
        self.rate_limiter = rate_limiter or RateLimiter(clock=clock)
        self.backlog = backlog or BoundedBacklog(max_inflight)
        self._sweep_stop = threading.Event()
        self._sweep_thread: threading.Thread | None = None
        if auto_sweep:
            self.start_sweeper(sweep_interval_s)

    # ---------- 后台确认超时扫描 ----------

    def start_sweeper(self, interval_s: float = 1.0) -> None:
        if self._sweep_thread is not None:
            return

        def _run() -> None:
            while not self._sweep_stop.wait(interval_s):
                try:
                    self.ota.sweep_timeouts()
                except Exception:  # noqa: BLE001 - 扫描失败不得影响其他路径
                    pass

        self._sweep_thread = threading.Thread(
            target=_run, name="ota-timeout-sweeper", daemon=True
        )
        self._sweep_thread.start()

    def stop_sweeper(self) -> None:
        self._sweep_stop.set()
        if self._sweep_thread is not None:
            self._sweep_thread.join(timeout=2.0)
            self._sweep_thread = None

    # ---------- 注册/凭据（管理面） ----------

    def register_device(
        self, device_id: str, model: str, *, initial_fw_version: str | None = None
    ) -> dict[str, Any]:
        device, secret, cred = self.auth.register_device(
            device_id, model, initial_fw_version=initial_fw_version
        )
        self.store.metrics.incr("device.registered")
        return {
            "device_id": device.device_id,
            "model": device.model,
            "credential_id": cred.credential_id,
            "secret": secret,
            "generation": cred.generation,
            "scopes": sorted(cred.scopes),
        }

    def rotate_credential(
        self, device_id: str, *, grace_period_s: float = 300.0
    ) -> dict[str, Any]:
        old_ids = self.store.list_credential_ids(device_id)
        secret, cred = self.auth.rotate_credential(
            device_id, grace_period_s=grace_period_s
        )
        old_grace = [
            {"credential_id": cid, "grace_until": self.store.credentials[cid].grace_until}
            for cid in old_ids
            if self.store.credentials[cid].grace_until is not None
        ]
        return {
            "device_id": device_id,
            "credential_id": cred.credential_id,
            "secret": secret,
            "generation": cred.generation,
            "grace_until": cred.grace_until,
            "retired_credentials": old_grace,
            "scopes": sorted(cred.scopes),
        }

    def revoke_credential(self, credential_id: str) -> dict[str, str]:
        self.auth.revoke_credential(credential_id)
        self.store.metrics.incr("credential.revoked")
        return {"credential_id": credential_id, "status": "REVOKED"}

    # ---------- 设备面：连接 / 遥测 ----------

    def connect(
        self, device_id: str, credential_id: str, secret: str
    ) -> dict[str, Any]:
        with self.backlog:
            # 先鉴权再建立会话：未注册/吊销/宽限过期的连接不会推进纪元
            ctx = self.auth.authenticate(
                device_id, credential_id, secret, Scope.TELEMETRY_PUBLISH.value
            )
            epoch = self.auth.begin_session(device_id)
            self.ota.mark_online(device_id, True)
            return {
                "device_id": device_id,
                "authenticated": True,
                "generation": ctx.generation,
                "grace": ctx.grace,
                "epoch": epoch,
                "scopes": sorted(ctx.scopes),
            }

    def disconnect(self, device_id: str) -> dict[str, Any]:
        self.ota.mark_online(device_id, False)
        self.store.metrics.incr("device.disconnect")
        return {"device_id": device_id, "online": False}

    def publish_telemetry(
        self,
        device_id: str,
        credential_id: str,
        secret: str,
        seq: int,
        ts: float,
        payload: Any,
        *,
        idempotency_key: str | None = None,
        epoch: int | None = None,
    ) -> dict[str, Any]:
        with self.backlog:
            ctx = self.auth.authenticate(
                device_id, credential_id, secret, Scope.TELEMETRY_PUBLISH.value,
                epoch=epoch,
            )
            self.rate_limiter.check(device_id)
            result = self.telemetry.publish(
                ctx, seq, ts, payload, idempotency_key=idempotency_key
            )
            return result.to_dict()

    # ---------- 影子 ----------

    def get_shadow(
        self, device_id: str, credential_id: str | None = None, secret: str | None = None
    ) -> dict[str, Any]:
        with self.backlog:
            if credential_id is not None:
                self.auth.authenticate(
                    device_id, credential_id, secret or "", Scope.SHADOW_READ.value
                )
            return self.shadow.view(device_id)

    def update_desired(
        self,
        device_id: str,
        changes: dict[str, Any],
        *,
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        with self.backlog:
            return self.shadow.update_desired(
                device_id, changes, expected_version=expected_version
            )

    # ---------- OTA ----------

    def register_firmware(
        self, *, model: str, version: str, min_from_version: str | None = None
    ) -> dict[str, Any]:
        fw = self.ota.register_firmware(
            model=model, version=version, min_from_version=min_from_version
        )
        return {
            "model": fw.model,
            "version": fw.version,
            "min_from_version": fw.min_from_version,
        }

    def create_rollout(self, **kwargs: Any) -> dict[str, Any]:
        ro = self.ota.create_rollout(**kwargs)
        return {
            "rollout_id": ro.rollout_id,
            "firmware_version": ro.firmware_version,
            "model": ro.model,
            "percent": ro.percent,
            "confirm_timeout_s": ro.confirm_timeout_s,
            "min_from_version": ro.min_from_version,
        }

    def assign_ota(self, device_id: str, rollout_id: str | None = None) -> dict[str, Any]:
        with self.backlog:
            delivery = self.ota.deliver_to_device(device_id, rollout_id)
            return delivery.to_dict()

    def pull_ota(
        self, device_id: str, credential_id: str, secret: str, *, epoch: int | None = None
    ) -> dict[str, Any]:
        with self.backlog:
            self.auth.authenticate(
                device_id, credential_id, secret, Scope.OTA_READ.value, epoch=epoch
            )
            delivery = self.ota.pull(device_id)
            return delivery.to_dict() if delivery else {"status": "UP_TO_DATE"}

    def confirm_ota(
        self, device_id: str, credential_id: str, secret: str, version: str,
        *, epoch: int | None = None,
    ) -> dict[str, Any]:
        with self.backlog:
            self.auth.authenticate(
                device_id, credential_id, secret, Scope.OTA_READ.value, epoch=epoch
            )
            self.rate_limiter.check(device_id)
            return self.ota.confirm(device_id, version)

    def report_ota_failure(
        self, device_id: str, credential_id: str, secret: str, reason: str,
        *, epoch: int | None = None,
    ) -> dict[str, Any]:
        with self.backlog:
            self.auth.authenticate(
                device_id, credential_id, secret, Scope.OTA_READ.value, epoch=epoch
            )
            return self.ota.report_failure(device_id, reason)

    def ota_state(self, device_id: str) -> dict[str, Any]:
        return self.ota.state_view(device_id)

    # ---------- 观测 / 持久化 ----------

    def metrics(self) -> dict[str, int]:
        return self.store.metrics.snapshot()

    def save_snapshot(self, path: str) -> None:
        self.store.save_snapshot_file(path)

    def load_snapshot(self, path: str) -> None:
        self.store.load_snapshot_file(path)
