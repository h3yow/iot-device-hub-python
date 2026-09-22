"""设备影子：持久状态、单调版本与新鲜度。

- reported 只能由遥测序号推进（见 telemetry 模块），服务端 desired 写入采用
  乐观版本控制：``expected_version`` 不匹配 -> SHADOW_VERSION_CONFLICT，
  调用方可重试；任何写入都在设备锁内整体完成，不存在半更新。
- 版本只增不减；旧序号或冲突写入绝不回退版本，也不会重复计数（序号/版本均
  单调）。
- 离线/超时数据的新鲜度由视图显式标注：reported 超过 ``stale_after_s`` 即 STALE，
  从未上报为 UNKNOWN，业务方据此判定，旧值不会伪装成最新值。
"""

from __future__ import annotations

from typing import Any

from .errors import MalformedPayloadError, ShadowVersionConflictError
from .models import Freshness, ShadowState
from .storage import HubStore


class ShadowManager:
    def __init__(self, store: HubStore, *, stale_after_s: float = 60.0) -> None:
        self.store = store
        self.stale_after_s = stale_after_s

    def get_state(self, device_id: str) -> ShadowState:
        return self.store.shadow_for(device_id)

    def freshness(self, state: ShadowState, now: float) -> str:
        if state.reported_at is None:
            return Freshness.UNKNOWN.value
        return (
            Freshness.FRESH.value
            if now - state.reported_at <= self.stale_after_s
            else Freshness.STALE.value
        )

    def view(self, device_id: str, *, now: float | None = None) -> dict[str, Any]:
        now = self.store.clock() if now is None else now
        with self.store.lock_for(device_id):
            state = self.get_state(device_id)
            return {
                "device_id": device_id,
                "version": state.version,
                "reported": dict(state.reported),
                "desired": dict(state.desired),
                "delta": state.delta(),
                "reported_seq": state.reported_seq,
                "reported_at": state.reported_at,
                "updated_at": state.updated_at,
                "freshness": self.freshness(state, now),
            }

    def update_desired(
        self,
        device_id: str,
        changes: dict[str, Any],
        *,
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        """合并写入 desired（值为 None 的键表示删除）。

        :param expected_version: 调用方持有的基线版本；不匹配则拒绝且不改动状态。
        """
        if not isinstance(changes, dict):
            raise MalformedPayloadError("desired changes must be a JSON object")
        with self.store.lock_for(device_id):
            state = self.get_state(device_id)
            if expected_version is not None and expected_version != state.version:
                self.store.metrics.incr("shadow.conflict")
                raise ShadowVersionConflictError(
                    details={
                        "expected_version": expected_version,
                        "current_version": state.version,
                    }
                )
            for k, v in changes.items():
                if v is None:
                    state.desired.pop(k, None)
                else:
                    state.desired[k] = v
            now = self.store.clock()
            state.updated_at = now
            state.version += 1
            self.store.metrics.incr("shadow.desired_updated")
            return self.view(device_id, now=now)
