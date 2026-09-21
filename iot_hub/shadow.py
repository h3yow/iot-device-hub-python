"""设备影子：持久状态、单调版本、新鲜度标记。

语义约定：
- version 单调递增，每次生效写入恰好 +1，绝不倒退、不重复计数；
- 写入采用"校验-复制-提交"：先在副本上合并，全部成功后原子替换，
  任何失败路径不留下半更新；
- base_version 提供乐观并发控制：不匹配抛 VERSION_CONFLICT，调用方重试；
- 设备离线时状态保留但 fresh=False，业务侧不得将其当作最新值。
"""
from __future__ import annotations

import threading

from .errors import ErrorCode, HubError
from .models import ShadowState


class ShadowStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._states: dict[str, ShadowState] = {}

    def _get_or_create(self, device_id: str) -> ShadowState:
        state = self._states.get(device_id)
        if state is None:
            state = ShadowState(device_id=device_id)
            self._states[device_id] = state
        return state

    def _apply(self, device_id: str, patch: dict, base_version: int | None,
               field: str, fresh: bool, now: float) -> ShadowState:
        if not isinstance(patch, dict) or not patch:
            raise HubError(ErrorCode.MALFORMED_PAYLOAD, "影子更新必须为非空对象")
        with self._lock:
            current = self._get_or_create(device_id)
            if base_version is not None and base_version != current.version:
                raise HubError(
                    ErrorCode.VERSION_CONFLICT,
                    f"期望版本 {base_version} 与当前版本 {current.version} 不一致",
                )
            # 在副本上合并，避免半更新
            merged = dict(getattr(current, field))
            merged.update(patch)
            new_state = ShadowState(
                device_id=device_id,
                reported=merged if field == "reported" else dict(current.reported),
                desired=merged if field == "desired" else dict(current.desired),
                version=current.version + 1,
                updated_at=now,
                fresh=fresh,
            )
            self._states[device_id] = new_state
            return new_state

    def update_reported(self, device_id: str, patch: dict, base_version: int | None,
                        online: bool, now: float) -> ShadowState:
        return self._apply(device_id, patch, base_version, "reported",
                           fresh=online, now=now)

    def update_desired(self, device_id: str, patch: dict, base_version: int | None,
                       now: float) -> ShadowState:
        current = self._states.get(device_id)
        fresh = current.fresh if current else False
        return self._apply(device_id, patch, base_version, "desired",
                           fresh=fresh, now=now)

    def get(self, device_id: str) -> ShadowState:
        with self._lock:
            state = self._states.get(device_id)
            if state is None:
                raise HubError(ErrorCode.NOT_FOUND, "影子不存在")
            return state

    def mark_offline(self, device_id: str) -> None:
        """离线：状态保留但标记为陈旧，不删除、不改版本号。"""
        with self._lock:
            state = self._states.get(device_id)
            if state is not None:
                self._states[device_id] = ShadowState(
                    device_id=state.device_id,
                    reported=dict(state.reported),
                    desired=dict(state.desired),
                    version=state.version,
                    updated_at=state.updated_at,
                    fresh=False,
                )
