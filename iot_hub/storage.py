"""线程安全、容量有界的本地存储。

设计要点：
- 每个设备一把可重入锁（``RLock``），同一设备的"鉴权结论 / 序号 / 影子版本 /
  OTA 状态"在同一把锁下原子推进，杜绝半更新；跨设备锁互不阻塞。
- 序号窗口与幂等表都有硬性容量上限，超限按既定策略压缩/淘汰，任何情况下
  内存占用有界。
- 支持 JSON 快照导出/恢复，影子版本、已确认固件版本等可跨进程持久化。
"""

from __future__ import annotations

import json
import threading
from collections import OrderedDict
from dataclasses import asdict
from typing import Any, Callable

from .models import (
    Credential,
    Device,
    DeviceOtaState,
    Firmware,
    OtaStatus,
    Rollout,
    ShadowState,
)


class SeqWindow:
    """单设备单调序号窗口（调用方持设备锁，本类不加锁）。

    约定（适配断线重连/乱序）：
    - 首条到达的序号建立连续水位 ``base``（不假定序号从 0 起）；
    - ``base`` 之上先到的乱序序号进入 ``pending``（已接收，等待空洞），
      空洞按序补齐后抬升 ``base``；
    - 判定：已在 ``pending`` 中的序号是设备重发 -> DUPLICATE；
      空洞里的新序号 -> ACCEPTED；``<= base`` 且不在放弃区间 -> DUPLICATE；
      落在已放弃缺口 -> OUT_OF_ORDER。
    - 跳跃距离超过 ``window`` 时，中间缺口显式放弃（abandoned 区间），
      保证 pending 集合与上层载荷缓冲始终有界。
    """

    def __init__(self, window: int = 10_000) -> None:
        self.window = window
        self.base = -1
        self.high = -1
        self._pending: set[int] = set()
        self.started = False
        # 被放弃的缺口区间 (lo, hi)：lo < seq <= hi 的序号从未接收也不再接收
        self._abandoned: list[tuple[int, int]] = []

    def inspect(self, seq: int) -> str:
        """返回 ACCEPTED / DUPLICATE / OUT_OF_ORDER（不修改状态）。"""
        if seq in self._pending:
            return "DUPLICATE"
        for alo, ahi in self._abandoned:
            if alo < seq <= ahi:
                return "OUT_OF_ORDER"
        if self.started and seq <= self.base:
            return "DUPLICATE"
        return "ACCEPTED"

    def accept(self, seq: int) -> str:
        """登记一个已通过校验的序号，返回判定结论并推进连续水位。"""
        verdict = self.inspect(seq)
        if verdict != "ACCEPTED":
            return verdict
        if not self.started:
            self.started = True
            self.base = seq
            self.high = seq
            return "ACCEPTED"
        if seq - self.base > self.window:
            # 巨跳：放弃 (base, seq) 之间尚未到达的缺口，保证内存有界
            self._abandoned.append((self.base, seq))
            if len(self._abandoned) > 64:
                self._abandoned.pop(0)
            self._pending.clear()
            self.base = seq
            self.high = max(self.high, seq)
            return "ACCEPTED"
        self._pending.add(seq)
        self.high = max(self.high, seq)
        while self.base + 1 in self._pending:
            self._pending.discard(self.base + 1)
            self.base += 1
        return "ACCEPTED"

    def to_snapshot(self) -> dict[str, Any]:
        return {
            "window": self.window,
            "base": self.base,
            "high": self.high,
            "pending": sorted(self._pending),
            "started": self.started,
            "abandoned": self._abandoned,
        }

    @classmethod
    def from_snapshot(cls, data: dict[str, Any]) -> "SeqWindow":
        w = cls(window=data["window"])
        w.base = data["base"]
        w.high = data["high"]
        w._pending = set(data["pending"])
        w.started = data.get("started", False)
        w._abandoned = [tuple(x) for x in data["abandoned"]]
        return w


class IdempotencyStore:
    """单设备幂等表（LRU 有界）。key -> (请求指纹, 首次结论)。"""

    def __init__(self, capacity: int = 4096) -> None:
        self.capacity = capacity
        self._items: OrderedDict[str, tuple[str, dict[str, Any]]] = OrderedDict()

    def lookup(self, key: str) -> tuple[str, dict[str, Any]] | None:
        item = self._items.get(key)
        if item is not None:
            self._items.move_to_end(key)
        return item

    def remember(self, key: str, fingerprint: str, result: dict[str, Any]) -> None:
        self._items[key] = (fingerprint, result)
        self._items.move_to_end(key)
        while len(self._items) > self.capacity:
            self._items.popitem(last=False)

    def to_snapshot(self) -> dict[str, Any]:
        return {"capacity": self.capacity, "items": list(self._items.items())}

    @classmethod
    def from_snapshot(cls, data: dict[str, Any]) -> "IdempotencyStore":
        s = cls(capacity=data["capacity"])
        for k, v in data["items"]:
            s._items[k] = (v[0], v[1])
        return s


class Metrics:
    """全局统计计数。所有增减在锁内完成，保证并发下不重复累加、不丢失。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._counters: dict[str, int] = {}

    def incr(self, name: str, amount: int = 1) -> int:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + amount
            return self._counters[name]

    def get(self, name: str) -> int:
        with self._lock:
            return self._counters.get(name, 0)

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counters)

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()


class HubStore:
    """聚合所有仓储与设备锁。"""

    def __init__(
        self,
        *,
        seq_window: int = 10_000,
        idem_capacity: int = 4096,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._clock = clock or __import__("time").time
        # 注册表
        self.devices: dict[str, Device] = {}
        self.credentials: dict[str, Credential] = {}
        self._device_credential_index: dict[str, list[str]] = {}
        # 遥测
        self.seq_windows: dict[str, SeqWindow] = {}
        self.idem_stores: dict[str, IdempotencyStore] = {}
        # 乱序遥测缓冲：seq -> (ts, payload)，连续后按序应用到影子
        self.telemetry_buffers: dict[str, dict[int, tuple[float, dict[str, Any]]]] = {}
        # 每设备会话纪元：每次 connect 原子 +1，旧连接携带旧 epoch 的写入被拒绝
        self.epochs: dict[str, int] = {}
        self._seq_window_size = seq_window
        self._idem_capacity = idem_capacity
        # 影子
        self.shadows: dict[str, ShadowState] = {}
        # OTA
        self.firmware: dict[tuple[str, str], Firmware] = {}
        self.rollouts: dict[str, Rollout] = {}
        self.ota_states: dict[str, DeviceOtaState] = {}
        # 离线下发队列（有界，容量由 service 层策略控制）
        self.ota_queues: dict[str, list[str]] = {}
        # 统计
        self.metrics = Metrics()
        # 设备锁注册表
        self._device_locks: dict[str, threading.RLock] = {}
        self._locks_guard = threading.RLock()

    def clock(self) -> float:
        return self._clock()

    # ---------- 设备锁 ----------

    def lock_for(self, device_id: str) -> threading.RLock:
        with self._locks_guard:
            lk = self._device_locks.get(device_id)
            if lk is None:
                lk = threading.RLock()
                self._device_locks[device_id] = lk
            return lk

    # ---------- 注册/凭据索引 ----------

    def list_credential_ids(self, device_id: str) -> list[str]:
        return list(self._device_credential_index.get(device_id, ()))

    def index_credential(self, cred: Credential) -> None:
        self._device_credential_index.setdefault(cred.device_id, []).append(cred.credential_id)

    # ---------- 懒加载每设备结构 ----------

    def seq_window_for(self, device_id: str) -> SeqWindow:
        w = self.seq_windows.get(device_id)
        if w is None:
            w = SeqWindow(self._seq_window_size)
            self.seq_windows[device_id] = w
        return w

    def idem_for(self, device_id: str) -> IdempotencyStore:
        s = self.idem_stores.get(device_id)
        if s is None:
            s = IdempotencyStore(self._idem_capacity)
            self.idem_stores[device_id] = s
        return s

    def telemetry_buffer_for(self, device_id: str) -> dict[int, tuple[float, dict[str, Any]]]:
        buf = self.telemetry_buffers.get(device_id)
        if buf is None:
            buf = {}
            self.telemetry_buffers[device_id] = buf
        return buf

    def shadow_for(self, device_id: str) -> ShadowState:
        sh = self.shadows.get(device_id)
        if sh is None:
            sh = ShadowState(device_id=device_id)
            self.shadows[device_id] = sh
        return sh

    def ota_state_for(self, device_id: str) -> DeviceOtaState:
        st = self.ota_states.get(device_id)
        if st is None:
            st = DeviceOtaState(device_id=device_id)
            self.ota_states[device_id] = st
        return st

    # ---------- 快照持久化（本地 JSON，无外部依赖） ----------

    def export_snapshot(self) -> dict[str, Any]:
        with self._locks_guard:
            return {
                "devices": [asdict(d) for d in self.devices.values()],
                "credentials": [
                    {**asdict(c), "scopes": sorted(c.scopes)} for c in self.credentials.values()
                ],
                "shadows": [asdict(s) for s in self.shadows.values()],
                "seq_windows": {k: v.to_snapshot() for k, v in self.seq_windows.items()},
                "idem": {k: v.to_snapshot() for k, v in self.idem_stores.items()},
                "telemetry_buffers": {
                    k: {str(seq): [tp[0], tp[1]] for seq, tp in d.items()}
                    for k, d in self.telemetry_buffers.items()
                },
                "firmware": [asdict(f) for f in self.firmware.values()],
                "rollouts": [asdict(r) for r in self.rollouts.values()],
                "ota_states": [
                    {**asdict(s), "status": s.status.value} for s in self.ota_states.values()
                ],
                "ota_queues": dict(self.ota_queues),
                "epochs": dict(self.epochs),
                "metrics": self.metrics.snapshot(),
            }

    def restore_snapshot(self, data: dict[str, Any]) -> None:
        with self._locks_guard:
            self.devices = {
                d["device_id"]: Device(**d) for d in data.get("devices", [])
            }
            self.credentials = {}
            self._device_credential_index = {}
            for c in data.get("credentials", []):
                cred = Credential(**{**c, "scopes": frozenset(c["scopes"])})
                self.credentials[cred.credential_id] = cred
                self.index_credential(cred)
            self.shadows = {
                s["device_id"]: ShadowState(**s) for s in data.get("shadows", [])
            }
            self.seq_windows = {
                k: SeqWindow.from_snapshot(v) for k, v in data.get("seq_windows", {}).items()
            }
            self.idem_stores = {
                k: IdempotencyStore.from_snapshot(v) for k, v in data.get("idem", {}).items()
            }
            self.telemetry_buffers = {
                dev: {int(seq): (float(tp[0]), tp[1]) for seq, tp in d.items()}
                for dev, d in data.get("telemetry_buffers", {}).items()
            }
            self.firmware = {
                (f["model"], f["version"]): Firmware(**f) for f in data.get("firmware", [])
            }
            self.rollouts = {r["rollout_id"]: Rollout(**r) for r in data.get("rollouts", [])}
            self.ota_states = {}
            for s in data.get("ota_states", []):
                status = OtaStatus(s.pop("status"))
                st = DeviceOtaState(**s)
                st.status = status
                self.ota_states[st.device_id] = st
            self.ota_queues = {k: list(v) for k, v in data.get("ota_queues", {}).items()}
            self.epochs = {k: int(v) for k, v in data.get("epochs", {}).items()}
            self.metrics.reset()
            for k, v in data.get("metrics", {}).items():
                self.metrics.incr(k, v)

    def save_snapshot_file(self, path: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.export_snapshot(), f, ensure_ascii=False)

    def load_snapshot_file(self, path: str) -> None:
        with open(path, encoding="utf-8") as f:
            self.restore_snapshot(json.load(f))
