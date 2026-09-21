"""遥测接入：幂等、序号、过期判定与断线续传。

判定规则（按序检查，拒绝原因可区分）：
1. 协议版本不兼容 -> PROTOCOL_VERSION_UNSUPPORTED
2. 载荷畸形（缺字段/类型错误）-> MALFORMED_PAYLOAD
3. message_id 已处理 -> DUPLICATE_MESSAGE（不重复生效）
4. seq <= 已应用最大序号 -> STALE_SEQUENCE（乱序/过期，不覆盖新状态）
5. 采样时间超出 [now-max_age, now+max_clock_skew] -> EXPIRED_DATA
全部通过后生效：记录 message_id、推进最大序号、更新最新状态。
断线重连续传：设备重发相同 message_id 得到 DUPLICATE_MESSAGE，
未送达的后续消息按序重发即可被接受，不丢不重。
"""
from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Any

from .errors import ErrorCode, HubError
from .models import SUPPORTED_PROTOCOL_VERSION, TelemetryMessage


class TelemetryIngestor:
    def __init__(self, max_clock_skew: float = 300.0, max_age: float = 3600.0,
                 dedup_capacity: int = 10000) -> None:
        self._lock = threading.Lock()
        self._max_clock_skew = max_clock_skew
        self._max_age = max_age
        self._dedup_capacity = dedup_capacity
        # (device_id, message_id) -> None，按插入序淘汰，容量有界
        self._seen: OrderedDict[tuple[str, str], None] = OrderedDict()
        self._max_seq: dict[str, int] = {}
        self._latest: dict[str, dict[str, Any]] = {}

    def validate(self, msg: TelemetryMessage) -> None:
        if msg.protocol_version != SUPPORTED_PROTOCOL_VERSION:
            raise HubError(ErrorCode.PROTOCOL_VERSION_UNSUPPORTED,
                           f"协议版本 {msg.protocol_version} 不受支持")
        if not msg.device_id or not msg.message_id:
            raise HubError(ErrorCode.MALFORMED_PAYLOAD, "缺少 device_id/message_id")
        if not isinstance(msg.seq, int) or msg.seq < 0:
            raise HubError(ErrorCode.MALFORMED_PAYLOAD, "seq 必须为非负整数")
        if not isinstance(msg.payload, dict):
            raise HubError(ErrorCode.MALFORMED_PAYLOAD, "payload 必须为对象")

    def ingest(self, msg: TelemetryMessage, now: float) -> str:
        """返回 'applied'；重复/乱序/过期抛可区分 HubError。"""
        self.validate(msg)
        with self._lock:
            key = (msg.device_id, msg.message_id)
            if key in self._seen:
                raise HubError(ErrorCode.DUPLICATE_MESSAGE, "消息已处理")
            last = self._max_seq.get(msg.device_id, -1)
            if msg.seq <= last:
                raise HubError(ErrorCode.STALE_SEQUENCE,
                               f"序号 {msg.seq} 不晚于已应用序号 {last}")
            if msg.timestamp < now - self._max_age or msg.timestamp > now + self._max_clock_skew:
                raise HubError(ErrorCode.EXPIRED_DATA, "采样时间超出可接受窗口")
            # 生效：全部状态一次性提交，失败路径无任何残留
            self._seen[key] = None
            while len(self._seen) > self._dedup_capacity:
                self._seen.popitem(last=False)
            self._max_seq[msg.device_id] = msg.seq
            self._latest[msg.device_id] = {
                "seq": msg.seq,
                "timestamp": msg.timestamp,
                "payload": dict(msg.payload),
            }
            return "applied"

    def last_seq(self, device_id: str) -> int:
        with self._lock:
            return self._max_seq.get(device_id, -1)

    def latest(self, device_id: str) -> dict | None:
        with self._lock:
            entry = self._latest.get(device_id)
            return dict(entry) if entry is not None else None
