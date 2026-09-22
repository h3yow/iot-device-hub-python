"""领域数据模型：纯数据类型（枚举 + dataclass），业务逻辑放在各功能模块。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

# 支持的接入协议版本；其余版本在边界被归一化为 PROTOCOL_INCOMPATIBLE
SUPPORTED_PROTOCOL_VERSIONS: tuple[str, ...] = ("1.0",)
PROTOCOL_HEADER = "x-iot-protocol"


class Scope(str, Enum):
    """凭据授权范围。"""

    TELEMETRY_PUBLISH = "telemetry:write"
    SHADOW_READ = "shadow:read"
    SHADOW_WRITE = "shadow:write"
    OTA_READ = "ota:read"
    OTA_ADMIN = "ota:admin"


class Freshness(str, Enum):
    """影子上报数据新鲜度。离线期间的旧值显式标记为 STALE，不参与"最新值"判定。"""

    FRESH = "FRESH"
    STALE = "STALE"
    UNKNOWN = "UNKNOWN"  # 该设备从未上报过


class OtaStatus(str, Enum):
    """单设备 OTA 推进状态。未确认前绝不是 CONFIRMED。"""

    NONE = "NONE"
    QUEUED = "QUEUED"          # 已分配但设备离线，排队等待
    DELIVERED = "DELIVERED"    # 已下发，等待设备确认（有截止时间）
    CONFIRMED = "CONFIRMED"    # 设备确认成功，成为新的可用版本
    FAILED = "FAILED"          # 设备上报失败或确认超时，触发回退
    ROLLED_BACK = "ROLLED_BACK"  # 已回退到确定的可用版本


# ---------------- 注册与凭据 ----------------


@dataclass
class Credential:
    credential_id: str
    device_id: str
    secret_hash: str
    scopes: frozenset[str]
    generation: int
    issued_at: float
    grace_until: float | None = None  # 轮换后旧凭据的宽限截止；None 表示当前凭据
    revoked: bool = False
    revoked_at: float | None = None

    def is_revoked(self) -> bool:
        return self.revoked

    def is_within_grace(self, now: float) -> bool:
        return self.grace_until is not None and now <= self.grace_until


@dataclass
class Device:
    device_id: str
    model: str
    created_at: float
    current_fw_version: str | None = None
    # 最近一个 CONFIRMED 版本，作为任何失败后的确定回退目标
    last_known_good_version: str | None = None
    last_seen_at: float | None = None
    online: bool = False


@dataclass
class AuthContext:
    """鉴权结论。失败路径由 errors 中的异常表达，成功才产生本对象。"""

    device_id: str
    credential_id: str
    generation: int
    scopes: frozenset[str]
    grace: bool  # 是否凭"轮换宽限期内的旧凭据"接入
    epoch: int = 0  # 本次连接的会话纪元；与 store 中最新纪元不一致即为僵尸连接


# ---------------- 遥测 ----------------


@dataclass
class TelemetryPoint:
    device_id: str
    seq: int
    ts: float
    payload: dict[str, Any]
    idempotency_key: str | None
    received_at: float


@dataclass
class TelemetryResult:
    """遥测判定结论。code 取值：ACCEPTED / TELEMETRY_DUPLICATE / 各种拒绝码。"""

    accepted: bool
    code: str
    applied: bool            # 本次调用是否真正生效（重复回放为 False）
    seq: int
    shadow_version: int
    detail: str
    freshness: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "accepted": self.accepted,
            "code": self.code,
            "applied": self.applied,
            "seq": self.seq,
            "shadow_version": self.shadow_version,
            "detail": self.detail,
            "freshness": self.freshness,
        }


# ---------------- 设备影子 ----------------


@dataclass
class ShadowState:
    device_id: str
    version: int = 0
    reported: dict[str, Any] = field(default_factory=dict)
    desired: dict[str, Any] = field(default_factory=dict)
    # 最近一次 reported 更新来自的遥测序号，防止旧序号回退写覆盖
    reported_seq: int = -1
    reported_at: float | None = None
    updated_at: float | None = None

    def delta(self) -> dict[str, Any]:
        """desired 与 reported 不一致的键。"""
        out: dict[str, Any] = {}
        for k, v in self.desired.items():
            if self.reported.get(k) != v:
                out[k] = v
        return out


# ---------------- OTA ----------------


@dataclass
class Firmware:
    version: str
    model: str
    min_from_version: str | None  # 允许升级的最低当前版本；None 表示不限制
    created_at: float


@dataclass
class Rollout:
    """分批下发策略：批次按 device_id 稳定哈希取百分位，percent=100 表示全量。"""

    rollout_id: str
    firmware_version: str
    model: str
    min_from_version: str | None
    confirm_timeout_s: float
    percent: int
    created_at: float

    def in_batch(self, device_id: str, percent_override: int | None = None) -> bool:
        pct = self.percent if percent_override is None else percent_override
        return _stable_bucket(device_id) < pct


@dataclass
class DeviceOtaState:
    device_id: str
    rollout_id: str | None = None
    from_version: str | None = None
    target_version: str | None = None
    status: OtaStatus = OtaStatus.NONE
    delivered_at: float | None = None
    deadline: float | None = None
    confirmed_at: float | None = None
    failure_reason: str | None = None
    attempts: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "device_id": self.device_id,
            "rollout_id": self.rollout_id,
            "from_version": self.from_version,
            "target_version": self.target_version,
            "status": self.status.value,
            "delivered_at": self.delivered_at,
            "deadline": self.deadline,
            "confirmed_at": self.confirmed_at,
            "failure_reason": self.failure_reason,
            "attempts": self.attempts,
        }


@dataclass
class OtaDelivery:
    """下发给设备的更新通知。"""

    device_id: str
    rollout_id: str
    version: str
    from_version: str | None
    deadline: float
    status: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "device_id": self.device_id,
            "rollout_id": self.rollout_id,
            "version": self.version,
            "from_version": self.from_version,
            "deadline": self.deadline,
            "status": self.status,
        }


def _stable_bucket(device_id: str) -> int:
    """把 device_id 稳定映射到 [0,100) 桶位，分批结论不随进程重启变化。"""
    import hashlib

    h = hashlib.sha256(device_id.encode("utf-8")).digest()
    return int.from_bytes(h[:4], "big") % 100
