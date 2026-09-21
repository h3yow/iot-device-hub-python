"""领域模型：纯数据定义，不含业务逻辑。"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

SUPPORTED_PROTOCOL_VERSION = 1


class Scope(str, Enum):
    TELEMETRY_WRITE = "telemetry:write"
    SHADOW_READ = "shadow:read"
    SHADOW_WRITE = "shadow:write"
    OTA_ACK = "ota:ack"


@dataclass
class Credential:
    device_id: str
    token: str
    scopes: frozenset[Scope]
    issued_at: float
    expires_at: float
    revoked: bool = False
    # 轮换宽限：旧凭据在 rotated_away_at 之前仍可用
    grace_until: float = 0.0

    def is_active(self, now: float) -> bool:
        return not self.revoked and now < self.expires_at


@dataclass
class Device:
    device_id: str
    model: str
    firmware_version: str
    registered_at: float
    credentials: list[Credential] = field(default_factory=list)
    online: bool = False
    last_seen_at: float = 0.0


@dataclass
class TelemetryMessage:
    device_id: str
    message_id: str          # 幂等标识
    seq: int                 # 设备侧单调序号
    timestamp: float         # 设备侧采样时间
    payload: dict[str, Any]
    protocol_version: int = SUPPORTED_PROTOCOL_VERSION


@dataclass
class ShadowState:
    device_id: str
    reported: dict[str, Any] = field(default_factory=dict)
    desired: dict[str, Any] = field(default_factory=dict)
    version: int = 0                 # 单调递增，每次生效写入 +1
    updated_at: float = 0.0
    fresh: bool = False              # 设备离线期间为 False（陈旧标记）


class OtaJobStatus(str, Enum):
    QUEUED = "QUEUED"          # 设备离线，排队等待
    PENDING = "PENDING"        # 已下发，等待确认
    SUCCEEDED = "SUCCEEDED"    # 设备确认成功
    FAILED = "FAILED"          # 设备上报失败，已回退
    ROLLED_BACK = "ROLLED_BACK"  # 确认超时，强制回退


@dataclass
class OtaJob:
    job_id: str
    campaign_id: str
    device_id: str
    target_version: str
    status: OtaJobStatus
    dispatched_at: float = 0.0
    ack_deadline: float = 0.0


@dataclass
class OtaCampaign:
    campaign_id: str
    model: str
    target_version: str
    batch_size: int
    ack_timeout: float
    jobs: dict[str, OtaJob] = field(default_factory=dict)  # device_id -> job
    created_at: float = field(default_factory=time.time)
