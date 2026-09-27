"""统一错误类型：所有内部失败归一化为可区分的 HubError，不向外泄漏底层异常。"""
from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    # 鉴权类
    UNKNOWN_DEVICE = "UNKNOWN_DEVICE"
    CREDENTIAL_INVALID = "CREDENTIAL_INVALID"
    CREDENTIAL_REVOKED = "CREDENTIAL_REVOKED"
    CREDENTIAL_EXPIRED = "CREDENTIAL_EXPIRED"
    SCOPE_DENIED = "SCOPE_DENIED"
    # 协议类
    MALFORMED_PAYLOAD = "MALFORMED_PAYLOAD"
    PROTOCOL_VERSION_UNSUPPORTED = "PROTOCOL_VERSION_UNSUPPORTED"
    # 遥测类
    DUPLICATE_MESSAGE = "DUPLICATE_MESSAGE"
    STALE_SEQUENCE = "STALE_SEQUENCE"
    EXPIRED_DATA = "EXPIRED_DATA"
    # 影子类
    VERSION_CONFLICT = "VERSION_CONFLICT"
    # OTA 类
    INCOMPATIBLE_FIRMWARE = "INCOMPATIBLE_FIRMWARE"
    OTA_NOT_PENDING = "OTA_NOT_PENDING"
    CAMPAIGN_NOT_FOUND = "CAMPAIGN_NOT_FOUND"
    # 离线指令类
    COMMAND_QUEUE_FULL = "COMMAND_QUEUE_FULL"
    COMMAND_EXPIRED = "COMMAND_EXPIRED"
    COMMAND_ALREADY_ACKED = "COMMAND_ALREADY_ACKED"
    COMMAND_NOT_PENDING = "COMMAND_NOT_PENDING"
    # 流控类
    RATE_LIMITED = "RATE_LIMITED"
    BACKLOG_FULL = "BACKLOG_FULL"
    # 通用
    NOT_FOUND = "NOT_FOUND"
    CONFLICT = "CONFLICT"
    INTERNAL = "INTERNAL"


class HubError(Exception):
    """对外暴露的唯一异常类型。code 可区分，message 不含内部实现细节。"""

    def __init__(self, code: ErrorCode, message: str = "") -> None:
        self.code = code
        self.message = message or code.value
        super().__init__(f"{code.value}: {self.message}")
