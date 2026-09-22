"""归一化错误类型。

所有对外可感知的失败都以 :class:`HubError` 表达，携带稳定的字符串错误码
``code`` 与 HTTP 状态码。底层/未知异常在 API 边界统一转成 ``INTERNAL_ERROR``，
不向调用方透出堆栈、内部路径等实现细节。
"""

from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    """稳定错误码枚举。字符串值即对外暴露的 code，禁止随版本随意改名。"""

    # ---- 通用 / 载荷 ----
    MALFORMED_PAYLOAD = "MALFORMED_PAYLOAD"
    PROTOCOL_INCOMPATIBLE = "PROTOCOL_INCOMPATIBLE"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    NOT_FOUND = "NOT_FOUND"
    CONFLICT = "CONFLICT"

    # ---- 注册与鉴权 ----
    DEVICE_NOT_REGISTERED = "DEVICE_NOT_REGISTERED"
    CREDENTIAL_INVALID = "CREDENTIAL_INVALID"
    CREDENTIAL_REVOKED = "CREDENTIAL_REVOKED"
    SCOPE_DENIED = "SCOPE_DENIED"
    CREDENTIAL_GRACE_EXPIRED = "CREDENTIAL_GRACE_EXPIRED"
    SESSION_STALE = "SESSION_STALE"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"

    # ---- 遥测 ----
    TELEMETRY_DUPLICATE = "TELEMETRY_DUPLICATE"
    TELEMETRY_OUT_OF_ORDER = "TELEMETRY_OUT_OF_ORDER"
    TELEMETRY_EXPIRED = "TELEMETRY_EXPIRED"

    # ---- 影子 ----
    SHADOW_VERSION_CONFLICT = "SHADOW_VERSION_CONFLICT"

    # ---- OTA ----
    OTA_INCOMPATIBLE = "OTA_INCOMPATIBLE"
    OTA_NO_AVAILABLE_VERSION = "OTA_NO_AVAILABLE_VERSION"
    OTA_STATE_INVALID = "OTA_STATE_INVALID"

    # ---- 限流 / 积压 ----
    RATE_LIMITED = "RATE_LIMITED"
    BACKLOG_FULL = "BACKLOG_FULL"


class HubError(Exception):
    """所有平台错误的基类。

    :param code: 稳定错误码
    :param message: 面向调用方的简短说明（不得包含内部实现细节）
    :param status_code: 建议的 HTTP 状态码
    :param details: 可选的结构化补充信息（必须是可安全外露的内容）
    """

    status_code: int = 400

    def __init__(
        self,
        code: ErrorCode | str,
        message: str,
        *,
        status_code: int | None = None,
        details: dict | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code.value if isinstance(code, ErrorCode) else code
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.details = dict(details or {})

    def to_dict(self) -> dict:
        """转为对外错误响应体。"""
        body = {"error": self.code, "message": self.message}
        if self.details:
            body["details"] = self.details
        return body


# ---------------- 通用 / 载荷 ----------------


class MalformedPayloadError(HubError):
    status_code = 422

    def __init__(self, message: str = "malformed payload", *, details: dict | None = None) -> None:
        super().__init__(ErrorCode.MALFORMED_PAYLOAD, message, details=details)


class ProtocolIncompatibleError(HubError):
    status_code = 400

    def __init__(self, message: str = "unsupported protocol version", *, details: dict | None = None) -> None:
        super().__init__(ErrorCode.PROTOCOL_INCOMPATIBLE, message, details=details)


class NotFoundError(HubError):
    status_code = 404

    def __init__(self, message: str, *, details: dict | None = None) -> None:
        super().__init__(ErrorCode.NOT_FOUND, message, details=details)


class ConflictError(HubError):
    status_code = 409

    def __init__(self, message: str, *, details: dict | None = None) -> None:
        super().__init__(ErrorCode.CONFLICT, message, details=details)


# ---------------- 注册与鉴权 ----------------


class DeviceNotRegisteredError(HubError):
    status_code = 404

    def __init__(self, *, details: dict | None = None) -> None:
        super().__init__(
            ErrorCode.DEVICE_NOT_REGISTERED,
            "device is not registered",
            details=details,
        )


class CredentialInvalidError(HubError):
    status_code = 401

    def __init__(self, *, details: dict | None = None) -> None:
        super().__init__(
            ErrorCode.CREDENTIAL_INVALID,
            "credential is missing or invalid",
            details=details,
        )


class CredentialRevokedError(HubError):
    status_code = 401

    def __init__(self, *, details: dict | None = None) -> None:
        super().__init__(
            ErrorCode.CREDENTIAL_REVOKED,
            "credential has been revoked",
            details=details,
        )


class ScopeDeniedError(HubError):
    status_code = 403

    def __init__(self, scope: str, *, details: dict | None = None) -> None:
        d = {"required_scope": scope}
        if details:
            d.update(details)
        super().__init__(
            ErrorCode.SCOPE_DENIED,
            f"credential is not authorized for scope '{scope}'",
            details=d,
        )


class CredentialGraceExpiredError(HubError):
    """旧凭据轮换宽限期已过（区别于吊销与无效凭据）。"""

    status_code = 401

    def __init__(self, *, details: dict | None = None) -> None:
        super().__init__(
            ErrorCode.CREDENTIAL_GRACE_EXPIRED,
            "previous credential is past its rotation grace window",
            details=details,
        )


class SessionStaleError(HubError):
    """同一设备已建立更新的连接会话，旧连接（僵尸连接）的写入必须被拒绝。"""

    status_code = 409

    def __init__(self, *, details: dict | None = None) -> None:
        super().__init__(
            ErrorCode.SESSION_STALE,
            "connection session is stale: a newer session for this device exists",
            details=details,
        )


class IdempotencyConflictError(HubError):
    status_code = 409

    def __init__(self, *, details: dict | None = None) -> None:
        super().__init__(
            ErrorCode.IDEMPOTENCY_CONFLICT,
            "idempotency key was already used with a different request",
            details=details,
        )


# ---------------- 遥测 ----------------


class TelemetryDuplicateError(HubError):
    """幂等键或 (设备, 序号) 完全重复（重试场景的稳定结论）。"""

    status_code = 200

    def __init__(self, *, details: dict | None = None) -> None:
        super().__init__(
            ErrorCode.TELEMETRY_DUPLICATE,
            "telemetry was already accepted (duplicate replay)",
            details=details,
        )


class TelemetryOutOfOrderError(HubError):
    """序号小于已生效序号：乱序/回退数据，不允许覆盖较新状态。"""

    status_code = 409

    def __init__(self, *, details: dict | None = None) -> None:
        super().__init__(
            ErrorCode.TELEMETRY_OUT_OF_ORDER,
            "telemetry sequence is older than the current applied sequence",
            details=details,
        )


class TelemetryExpiredError(HubError):
    """载荷时间戳超过可接受窗口：过期数据（区别于乱序）。"""

    status_code = 422

    def __init__(
        self,
        message: str = "telemetry timestamp is outside the accepted freshness window",
        *,
        details: dict | None = None,
    ) -> None:
        super().__init__(ErrorCode.TELEMETRY_EXPIRED, message, details=details)


# ---------------- 影子 ----------------


class ShadowVersionConflictError(HubError):
    status_code = 409

    def __init__(self, *, details: dict | None = None) -> None:
        super().__init__(
            ErrorCode.SHADOW_VERSION_CONFLICT,
            "shadow version conflict: expected version does not match",
            details=details,
        )


# ---------------- OTA ----------------


class OtaIncompatibleError(HubError):
    status_code = 422

    def __init__(self, message: str, *, details: dict | None = None) -> None:
        super().__init__(ErrorCode.OTA_INCOMPATIBLE, message, details=details)


class OtaNoAvailableVersionError(HubError):
    status_code = 409

    def __init__(self, *, details: dict | None = None) -> None:
        super().__init__(
            ErrorCode.OTA_NO_AVAILABLE_VERSION,
            "no confirmed/available firmware version to fall back to",
            details=details,
        )


class OtaStateInvalidError(HubError):
    status_code = 409

    def __init__(self, message: str, *, details: dict | None = None) -> None:
        super().__init__(ErrorCode.OTA_STATE_INVALID, message, details=details)


# ---------------- 限流 / 积压 ----------------


class RateLimitedError(HubError):
    status_code = 429

    def __init__(self, message: str = "rate limit exceeded", *, details: dict | None = None) -> None:
        super().__init__(ErrorCode.RATE_LIMITED, message, details=details)


class BacklogFullError(HubError):
    status_code = 503

    def __init__(self, message: str = "backlog is full", *, details: dict | None = None) -> None:
        super().__init__(ErrorCode.BACKLOG_FULL, message, details=details)
