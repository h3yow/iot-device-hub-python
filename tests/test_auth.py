"""鉴权链路：注册、签发、吊销、轮换宽限、越权。"""
import logging

import pytest

from iot_hub.errors import ErrorCode, HubError
from iot_hub.models import Scope
from tests.conftest import ALL_SCOPES

log = logging.getLogger("test")


def expect_error(code: ErrorCode, fn, rationale: str):
    with pytest.raises(HubError) as exc_info:
        fn()
    log.info("判定依据: %s -> 实际错误码 %s", rationale, exc_info.value.code.value)
    assert exc_info.value.code == code, (
        f"期望 {code.value}，实际 {exc_info.value.code.value}")


def test_unknown_device_rejected(hub, clock):
    log.info("输入: 未注册设备的 token 鉴权")
    expect_error(ErrorCode.CREDENTIAL_INVALID,
                 lambda: hub.registry.authenticate("no-such-token", Scope.SHADOW_READ, clock()),
                 "token 不存在，应为 CREDENTIAL_INVALID")


def test_register_duplicate_rejected(hub, registered, clock):
    registered("dev-1")
    log.info("输入: 重复注册 device_id=dev-1")
    expect_error(ErrorCode.CONFLICT,
                 lambda: hub.registry.register("dev-1", "sensor-A", "1.0.0", clock()),
                 "device_id 已存在，应为 CONFLICT")


def test_revoked_credential_rejected(hub, registered, clock):
    device_id, token = registered()
    hub.registry.revoke_credential(device_id, token)
    log.info("输入: 已吊销 token 鉴权")
    expect_error(ErrorCode.CREDENTIAL_REVOKED,
                 lambda: hub.registry.authenticate(token, Scope.SHADOW_READ, clock()),
                 "凭据已吊销，应为 CREDENTIAL_REVOKED 且区别于其他失败")


def test_expired_credential_rejected(hub, registered, clock):
    device_id, token = registered()
    clock.advance(3601)
    log.info("输入: 过期 token 鉴权 (now 超过 expires_at)")
    expect_error(ErrorCode.CREDENTIAL_EXPIRED,
                 lambda: hub.registry.authenticate(token, Scope.SHADOW_READ, clock()),
                 "凭据过期，应为 CREDENTIAL_EXPIRED")


def test_scope_denied(hub, clock):
    hub.registry.register("dev-1", "sensor-A", "1.0.0", clock())
    cred = hub.registry.issue_credential("dev-1", {Scope.SHADOW_READ}, clock())
    log.info("输入: 仅 shadow:read 凭据执行 telemetry:write")
    expect_error(ErrorCode.SCOPE_DENIED,
                 lambda: hub.registry.authenticate(cred.token, Scope.TELEMETRY_WRITE, clock()),
                 "缺少 telemetry:write 授权范围，应为 SCOPE_DENIED")


def test_rotation_grace_semantics(hub, registered, clock):
    device_id, old_token = registered()
    hub.registry.rotate_credential(device_id, ALL_SCOPES, clock())
    log.info("输入: 轮换后宽限期内用旧 token 鉴权")
    device = hub.registry.authenticate(old_token, Scope.SHADOW_READ, clock())
    assert device.device_id == device_id
    log.info("判定依据: 宽限期内旧凭据仍可用 -> 通过")

    clock.advance(301)  # 宽限期 300s 结束
    log.info("输入: 宽限期结束后用旧 token 鉴权")
    expect_error(ErrorCode.CREDENTIAL_EXPIRED,
                 lambda: hub.registry.authenticate(old_token, Scope.SHADOW_READ, clock()),
                 "宽限结束，旧凭据按 CREDENTIAL_EXPIRED 拒绝（语义稳定）")


def test_revoke_during_grace_takes_precedence(hub, registered, clock):
    device_id, old_token = registered()
    hub.registry.rotate_credential(device_id, ALL_SCOPES, clock())
    hub.registry.revoke_credential(device_id, old_token)
    log.info("输入: 宽限期内显式吊销旧 token 后鉴权")
    expect_error(ErrorCode.CREDENTIAL_REVOKED,
                 lambda: hub.registry.authenticate(old_token, Scope.SHADOW_READ, clock()),
                 "吊销优先于宽限，应为 CREDENTIAL_REVOKED")


def test_cross_device_scope_denied(hub, registered, clock):
    _, token_a = registered("dev-A")
    registered("dev-B")
    log.info("输入: 设备 A 的凭据修改设备 B 的影子")
    expect_error(ErrorCode.SCOPE_DENIED,
                 lambda: hub.update_reported(token_a, "dev-B", {"x": 1}, None),
                 "跨设备写入，应为 SCOPE_DENIED，不得串用状态")
