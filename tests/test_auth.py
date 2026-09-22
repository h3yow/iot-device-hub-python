"""鉴权链路：未注册、凭据无效、吊销、轮换宽限、越权 scope 的结论必须可区分。"""

from __future__ import annotations

import logging

import pytest

from iot_hub.errors import (
    CredentialGraceExpiredError,
    CredentialInvalidError,
    CredentialRevokedError,
    DeviceNotRegisteredError,
    ScopeDeniedError,
)

log = logging.getLogger("tests.auth")


def test_unregistered_device_rejected(service, registered):
    cred = registered["credential_id"]
    inp = ("ghost", cred, registered["secret"])
    log.info("INPUT connect unregistered device_id=%s", inp[0])
    with pytest.raises(DeviceNotRegisteredError) as ei:
        service.connect("ghost", cred, registered["secret"])
    log.info("DECISION code=%s reason=设备未注册", ei.value.code)
    assert ei.value.code == "DEVICE_NOT_REGISTERED"


def test_wrong_secret_vs_revoked_are_distinct(service, registered):
    cid = registered["credential_id"]
    # 密钥错误
    with pytest.raises(CredentialInvalidError) as ei:
        service.connect("dev-1", cid, "wrong-secret")
    log.info("INPUT wrong secret => DECISION %s", ei.value.code)
    assert ei.value.code == "CREDENTIAL_INVALID"

    # 不存在的 credential_id
    with pytest.raises(CredentialInvalidError) as ei:
        service.connect("dev-1", "nope", registered["secret"])
    assert ei.value.code == "CREDENTIAL_INVALID"

    # 吊销后同一密钥 -> REVOKED（与 INVALID 区分）
    service.revoke_credential(cid)
    log.info("INPUT connect after revoke credential=%s", cid)
    with pytest.raises(CredentialRevokedError) as ei:
        service.connect("dev-1", cid, registered["secret"])
    log.info("DECISION %s details=%s", ei.value.code, ei.value.details)
    assert ei.value.code == "CREDENTIAL_REVOKED"
    assert ei.value.details["revoked_at"] is not None


def test_rotation_grace_window_semantics(service, registered, clock):
    old_cid, old_secret = registered["credential_id"], registered["secret"]
    rotated = service.rotate_credential("dev-1", grace_period_s=100.0)
    log.info(
        "INPUT rotate old_cid=%s new_generation=%s grace_until=%s",
        old_cid, rotated["generation"], rotated["grace_until"],
    )

    # 新凭据立即可用
    r = service.connect("dev-1", rotated["credential_id"], rotated["secret"])
    assert r["authenticated"] and r["grace"] is False

    # 宽限期内旧凭据仍可接入，但 grace=True
    clock.advance(50)
    r = service.connect("dev-1", old_cid, old_secret)
    log.info("DECISION old cred inside grace => grace=%s gen=%s", r["grace"], r["generation"])
    assert r["grace"] is True and r["generation"] == 1

    # 宽限期后 -> GRACE_EXPIRED（区别于 REVOKED / INVALID）
    clock.advance(51)
    with pytest.raises(CredentialGraceExpiredError) as ei:
        service.connect("dev-1", old_cid, old_secret)
    log.info("DECISION old cred after grace => %s", ei.value.code)
    assert ei.value.code == "CREDENTIAL_GRACE_EXPIRED"
    # 新凭据不受影响
    r = service.connect("dev-1", rotated["credential_id"], rotated["secret"])
    assert r["grace"] is False


def test_rotate_with_zero_grace_revokes_immediately(service, registered):
    rotated = service.rotate_credential("dev-1", grace_period_s=0)
    with pytest.raises(CredentialRevokedError):
        service.connect("dev-1", registered["credential_id"], registered["secret"])
    r = service.connect("dev-1", rotated["credential_id"], rotated["secret"])
    assert r["authenticated"]


def test_scope_denied_is_distinct(service, clock):
    # 仅签发 ota:read 的窄凭据
    from iot_hub.models import Scope
    from iot_hub.auth import AuthManager

    service.register_device("dev-2", "sensor-v1")
    secret, cred = AuthManager(service.store).rotate_credential(
        "dev-2", grace_period_s=-1,
        scopes=frozenset({Scope.OTA_READ.value}),
    )
    log.info("INPUT telemetry with narrow scopes=%s", sorted(cred.scopes))
    with pytest.raises(ScopeDeniedError) as ei:
        service.publish_telemetry("dev-2", cred.credential_id, secret, 1, clock(), {"t": 1})
    log.info("DECISION %s required=%s", ei.value.code, ei.value.details["required_scope"])
    assert ei.value.code == "SCOPE_DENIED"
    assert ei.value.details["required_scope"] == "telemetry:write"


def test_cross_device_credential_cannot_be_used(service, registered):
    service.register_device("dev-3", "sensor-v1")
    # dev-1 的凭据拿到 dev-3 上用
    with pytest.raises(CredentialInvalidError):
        service.connect("dev-3", registered["credential_id"], registered["secret"])
