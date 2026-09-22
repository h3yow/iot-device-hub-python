"""遥测异常链路：重复上报、乱序/过期数据、幂等冲突、断线重连续传。"""

from __future__ import annotations

import logging

import pytest

from iot_hub.errors import (
    IdempotencyConflictError,
    TelemetryExpiredError,
    TelemetryOutOfOrderError,
)

log = logging.getLogger("tests.telemetry")


def _pub(service, reg, seq, payload, *, key=None, ts=None):
    t = service.store.clock() if ts is None else ts
    log.info("INPUT telemetry device=dev-1 seq=%s ts=%s payload=%r key=%s", seq, t, payload, key)
    r = service.publish_telemetry(
        "dev-1", reg["credential_id"], reg["secret"], seq, t, payload, idempotency_key=key
    )
    log.info("DECISION code=%s applied=%s shadow_version=%s", r["code"], r["applied"], r["shadow_version"])
    return r


def test_in_order_advances_shadow_version(service, registered, clock):
    r1 = _pub(service, registered, 1, {"temp": 1})
    r2 = _pub(service, registered, 2, {"temp": 2})
    assert r1["applied"] and r1["code"] == "ACCEPTED"
    assert r2["shadow_version"] == 2
    view = service.get_shadow("dev-1")
    assert view["reported"] == {"temp": 2} and view["freshness"] == "FRESH"


def test_exact_duplicate_does_not_reapply(service, registered, clock):
    r1 = _pub(service, registered, 1, {"temp": 1})
    r2 = _pub(service, registered, 1, {"temp": 1})
    assert r1["applied"] is True
    assert r2["code"] == "TELEMETRY_DUPLICATE" and r2["applied"] is False
    assert r2["shadow_version"] == r1["shadow_version"]  # 版本不重复计数
    assert service.metrics()["telemetry.duplicate"] == 1
    assert service.get_shadow("dev-1")["version"] == 1


def test_idempotency_key_replay_returns_first_result(service, registered, clock):
    r1 = _pub(service, registered, 10, {"a": 1}, key="k-1")
    # 断线重连后用同一幂等键重放（即使再次发送）
    r2 = _pub(service, registered, 10, {"a": 1}, key="k-1")
    assert r1["applied"] and r2["code"] == "ACCEPTED" and r2["applied"] is False


def test_idempotency_key_same_key_different_body_conflict(service, registered, clock):
    _pub(service, registered, 10, {"a": 1}, key="k-9")
    log.info("INPUT same idempotency key k-9 with different body")
    with pytest.raises(IdempotencyConflictError) as ei:
        _pub(service, registered, 11, {"a": 2}, key="k-9")
    log.info("DECISION %s", ei.value.code)
    assert ei.value.code == "IDEMPOTENCY_CONFLICT"
    # 冲突写入未生效：影子版本停留在 1
    assert service.get_shadow("dev-1")["version"] == 1


def test_out_of_order_holds_gap_then_fills_without_loss(service, registered, clock):
    """断线重连续传：1 已生效，3 先到(空洞缓冲)，2 补齐时按序应用，旧值不覆盖新值。"""
    r1 = _pub(service, registered, 1, {"v": 1})
    assert r1["applied"] and service.get_shadow("dev-1")["reported"] == {"v": 1}

    # 3 先到：已接收但因缺口 (2) 未应用到影子，影子保持较旧但确定的状态
    r3 = _pub(service, registered, 3, {"v": 3})
    assert r3["code"] == "ACCEPTED" and r3["applied"] is False
    assert service.get_shadow("dev-1")["reported"] == {"v": 1}

    # 3 在 2 到达前被设备重发 -> DUPLICATE（已接收），不重复占位
    r3_re = _pub(service, registered, 3, {"v": 3})
    assert r3_re["code"] == "TELEMETRY_DUPLICATE" and r3_re["applied"] is False

    # 2 补齐：2 与 3 按 seq 顺序依次应用，最终值为较新的 3，版本 +2
    r2 = _pub(service, registered, 2, {"v": 2})
    assert r2["applied"]
    view = service.get_shadow("dev-1")
    log.info("after fill reported=%r reported_seq=%s version=%s",
             view["reported"], view["reported_seq"], view["version"])
    assert view["reported"] == {"v": 3}
    assert view["reported_seq"] == 3 and view["version"] == 3

    # 全部序号再重放一遍：均判定重复，影子不再变化，无丢失无重复生效
    for s in (1, 2, 3):
        r = _pub(service, registered, s, {"v": s})
        assert r["code"] == "TELEMETRY_DUPLICATE" and r["applied"] is False
    assert service.get_shadow("dev-1")["version"] == 3


def test_sequence_below_first_watermark_is_duplicate(service, registered, clock):
    """首条即 seq=3：之后的 1/2 属于上一会话旧序号，结论为重复/旧序，不覆盖状态。"""
    r3 = _pub(service, registered, 3, {"v": 3})
    assert r3["applied"]
    r2 = _pub(service, registered, 2, {"v": 2})
    log.info("DECISION seq=2 after first-seen=3 => %s", r2["code"])
    assert r2["code"] == "TELEMETRY_DUPLICATE" and r2["applied"] is False
    assert service.get_shadow("dev-1")["reported"] == {"v": 3}


def test_stale_late_sequence_after_gap_is_out_of_order(service, registered, clock):
    _pub(service, registered, 1, {"v": 1})
    _pub(service, registered, 50_000, {"v": 2})  # 超过序号窗口，中间缺口放弃
    log.info("INPUT late seq=1 beyond compacted window")
    with pytest.raises(TelemetryOutOfOrderError) as ei:
        _pub(service, registered, 50, {"v": 3})
    log.info("DECISION %s base=%s", ei.value.code, ei.value.details["applied_base"])
    assert ei.value.code == "TELEMETRY_OUT_OF_ORDER"


def test_expired_payload_rejected_distinctly(service, registered, clock):
    old_ts = clock() - 10_000
    log.info("INPUT expired telemetry age_s=10000")
    with pytest.raises(TelemetryExpiredError) as ei:
        service.publish_telemetry(
            "dev-1", registered["credential_id"], registered["secret"],
            1, old_ts, {"temp": 9},
        )
    log.info("DECISION %s", ei.value.code)
    assert ei.value.code == "TELEMETRY_EXPIRED"
    assert service.get_shadow("dev-1")["version"] == 0  # 过期数据不落状态


def test_future_timestamp_rejected_as_expired(service, registered, clock):
    future_ts = clock() + 600
    with pytest.raises(TelemetryExpiredError) as ei:
        service.publish_telemetry(
            "dev-1", registered["credential_id"], registered["secret"],
            1, future_ts, {"temp": 9},
        )
    assert ei.value.code == "TELEMETRY_EXPIRED"


def test_rejected_then_retried_sequence_can_still_apply(service, registered, clock):
    """seq=2 先因过期被拒，数据修正后重试仍应生效（拒绝不残留序号占用）。"""
    with pytest.raises(TelemetryExpiredError):
        service.publish_telemetry(
            "dev-1", registered["credential_id"], registered["secret"],
            2, clock() - 9999, {"x": 0},
        )
    r = _pub(service, registered, 2, {"x": 1})
    assert r["applied"] and r["code"] == "ACCEPTED"
