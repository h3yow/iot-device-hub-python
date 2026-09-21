"""遥测：幂等、乱序、过期、断线续传、拒绝原因可区分。"""
import logging

import pytest

from iot_hub.errors import ErrorCode, HubError
from iot_hub.models import TelemetryMessage
from tests.test_auth import expect_error

log = logging.getLogger("test")


def msg(device_id="dev-1", message_id="m1", seq=1, ts=1_000_000.0,
        payload=None, pv=1) -> TelemetryMessage:
    return TelemetryMessage(device_id=device_id, message_id=message_id, seq=seq,
                            timestamp=ts, payload=payload or {"temp": 20},
                            protocol_version=pv)


def test_ingest_applied_and_counted_once(hub, registered, clock):
    _, token = registered()
    result = hub.ingest_telemetry(token, msg())
    log.info("输入: seq=1 首条遥测 -> 结果 %s", result)
    assert result == "applied"
    assert hub.stats.snapshot()["telemetry/applied"] == 1


def test_duplicate_message_id_rejected_not_double_counted(hub, registered, clock):
    _, token = registered()
    hub.ingest_telemetry(token, msg(message_id="m1", seq=1))
    log.info("输入: 相同 message_id=m1 重复上报（断线重发场景）")
    expect_error(ErrorCode.DUPLICATE_MESSAGE,
                 lambda: hub.ingest_telemetry(token, msg(message_id="m1", seq=2)),
                 "message_id 已处理，应为 DUPLICATE_MESSAGE 且不重复生效")
    snap = hub.stats.snapshot()
    assert snap["telemetry/applied"] == 1
    assert snap["telemetry/duplicate_message"] == 1
    assert hub.telemetry.last_seq("dev-1") == 1  # 序号未被重复消息推进


def test_out_of_order_rejected_and_state_not_overwritten(hub, registered, clock):
    _, token = registered()
    hub.ingest_telemetry(token, msg(message_id="m1", seq=5, payload={"temp": 25}))
    log.info("输入: 已应用 seq=5 后收到乱序 seq=3")
    expect_error(ErrorCode.STALE_SEQUENCE,
                 lambda: hub.ingest_telemetry(token, msg(message_id="m2", seq=3)),
                 "seq 3 <= 已应用 5，应为 STALE_SEQUENCE")
    latest = hub.telemetry.latest("dev-1")
    assert latest["seq"] == 5 and latest["payload"]["temp"] == 25
    log.info("判定依据: 最新状态仍为 seq=5 的值，乱序数据未覆盖 -> %s", latest)


def test_expired_and_future_data_rejected(hub, registered, clock):
    _, token = registered()
    log.info("输入: 采样时间早于窗口 (now-3600)")
    expect_error(ErrorCode.EXPIRED_DATA,
                 lambda: hub.ingest_telemetry(token, msg(seq=1, ts=clock() - 3601)),
                 "数据过旧，应为 EXPIRED_DATA")
    log.info("输入: 采样时间超出时钟偏移窗口 (now+300)")
    expect_error(ErrorCode.EXPIRED_DATA,
                 lambda: hub.ingest_telemetry(token, msg(seq=1, ts=clock() + 301)),
                 "时间戳超前过多，应为 EXPIRED_DATA")


def test_malformed_and_protocol_version(hub, registered, clock):
    _, token = registered()
    log.info("输入: 协议版本 99")
    expect_error(ErrorCode.PROTOCOL_VERSION_UNSUPPORTED,
                 lambda: hub.ingest_telemetry(token, msg(pv=99)),
                 "协议版本不兼容，应为 PROTOCOL_VERSION_UNSUPPORTED")
    log.info("输入: 空 message_id")
    expect_error(ErrorCode.MALFORMED_PAYLOAD,
                 lambda: hub.ingest_telemetry(token, msg(message_id="")),
                 "缺少幂等标识，应为 MALFORMED_PAYLOAD")


def test_reconnect_resume_no_loss_no_duplicate(hub, registered, clock):
    """断线重连续传：已送达的重发被判重，未送达的按序补传生效。"""
    _, token = registered()
    for i in range(1, 4):  # 断线前已送达 seq 1..3
        hub.ingest_telemetry(token, msg(message_id=f"m{i}", seq=i))
    log.info("输入: 重连后设备重发 seq 2,3 并补发 seq 4,5")
    for i in (2, 3):
        expect_error(ErrorCode.DUPLICATE_MESSAGE,
                     lambda i=i: hub.ingest_telemetry(token, msg(message_id=f"m{i}", seq=i)),
                     f"重发 seq={i} 应为 DUPLICATE_MESSAGE")
    for i in (4, 5):
        assert hub.ingest_telemetry(token, msg(message_id=f"m{i}", seq=i)) == "applied"
    assert hub.telemetry.last_seq("dev-1") == 5
    snap = hub.stats.snapshot()
    assert snap["telemetry/applied"] == 5  # 恰好 5 条生效，不丢不重
    log.info("判定依据: applied=%d duplicate=%d -> 续传不丢不重",
             snap["telemetry/applied"], snap["telemetry/duplicate_message"])


def test_telemetry_requires_registered_device(hub, clock):
    log.info("输入: 未注册设备上报")
    expect_error(ErrorCode.CREDENTIAL_INVALID,
                 lambda: hub.ingest_telemetry("bad-token", msg()),
                 "凭据不存在，应为 CREDENTIAL_INVALID")


def test_cross_device_report_denied(hub, registered, clock):
    _, token_a = registered("dev-A")
    registered("dev-B")
    log.info("输入: 设备 A 凭据上报 device_id=dev-B")
    expect_error(ErrorCode.SCOPE_DENIED,
                 lambda: hub.ingest_telemetry(token_a, msg(device_id="dev-B")),
                 "不得代其他设备上报，应为 SCOPE_DENIED")
