"""限流与积压：超限拒绝、容量有界、失败无残留。"""
import logging

from iot_hub.errors import ErrorCode
from iot_hub.ratelimit import Backlog, RateLimiter
from iot_hub.service import DeviceHubService
from tests.conftest import ALL_SCOPES
from tests.test_auth import expect_error
from tests.test_telemetry import msg

log = logging.getLogger("test")


def test_per_device_rate_limit(hub, registered, clock):
    _, token = registered()
    for i in range(20):  # 设备突发容量 20
        hub.ingest_telemetry(token, msg(message_id=f"m{i}", seq=i + 1))
    log.info("输入: 突发容量耗尽后第 21 条")
    expect_error(ErrorCode.RATE_LIMITED,
                 lambda: hub.ingest_telemetry(token, msg(message_id="m20", seq=21)),
                 "超出设备限速，应为 RATE_LIMITED")
    assert hub.telemetry.last_seq("dev-1") == 20  # 被拒消息不产生任何状态


def test_rate_refills_over_time(hub, registered, clock):
    _, token = registered()
    for i in range(20):
        hub.ingest_telemetry(token, msg(message_id=f"m{i}", seq=i + 1))
    clock.advance(1.0)  # 10/s 补充 10 个令牌
    assert hub.ingest_telemetry(token, msg(message_id="m20", seq=21)) == "applied"
    log.info("判定依据: 1s 后令牌补充，恢复接收")


def test_global_rate_limit(clock):
    hub = DeviceHubService(now_fn=clock)
    hub.rate_limiter = RateLimiter(1e9, 1_000_000, global_rate=1.0, global_burst=2)
    tokens = []
    for i in range(3):
        hub.registry.register(f"dev-{i}", "m", "1.0.0", clock())
        tokens.append(hub.registry.issue_credential(f"dev-{i}", ALL_SCOPES, clock()).token)
    hub.ingest_telemetry(tokens[0], msg(device_id="dev-0", message_id="a", seq=1))
    hub.ingest_telemetry(tokens[1], msg(device_id="dev-1", message_id="b", seq=1))
    log.info("输入: 全局突发 2 耗尽后第三台设备上报")
    expect_error(ErrorCode.RATE_LIMITED,
                 lambda: hub.ingest_telemetry(tokens[2], msg(device_id="dev-2", message_id="c", seq=1)),
                 "超出全局限速，应为 RATE_LIMITED")


def test_backlog_bounded():
    backlog = Backlog(capacity=3)
    for i in range(3):
        backlog.push(i)
    log.info("输入: 容量 3 的队列压入第 4 项")
    expect_error(ErrorCode.BACKLOG_FULL, lambda: backlog.push(3),
                 "积压超限，应为 BACKLOG_FULL，内存有界")
    assert len(backlog) == 3
    assert [backlog.pop() for _ in range(3)] == [0, 1, 2]
    assert backlog.pop() is None
