"""限速与积压：设备维度、全局维度与有界在途，超限原因可区分且不残留占用。"""

from __future__ import annotations

import logging

import pytest

from iot_hub.errors import BacklogFullError, RateLimitedError
from iot_hub.ratelimit import BoundedBacklog, RateLimiter

from .conftest import FakeClock

log = logging.getLogger("tests.ratelimit")


def test_device_bucket_then_global_bucket_are_distinct():
    clk = FakeClock()
    rl = RateLimiter(
        global_rate=1, global_burst=2,
        device_rate=1, device_burst=2, clock=clk,
    )
    rl.check("a")
    rl.check("a")  # 设备桶 2 个令牌用完
    with pytest.raises(RateLimitedError) as ei:
        rl.check("a")  # 第三次：设备 a 桶空
    assert ei.value.details["dimension"] == "device"
    log.info("DECISION device limit => %s dimension=device", ei.value.code)

    # 重置：全局容量仅 1
    rl = RateLimiter(global_rate=1, global_burst=1, device_rate=10, device_burst=10, clock=clk)
    rl.check("a")  # 全局唯一令牌被 a 消耗
    with pytest.raises(RateLimitedError) as ei:
        rl.check("b")  # 设备 b 有令牌，全局桶空
    assert ei.value.details["dimension"] == "global"
    log.info("DECISION global limit => %s dimension=global", ei.value.code)

    clk.advance(10)
    rl.check("b")  # 补充后恢复


def test_service_enforces_device_rate_limit(service, registered, clock):
    # service fixture 限速很高，这里收紧
    service.rate_limiter = RateLimiter(
        global_rate=1000, global_burst=1000, device_rate=1, device_burst=2, clock=clock
    )
    service.publish_telemetry(
        "dev-1", registered["credential_id"], registered["secret"], 1, clock(), {"a": 1}
    )
    service.publish_telemetry(
        "dev-1", registered["credential_id"], registered["secret"], 2, clock(), {"a": 2}
    )
    with pytest.raises(RateLimitedError) as ei:
        service.publish_telemetry(
            "dev-1", registered["credential_id"], registered["secret"], 3, clock(), {"a": 3}
        )
    assert ei.value.code == "RATE_LIMITED"
    log.info("DECISION service telemetry rate limited => %s", ei.value.code)
    # 被限流的请求未生效：序号 3 未登记
    assert service.get_shadow("dev-1")["version"] == 2


def test_bounded_backlog_blocks_and_releases_even_on_error():
    bl = BoundedBacklog(2)
    bl.acquire()
    bl.acquire()
    with pytest.raises(BacklogFullError) as ei:
        bl.acquire()
    assert ei.value.code == "BACKLOG_FULL"
    log.info("DECISION backlog full => %s active=%s", ei.value.code, bl.active)

    # 异常路径也必须释放，不残留占用
    bl.release()
    assert bl.active == 1  # 腾出一个槽
    with pytest.raises(RuntimeError):
        with bl:
            assert bl.active == 2
            raise RuntimeError("boom mid-operation")
    assert bl.active == 1  # __exit__ 已释放
    log.info("DECISION exception path released backlog cleanly, active=%s", bl.active)

    bl.release()
    assert bl.active == 0
    # 不能多释放
    bl.release()
    assert bl.active == 0


def test_ota_queue_backlog_limit(service, registered, clock):
    from iot_hub.ota import OtaManager
    service.ota = OtaManager(service.store, queue_capacity_per_rollout=1)
    service.register_firmware(model="sensor-v1", version="2.0.0", min_from_version="1.0.0")
    service.create_rollout(rollout_id="ro", firmware_version="2.0.0", model="sensor-v1")
    # dev-1 离线占满队列槽
    d = service.assign_ota("dev-1", "ro")
    assert d["status"] == "QUEUED"
    # 第二台离线设备：队列满 -> BACKLOG_FULL，且不残留 QUEUED 状态
    service.register_device("dev-2", "sensor-v1", initial_fw_version="1.0.0")
    with pytest.raises(BacklogFullError):
        service.assign_ota("dev-2", "ro")
    assert service.ota_state("dev-2")["status"] == "NONE"
    assert service.store.ota_queues["ro"] == ["dev-1"]
    log.info("DECISION OTA queue full => BACKLOG_FULL, no partial state for dev-2")
