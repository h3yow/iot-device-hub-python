"""OTA：分批下发、确认、失败回退、超时回退、离线排队、兼容性拒绝。"""
import logging

from iot_hub.errors import ErrorCode
from iot_hub.models import OtaJobStatus, Scope
from tests.test_auth import expect_error

log = logging.getLogger("test")


def setup_device(hub, clock, device_id="dev-1", model="sensor-A", fw="1.0.0", online=True):
    hub.registry.register(device_id, model, fw, clock())
    token = hub.registry.issue_credential(device_id, {Scope.OTA_ACK}, clock()).token
    if online:
        hub.registry.set_online(device_id, True, clock())
    return token


def make_campaign(hub, clock, batch_size=2, ack_timeout=60.0):
    hub.ota.create_campaign("c1", "sensor-A", "2.0.0", batch_size, ack_timeout, clock())


def test_dispatch_online_pending_offline_queued(hub, clock):
    make_campaign(hub, clock)
    setup_device(hub, clock, "dev-on", online=True)
    setup_device(hub, clock, "dev-off", online=False)
    j1 = hub.dispatch_ota("c1", "dev-on")
    j2 = hub.dispatch_ota("c1", "dev-off")
    log.info("输入: 在线/离线设备各一台下发 -> %s / %s", j1.status.value, j2.status.value)
    assert j1.status == OtaJobStatus.PENDING and j1.ack_deadline > 0
    assert j2.status == OtaJobStatus.QUEUED  # 离线按策略排队


def test_batch_size_limits_pending(hub, clock):
    make_campaign(hub, clock, batch_size=2)
    for i in range(4):
        setup_device(hub, clock, f"dev-{i}", online=True)
        hub.dispatch_ota("c1", f"dev-{i}")
    statuses = [hub.ota.get_campaign("c1").jobs[f"dev-{i}"].status for i in range(4)]
    log.info("输入: batch_size=2 下发 4 台 -> %s", [s.value for s in statuses])
    assert statuses.count(OtaJobStatus.PENDING) == 2
    assert statuses.count(OtaJobStatus.QUEUED) == 2


def test_ack_success_advances_firmware(hub, clock):
    make_campaign(hub, clock)
    token = setup_device(hub, clock)
    job = hub.dispatch_ota("c1", "dev-1")
    hub.ack_ota(token, "dev-1", job.job_id, success=True)
    device = hub.registry.get_device("dev-1")
    log.info("判定依据: 确认成功后固件 %s（未确认前不得视为生效）", device.firmware_version)
    assert device.firmware_version == "2.0.0"
    assert hub.ota.job_for("dev-1").status == OtaJobStatus.SUCCEEDED


def test_ack_failure_rolls_back_to_previous_version(hub, clock):
    make_campaign(hub, clock)
    token = setup_device(hub, clock)
    job = hub.dispatch_ota("c1", "dev-1")
    hub.ack_ota(token, "dev-1", job.job_id, success=False)
    device = hub.registry.get_device("dev-1")
    log.info("输入: 升级失败确认 -> 固件保持 %s", device.firmware_version)
    assert device.firmware_version == "1.0.0"  # 回退到确定的可用版本
    assert hub.ota.job_for("dev-1").status == OtaJobStatus.FAILED


def test_unconfirmed_update_not_effective(hub, clock):
    make_campaign(hub, clock)
    setup_device(hub, clock)
    hub.dispatch_ota("c1", "dev-1")
    device = hub.registry.get_device("dev-1")
    log.info("判定依据: PENDING 未确认，固件仍为 %s", device.firmware_version)
    assert device.firmware_version == "1.0.0"


def test_ack_timeout_rolls_back(hub, clock):
    make_campaign(hub, clock, ack_timeout=60.0)
    setup_device(hub, clock)
    hub.dispatch_ota("c1", "dev-1")
    clock.advance(61)
    rolled = hub.sweep_ota_timeouts()
    log.info("输入: 超时 61s 后清扫 -> 回退 %d 个任务", len(rolled))
    assert len(rolled) == 1 and rolled[0].status == OtaJobStatus.ROLLED_BACK
    assert hub.registry.get_device("dev-1").firmware_version == "1.0.0"


def test_ack_after_terminal_state_rejected(hub, clock):
    make_campaign(hub, clock)
    token = setup_device(hub, clock)
    job = hub.dispatch_ota("c1", "dev-1")
    hub.ack_ota(token, "dev-1", job.job_id, success=True)
    log.info("输入: 已 SUCCEEDED 的任务重复确认")
    expect_error(ErrorCode.OTA_NOT_PENDING,
                 lambda: hub.ack_ota(token, "dev-1", job.job_id, success=True),
                 "终态任务不可再确认，应为 OTA_NOT_PENDING")


def test_incompatible_model_and_same_version_rejected(hub, clock):
    make_campaign(hub, clock)
    setup_device(hub, clock, "dev-x", model="sensor-B")
    log.info("输入: 型号 sensor-B 设备下发 sensor-A 固件")
    expect_error(ErrorCode.INCOMPATIBLE_FIRMWARE,
                 lambda: hub.dispatch_ota("c1", "dev-x"),
                 "型号不兼容，应为 INCOMPATIBLE_FIRMWARE")
    setup_device(hub, clock, "dev-y", fw="2.0.0")
    log.info("输入: 已处于目标版本的设备下发")
    expect_error(ErrorCode.INCOMPATIBLE_FIRMWARE,
                 lambda: hub.dispatch_ota("c1", "dev-y"),
                 "版本相同无升级意义，应为 INCOMPATIBLE_FIRMWARE")


def test_offline_device_promoted_after_reconnect(hub, clock):
    make_campaign(hub, clock)
    setup_device(hub, clock, "dev-off", online=False)
    job = hub.dispatch_ota("c1", "dev-off")
    assert job.status == OtaJobStatus.QUEUED
    hub.registry.set_online("dev-off", True, clock())
    promoted = hub.promote_ota("c1")
    log.info("输入: 设备重连后提升 -> %s", [j.status.value for j in promoted])
    assert len(promoted) == 1 and promoted[0].status == OtaJobStatus.PENDING
    assert promoted[0].ack_deadline > clock()


def test_redispatch_idempotent(hub, clock):
    make_campaign(hub, clock)
    setup_device(hub, clock)
    j1 = hub.dispatch_ota("c1", "dev-1")
    j2 = hub.dispatch_ota("c1", "dev-1")
    log.info("判定依据: 重复下发返回同一任务 job_id=%s", j1.job_id)
    assert j1.job_id == j2.job_id
    assert len(hub.ota.get_campaign("c1").jobs) == 1
