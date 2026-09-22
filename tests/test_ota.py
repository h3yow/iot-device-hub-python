"""OTA 异常链路：分批、离线排队、确认超时/失败回退、未确认不生效、兼容性拒绝。"""

from __future__ import annotations

import logging

import pytest

from iot_hub.errors import (
    OtaIncompatibleError,
    OtaNoAvailableVersionError,
    OtaStateInvalidError,
)

log = logging.getLogger("tests.ota")


def _setup_fw(service):
    service.register_firmware(model="sensor-v1", version="1.1.0")
    service.register_firmware(model="sensor-v1", version="2.0.0", min_from_version="1.0.0")
    ro = service.create_rollout(
        rollout_id="ro-200", firmware_version="2.0.0", model="sensor-v1",
        percent=100, confirm_timeout_s=60.0,
    )
    return ro


def test_model_mismatch_and_version_too_old_rejected(service, registered, clock):
    service.register_firmware(model="sensor-v2", version="2.0.0")
    ro = service.create_rollout(
        rollout_id="ro-x", firmware_version="2.0.0", model="sensor-v2", percent=100
    )
    log.info("INPUT assign model=sensor-v1 device to model=sensor-v2 rollout")
    with pytest.raises(OtaIncompatibleError) as ei:
        service.assign_ota("dev-1", "ro-x")
    log.info("DECISION %s reason=%s", ei.value.code, ei.value.details["reason"])
    assert ei.value.details["reason"] == "MODEL_MISMATCH"

    # 当前版本过低
    service.register_firmware(model="sensor-v1", version="3.0.0", min_from_version="2.5.0")
    service.create_rollout(
        rollout_id="ro-300", firmware_version="3.0.0", model="sensor-v1", percent=100
    )
    with pytest.raises(OtaIncompatibleError) as ei:
        service.assign_ota("dev-1", "ro-300")
    log.info("DECISION %s reason=%s current=1.0.0", ei.value.code, ei.value.details["reason"])
    assert ei.value.details["reason"] == "VERSION_TOO_OLD"
    # 被拒绝不得产生进行中状态
    assert service.ota_state("dev-1")["status"] == "NONE"


def test_canary_batching_is_stable_and_distinct(service, clock):
    service.register_firmware(model="sensor-v1", version="2.0.0", min_from_version="1.0.0")
    service.create_rollout(
        rollout_id="ro-canary", firmware_version="2.0.0", model="sensor-v1", percent=50
    )
    in_batch, out_batch = [], []
    for i in range(60):
        did = f"canary-{i:03d}"
        service.register_device(did, "sensor-v1", initial_fw_version="1.0.0")
        ro = service.store.rollouts["ro-canary"]
        (in_batch if ro.in_batch(did) else out_batch).append(did)
    log.info("canary split: in=%d out=%d (stable hash buckets)", len(in_batch), len(out_batch))
    assert in_batch and out_batch  # 50% 下两侧都应有设备
    # 稳定：重复计算结论一致
    ro = service.store.rollouts["ro-canary"]
    assert all(ro.in_batch(d) for d in in_batch)
    # 批次外设备显式指定该 rollout 应被明确拒绝
    from iot_hub.errors import OtaIncompatibleError as E
    with pytest.raises(E) as ei:
        service.assign_ota(out_batch[0], "ro-canary")
    assert ei.value.details["reason"] == "NOT_IN_BATCH"


def test_offline_device_queued_then_delivered_on_reconnect(service, registered, clock):
    _setup_fw(service)
    # 设备离线：分配后进入 QUEUED，不改变当前版本
    d = service.assign_ota("dev-1", "ro-200")
    log.info("INPUT assign while offline => %s", d["status"])
    assert d["status"] == "QUEUED"
    st = service.ota_state("dev-1")
    assert st["status"] == "QUEUED" and st["current_fw_version"] == "1.0.0"

    # 上线拉取 -> DELIVERED，带确认截止
    d = service.pull_ota("dev-1", registered["credential_id"], registered["secret"])
    assert d["status"] == "DELIVERED" and d["version"] == "2.0.0"
    assert service.ota_state("dev-1")["status"] == "DELIVERED"


def test_confirm_timeout_rolls_back_to_last_known_good(service, registered, clock):
    _setup_fw(service)
    service.ota.mark_online("dev-1", True)
    service.assign_ota("dev-1", "ro-200")
    assert service.ota_state("dev-1")["status"] == "DELIVERED"
    # 未确认：当前版本与 last_known_good 都不变
    assert service.ota_state("dev-1")["current_fw_version"] == "1.0.0"

    clock.advance(61)
    service.ota.mark_online("dev-1", True)  # 触发 sweep
    st = service.ota_state("dev-1")
    log.info("DECISION after confirm timeout => status=%s current=%s lkg=%s",
             st["status"], st["current_fw_version"], st["last_known_good_version"])
    assert st["status"] == "ROLLED_BACK"
    assert st["current_fw_version"] == "1.0.0" == st["last_known_good_version"]
    assert st["target_version"] == "1.0.0" and st["failure_reason"] == "CONFIRM_TIMEOUT"


def test_device_reported_failure_rolls_back(service, registered, clock):
    _setup_fw(service)
    service.ota.mark_online("dev-1", True)
    service.assign_ota("dev-1", "ro-200")
    st = service.report_ota_failure(
        "dev-1", registered["credential_id"], registered["secret"], "flash_checksum_error"
    )
    log.info("DECISION device failure => %s current=%s", st["status"], st["current_fw_version"])
    assert st["status"] == "ROLLED_BACK" and st["current_fw_version"] == "1.0.0"
    assert st["failure_reason"] == "DEVICE_REPORTED:flash_checksum_error"
    # 回退是终态结论；再次确认没有进行中的更新
    with pytest.raises(OtaStateInvalidError):
        service.confirm_ota("dev-1", registered["credential_id"], registered["secret"], "2.0.0")


def test_confirmed_version_becomes_effective_and_is_new_fallback(service, registered, clock):
    _setup_fw(service)
    service.ota.mark_online("dev-1", True)
    service.assign_ota("dev-1", "ro-200")
    st = service.confirm_ota(
        "dev-1", registered["credential_id"], registered["secret"], "2.0.0"
    )
    log.info("DECISION confirm 2.0.0 => status=%s", st["status"])
    assert st["status"] == "CONFIRMED"
    assert service.ota_state("dev-1")["last_known_good_version"] == "2.0.0"

    # 后续 3.0.0（要求从 2.0.0 起）下发失败时，应回退到 2.0.0
    service.register_firmware(model="sensor-v1", version="3.0.0", min_from_version="2.0.0")
    service.create_rollout(
        rollout_id="ro-300", firmware_version="3.0.0", model="sensor-v1",
        percent=100, confirm_timeout_s=60.0,
    )
    service.assign_ota("dev-1", "ro-300")
    service.report_ota_failure(
        "dev-1", registered["credential_id"], registered["secret"], "power_loss"
    )
    st = service.ota_state("dev-1")
    assert st["status"] == "ROLLED_BACK" and st["current_fw_version"] == "2.0.0"
    log.info("DECISION 3.0.0 failed => deterministic fallback to %s", st["current_fw_version"])


def test_confirm_wrong_version_is_rejected(service, registered, clock):
    _setup_fw(service)
    service.ota.mark_online("dev-1", True)
    service.assign_ota("dev-1", "ro-200")
    with pytest.raises(OtaIncompatibleError) as ei:
        service.confirm_ota(
            "dev-1", registered["credential_id"], registered["secret"], "9.9.9"
        )
    log.info("DECISION confirm wrong version => %s", ei.value.code)
    # 拒绝确认：状态仍是 DELIVERED，当前版本不变（未确认不生效）
    st = service.ota_state("dev-1")
    assert st["status"] == "DELIVERED" and st["current_fw_version"] == "1.0.0"


def test_no_last_known_good_gives_deterministic_failure(service, clock):
    # 出厂无版本设备
    res = service.register_device("dev-naked", "sensor-v1")
    service.register_firmware(model="sensor-v1", version="1.0.0", min_from_version=None)
    service.create_rollout(
        rollout_id="ro-init", firmware_version="1.0.0", model="sensor-v1",
        percent=100, confirm_timeout_s=60.0,
    )
    # 把出厂版本清空，模拟没有任何已知良好版本
    service.store.devices["dev-naked"].current_fw_version = None
    service.store.devices["dev-naked"].last_known_good_version = None
    service.ota.mark_online("dev-naked", True)
    service.assign_ota("dev-naked", "ro-init")
    with pytest.raises(OtaNoAvailableVersionError) as ei:
        service.report_ota_failure(
            "dev-naked", res["credential_id"], res["secret"], "boot_fail"
        )
    log.info("DECISION no fallback available => %s", ei.value.code)
    assert ei.value.code == "OTA_NO_AVAILABLE_VERSION"


def test_no_last_known_good_timeout_sweep_is_terminal_and_idempotent(service, clock):
    """无 LKG 设备确认超时：sweep 落 FAILED 终态，重复扫描不重复计数/不抛穿。"""
    service.register_device("dev-naked2", "sensor-v1")
    service.register_firmware(model="sensor-v1", version="1.0.0")
    service.create_rollout(
        rollout_id="ro-init2", firmware_version="1.0.0", model="sensor-v1",
        percent=100, confirm_timeout_s=60.0,
    )
    service.store.devices["dev-naked2"].current_fw_version = None
    service.store.devices["dev-naked2"].last_known_good_version = None
    service.ota.mark_online("dev-naked2", True)
    service.assign_ota("dev-naked2", "ro-init2")
    clock.advance(61)
    processed = service.ota.sweep_timeouts()
    st = service.ota_state("dev-naked2")
    assert processed == 1 and st["status"] == "FAILED"
    assert st["failure_reason"] == "CONFIRM_TIMEOUT"
    # 再次扫描幂等：无异常、无重复计数
    assert service.ota.sweep_timeouts() == 0
    assert service.ota_state("dev-naked2")["status"] == "FAILED"
    assert service.metrics()["ota.failed_no_lkg"] == 1
    log.info("DECISION no-LKG timeout => FAILED terminal, sweep idempotent")
