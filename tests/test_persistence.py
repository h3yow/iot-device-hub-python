"""本地 JSON 快照：重启后影子版本、序号去重、凭据状态与 OTA 结论保持一致。"""

from __future__ import annotations

import logging
import os

from iot_hub.ratelimit import BoundedBacklog, RateLimiter
from iot_hub.service import HubService
from iot_hub.storage import HubStore

from .conftest import FakeClock

log = logging.getLogger("tests.persistence")


def _service(clock):
    store = HubStore(clock=clock)
    return HubService(
        store,
        rate_limiter=RateLimiter(
            global_rate=1e9, global_burst=1e9, device_rate=1e9, device_burst=1e9, clock=clock
        ),
        backlog=BoundedBacklog(1_000_000), clock=clock, auto_sweep=False,
    )


def test_snapshot_roundtrip_preserves_shadow_seq_credential_and_ota(tmp_path, clock):
    svc = _service(clock)
    reg = svc.register_device("dev-1", "sensor-v1", initial_fw_version="1.0.0")
    conn = svc.connect("dev-1", reg["credential_id"], reg["secret"])
    svc.publish_telemetry(
        "dev-1", reg["credential_id"], reg["secret"], 1, clock(), {"t": 1},
        epoch=conn["epoch"],
    )
    svc.publish_telemetry(
        "dev-1", reg["credential_id"], reg["secret"], 2, clock(), {"t": 2},
        epoch=conn["epoch"],
    )
    svc.update_desired("dev-1", {"mode": "eco"})
    # OTA 已确认
    svc.register_firmware(model="sensor-v1", version="2.0.0", min_from_version="1.0.0")
    svc.create_rollout(rollout_id="ro", firmware_version="2.0.0", model="sensor-v1",
                       percent=100, confirm_timeout_s=3600)
    svc.ota.mark_online("dev-1", True)
    svc.assign_ota("dev-1", "ro")
    svc.confirm_ota("dev-1", reg["credential_id"], reg["secret"], "2.0.0", epoch=conn["epoch"])
    # 吊销另一台设备凭据，确认吊销状态持久
    reg2 = svc.register_device("dev-2", "sensor-v1")
    svc.revoke_credential(reg2["credential_id"])

    path = os.path.join(tmp_path, "snapshot.json")
    svc.save_snapshot(path)
    log.info("snapshot saved to %s size=%d bytes", path, os.path.getsize(path))

    # 新进程语义：新 clock + 新 service，恢复快照
    clock2 = FakeClock(start=clock.t + 10)
    svc2 = _service(clock2)
    svc2.load_snapshot(path)

    view = svc2.get_shadow("dev-1")
    assert view["version"] == 3 and view["reported"] == {"t": 2}
    assert view["desired"] == {"mode": "eco"} and view["reported_seq"] == 2
    log.info("RESTORED shadow version=%s reported=%s", view["version"], view["reported"])

    # 序号窗口恢复：旧序号重放判重复
    from iot_hub.errors import CredentialRevokedError
    import pytest
    # 注意：恢复后 epochs 也保留；新上报沿用旧 epoch 仍然合法（未重连）
    r = svc2.publish_telemetry(
        "dev-1", reg["credential_id"], reg["secret"], 2, clock2(), {"t": 2},
        epoch=conn["epoch"],
    )
    assert r["code"] == "TELEMETRY_DUPLICATE" and r["applied"] is False

    # OTA 结论与已知良好版本恢复
    st = svc2.ota_state("dev-1")
    assert st["status"] == "CONFIRMED"
    assert st["current_fw_version"] == "2.0.0" and st["last_known_good_version"] == "2.0.0"

    # 吊销状态恢复
    with pytest.raises(CredentialRevokedError):
        svc2.connect("dev-2", reg2["credential_id"], reg2["secret"])
    log.info("RESTORED decisions: seq dedup, OTA confirmed, credential revoked — all stable")
