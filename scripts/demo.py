"""完全本地的端到端演示：无需消息代理、云端或真实设备。

运行：uv run python scripts/demo.py
演示链路：
1. 注册 + 连接（会话纪元）
2. 遥测正常/重复/乱序空洞补齐
3. 凭据轮换宽限期语义
4. 影子版本与新鲜度
5. OTA 下发 -> 设备失败 -> 确定回退
6. 快照保存/恢复
"""

from __future__ import annotations

import json
import logging
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from iot_hub.errors import HubError
from iot_hub.ratelimit import BoundedBacklog, RateLimiter
from iot_hub.service import HubService
from iot_hub.storage import HubStore

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger("demo")


class Clock:
    def __init__(self):
        self.t = 1_000_000.0

    def __call__(self):
        return self.t

    def advance(self, s):
        self.t += s


def show(title, value):
    print(f"\n=== {title} ===")
    print(json.dumps(value, indent=2, ensure_ascii=False, default=str))


def try_call(label, fn):
    try:
        result = fn()
        print(f"[OK]   {label}")
        return result
    except HubError as e:
        print(f"[DENY] {label} -> code={e.code} message={e.message} details={e.details}")
        return None


def main():
    clk = Clock()
    store = HubStore(clock=clk)
    svc = HubService(
        store,
        rate_limiter=RateLimiter(clock=clk),
        backlog=BoundedBacklog(1024),
        clock=clk, auto_sweep=False,
    )

    # 1) 注册 + 连接
    reg = svc.register_device("sensor-001", "sensor-v1", initial_fw_version="1.0.0")
    conn = svc.connect("sensor-001", reg["credential_id"], reg["secret"])
    show("连接成功（含会话纪元）", conn)

    # 2) 遥测：正常 -> 乱序(空洞) -> 补齐 -> 重复
    t = clk()
    svc.publish_telemetry("sensor-001", reg["credential_id"], reg["secret"], 1, t, {"temp": 21.0}, epoch=conn["epoch"])
    svc.publish_telemetry("sensor-001", reg["credential_id"], reg["secret"], 3, t, {"temp": 23.0}, epoch=conn["epoch"])
    show("seq=3 先到（缺口 seq=2 缓冲，影子仍是 seq=1）", svc.get_shadow("sensor-001"))
    svc.publish_telemetry("sensor-001", reg["credential_id"], reg["secret"], 2, t, {"temp": 22.0}, epoch=conn["epoch"])
    show("seq=2 补齐（按序应用，最终值=23.0，版本=3）", svc.get_shadow("sensor-001"))
    dup = svc.publish_telemetry("sensor-001", reg["credential_id"], reg["secret"], 2, t, {"temp": 22.0}, epoch=conn["epoch"])
    show("seq=2 重放（不重复生效）", dup)

    # 3) 凭据轮换宽限
    rot = svc.rotate_credential("sensor-001", grace_period_s=300)
    show("轮换出新凭据（旧凭据进入 300s 宽限）", {"new_generation": rot["generation"], "new_grace_until": rot["grace_until"], "retired": rot["retired_credentials"]})
    try_call("宽限期内旧凭据连接（允许，grace=true）",
             lambda: svc.connect("sensor-001", reg["credential_id"], reg["secret"]))
    # 注意 connect 会建立新会话；为了继续演示用旧凭据，直接演示宽限过期：
    clk.advance(301)
    try_call("宽限期外旧凭据连接（GRACE_EXPIRED）",
             lambda: svc.connect("sensor-001", reg["credential_id"], reg["secret"]))

    # 4) 影子新鲜度
    show("离线 301s 后的影子（freshness=STALE，不参与最新值判定）", svc.get_shadow("sensor-001"))

    # 5) OTA：下发 -> 设备失败 -> 回退
    new_conn = svc.connect("sensor-001", rot["credential_id"], rot["secret"])
    svc.register_firmware(model="sensor-v1", version="2.0.0", min_from_version="1.0.0")
    svc.create_rollout(rollout_id="ro-200", firmware_version="2.0.0", model="sensor-v1",
                       percent=100, confirm_timeout_s=60)
    svc.ota.mark_online("sensor-001", True)
    delivery = svc.assign_ota("sensor-001", "ro-200")
    show("OTA 已下发（DELIVERED，60s 确认截止）", delivery)
    svc.report_ota_failure("sensor-001", rot["credential_id"], rot["secret"],
                           "checksum_mismatch", epoch=new_conn["epoch"])
    show("设备升级失败 -> 确定回退到 1.0.0", svc.ota_state("sensor-001"))

    # 6) 快照持久化
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "hub.json")
        svc.save_snapshot(path)
        svc2 = HubService(auto_sweep=False)
        svc2.load_snapshot(path)
        show("快照恢复后的影子", svc2.get_shadow("sensor-001"))
        print(f"\n[OK] 快照已写入并恢复: {path} ({os.path.getsize(path)} bytes)")

    show("统计计数", svc.metrics())
    print("\n演示完成：所有结论均为本地、确定、可解释的错误码。")


if __name__ == "__main__":
    main()
