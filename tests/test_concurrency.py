"""并发接入：鉴权结论、影子版本、下发进度与统计计数一致。"""
import logging
import threading

from iot_hub.models import Scope
from iot_hub.ratelimit import RateLimiter
from iot_hub.service import DeviceHubService
from tests.conftest import ALL_SCOPES
from tests.test_telemetry import msg

log = logging.getLogger("test")

N_DEVICES = 20
MSGS_PER_DEVICE = 10


def make_hub(clock):
    hub = DeviceHubService(now_fn=clock)
    hub.rate_limiter = RateLimiter(1e9, 1_000_000, 1e9, 1_000_000)  # 排除限速干扰
    return hub


def test_many_devices_concurrent_ingest_counted_exactly(clock):
    hub = make_hub(clock)
    tokens = {}
    for i in range(N_DEVICES):
        hub.registry.register(f"dev-{i}", "m", "1.0.0", clock())
        tokens[f"dev-{i}"] = hub.registry.issue_credential(
            f"dev-{i}", ALL_SCOPES, clock()).token

    def worker(dev: str) -> None:
        for j in range(MSGS_PER_DEVICE):
            hub.ingest_telemetry(tokens[dev],
                                 msg(device_id=dev, message_id=f"{dev}-m{j}", seq=j + 1))

    threads = [threading.Thread(target=worker, args=(f"dev-{i}",)) for i in range(N_DEVICES)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    snap = hub.stats.snapshot()
    log.info("输入: %d 设备 x %d 条并发上报 -> applied=%d",
             N_DEVICES, MSGS_PER_DEVICE, snap["telemetry/applied"])
    assert snap["telemetry/applied"] == N_DEVICES * MSGS_PER_DEVICE
    for i in range(N_DEVICES):
        assert hub.telemetry.last_seq(f"dev-{i}") == MSGS_PER_DEVICE  # 无跨设备串用


def test_concurrent_auth_same_device_consistent(clock):
    hub = make_hub(clock)
    hub.registry.register("dev-1", "m", "1.0.0", clock())
    token = hub.registry.issue_credential("dev-1", ALL_SCOPES, clock()).token
    outcomes = []

    def worker() -> None:
        try:
            device = hub.registry.authenticate(token, Scope.SHADOW_READ, clock())
            outcomes.append(device.device_id)
        except Exception as exc:  # noqa: BLE001
            outcomes.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    log.info("输入: 50 个并发鉴权同一凭据 -> 全部结论 %s", set(map(str, outcomes)))
    assert all(o == "dev-1" for o in outcomes)  # 鉴权结论一致


def test_concurrent_shadow_and_telemetry_no_cross_talk(clock):
    """影子与遥测并发：各自状态独立，统计不重复累加。"""
    hub = make_hub(clock)
    hub.registry.register("dev-1", "m", "1.0.0", clock())
    token = hub.registry.issue_credential("dev-1", ALL_SCOPES, clock()).token

    def telemetry_worker(worker_id: int) -> None:
        for j in range(50):
            seq = worker_id * 50 + j + 1  # 每线程独立序号区间，避免互相判重
            hub.ingest_telemetry(token, msg(message_id=f"m{seq}", seq=seq))

    def shadow_worker() -> None:
        for _ in range(50):
            state = hub.get_shadow("dev-1") if hub.shadow._states.get("dev-1") else None
            base = state.version if state else None
            try:
                hub.update_reported(token, "dev-1", {"tick": 1}, base_version=base)
            except Exception:  # noqa: BLE001,S112 - 冲突时读最新版本重试
                continue

    threads = ([threading.Thread(target=telemetry_worker, args=(w,)) for w in range(4)]
               + [threading.Thread(target=shadow_worker) for _ in range(4)])
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    snap = hub.stats.snapshot()
    log.info("判定依据: applied=%d 影子版本=%d", snap["telemetry/applied"],
             hub.get_shadow("dev-1").version)
    assert snap["telemetry/applied"] == 200  # 4 线程 x 50，恰好生效一次
    assert hub.telemetry.last_seq("dev-1") == 200  # 4 线程 x 50 的最大序号
    # 影子版本 = 生效次数，与计数一致，无重复累加
    assert hub.get_shadow("dev-1").version == snap["shadow/applied"]
