"""影子：单调版本、并发写入、回退更新、离线新鲜度。"""
import logging
import threading

from iot_hub.errors import ErrorCode
from tests.test_auth import expect_error

log = logging.getLogger("test")


def test_version_monotonic_and_counted_once(hub, registered, clock):
    _, token = registered()
    s1 = hub.update_reported(token, "dev-1", {"temp": 20}, base_version=None)
    s2 = hub.update_reported(token, "dev-1", {"temp": 21}, base_version=s1.version)
    log.info("输入: 两次顺序写入 -> 版本 %d -> %d", s1.version, s2.version)
    assert (s1.version, s2.version) == (1, 2)
    assert hub.stats.snapshot()["shadow/applied"] == 2  # 每次生效恰好计一次


def test_stale_base_version_conflict_no_partial_update(hub, registered, clock):
    _, token = registered()
    s1 = hub.update_reported(token, "dev-1", {"a": 1, "b": 2}, base_version=None)
    log.info("输入: 基于过期版本 v=%d 的回退写入", s1.version - 1)
    expect_error(ErrorCode.VERSION_CONFLICT,
                 lambda: hub.update_reported(token, "dev-1", {"a": 99},
                                             base_version=s1.version - 1),
                 "base_version 落后，应为 VERSION_CONFLICT 且不产生半更新")
    state = hub.get_shadow("dev-1")
    assert state.reported == {"a": 1, "b": 2} and state.version == 1
    log.info("判定依据: 状态保持 %s version=%d，无半更新、无版本倒退",
             state.reported, state.version)


def test_concurrent_writes_single_winner_no_lost_version(clock):
    """同一设备并发写入：N 个写者以相同 base_version 竞争，恰有一个生效。"""
    from iot_hub.ratelimit import RateLimiter
    from iot_hub.service import DeviceHubService
    from tests.conftest import ALL_SCOPES

    hub = DeviceHubService(now_fn=clock)
    hub.rate_limiter = RateLimiter(1e9, 1_000_000, 1e9, 1_000_000)  # 排除限速干扰
    hub.registry.register("dev-1", "sensor-A", "1.0.0", clock())
    token = hub.registry.issue_credential("dev-1", ALL_SCOPES, clock()).token
    hub.update_reported(token, "dev-1", {"init": True}, base_version=None)
    results, errors = [], []

    def writer(i: int) -> None:
        try:
            results.append(hub.update_reported(token, "dev-1", {f"w{i}": i},
                                               base_version=1))
        except Exception as exc:  # noqa: BLE001 - 测试收集所有失败
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    log.info("输入: 20 个并发写入 base_version=1 -> 成功 %d 冲突 %d",
             len(results), len(errors))
    assert len(results) == 1
    assert all(getattr(e, "code", None) == ErrorCode.VERSION_CONFLICT for e in errors)
    state = hub.get_shadow("dev-1")
    assert state.version == 2  # 版本恰好推进一次，不重复计数
    assert hub.stats.snapshot()["shadow/applied"] == 2


def test_offline_shadow_marked_stale_not_latest(hub, registered, clock):
    _, token = registered()
    hub.connect(token)
    s = hub.update_reported(token, "dev-1", {"temp": 22}, base_version=None)
    assert s.fresh is True
    hub.disconnect("dev-1")
    state = hub.get_shadow("dev-1")
    log.info("输入: 设备离线后读取影子 -> fresh=%s version=%d", state.fresh, state.version)
    assert state.fresh is False  # 陈旧标记，业务不得当作最新值
    assert state.version == s.version  # 标记不改变版本号
    assert state.reported == {"temp": 22}  # 状态保留


def test_desired_merge_keeps_reported(hub, registered, clock):
    _, token = registered()
    hub.update_reported(token, "dev-1", {"temp": 20}, base_version=None)
    s = hub.update_desired("dev-1", {"target": 26}, base_version=None)
    assert s.reported == {"temp": 20} and s.desired == {"target": 26}
    assert s.version == 2
    log.info("判定依据: desired 合并不影响 reported -> %s / %s", s.reported, s.desired)


def test_shadow_version_conflict_counted(hub, registered, clock):
    _, token = registered()
    hub.update_reported(token, "dev-1", {"a": 1}, base_version=None)
    expect_error(ErrorCode.VERSION_CONFLICT,
                 lambda: hub.update_reported(token, "dev-1", {"a": 2}, base_version=0),
                 "版本冲突")
    snap = hub.stats.snapshot()
    assert snap["shadow/applied"] == 1 and snap["shadow/version_conflict"] == 1
