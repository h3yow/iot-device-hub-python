"""设备影子：单调版本、乐观并发、回退写入拒绝与新鲜度标记。"""

from __future__ import annotations

import logging
import threading

import pytest

from iot_hub.errors import ShadowVersionConflictError

log = logging.getLogger("tests.shadow")


def _pub(service, reg, seq, payload):
    return service.publish_telemetry(
        "dev-1", reg["credential_id"], reg["secret"], seq, service.store.clock(), payload
    )


def test_desired_update_increments_version_and_delta(service, registered, clock):
    v = service.update_desired("dev-1", {"power": "on"})
    log.info("INPUT desired power=on => version=%s delta=%r", v["version"], v["delta"])
    assert v["version"] == 1 and v["delta"] == {"power": "on"}
    _pub(service, registered, 1, {"power": "on"})
    v = service.get_shadow("dev-1")
    assert v["delta"] == {} and v["version"] == 2


def test_optimistic_version_conflict_rejects_stale_write(service, registered):
    service.update_desired("dev-1", {"a": 1})
    # 持有过期版本 0 的写入必须被拒绝且不留半更新
    with pytest.raises(ShadowVersionConflictError) as ei:
        service.update_desired("dev-1", {"a": 2}, expected_version=0)
    log.info("DECISION stale expected_version=0 => %s current=1", ei.value.code)
    assert ei.value.code == "SHADOW_VERSION_CONFLICT"
    view = service.get_shadow("dev-1")
    assert view["desired"] == {"a": 1} and view["version"] == 1
    # 带正确版本可以成功
    v = service.update_desired("dev-1", {"a": 2}, expected_version=1)
    assert v["version"] == 2 and v["desired"] == {"a": 2}


def test_version_never_goes_back_and_no_half_update(service, registered, clock):
    _pub(service, registered, 1, {"t": 1})
    _pub(service, registered, 2, {"t": 2})
    # 遥测与 desired 交错，版本只增不减
    service.update_desired("dev-1", {"x": 0})
    _pub(service, registered, 3, {"t": 3})
    view = service.get_shadow("dev-1")
    assert view["version"] == 4 and view["reported"] == {"t": 3}
    # 删除键也原子生效
    service.update_desired("dev-1", {"x": None})
    assert service.get_shadow("dev-1")["desired"] == {}


def test_concurrent_desired_writes_each_count_once(service):
    errors = []
    versions = []

    def worker(i):
        try:
            # 不带 expected_version 的最后写入合并语义：每次都成功且版本恰好 +1
            v = service.update_desired("dev-1", {f"k{i}": i})
            versions.append(v["version"])
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(40)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert sorted(versions) == list(range(1, 41))
    view = service.get_shadow("dev-1")
    assert view["version"] == 40 and len(view["desired"]) == 40
    log.info("DECISION 40 concurrent writes => final version=%s (no duplicate counts)", view["version"])


def test_concurrent_telemetry_preserves_monotonic_version(service, registered):
    errors: list = []

    def worker(seq):
        try:
            _pub(service, registered, seq, {"last": seq})
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(1, 101)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    view = service.get_shadow("dev-1")
    log.info("DECISION 100 concurrent telemetry => version=%s reported.last=%s",
             view["version"], view["reported"]["last"])
    assert view["version"] == 100 and view["reported_seq"] == 100
    assert view["reported"]["last"] == 100  # 最高序号值最终生效


def test_freshness_marks_stale_and_unknown(service, registered, clock):
    v = service.get_shadow("dev-1")
    assert v["freshness"] == "UNKNOWN"
    _pub(service, registered, 1, {"t": 1})
    assert service.get_shadow("dev-1")["freshness"] == "FRESH"
    clock.advance(120)
    v = service.get_shadow("dev-1")
    log.info("DECISION after 120s offline => freshness=%s (not treated as latest)", v["freshness"])
    assert v["freshness"] == "STALE"
