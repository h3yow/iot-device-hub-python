"""并发一致性：海量设备并发、同设备多连接、跨设备状态隔离、计数不重复累加。"""

from __future__ import annotations

import logging
import threading

log = logging.getLogger("tests.concurrency")


def test_many_devices_concurrent_authentication(service, clock):
    n = 200
    records = {}
    for i in range(n):
        r = service.register_device(f"bulk-{i:04d}", "sensor-v1", initial_fw_version="1.0.0")
        records[f"bulk-{i:04d}"] = (r["credential_id"], r["secret"])

    ok = {"v": 0}
    denied = []
    barrier = threading.Barrier(n)
    lock = threading.Lock()

    def worker(i):
        did = f"bulk-{i:04d}"
        cid, sec = records[did]
        barrier.wait()
        try:
            r = service.connect(did, cid, sec)
            assert r["device_id"] == did
            with lock:
                ok["v"] += 1
        except Exception as exc:  # noqa: BLE001
            with lock:
                denied.append((did, repr(exc)))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    log.info("DECISION concurrent auth: ok=%d denied=%d", ok["v"], len(denied))
    assert ok["v"] == n and not denied
    assert service.metrics()["auth.ok"] == n
    # 每设备的 last_seen 都应是自己的，不串用
    assert all(
        service.store.devices[f"bulk-{i:04d}"].last_seen_at == clock() for i in range(n)
    )


def test_revoked_credential_denied_concurrently(service, registered):
    n = 50
    barrier = threading.Barrier(n)
    results: list[str] = []
    lock = threading.Lock()

    def worker(i):
        barrier.wait()
        try:
            service.connect(
                "dev-1", registered["credential_id"], registered["secret"]
            )
            verdict = "OK"
        except Exception as exc:  # noqa: BLE001
            verdict = getattr(exc, "code", type(exc).__name__)
        with lock:
            results.append(verdict)

    # 一半线程在屏障处等待，先启动一个独立线程在屏障释放后立刻吊销
    revoke_t = threading.Thread(
        target=lambda: (barrier.wait(), service.revoke_credential(registered["credential_id"]))
    )
    revoke_t.start()
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n - 1)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    revoke_t.join()

    allowed = [r for r in results if r == "OK"]
    revoked = [r for r in results if r == "CREDENTIAL_REVOKED"]
    log.info("DECISION race vs revoke: allowed=%d revoked=%d (no other codes)",
             len(allowed), len(revoked))
    assert len(allowed) + len(revoked) == n - 1  # 只有两种确定结论
    assert set(results) <= {"OK", "CREDENTIAL_REVOKED"}
    # 吊销之后任何新连接必失败
    from iot_hub.errors import CredentialRevokedError
    import pytest
    with pytest.raises(CredentialRevokedError):
        service.connect("dev-1", registered["credential_id"], registered["secret"])


def test_multi_connection_telemetry_serialized_per_device(service, registered):
    """同一设备多连接分片并发展报：每个连接先 connect 拿会话纪元，各自序号段全部生效。"""
    n_conns = 10
    per_conn = 20
    errors: list = []
    sessions: list[tuple[int, int]] = []  # (连接序号, epoch)
    lock = threading.Lock()

    # 每个工作连接在上报前重新 connect，得到自己的纪元（纪元单调递增）
    creds = registered["credential_id"], registered["secret"]
    for i in range(n_conns):
        r = service.connect("dev-1", creds[0], creds[1])
        sessions.append((i, r["epoch"]))
    assert [e for _, e in sessions] == list(range(1, n_conns + 1))

    barrier = threading.Barrier(n_conns)

    def worker(cid_index, epoch):
        # 每个会话的序号空间独立、都从 1 开始；旧会话的写入被纪元挡掉
        barrier.wait()
        for seq in range(1, per_conn + 1):
            try:
                service.publish_telemetry(
                    "dev-1", creds[0], creds[1],
                    seq, service.store.clock(),
                    {"conn": cid_index, "seq_seen": seq}, epoch=epoch,
                )
            except Exception as exc:  # noqa: BLE001
                with lock:
                    errors.append(exc)

    threads = [
        threading.Thread(target=worker, args=(i, e)) for i, e in sessions
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # 只有最后一个连接是当前会话；其余连接的写入全部以 SESSION_STALE 拒绝
    stale = [e for e in errors if getattr(e, "code", "") == "SESSION_STALE"]
    others = [e for e in errors if getattr(e, "code", "") != "SESSION_STALE"]
    log.info("DECISION %d parallel connections: stale_rejected=%d unexpected=%d",
             n_conns, len(stale), len(others))
    assert len(stale) == (n_conns - 1) * per_conn
    assert not others
    # 只有最新会话（最后一个连接，序号段 181..200）生效：重置后的新序号空间
    view = service.get_shadow("dev-1")
    assert view["version"] == per_conn
    assert view["reported_seq"] == per_conn
    assert view["reported"]["seq_seen"] == per_conn


def test_zombie_connection_rejected_after_reconnect(service, registered):
    """旧连接在设备重连后上报：稳定结论 SESSION_STALE，旧序号不污染新会话。"""
    creds = registered["credential_id"], registered["secret"]
    old = service.connect("dev-1", creds[0], creds[1])
    service.publish_telemetry("dev-1", creds[0], creds[1], 1, service.store.clock(), {"a": 1},
                              epoch=old["epoch"])
    # 设备断线重连，新会话
    new = service.connect("dev-1", creds[0], creds[1])
    assert new["epoch"] == old["epoch"] + 1

    from iot_hub.errors import SessionStaleError
    import pytest
    with pytest.raises(SessionStaleError) as ei:
        service.publish_telemetry(
            "dev-1", creds[0], creds[1], 2, service.store.clock(), {"a": 2},
            epoch=old["epoch"],
        )
    log.info("DECISION stale epoch=%s current=%s => %s",
             old["epoch"], new["epoch"], ei.value.code)
    assert ei.value.code == "SESSION_STALE"
    # 新会话序号从 1 重新计数且正常生效；旧写入未残留
    r = service.publish_telemetry(
        "dev-1", creds[0], creds[1], 1, service.store.clock(), {"a": 100},
        epoch=new["epoch"],
    )
    assert r["applied"] and service.get_shadow("dev-1")["reported"] == {"a": 100}


def test_concurrent_out_of_order_batch_replays_as_duplicates_once(service, registered):
    """多连接同时重发同一批序号：每个序号只生效一次，不重复计数。"""
    n_threads = 16
    seqs = list(range(1, 31))
    barrier = threading.Barrier(n_threads)
    results: dict = {"applied": 0, "duplicate": 0}
    lock = threading.Lock()

    def worker():
        barrier.wait()
        for seq in seqs:
            r = service.publish_telemetry(
                "dev-1", registered["credential_id"], registered["secret"],
                seq, service.store.clock(), {"s": seq},
            )
            with lock:
                results["applied" if r["applied"] else "duplicate"] += 1

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    view = service.get_shadow("dev-1")
    log.info("DECISION %d parallel replays of 30 seqs: applied=%s duplicate=%s version=%s",
             n_threads, results["applied"], results["duplicate"], view["version"])
    assert results["applied"] == 30
    assert results["duplicate"] == 30 * (n_threads - 1)
    assert view["version"] == 30 and view["reported_seq"] == 30


def test_no_cross_device_state_leakage(service, clock):
    a = service.register_device("dev-a", "sensor-v1")
    b = service.register_device("dev-b", "sensor-v1")
    service.publish_telemetry("dev-a", a["credential_id"], a["secret"], 1, clock(), {"owner": "A"})
    service.publish_telemetry("dev-b", b["credential_id"], b["secret"], 1, clock(), {"owner": "B"})

    # A 的凭据不能访问 B 的影子；影子内容互不串用
    from iot_hub.errors import CredentialInvalidError
    import pytest
    with pytest.raises(CredentialInvalidError):
        service.get_shadow("dev-b", a["credential_id"], a["secret"])

    va = service.get_shadow("dev-a")
    vb = service.get_shadow("dev-b")
    assert va["reported"] == {"owner": "A"} and vb["reported"] == {"owner": "B"}
    assert va["version"] == vb["version"] == 1
    log.info("DECISION cross-device access denied; reported states stay isolated")


def test_concurrent_ota_confirmations_effect_once(service, registered, clock):
    """同一更新被多连接同时确认：只生效一次，版本/计数不重复。"""
    service.register_firmware(model="sensor-v1", version="2.0.0", min_from_version="1.0.0")
    service.create_rollout(
        rollout_id="ro", firmware_version="2.0.0", model="sensor-v1",
        percent=100, confirm_timeout_s=3600,
    )
    service.ota.mark_online("dev-1", True)
    service.assign_ota("dev-1", "ro")

    statuses: list[str] = []
    errors: list[str] = []
    n = 20
    barrier = threading.Barrier(n)
    lock = threading.Lock()

    def worker():
        barrier.wait()
        try:
            st = service.confirm_ota(
                "dev-1", registered["credential_id"], registered["secret"], "2.0.0"
            )
            with lock:
                statuses.append(st["status"])
        except Exception as exc:  # noqa: BLE001
            with lock:
                errors.append(getattr(exc, "code", type(exc).__name__))

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    confirmed = [s for s in statuses if s == "CONFIRMED"]
    log.info("DECISION concurrent confirm: confirmed=%d rejected=%s",
             len(confirmed), errors)
    assert len(confirmed) == 1
    assert all(e == "OTA_STATE_INVALID" for e in errors)
    assert service.metrics()["ota.confirmed"] == 1
    assert service.ota_state("dev-1")["current_fw_version"] == "2.0.0"
