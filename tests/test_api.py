"""HTTP 边界：协议版本校验、错误归一化与端到端鉴权区分。"""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from iot_hub.api import create_app
from iot_hub.ratelimit import BoundedBacklog, RateLimiter
from iot_hub.service import HubService
from iot_hub.storage import HubStore


log = logging.getLogger("tests.api")

H = {"x-iot-protocol": "1.0"}


@pytest.fixture
def client(clock):
    store = HubStore(clock=clock)
    limiter = RateLimiter(
        global_rate=1_000_000, global_burst=1_000_000,
        device_rate=1_000_000, device_burst=1_000_000, clock=clock,
    )
    svc = HubService(store, rate_limiter=limiter, backlog=BoundedBacklog(1_000_000),
                     clock=clock, auto_sweep=False)
    return TestClient(create_app(svc), raise_server_exceptions=False), svc, clock


def _register(client, did="dev-1", model="sensor-v1", fw="1.0.0"):
    c, _, _ = client
    r = c.post("/admin/devices", headers=H,
               json={"device_id": did, "model": model, "initial_fw_version": fw})
    assert r.status_code == 201, r.text
    return r.json()


def test_health_and_root(client):
    c, _, _ = client
    assert c.get("/health").json() == {"status": "ok"}
    assert c.get("/").json()["service"] == "local-iot-device-hub"


def test_missing_or_bad_protocol_version(client):
    c, _, _ = client
    r = c.post("/admin/devices", json={"device_id": "x", "model": "m"})
    log.info("INPUT no protocol header => %s %s", r.status_code, r.json()["error"])
    assert r.status_code == 400 and r.json()["error"] == "PROTOCOL_INCOMPATIBLE"
    r = c.post("/admin/devices", headers={"x-iot-protocol": "9.9"},
               json={"device_id": "x", "model": "m"})
    assert r.json()["error"] == "PROTOCOL_INCOMPATIBLE"
    assert r.json()["details"]["supported"] == ["1.0"]


def test_malformed_json_and_shape_normalized(client):
    c, _, _ = client
    r = c.post("/admin/devices", headers=H, content=b"{not-json")
    log.info("INPUT malformed JSON => %s %s", r.status_code, r.json()["error"])
    assert r.status_code == 422 and r.json()["error"] == "MALFORMED_PAYLOAD"
    # 缺必填字段：pydantic 校验错误也归一化
    r = c.post("/telemetry", headers={**H, "x-iot-device-id": "d"}, json={"seq": "x"})
    assert r.status_code == 422 and r.json()["error"] == "MALFORMED_PAYLOAD"


def test_auth_error_codes_end_to_end(client):
    c, _, clk = client
    reg = _register(client)
    cred, sec = reg["credential_id"], reg["secret"]

    def telemetry(cid, sec_, seq=1):
        return c.post("/telemetry",
                      headers={**H, "x-iot-device-id": "dev-1",
                               "x-iot-credential": cid, "x-iot-secret": sec_},
                      json={"seq": seq, "ts": clk(), "payload": {"a": 1}})

    r = telemetry(cred, "wrong")
    log.info("HTTP wrong secret => %s", r.json()["error"])
    assert r.status_code == 401 and r.json()["error"] == "CREDENTIAL_INVALID"

    # 未注册设备
    r = c.post("/telemetry",
               headers={**H, "x-iot-device-id": "ghost",
                        "x-iot-credential": cred, "x-iot-secret": sec},
               json={"seq": 1, "ts": clk(), "payload": {}})
    assert r.status_code == 404 and r.json()["error"] == "DEVICE_NOT_REGISTERED"

    # 正常上报
    r = telemetry(cred, sec)
    assert r.status_code == 200 and r.json()["code"] == "ACCEPTED"
    # 重复
    r = telemetry(cred, sec)
    assert r.json()["code"] == "TELEMETRY_DUPLICATE" and r.json()["applied"] is False

    # 吊销
    r = c.post(f"/admin/credentials/{cred}/revoke", headers=H)
    assert r.status_code == 200
    r = telemetry(cred, sec)
    assert r.status_code == 401 and r.json()["error"] == "CREDENTIAL_REVOKED"


def test_session_epoch_enforced_over_http(client):
    c, _, clk = client
    reg = _register(client)
    hdr = {**H, "x-iot-device-id": "dev-1",
           "x-iot-credential": reg["credential_id"], "x-iot-secret": reg["secret"]}
    e1 = c.post("/devices/connect", headers=hdr).json()["epoch"]
    e2 = c.post("/devices/connect", headers=hdr).json()["epoch"]
    r = c.post("/telemetry", headers={**hdr, "x-iot-epoch": str(e1)},
               json={"seq": 1, "ts": clk(), "payload": {"x": 1}})
    log.info("HTTP stale epoch => %s %s", r.status_code, r.json()["error"])
    assert r.status_code == 409 and r.json()["error"] == "SESSION_STALE"
    r = c.post("/telemetry", headers={**hdr, "x-iot-epoch": str(e2)},
               json={"seq": 1, "ts": clk(), "payload": {"x": 1}})
    assert r.status_code == 200 and r.json()["applied"]


def test_internal_exception_is_normalized_and_not_leaked(client, monkeypatch):
    c, svc, _ = client

    def boom(*a, **k):
        raise RuntimeError("db password=supersecret path=/etc/secret")

    monkeypatch.setattr(svc, "metrics", boom)
    r = c.get("/admin/metrics", headers=H)
    log.info("INPUT triggers internal error => status=%s body=%r", r.status_code, r.json())
    assert r.status_code == 500 and r.json()["error"] == "INTERNAL_ERROR"
    assert "supersecret" not in r.text and "RuntimeError" not in r.text


def test_full_ota_flow_over_http(client):
    c, _, clk = client
    reg = _register(client)
    hdr = {**H, "x-iot-device-id": "dev-1",
           "x-iot-credential": reg["credential_id"], "x-iot-secret": reg["secret"]}
    # 固件 + rollout
    r = c.post("/admin/firmware", headers=H,
               json={"model": "sensor-v1", "version": "2.0.0", "min_from_version": "1.0.0"})
    assert r.status_code == 201
    r = c.post("/admin/rollouts", headers=H,
               json={"firmware_version": "2.0.0", "model": "sensor-v1",
                     "rollout_id": "ro", "percent": 100, "confirm_timeout_s": 60})
    assert r.status_code == 201
    # 离线分配 -> QUEUED
    r = c.post("/admin/devices/dev-1/ota/assign", headers=H, json={"rollout_id": "ro"})
    assert r.json()["status"] == "QUEUED"
    # 连接拉取 -> DELIVERED
    connect = c.post("/devices/connect", headers=hdr).json()
    r = c.post("/ota/pull", headers={**hdr, "x-iot-epoch": str(connect["epoch"])})
    assert r.json()["status"] == "DELIVERED" and r.json()["version"] == "2.0.0"
    # 确认
    r = c.post("/ota/confirm", headers={**hdr, "x-iot-epoch": str(connect["epoch"])},
               json={"version": "2.0.0"})
    assert r.json()["status"] == "CONFIRMED"
    state = c.get("/admin/devices/dev-1/ota", headers=H).json()
    assert state["current_fw_version"] == "2.0.0"
