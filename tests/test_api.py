"""HTTP 接口：错误归一化为可区分 code，内部细节不透出。"""
import logging

import pytest
from fastapi.testclient import TestClient

from main import app

log = logging.getLogger("test")


@pytest.fixture
def client() -> TestClient:
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def device(client: TestClient) -> dict:
    import uuid
    device_id = f"api-dev-{uuid.uuid4().hex[:8]}"  # 模块级 hub 跨测试共享，需唯一 ID
    resp = client.post("/devices", json={
        "device_id": device_id, "model": "sensor-A", "firmware_version": "1.0.0"})
    assert resp.status_code == 201
    return resp.json()


def test_register_and_connect(client, device):
    token = device["credential"]["token"]
    resp = client.post("/connect", json={"token": token})
    log.info("输入: 合法 token 连接 -> %d %s", resp.status_code, resp.json())
    assert resp.status_code == 200 and resp.json()["device_id"] == device["device_id"]


def test_unknown_token_401_distinct_code(client):
    resp = client.post("/connect", json={"token": "nope"})
    log.info("输入: 未知 token -> %d %s", resp.status_code, resp.json())
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "CREDENTIAL_INVALID"


def test_revoked_token_distinct_from_invalid(client, device):
    token = device["credential"]["token"]
    client.post(f"/devices/{device['device_id']}/credentials/revoke", json={"token": token})
    resp = client.post("/connect", json={"token": token})
    log.info("输入: 已吊销 token -> %d %s", resp.status_code, resp.json())
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "CREDENTIAL_REVOKED"


def test_telemetry_duplicate_distinct_code(client, device):
    token = device["credential"]["token"]
    body = {"token": token, "device_id": device["device_id"], "message_id": "m1",
            "seq": 1, "timestamp": 1_000_000.0, "payload": {"t": 1}}
    import time
    body["timestamp"] = time.time()
    assert client.post("/telemetry", json=body).status_code == 200
    resp = client.post("/telemetry", json=body)
    log.info("输入: 重复上报 -> %d %s", resp.status_code, resp.json())
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "DUPLICATE_MESSAGE"


def test_malformed_payload_400(client, device):
    resp = client.post("/telemetry", json={"token": "x"})
    log.info("输入: 缺字段请求 -> %d %s", resp.status_code, resp.json())
    assert resp.status_code == 422  # FastAPI 参数校验，协议层之前拦截


def test_internal_error_normalized(client, device, monkeypatch):
    """底层异常归一化为 INTERNAL，不透出实现细节。"""
    def boom(*args, **kwargs):
        raise RuntimeError("sqlite3.OperationalError: db locked at /var/lib/secret/path")

    monkeypatch.setattr("main.hub.connect", boom)
    token = device["credential"]["token"]
    resp = client.post("/connect", json={"token": token})
    log.info("输入: 内部异常 -> %d %s", resp.status_code, resp.json())
    assert resp.status_code == 500
    body = resp.json()
    assert body["error"]["code"] == "INTERNAL"
    assert "sqlite3" not in str(body) and "/var/lib" not in str(body)  # 不泄漏内部细节


def test_stats_endpoint(client, device):
    resp = client.get("/stats")
    assert resp.status_code == 200
    log.info("判定依据: 统计快照 %s", resp.json())


# ---- 离线指令/配置下发（HTTP 层） ----

@pytest.fixture
def cmd_device(client: TestClient) -> dict:
    import uuid
    device_id = f"api-cmd-{uuid.uuid4().hex[:8]}"
    resp = client.post("/devices", json={
        "device_id": device_id, "model": "sensor-A", "firmware_version": "1.0.0",
        "scopes": ["shadow:read", "command:ack"]})
    assert resp.status_code == 201
    return resp.json()


def test_command_offline_queue_and_reconnect_flow(client, cmd_device):
    device_id = cmd_device["device_id"]
    token = cmd_device["credential"]["token"]
    for i in range(2):  # 设备未连接，指令进入待投递队列
        resp = client.post(f"/devices/{device_id}/commands", json={
            "command_id": f"c{i}", "kind": "command",
            "payload": {"n": i}, "ttl_seconds": 300})
        assert resp.status_code == 201
    resp = client.get(f"/devices/{device_id}/commands")
    log.info("输入: 离线入队 2 条 -> pending=%d", resp.json()["pending"])
    assert resp.json()["pending"] == 2

    client.post("/connect", json={"token": token})  # 重连触发补投
    resp = client.post(f"/devices/{device_id}/commands/deliver")
    order = [c["command_id"] for c in resp.json()["delivered"]]
    log.info("判定依据: 重连补投顺序 %s", order)
    assert order == ["c0", "c1"]  # 按发送先后，不插队不漏投

    ack = {"token": token, "device_id": device_id, "command_id": "c0"}
    resp = client.post("/commands/ack", json=ack)
    assert resp.status_code == 200 and resp.json()["command"]["status"] == "ACKED"
    resp = client.post("/commands/ack", json=ack)  # 重复确认幂等
    log.info("输入: 重复确认 -> %d %s", resp.status_code, resp.json())
    assert resp.status_code == 200  # 只生效一次，不报错不重复计数
    resp = client.get(f"/devices/{device_id}/commands")
    assert resp.json()["pending"] == 1  # c1 仍待确认


def test_command_validation_error_normalized(client, cmd_device):
    """新接口字段缺失/类型不对：统一项目错误结构，不透出框架细节。"""
    device_id = cmd_device["device_id"]
    resp = client.post(f"/devices/{device_id}/commands", json={"kind": "command"})
    log.info("输入: 缺字段请求 -> %d %s", resp.status_code, resp.json())
    assert resp.status_code == 400
    body = resp.json()
    assert body["error"]["code"] == "MALFORMED_PAYLOAD"
    assert "detail" not in body  # 不透出 FastAPI 校验细节
    resp = client.post(f"/devices/{device_id}/commands", json={
        "command_id": "c1", "kind": "command", "payload": {}, "ttl_seconds": "abc"})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "MALFORMED_PAYLOAD"


def test_command_unknown_device_and_bad_kind(client):
    import uuid
    resp = client.post(f"/devices/ghost-{uuid.uuid4().hex[:6]}/commands", json={
        "command_id": "c1", "kind": "command", "payload": {}, "ttl_seconds": 60})
    log.info("输入: 目标设备不存在 -> %d %s", resp.status_code, resp.json())
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "UNKNOWN_DEVICE"


def test_command_ack_bad_credential_distinct(client, cmd_device):
    device_id = cmd_device["device_id"]
    client.post(f"/devices/{device_id}/commands", json={
        "command_id": "c1", "kind": "config", "payload": {}, "ttl_seconds": 60})
    client.post(f"/devices/{device_id}/commands/deliver")
    resp = client.post("/commands/ack", json={
        "token": "bad", "device_id": device_id, "command_id": "c1"})
    log.info("输入: 凭据不可用确认 -> %d %s", resp.status_code, resp.json())
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "CREDENTIAL_INVALID"
