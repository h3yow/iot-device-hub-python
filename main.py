"""IoT 设备接入平台：本地可运行的 HTTP 接口层。

仅做参数解析与错误映射，业务逻辑全部在 iot_hub 包内；
底层异常统一经 HubError 归一化后对外暴露 code，不泄漏内部细节。
"""
from __future__ import annotations

import time
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from iot_hub.errors import ErrorCode, HubError
from iot_hub.models import Scope, TelemetryMessage
from iot_hub.service import DeviceHubService

app = FastAPI(title="IoT Device Hub")
hub = DeviceHubService()

_STATUS = {
    ErrorCode.UNKNOWN_DEVICE: 404,
    ErrorCode.CREDENTIAL_INVALID: 401,
    ErrorCode.CREDENTIAL_REVOKED: 401,
    ErrorCode.CREDENTIAL_EXPIRED: 401,
    ErrorCode.SCOPE_DENIED: 403,
    ErrorCode.MALFORMED_PAYLOAD: 400,
    ErrorCode.PROTOCOL_VERSION_UNSUPPORTED: 400,
    ErrorCode.DUPLICATE_MESSAGE: 409,
    ErrorCode.STALE_SEQUENCE: 409,
    ErrorCode.EXPIRED_DATA: 409,
    ErrorCode.VERSION_CONFLICT: 409,
    ErrorCode.INCOMPATIBLE_FIRMWARE: 422,
    ErrorCode.OTA_NOT_PENDING: 409,
    ErrorCode.CAMPAIGN_NOT_FOUND: 404,
    ErrorCode.RATE_LIMITED: 429,
    ErrorCode.BACKLOG_FULL: 429,
    ErrorCode.COMMAND_QUEUE_FULL: 429,
    ErrorCode.COMMAND_EXPIRED: 409,
    ErrorCode.COMMAND_ALREADY_ACKED: 409,
    ErrorCode.COMMAND_NOT_PENDING: 409,
    ErrorCode.NOT_FOUND: 404,
    ErrorCode.CONFLICT: 409,
    ErrorCode.INTERNAL: 500,
}


@app.exception_handler(HubError)
async def hub_error_handler(_: Request, exc: HubError) -> JSONResponse:
    return JSONResponse(
        status_code=_STATUS.get(exc.code, 500),
        content={"error": {"code": exc.code.value, "message": exc.message}},
    )


@app.exception_handler(Exception)
async def unknown_error_handler(_: Request, exc: Exception) -> JSONResponse:
    # 底层异常不直接透出，归一化为 INTERNAL
    return JSONResponse(
        status_code=500,
        content={"error": {"code": ErrorCode.INTERNAL.value, "message": "内部错误"}},
    )


# ---- 请求模型 ----

class RegisterRequest(BaseModel):
    device_id: str
    model: str
    firmware_version: str
    scopes: list[str] = ["telemetry:write", "shadow:read", "shadow:write",
                         "ota:ack", "command:ack"]


class CredentialRequest(BaseModel):
    scopes: list[str] = ["telemetry:write", "shadow:read", "shadow:write",
                         "ota:ack", "command:ack"]


class RevokeRequest(BaseModel):
    token: str


class ConnectRequest(BaseModel):
    token: str


class TelemetryRequest(BaseModel):
    token: str
    device_id: str
    message_id: str
    seq: int
    timestamp: float
    payload: dict[str, Any]
    protocol_version: int = 1


class ShadowPatchRequest(BaseModel):
    token: str | None = None
    patch: dict[str, Any]
    base_version: int | None = None


class CampaignRequest(BaseModel):
    campaign_id: str
    model: str
    target_version: str
    batch_size: int = 10
    ack_timeout: float = 60.0


class OtaAckRequest(BaseModel):
    token: str
    device_id: str
    job_id: str
    success: bool


def _parse_scopes(raw: list[str]) -> set[Scope]:
    try:
        return {Scope(s) for s in raw}
    except ValueError:
        raise HubError(ErrorCode.MALFORMED_PAYLOAD, "未知授权范围")


async def _read_json_object(request: Request) -> dict:
    """手工解析新接口请求体：字段缺失/类型不对/取值非法一律归一化为 MALFORMED_PAYLOAD，
    不把框架校验细节或原始内容透出去。"""
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001
        raise HubError(ErrorCode.MALFORMED_PAYLOAD, "请求体必须是 JSON 对象")
    if not isinstance(body, dict):
        raise HubError(ErrorCode.MALFORMED_PAYLOAD, "请求体必须是 JSON 对象")
    return body


def _require_str(body: dict, field: str) -> str:
    value = body.get(field)
    if not isinstance(value, str) or not value:
        raise HubError(ErrorCode.MALFORMED_PAYLOAD, f"字段 {field} 缺失或非法")
    return value


def _require_payload(body: dict) -> dict:
    value = body.get("payload")
    if not isinstance(value, dict) or not value:
        raise HubError(ErrorCode.MALFORMED_PAYLOAD, "字段 payload 必须为非空对象")
    return value


def _optional_ttl(body: dict) -> float | None:
    ttl = body.get("ttl")
    if ttl is None:
        return None
    if isinstance(ttl, bool) or not isinstance(ttl, (int, float)) or ttl <= 0:
        raise HubError(ErrorCode.MALFORMED_PAYLOAD, "字段 ttl 必须为正数")
    return float(ttl)


def _cred_json(cred) -> dict:
    return {"device_id": cred.device_id, "token": cred.token,
            "scopes": sorted(s.value for s in cred.scopes),
            "expires_at": cred.expires_at}


def _shadow_json(state) -> dict:
    return {"device_id": state.device_id, "reported": state.reported,
            "desired": state.desired, "version": state.version,
            "updated_at": state.updated_at, "fresh": state.fresh}


def _job_json(job) -> dict:
    return {"job_id": job.job_id, "campaign_id": job.campaign_id,
            "device_id": job.device_id, "target_version": job.target_version,
            "status": job.status.value, "ack_deadline": job.ack_deadline}


def _command_json(cmd) -> dict:
    return {"command_id": cmd.command_id, "device_id": cmd.device_id,
            "kind": cmd.kind, "payload": cmd.payload, "seq": cmd.seq,
            "status": cmd.status.value, "created_at": cmd.created_at,
            "expires_at": cmd.expires_at, "delivered_at": cmd.delivered_at,
            "delivery_count": cmd.delivery_count, "acked_at": cmd.acked_at,
            "terminal_reason": cmd.terminal_reason}


# ---- 设备与凭据 ----

@app.post("/devices", status_code=201)
def register(req: RegisterRequest) -> dict:
    hub.registry.register(req.device_id, req.model, req.firmware_version, time.time())
    cred = hub.registry.issue_credential(req.device_id, _parse_scopes(req.scopes), time.time())
    return {"device_id": req.device_id, "credential": _cred_json(cred)}


@app.post("/devices/{device_id}/credentials", status_code=201)
def rotate_credential(device_id: str, req: CredentialRequest) -> dict:
    cred = hub.registry.rotate_credential(device_id, _parse_scopes(req.scopes), time.time())
    return {"credential": _cred_json(cred)}


@app.post("/devices/{device_id}/credentials/revoke")
def revoke_credential(device_id: str, req: RevokeRequest) -> dict:
    hub.registry.revoke_credential(device_id, req.token)
    return {"revoked": True}


@app.post("/connect")
def connect(req: ConnectRequest) -> dict:
    return {"device_id": hub.connect(req.token)}


@app.post("/devices/{device_id}/disconnect")
def disconnect(device_id: str) -> dict:
    hub.disconnect(device_id)
    return {"disconnected": True}


# ---- 遥测 ----

@app.post("/telemetry")
def ingest_telemetry(req: TelemetryRequest) -> dict:
    msg = TelemetryMessage(
        device_id=req.device_id, message_id=req.message_id, seq=req.seq,
        timestamp=req.timestamp, payload=req.payload,
        protocol_version=req.protocol_version,
    )
    return {"result": hub.ingest_telemetry(req.token, msg)}


@app.get("/devices/{device_id}/telemetry/latest")
def latest_telemetry(device_id: str) -> dict:
    latest = hub.telemetry.latest(device_id)
    if latest is None:
        raise HubError(ErrorCode.NOT_FOUND, "暂无遥测数据")
    return latest


# ---- 影子 ----

@app.get("/devices/{device_id}/shadow")
def get_shadow(device_id: str) -> dict:
    return _shadow_json(hub.get_shadow(device_id))


@app.put("/devices/{device_id}/shadow/reported")
def update_reported(device_id: str, req: ShadowPatchRequest) -> dict:
    if req.token is None:
        raise HubError(ErrorCode.CREDENTIAL_INVALID, "缺少凭据")
    return _shadow_json(hub.update_reported(req.token, device_id, req.patch, req.base_version))


@app.put("/devices/{device_id}/shadow/desired")
def update_desired(device_id: str, req: ShadowPatchRequest) -> dict:
    return _shadow_json(hub.update_desired(device_id, req.patch, req.base_version))


# ---- OTA ----

@app.post("/ota/campaigns", status_code=201)
def create_campaign(req: CampaignRequest) -> dict:
    campaign = hub.ota.create_campaign(
        req.campaign_id, req.model, req.target_version,
        req.batch_size, req.ack_timeout, time.time())
    return {"campaign_id": campaign.campaign_id}


@app.post("/ota/campaigns/{campaign_id}/dispatch/{device_id}")
def dispatch(campaign_id: str, device_id: str) -> dict:
    return {"job": _job_json(hub.dispatch_ota(campaign_id, device_id))}


@app.post("/ota/campaigns/{campaign_id}/promote")
def promote(campaign_id: str) -> dict:
    return {"promoted": [_job_json(j) for j in hub.promote_ota(campaign_id)]}


@app.post("/ota/ack")
def ota_ack(req: OtaAckRequest) -> dict:
    return {"job": _job_json(hub.ack_ota(req.token, req.device_id, req.job_id, req.success))}


@app.post("/ota/sweep")
def sweep() -> dict:
    return {"rolled_back": [_job_json(j) for j in hub.sweep_ota_timeouts()]}


@app.get("/ota/campaigns/{campaign_id}")
def get_campaign(campaign_id: str) -> dict:
    campaign = hub.ota.get_campaign(campaign_id)
    return {"campaign_id": campaign.campaign_id, "model": campaign.model,
            "target_version": campaign.target_version,
            "jobs": [_job_json(j) for j in campaign.jobs.values()]}


# ---- 离线指令/配置下发 ----

@app.post("/devices/{device_id}/commands", status_code=201)
async def send_command(device_id: str, request: Request) -> dict:
    """业务侧下发配置/指令：离线进队列，重连按序补投；容量/有效期受限。"""
    body = await _read_json_object(request)
    kind = _require_str(body, "kind")
    idem = _require_str(body, "idempotency_key")
    payload = _require_payload(body)
    ttl = _optional_ttl(body)
    command = hub.send_command(device_id, kind, payload, idem, ttl=ttl)
    return {"command": _command_json(command)}


@app.post("/commands/ack")
async def ack_command(request: Request) -> dict:
    """设备确认指令；重复确认只给 COMMAND_ALREADY_ACKED，不重复生效。"""
    body = await _read_json_object(request)
    token = _require_str(body, "token")
    device_id = _require_str(body, "device_id")
    command_id = _require_str(body, "command_id")
    success = body.get("success")
    if not isinstance(success, bool):
        raise HubError(ErrorCode.MALFORMED_PAYLOAD, "字段 success 必须为布尔值")
    command = hub.ack_command(token, device_id, command_id, success)
    return {"command": _command_json(command)}


@app.get("/devices/{device_id}/commands")
def list_commands(device_id: str) -> dict:
    """查看设备待投递（QUEUED）与已投递待确认（PENDING）指令，按发送先后返回。"""
    hub.registry.get_device(device_id)  # 目标设备不存在 -> UNKNOWN_DEVICE
    queued = [_command_json(c) for c in hub.commands.queued_for(device_id)]
    pending = [_command_json(c) for c in hub.commands.pending_for(device_id)]
    return {"queued": queued, "pending": pending,
            "active": len(queued) + len(pending)}


@app.post("/commands/sweep")
def sweep_commands() -> dict:
    """清扫所有已过有效期仍未走完的指令。"""
    expired = hub.sweep_expired_commands()
    return {"expired": [_command_json(c) for c in expired]}


# ---- 统计 ----

@app.get("/stats")
def stats() -> dict:
    return {"stats": hub.stats.snapshot(), "backlog": len(hub.backlog),
            "commands_active": hub.commands.count_active()}
