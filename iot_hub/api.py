"""FastAPI 边界层：协议版本校验、错误归一化与 HTTP 路由。

边界保证：
- 未知/底层异常统一转成 INTERNAL_ERROR（500），不向设备侧透出堆栈；
- 缺少或不支持的协议版本头 -> PROTOCOL_INCOMPATIBLE；
- 请求体无法解析 -> MALFORMED_PAYLOAD；
- 业务错误一律 ``{"error": <稳定码>, "message": ..., "details": ...}``。
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .errors import (
    ErrorCode,
    HubError,
    MalformedPayloadError,
    ProtocolIncompatibleError,
)
from .models import PROTOCOL_HEADER, SUPPORTED_PROTOCOL_VERSIONS
from .service import HubService

logger = logging.getLogger("iot_hub.api")

_DEVICE_CRED_HEADER = "x-iot-credential"
_DEVICE_SECRET_HEADER = "x-iot-secret"


def create_app(service: HubService | None = None) -> FastAPI:
    svc = service or HubService()
    app = FastAPI(title="Local IoT Device Hub", version="0.1.0")
    app.state.service = svc

    # ---------- 错误归一化 ----------

    @app.exception_handler(HubError)
    async def _hub_error_handler(_: Request, exc: HubError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content=exc.to_dict())

    @app.exception_handler(RequestValidationError)
    async def _validation_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        err = MalformedPayloadError(
            "request body or parameters are malformed",
            details={"issues": _safe_issues(exc.errors())},
        )
        return JSONResponse(status_code=err.status_code, content=err.to_dict())

    @app.exception_handler(Exception)
    async def _unexpected_handler(_: Request, exc: Exception) -> JSONResponse:
        logger.exception("unexpected internal error: %s", exc)
        err = HubError(
            ErrorCode.INTERNAL_ERROR,
            "internal error",
            status_code=500,
        )
        return JSONResponse(status_code=500, content=err.to_dict())

    def _check_protocol(request: Request) -> None:
        version = request.headers.get(PROTOCOL_HEADER)
        if not version:
            raise ProtocolIncompatibleError(
                f"missing required header {PROTOCOL_HEADER}",
                details={"supported": list(SUPPORTED_PROTOCOL_VERSIONS)},
            )
        if version not in SUPPORTED_PROTOCOL_VERSIONS:
            raise ProtocolIncompatibleError(
                details={
                    "received": version,
                    "supported": list(SUPPORTED_PROTOCOL_VERSIONS),
                }
            )

    async def _json_body(request: Request) -> dict[str, Any]:
        try:
            body = await request.json()
        except Exception as exc:  # noqa: BLE001
            raise MalformedPayloadError("request body is not valid JSON") from exc
        if not isinstance(body, dict):
            raise MalformedPayloadError("request body must be a JSON object")
        return body

    def _require(body: dict[str, Any], key: str) -> Any:
        if key not in body or body[key] is None:
            raise MalformedPayloadError(f"missing required field '{key}'")
        return body[key]

    # ---------- 健康检查 ----------

    @app.get("/")
    async def root() -> dict[str, Any]:
        return {
            "service": "local-iot-device-hub",
            "protocol_versions": list(SUPPORTED_PROTOCOL_VERSIONS),
        }

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    # ---------- 管理面 ----------

    @app.post("/admin/devices")
    async def admin_register(request: Request) -> JSONResponse:
        _check_protocol(request)
        body = await _json_body(request)
        result = svc.register_device(
            device_id=str(_require(body, "device_id")),
            model=str(_require(body, "model")),
            initial_fw_version=body.get("initial_fw_version"),
        )
        return JSONResponse(status_code=201, content=result)

    @app.post("/admin/devices/{device_id}/credentials/rotate")
    async def admin_rotate(device_id: str, request: Request) -> dict[str, Any]:
        _check_protocol(request)
        body = await _json_body(request)
        try:
            grace = float(body.get("grace_period_s", 300.0))
        except (TypeError, ValueError) as exc:
            raise MalformedPayloadError("grace_period_s must be a number") from exc
        return svc.rotate_credential(device_id, grace_period_s=grace)

    @app.post("/admin/credentials/{credential_id}/revoke")
    async def admin_revoke(credential_id: str, request: Request) -> dict[str, Any]:
        _check_protocol(request)
        return svc.revoke_credential(credential_id)

    @app.get("/admin/devices/{device_id}/shadow")
    async def admin_shadow_get(device_id: str, request: Request) -> dict[str, Any]:
        _check_protocol(request)
        return svc.get_shadow(device_id)

    @app.put("/admin/devices/{device_id}/shadow/desired")
    async def admin_shadow_desired(device_id: str, request: Request) -> dict[str, Any]:
        _check_protocol(request)
        body = await _json_body(request)
        ev = body.get("expected_version")
        if ev is not None and not isinstance(ev, int):
            raise MalformedPayloadError("expected_version must be an integer")
        changes = body.get("changes", {})
        if not isinstance(changes, dict):
            raise MalformedPayloadError("changes must be a JSON object")
        return svc.update_desired(device_id, changes, expected_version=ev)

    @app.post("/admin/firmware")
    async def admin_firmware(request: Request) -> JSONResponse:
        _check_protocol(request)
        body = await _json_body(request)
        result = svc.register_firmware(
            model=str(_require(body, "model")),
            version=str(_require(body, "version")),
            min_from_version=body.get("min_from_version"),
        )
        return JSONResponse(status_code=201, content=result)

    @app.post("/admin/rollouts")
    async def admin_rollout(request: Request) -> JSONResponse:
        _check_protocol(request)
        body = await _json_body(request)
        try:
            kwargs = {
                "firmware_version": str(_require(body, "firmware_version")),
                "model": str(_require(body, "model")),
                "percent": int(body.get("percent", 100)),
                "confirm_timeout_s": float(body.get("confirm_timeout_s", 3600.0)),
            }
        except (TypeError, ValueError) as exc:
            raise MalformedPayloadError("percent must be int and confirm_timeout_s number") from exc
        if "rollout_id" in body:
            kwargs["rollout_id"] = str(body["rollout_id"])
        if "min_from_version" in body:
            kwargs["min_from_version"] = body["min_from_version"]
        return JSONResponse(status_code=201, content=svc.create_rollout(**kwargs))

    @app.post("/admin/devices/{device_id}/ota/assign")
    async def admin_assign(device_id: str, request: Request) -> dict[str, Any]:
        _check_protocol(request)
        body = await _json_body(request)
        return svc.assign_ota(device_id, body.get("rollout_id"))

    @app.get("/admin/devices/{device_id}/ota")
    async def admin_ota_state(device_id: str, request: Request) -> dict[str, Any]:
        _check_protocol(request)
        return svc.ota_state(device_id)

    @app.get("/admin/metrics")
    async def admin_metrics(request: Request) -> dict[str, Any]:
        _check_protocol(request)
        return svc.metrics()

    # ---------- 设备面 ----------

    def _device_headers(request: Request) -> tuple[str, str, str, int | None]:
        _check_protocol(request)
        device_id = request.headers.get("x-iot-device-id", "")
        credential_id = request.headers.get(_DEVICE_CRED_HEADER, "")
        secret = request.headers.get(_DEVICE_SECRET_HEADER, "")
        epoch_raw = request.headers.get("x-iot-epoch")
        if not device_id:
            raise MalformedPayloadError("missing x-iot-device-id header")
        epoch: int | None = None
        if epoch_raw is not None:
            if not epoch_raw.isdigit():
                raise MalformedPayloadError("x-iot-epoch must be a non-negative integer")
            epoch = int(epoch_raw)
        return device_id, credential_id, secret, epoch

    @app.post("/devices/connect")
    async def device_connect(request: Request) -> dict[str, Any]:
        device_id, cred_id, secret, _epoch = _device_headers(request)
        return svc.connect(device_id, cred_id, secret)

    @app.post("/devices/disconnect")
    async def device_disconnect(request: Request) -> dict[str, Any]:
        _check_protocol(request)
        device_id = request.headers.get("x-iot-device-id", "")
        if not device_id:
            raise MalformedPayloadError("missing x-iot-device-id header")
        return svc.disconnect(device_id)

    @app.post("/telemetry")
    async def publish_telemetry(request: Request) -> dict[str, Any]:
        device_id, cred_id, secret, epoch = _device_headers(request)
        body = await _json_body(request)
        try:
            seq = int(body["seq"])
            ts = float(body["ts"])
        except (KeyError, TypeError, ValueError) as exc:
            raise MalformedPayloadError(
                "seq must be an integer and ts must be a number"
            ) from exc
        return svc.publish_telemetry(
            device_id,
            cred_id,
            secret,
            seq=seq,
            ts=ts,
            payload=body.get("payload", {}),
            idempotency_key=body.get("idempotency_key"),
            epoch=epoch,
        )

    @app.get("/devices/{device_id}/shadow")
    async def device_shadow(device_id: str, request: Request) -> dict[str, Any]:
        cred_id = request.headers.get(_DEVICE_CRED_HEADER)
        secret = request.headers.get(_DEVICE_SECRET_HEADER)
        return svc.get_shadow(device_id, cred_id, secret)

    @app.post("/ota/pull")
    async def ota_pull(request: Request) -> dict[str, Any]:
        device_id, cred_id, secret, epoch = _device_headers(request)
        return svc.pull_ota(device_id, cred_id, secret, epoch=epoch)

    @app.post("/ota/confirm")
    async def ota_confirm(request: Request) -> dict[str, Any]:
        device_id, cred_id, secret, epoch = _device_headers(request)
        body = await _json_body(request)
        return svc.confirm_ota(device_id, cred_id, secret, str(body["version"]), epoch=epoch)

    @app.post("/ota/failure")
    async def ota_failure(request: Request) -> dict[str, Any]:
        device_id, cred_id, secret, epoch = _device_headers(request)
        body = await _json_body(request)
        return svc.report_ota_failure(
            device_id, cred_id, secret, str(body.get("reason", "unknown")), epoch=epoch
        )

    return app


def _safe_issues(issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """裁掉 pydantic 错误中的上下文对象等不可外露内容。"""
    out = []
    for item in issues:
        out.append(
            {
                "loc": [str(x) for x in item.get("loc", [])],
                "type": item.get("type", ""),
                "msg": item.get("msg", ""),
            }
        )
    return out
