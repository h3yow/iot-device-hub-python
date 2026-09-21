"""设备注册、凭据签发/吊销/轮换与鉴权。

轮换宽限语义（确定且稳定）：
- rotate_credential 签发新凭据后，所有未吊销旧凭据进入宽限期，
  在 rotated_at + rotation_grace 之前仍可用于鉴权；
- 宽限结束后旧凭据视为 CREDENTIAL_EXPIRED；
- 宽限期内凭据被显式吊销则立即拒绝（CREDENTIAL_REVOKED），吊销优先于宽限。
"""
from __future__ import annotations

import secrets
import threading
import time

from .errors import ErrorCode, HubError
from .models import Credential, Device, Scope


class DeviceRegistry:
    def __init__(self, credential_ttl: float = 3600.0, rotation_grace: float = 300.0) -> None:
        self._lock = threading.RLock()
        self._devices: dict[str, Device] = {}
        self._credential_ttl = credential_ttl
        self._rotation_grace = rotation_grace

    def register(self, device_id: str, model: str, firmware_version: str, now: float) -> Device:
        if not device_id or not model or not firmware_version:
            raise HubError(ErrorCode.MALFORMED_PAYLOAD, "device_id/model/firmware_version 不能为空")
        with self._lock:
            if device_id in self._devices:
                raise HubError(ErrorCode.CONFLICT, "设备已注册")
            device = Device(device_id=device_id, model=model,
                            firmware_version=firmware_version, registered_at=now)
            self._devices[device_id] = device
            return device

    def issue_credential(self, device_id: str, scopes: set[Scope], now: float) -> Credential:
        with self._lock:
            device = self._get(device_id)
            cred = Credential(
                device_id=device_id,
                token=secrets.token_urlsafe(24),
                scopes=frozenset(scopes),
                issued_at=now,
                expires_at=now + self._credential_ttl,
            )
            device.credentials.append(cred)
            return cred

    def rotate_credential(self, device_id: str, scopes: set[Scope], now: float) -> Credential:
        """签发新凭据；未吊销旧凭据进入宽限期（宽限语义见模块文档）。"""
        with self._lock:
            device = self._get(device_id)
            for old in device.credentials:
                if not old.revoked:
                    old.grace_until = now + self._rotation_grace
            return self.issue_credential(device_id, scopes, now)

    def revoke_credential(self, device_id: str, token: str) -> None:
        with self._lock:
            device = self._get(device_id)
            for cred in device.credentials:
                if cred.token == token:
                    cred.revoked = True
                    return
            raise HubError(ErrorCode.CREDENTIAL_INVALID, "凭据不存在")

    def authenticate(self, token: str, scope: Scope, now: float) -> Device:
        """按 token + 所需 scope 鉴权；失败抛出可区分的 HubError。"""
        if not token:
            raise HubError(ErrorCode.CREDENTIAL_INVALID, "缺少凭据")
        with self._lock:
            for device in self._devices.values():
                for cred in device.credentials:
                    if secrets.compare_digest(cred.token, token):
                        return self._check(device, cred, scope, now)
            raise HubError(ErrorCode.CREDENTIAL_INVALID, "凭据不存在")

    def _check(self, device: Device, cred: Credential, scope: Scope, now: float) -> Device:
        if cred.revoked:
            raise HubError(ErrorCode.CREDENTIAL_REVOKED, "凭据已吊销")
        # 轮换后的旧凭据有效窗口收窄为 min(expires_at, grace_until)，语义确定
        valid_until = min(cred.expires_at, cred.grace_until) if cred.grace_until > 0.0 \
            else cred.expires_at
        if now >= valid_until:
            raise HubError(ErrorCode.CREDENTIAL_EXPIRED, "凭据已过期")
        if scope not in cred.scopes:
            raise HubError(ErrorCode.SCOPE_DENIED, f"缺少授权范围 {scope.value}")
        return device

    def get_device(self, device_id: str) -> Device:
        with self._lock:
            return self._get(device_id)

    def set_online(self, device_id: str, online: bool, now: float) -> None:
        with self._lock:
            device = self._get(device_id)
            device.online = online
            device.last_seen_at = now

    def _get(self, device_id: str) -> Device:
        device = self._devices.get(device_id)
        if device is None:
            raise HubError(ErrorCode.UNKNOWN_DEVICE, "设备未注册")
        return device
