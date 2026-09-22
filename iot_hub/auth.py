"""设备注册、凭据签发/轮换/吊销与接入鉴权。

凭据轮换的确定语义：
- 轮换签发新一代凭据后，旧一代凭据进入"宽限期" ``grace_until``：
  宽限期内仍可接入，鉴权结论 ``AuthContext.grace=True``；
  宽限期外再使用旧凭据，结论固定为 ``CREDENTIAL_GRACE_EXPIRED``。
- 吊销立即生效，结论固定为 ``CREDENTIAL_REVOKED``。
- 未注册设备 -> ``DEVICE_NOT_REGISTERED``；密钥错误 -> ``CREDENTIAL_INVALID``；
  scope 不足 -> ``SCOPE_DENIED``。三类拒绝原因互不混淆。
"""

from __future__ import annotations

import hashlib
import hmac
import secrets

from .errors import (
    ConflictError,
    CredentialGraceExpiredError,
    CredentialInvalidError,
    CredentialRevokedError,
    DeviceNotRegisteredError,
    NotFoundError,
    ScopeDeniedError,
    SessionStaleError,
)
from .models import AuthContext, Credential, Device, Scope
from .storage import HubStore

_DEFAULT_SCOPES = frozenset(
    {Scope.TELEMETRY_PUBLISH.value, Scope.SHADOW_READ.value, Scope.OTA_READ.value}
)


def hash_secret(device_id: str, secret: str) -> str:
    return hashlib.sha256(f"{device_id}:{secret}".encode("utf-8")).hexdigest()


class AuthManager:
    def __init__(self, store: HubStore) -> None:
        self.store = store

    # ---------- 注册 / 注销 ----------

    def register_device(
        self,
        device_id: str,
        model: str,
        *,
        initial_fw_version: str | None = None,
    ) -> tuple[Device, str, Credential]:
        """注册设备并签发第一代凭据。返回 (设备, 明文密钥, 凭据)。"""
        with self.store.lock_for(device_id):
            if device_id in self.store.devices:
                raise ConflictError(f"device '{device_id}' is already registered")
            now = self.store.clock()
            device = Device(
                device_id=device_id,
                model=model,
                created_at=now,
                current_fw_version=initial_fw_version,
                last_known_good_version=initial_fw_version,
            )
            self.store.devices[device_id] = device
            secret, cred = self._issue_credential(device_id, generation=1)
            return device, secret, cred

    def deregister_device(self, device_id: str) -> None:
        with self.store.lock_for(device_id):
            if device_id not in self.store.devices:
                raise NotFoundError(f"device '{device_id}' is not registered")
            for cid in self.store.list_credential_ids(device_id):
                self.store.credentials.pop(cid, None)
            self.store.devices.pop(device_id, None)
            self.store.shadows.pop(device_id, None)
            self.store.seq_windows.pop(device_id, None)
            self.store.idem_stores.pop(device_id, None)
            self.store.telemetry_buffers.pop(device_id, None)
            self.store.ota_states.pop(device_id, None)

    def get_device(self, device_id: str) -> Device:
        dev = self.store.devices.get(device_id)
        if dev is None:
            raise DeviceNotRegisteredError()
        return dev

    # ---------- 凭据签发 / 轮换 / 吊销 ----------

    def _issue_credential(
        self,
        device_id: str,
        *,
        generation: int,
        scopes: frozenset[str] | None = None,
    ) -> tuple[str, Credential]:
        plain = secrets.token_hex(16)
        cred = Credential(
            credential_id=f"{device_id}-g{generation}-{secrets.token_hex(4)}",
            device_id=device_id,
            secret_hash=hash_secret(device_id, plain),
            scopes=scopes if scopes is not None else _DEFAULT_SCOPES,
            generation=generation,
            issued_at=self.store.clock(),
        )
        self.store.credentials[cred.credential_id] = cred
        self.store.index_credential(cred)
        return plain, cred

    def rotate_credential(
        self,
        device_id: str,
        *,
        grace_period_s: float = 300.0,
        scopes: frozenset[str] | None = None,
    ) -> tuple[str, Credential]:
        """签发新一代凭据；旧凭据进入宽限期（grace_period_s<=0 表示立即吊销）。"""
        with self.store.lock_for(device_id):
            if device_id not in self.store.devices:
                raise DeviceNotRegisteredError()
            existing = [
                self.store.credentials[cid]
                for cid in self.store.list_credential_ids(device_id)
            ]
            max_gen = max((c.generation for c in existing), default=0)
            now = self.store.clock()
            for c in existing:
                if not c.revoked and c.grace_until is None:
                    if grace_period_s > 0:
                        c.grace_until = now + grace_period_s
                    else:
                        c.revoked = True
                        c.revoked_at = now
            return self._issue_credential(
                device_id, generation=max_gen + 1, scopes=scopes
            )

    def revoke_credential(self, credential_id: str) -> None:
        cred = self.store.credentials.get(credential_id)
        if cred is None:
            raise NotFoundError(f"credential '{credential_id}' does not exist")
        with self.store.lock_for(cred.device_id):
            cred = self.store.credentials.get(credential_id)
            if cred is None or cred.revoked:
                return
            cred.revoked = True
            cred.revoked_at = self.store.clock()

    # ---------- 会话 ----------

    def begin_session(self, device_id: str) -> int:
        """建立新连接会话：纪元 +1，并重置本会话的序号/缓冲状态（幂等表保留）。"""
        with self.store.lock_for(device_id):
            if device_id not in self.store.devices:
                raise DeviceNotRegisteredError()
            epoch = self.store.epochs.get(device_id, 0) + 1
            self.store.epochs[device_id] = epoch
            # 序号窗口与遥测缓冲属于"会话序号空间"，新会话重置；
            # 影子的 reported_seq 是本会话应用水位，一并重置；
            # 幂等表、影子版本与 reported 内容是跨会话持久语义，保留。
            self.store.seq_windows.pop(device_id, None)
            self.store.telemetry_buffers.pop(device_id, None)
            shadow = self.store.shadows.get(device_id)
            if shadow is not None:
                shadow.reported_seq = -1
            return epoch

    def assert_session_current(self, device_id: str, epoch: int) -> None:
        if self.store.epochs.get(device_id, 0) != epoch:
            self.store.metrics.incr("auth.session_stale")
            raise SessionStaleError(
                details={
                    "session_epoch": epoch,
                    "current_epoch": self.store.epochs.get(device_id, 0),
                }
            )

    # ---------- 鉴权 ----------

    def authenticate(
        self,
        device_id: str,
        credential_id: str,
        secret: str,
        required_scope: str,
        *,
        epoch: int | None = None,
    ) -> AuthContext:
        """校验设备接入，失败以可区分的 HubError 子类表达。

        :param epoch: 调用方持有的会话纪元；若设备已建立更新会话则抛 SESSION_STALE。
        """
        if device_id not in self.store.devices:
            self.store.metrics.incr("auth.denied.unregistered")
            raise DeviceNotRegisteredError()
        with self.store.lock_for(device_id):
            device = self.store.devices.get(device_id)
            if device is None:
                self.store.metrics.incr("auth.denied.unregistered")
                raise DeviceNotRegisteredError()

            cred = self.store.credentials.get(credential_id)
            if cred is None or cred.device_id != device_id:
                self.store.metrics.incr("auth.denied.invalid")
                raise CredentialInvalidError()

            now = self.store.clock()
            if cred.revoked:
                self.store.metrics.incr("auth.denied.revoked")
                raise CredentialRevokedError(
                    details={"credential_id": credential_id, "revoked_at": cred.revoked_at}
                )

            grace = cred.grace_until is not None
            if grace and now > cred.grace_until:  # type: ignore[operator]
                self.store.metrics.incr("auth.denied.grace_expired")
                raise CredentialGraceExpiredError(
                    details={
                        "credential_id": credential_id,
                        "grace_until": cred.grace_until,
                    }
                )

            if not hmac.compare_digest(cred.secret_hash, hash_secret(device_id, secret)):
                self.store.metrics.incr("auth.denied.invalid")
                raise CredentialInvalidError(
                    details={"credential_id": credential_id}
                )

            if required_scope not in cred.scopes:
                self.store.metrics.incr("auth.denied.scope")
                raise ScopeDeniedError(
                    required_scope,
                    details={"credential_id": credential_id, "scopes": sorted(cred.scopes)},
                )

            if epoch is not None:
                self.assert_session_current(device_id, epoch)
            else:
                epoch = self.store.epochs.get(device_id, 0)

            device.last_seen_at = now
            self.store.metrics.incr("auth.ok")
            if grace:
                self.store.metrics.incr("auth.ok.grace")
            return AuthContext(
                device_id=device_id,
                credential_id=credential_id,
                generation=cred.generation,
                scopes=frozenset(cred.scopes),
                grace=grace,
                epoch=epoch,
            )
