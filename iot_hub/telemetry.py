"""遥测接入：幂等标识 + 单调序号 + 新鲜度判定。

固定判定顺序（同一把设备锁内完成，结论确定、不产生半更新）：
1. 载荷类型校验（结构畸形在 API 边界已归一化为 MALFORMED_PAYLOAD）；
2. 时间戳新鲜窗口：过旧或过于超前 -> TELEMETRY_EXPIRED；
3. 幂等键：同键不同指纹 -> IDEMPOTENCY_CONFLICT；同键同指纹 -> 回放首次结论；
4. 序号窗口：已生效/已填充 -> TELEMETRY_DUPLICATE(applied=False)；
   旧序号迟到 -> TELEMETRY_OUT_OF_ORDER；
5. 生效：登记序号、合并影子 reported、影子版本 +1（与序号同一原子段）。

断线重连续传：已生效水位之上先到的乱序序号进入空洞缓冲（载荷不应用到影子），
空洞按序补齐后再按 seq 顺序合并，较新值最终生效、旧值不覆盖新值；任意序号
最多应用一次。首条上报即建立会话水位（设备需从约定起始序号发起，断线重连后
重发未确认消息，低于水位的旧序号结论为重复）。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .errors import (
    IdempotencyConflictError,
    MalformedPayloadError,
    TelemetryExpiredError,
    TelemetryOutOfOrderError,
)
from .models import AuthContext, Freshness, TelemetryResult
from .storage import HubStore


def fingerprint(seq: int, ts: float, payload: dict[str, Any]) -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(f"{seq}|{ts}|{body}".encode("utf-8")).hexdigest()


class TelemetryManager:
    def __init__(
        self,
        store: HubStore,
        *,
        max_age_s: float = 300.0,
        future_skew_s: float = 60.0,
        fresh_ttl_s: float = 60.0,
    ) -> None:
        self.store = store
        self.max_age_s = max_age_s
        self.future_skew_s = future_skew_s
        self.fresh_ttl_s = fresh_ttl_s

    def publish(
        self,
        ctx: AuthContext,
        seq: int,
        ts: float,
        payload: Any,
        idempotency_key: str | None = None,
    ) -> TelemetryResult:
        device_id = ctx.device_id
        now = self.store.clock()
        received_at = now

        if not isinstance(payload, dict):
            self.store.metrics.incr("telemetry.rejected.malformed")
            raise MalformedPayloadError(
                "telemetry payload must be a JSON object",
                details={"seq": seq},
            )
        if not isinstance(seq, int) or isinstance(seq, bool) or seq < 0:
            self.store.metrics.incr("telemetry.rejected.malformed")
            raise MalformedPayloadError("seq must be a non-negative integer")
        if not isinstance(ts, (int, float)) or isinstance(ts, bool):
            self.store.metrics.incr("telemetry.rejected.malformed")
            raise MalformedPayloadError("ts must be a number (unix seconds)")

        age = received_at - ts
        if age > self.max_age_s:
            self.store.metrics.incr("telemetry.expired")
            raise TelemetryExpiredError(
                details={"age_s": round(age, 3), "max_age_s": self.max_age_s}
            )
        if ts - received_at > self.future_skew_s:
            self.store.metrics.incr("telemetry.expired")
            raise TelemetryExpiredError(
                message="telemetry timestamp is too far in the future",
                details={
                    "future_skew_s": round(ts - received_at, 3),
                    "allowed_s": self.future_skew_s,
                },
            )

        fp = fingerprint(seq, float(ts), payload)
        idem = self.store.idem_for(device_id)

        with self.store.lock_for(device_id):
            if idempotency_key is not None:
                seen = idem.lookup(idempotency_key)
                if seen is not None:
                    stored_fp, stored_result = seen
                    if stored_fp != fp:
                        self.store.metrics.incr("telemetry.idempotency_conflict")
                        raise IdempotencyConflictError(
                            details={"idempotency_key": idempotency_key}
                        )
                    if stored_result["code"] == "ACCEPTED":
                        self.store.metrics.incr("telemetry.duplicate")
                    return TelemetryResult(
                        accepted=stored_result["accepted"],
                        code=stored_result["code"],
                        applied=False,  # 回放一律不重复生效
                        seq=stored_result["seq"],
                        shadow_version=stored_result["shadow_version"],
                        detail=stored_result["detail"],
                        freshness=stored_result["freshness"],
                    )

            window = self.store.seq_window_for(device_id)
            verdict = window.inspect(seq)
            if verdict == "DUPLICATE":
                result = TelemetryResult(
                    accepted=True,
                    code="TELEMETRY_DUPLICATE",
                    applied=False,
                    seq=seq,
                    shadow_version=self.store.shadow_for(device_id).version,
                    detail="sequence was already applied; state unchanged",
                    freshness=self._freshness(ts, now),
                )
                self.store.metrics.incr("telemetry.duplicate")
                if idempotency_key is not None:
                    idem.remember(idempotency_key, fp, result.to_dict())
                return result
            if verdict == "OUT_OF_ORDER":
                self.store.metrics.incr("telemetry.out_of_order")
                raise TelemetryOutOfOrderError(
                    details={
                        "seq": seq,
                        "applied_base": window.base,
                    }
                )

            # ---- 生效段（同一设备锁内原子完成）----
            # 序号窗口负责"接收/重复/乱序"判定；影子 reported_seq 是"已应用水位"。
            # 先到的乱序载荷进入有界缓冲，空洞补齐后按 seq 顺序合并到影子，
            # 因此较新值最终生效，旧值绝不会覆盖新值；每个序号最多应用一次。
            window = self.store.seq_window_for(device_id)
            buffer = self.store.telemetry_buffer_for(device_id)
            is_first = not window.started
            if not is_first and seq - window.base > window.window:
                # 巨跳：窗口内未到达的缺口被放弃，旧载荷缓冲随之清空（内存有界）
                buffer.clear()
            window.accept(seq)
            buffer[seq] = (float(ts), payload)

            shadow = self.store.shadow_for(device_id)
            if is_first:
                # 首条接收立即建立影子水位，下面循环消费 seq 自身
                shadow.reported_seq = seq - 1
            applied_count = 0
            while shadow.reported_seq + 1 in buffer:
                s = shadow.reported_seq + 1
                pts, ppayload = buffer.pop(s)
                shadow.reported.update(ppayload)
                shadow.reported_seq = s
                shadow.reported_at = pts
                shadow.updated_at = now
                shadow.version += 1
                applied_count += 1
            applied_now = seq <= shadow.reported_seq

            result = TelemetryResult(
                accepted=True,
                code="ACCEPTED",
                applied=applied_now,
                seq=seq,
                shadow_version=shadow.version,
                detail=(
                    "telemetry applied"
                    if applied_now
                    else "telemetry buffered behind a sequence gap"
                ),
                freshness=self._freshness(ts, now),
            )
            if idempotency_key is not None:
                idem.remember(idempotency_key, fp, result.to_dict())
            self.store.metrics.incr("telemetry.accepted")
            return result

    def _freshness(self, ts: float, now: float) -> str:
        return Freshness.FRESH.value if now - ts <= self.fresh_ttl_s else Freshness.STALE.value
