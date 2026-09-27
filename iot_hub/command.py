"""离线指令/配置下发：每设备待投递队列、按序补投、确认幂等、有效期与容量上限。

核心约定（确定且可解释）：
- 每台设备一条 FIFO 队列：后发的指令 seq 更大、排在后面，补投严格按发送先后；
- 容量双上限：单设备 per_device_capacity、全局 global_capacity，任一装满即
  COMMAND_QUEUE_FULL（与重复确认/设备不存在/凭据不可用等错误码可区分），不静默丢弃；
- 拒绝路径不产生半条记录：幂等命中直接返回旧指令，容量检查在分配序号/写状态之前，
  一次失败不推进 seq、不占用任何后续额度；
- 有效期：每条指令 created_at + ttl；补投前先剔除已过期 QUEUED（EXPIRED，结论
  expired_on_drain），清扫把已投递但一直未确认的过期 PENDING 置 EXPIRED；
- 状态机 QUEUED -> PENDING -> SUCCEEDED/FAILED；未确认绝不视为已执行；
- 确认幂等：同一 command_id 重复确认 -> COMMAND_ALREADY_ACKED，状态/统计只生效一次；
- 所有状态变更在同一把设备队列锁内“校验-提交”，并发确认/清理/补投结论一致。
"""
from __future__ import annotations

import threading
import uuid
from collections import OrderedDict

from .errors import ErrorCode, HubError
from .models import Command, CommandStatus

TERMINAL = (CommandStatus.SUCCEEDED, CommandStatus.FAILED, CommandStatus.EXPIRED)
_VALID_KINDS = ("config", "command")


class CommandQueue:
    def __init__(self, per_device_capacity: int = 100, global_capacity: int = 10000,
                 default_ttl: float = 86400.0) -> None:
        self._lock = threading.RLock()
        self._per_device_capacity = per_device_capacity
        self._global_capacity = global_capacity
        self._default_ttl = default_ttl
        # device_id -> OrderedDict[command_id, Command]，按发送先后（seq）排列
        self._devices: dict[str, OrderedDict[str, Command]] = {}
        # (device_id, idempotency_key) -> command_id，用于重复下发只生效一次
        self._idem: dict[tuple[str, str], str] = {}
        self._seq: dict[str, int] = {}

    # ---- 下发 ----

    def enqueue(self, device_id: str, kind: str, payload: dict,
                idempotency_key: str, now: float, ttl: float | None = None,
                online: bool = False) -> tuple[Command, bool]:
        """业务侧下发一条配置/指令。

        设备离线 -> QUEUED 等待补投；设备在线 -> 直接 PENDING（已投递）。
        同一 (device_id, idempotency_key) 重复下发返回原指令，不产生第二条、
        不占容量、不推进序号。返回 (指令, 是否本次新建)。
        """
        if not device_id:
            raise HubError(ErrorCode.MALFORMED_PAYLOAD, "device_id 不能为空")
        if kind not in _VALID_KINDS:
            raise HubError(ErrorCode.MALFORMED_PAYLOAD, "kind 只能是 config 或 command")
        if not isinstance(payload, dict) or not payload:
            raise HubError(ErrorCode.MALFORMED_PAYLOAD, "payload 必须为非空对象")
        if not idempotency_key:
            raise HubError(ErrorCode.MALFORMED_PAYLOAD, "缺少 idempotency_key")
        effective_ttl = self._default_ttl if ttl is None else ttl
        if not isinstance(effective_ttl, (int, float)) or isinstance(effective_ttl, bool) \
                or effective_ttl <= 0:
            raise HubError(ErrorCode.MALFORMED_PAYLOAD, "ttl 必须为正数")

        with self._lock:
            queue = self._devices.get(device_id)
            if queue is not None:
                existing_id = self._idem.get((device_id, idempotency_key))
                if existing_id is not None and existing_id in queue:
                    return queue[existing_id], False  # 幂等命中：无任何新状态
            # 容量检查在写状态之前；失败不推进 seq、不占额度
            per_device = self._count_active(device_id)
            if per_device >= self._per_device_capacity:
                raise HubError(ErrorCode.COMMAND_QUEUE_FULL, "该设备待投递队列已满")
            if self._count_active_global() >= self._global_capacity:
                raise HubError(ErrorCode.COMMAND_QUEUE_FULL, "全局待投递队列已满")

            if queue is None:
                queue = OrderedDict()
                self._devices[device_id] = queue
            seq = self._seq.get(device_id, 0) + 1
            command = Command(
                command_id=uuid.uuid4().hex,
                device_id=device_id,
                kind=kind,
                payload=dict(payload),
                created_at=now,
                expires_at=now + effective_ttl,
                idempotency_key=idempotency_key,
                seq=seq,
                status=CommandStatus.PENDING if online else CommandStatus.QUEUED,
                delivered_at=now if online else 0.0,
                delivery_count=1 if online else 0,
            )
            # 全部字段就绪后一次性提交
            queue[command.command_id] = command
            self._seq[device_id] = seq
            self._idem[(device_id, idempotency_key)] = command.command_id
            return command, True

    # ---- 重连补投 ----

    def drain(self, device_id: str, now: float) -> tuple[list[Command], list[Command]]:
        """设备重连：先剔除过期 QUEUED，再按 seq 顺序补投其余 QUEUED。

        返回 (delivered, expired)：
        - delivered：本次由 QUEUED 提升为 PENDING 的指令（严格 FIFO，首次投递）；
        - expired：补投前剔除的过期指令，状态 EXPIRED 且带可解释结论。
        已处于 PENDING（在旧连接上投递但未确认）的指令不在此重复计数，调用方
        可通过 pending_for 重推；二者各自统计，绝不重复投递计数。
        """
        with self._lock:
            queue = self._devices.get(device_id)
            if queue is None:
                return [], []
            delivered: list[Command] = []
            expired: list[Command] = []
            # OrderedDict 顺序即发送先后；过期者先标记剔除，其余按序投递
            for command in list(queue.values()):
                if command.status != CommandStatus.QUEUED:
                    continue
                if now >= command.expires_at:
                    command.status = CommandStatus.EXPIRED
                    command.terminal_reason = "expired_on_drain"
                    expired.append(command)
                else:
                    command.status = CommandStatus.PENDING
                    command.delivered_at = now
                    command.delivery_count += 1
                    delivered.append(command)
            self._prune(queue, device_id)
            return delivered, expired

    # ---- 确认 ----

    def ack(self, device_id: str, command_id: str, success: bool, now: float) -> Command:
        """设备确认：仅 PENDING 可确认；终态重复确认只给结论、不再生效。"""
        with self._lock:
            queue = self._devices.get(device_id)
            command = queue.get(command_id) if queue is not None else None
            if command is None:
                raise HubError(ErrorCode.NOT_FOUND, "指令不存在")
            if command.status in (CommandStatus.SUCCEEDED, CommandStatus.FAILED):
                # 重复确认：原终态与统计保持不变，只生效一次
                raise HubError(ErrorCode.COMMAND_ALREADY_ACKED,
                               f"指令已确认（{command.status.value}），重复确认不生效")
            if command.status == CommandStatus.EXPIRED:
                raise HubError(ErrorCode.COMMAND_EXPIRED, "指令已过期失效")
            if command.status == CommandStatus.QUEUED:
                raise HubError(ErrorCode.COMMAND_NOT_PENDING, "指令尚未投递，不可确认")
            # PENDING：未确认前绝不算已执行；此刻才进入终态
            command.status = CommandStatus.SUCCEEDED if success else CommandStatus.FAILED
            command.acked_at = now
            self._prune(queue, device_id)
            return command

    # ---- 过期清扫 ----

    def sweep_expired(self, now: float) -> list[Command]:
        """把已过有效期仍未走完的 QUEUED/PENDING 指令统一置 EXPIRED。

        补投前的 QUEUED 过期结论为 expired_on_drain；清扫发现的过期结论为
        expired_while_queued（从未投递）或 expired_while_pending（投递后未确认）。
        """
        expired_all: list[Command] = []
        with self._lock:
            for device_id, queue in list(self._devices.items()):
                for command in list(queue.values()):
                    if command.status in TERMINAL or now < command.expires_at:
                        continue
                    command.status = CommandStatus.EXPIRED
                    command.terminal_reason = (
                        "expired_while_pending" if command.delivery_count > 0
                        else "expired_while_queued"
                    )
                    expired_all.append(command)
                self._prune(queue, device_id)
            return expired_all

    # ---- 查询 ----

    def pending_for(self, device_id: str) -> list[Command]:
        """已投递未确认（PENDING）的指令，按发送先后返回；用于重连后重推。"""
        with self._lock:
            queue = self._devices.get(device_id)
            if queue is None:
                return []
            return [c for c in queue.values() if c.status == CommandStatus.PENDING]

    def queued_for(self, device_id: str) -> list[Command]:
        with self._lock:
            queue = self._devices.get(device_id)
            if queue is None:
                return []
            return [c for c in queue.values() if c.status == CommandStatus.QUEUED]

    def get(self, device_id: str, command_id: str) -> Command:
        with self._lock:
            queue = self._devices.get(device_id)
            command = queue.get(command_id) if queue is not None else None
            if command is None:
                raise HubError(ErrorCode.NOT_FOUND, "指令不存在")
            return command

    def count_active(self, device_id: str | None = None) -> int:
        """未走完（QUEUED+PENDING）的待投递数量；可按设备或全局统计。"""
        with self._lock:
            if device_id is not None:
                return self._count_active(device_id)
            return self._count_active_global()

    # ---- 内部：调用方必须持锁 ----

    def _count_active(self, device_id: str) -> int:
        queue = self._devices.get(device_id)
        if queue is None:
            return 0
        return sum(1 for c in queue.values() if c.status not in TERMINAL)

    def _count_active_global(self) -> int:
        return sum(
            1 for queue in self._devices.values()
            for c in queue.values() if c.status not in TERMINAL
        )

    def _prune(self, queue: OrderedDict[str, Command], device_id: str) -> None:
        """回收终态记录，保持内存有界；只删终态，绝不影响待投递顺序与计数。

        终态记录至少保留最近若干条，用于把“重复确认”与“指令不存在”区分开。
        """
        keep_terminal = 64
        terminal_ids = [cid for cid, c in queue.items() if c.status in TERMINAL]
        stale = terminal_ids[:-keep_terminal] if len(terminal_ids) > keep_terminal else []
        for cid in stale:
            command = queue.pop(cid)
            self._idem.pop((device_id, command.idempotency_key), None)
        if not queue:
            self._devices.pop(device_id, None)
            self._seq.pop(device_id, None)
