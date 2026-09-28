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

两类“终身台账”与有界活动队列分离（本进程生命周期内不回收，进程重启按既有定位不保留）：
- 幂等台账 _idem：(device_id, idempotency_key) -> 原指令。只要平台受理过，无论该指令
  确认掉多久、活动队列里回收掉多少历史，同键重发一律返回原指令：不新建、不改载荷、
  不重复排队、不会被执行第二次；
- 终态台账 _terminal：device_id -> command_id -> 终态指令。记录被活动队列回收后，
  再确认仍稳定给出原终态结论（已确认 -> COMMAND_ALREADY_ACKED，已过期 ->
  COMMAND_EXPIRED），不会退化成 NOT_FOUND，也不改写原终态；
- 过期台账 _expiry：每设备有界 FIFO 的 ExpiryConclusion，业务侧可查“具体哪一条、
  什么原因、何时被剔除”，而不是只有一个汇总数量。
"""
from __future__ import annotations

import threading
import uuid
from collections import OrderedDict, deque

from .errors import ErrorCode, HubError
from .models import Command, CommandStatus, ExpiryConclusion

TERMINAL = (CommandStatus.SUCCEEDED, CommandStatus.FAILED, CommandStatus.EXPIRED)
ACKED_TERMINAL = (CommandStatus.SUCCEEDED, CommandStatus.FAILED)
_VALID_KINDS = ("config", "command")


class CommandQueue:
    def __init__(self, per_device_capacity: int = 100, global_capacity: int = 10000,
                 default_ttl: float = 86400.0,
                 expiry_history_per_device: int = 1000,
                 terminal_keep: int = 64) -> None:
        self._lock = threading.RLock()
        self._per_device_capacity = per_device_capacity
        self._global_capacity = global_capacity
        self._default_ttl = default_ttl
        self._expiry_history = expiry_history_per_device
        self._terminal_keep = terminal_keep
        # device_id -> OrderedDict[command_id, Command]：活动队列（QUEUED/PENDING）
        # 外加最近若干条终态记录，按发送先后（seq）排列，终态部分可被回收
        self._devices: dict[str, OrderedDict[str, Command]] = {}
        # 【终身台账】(device_id, idempotency_key) -> 原指令；不随队列回收删除，
        # 保证同键重发在任意时刻（哪怕隔了很久、回收了再多历史）都返回原指令
        self._idem: dict[tuple[str, str], Command] = {}
        # 【终身台账】device_id -> command_id -> 终态指令；记录回收后重复确认仍可判别
        self._terminal: dict[str, dict[str, Command]] = {}
        # device_id -> 有界 FIFO 的过期结论（不可变快照），供业务侧解释性查询
        self._expiry: dict[str, deque[ExpiryConclusion]] = {}
        self._seq: dict[str, int] = {}

    # ---- 下发 ----

    def enqueue(self, device_id: str, kind: str, payload: dict,
                idempotency_key: str, now: float, ttl: float | None = None,
                online: bool = False) -> tuple[Command, bool]:
        """业务侧下发一条配置/指令。

        设备离线 -> QUEUED 等待补投；设备在线 -> 直接 PENDING（已投递）。
        同一 (device_id, idempotency_key) 只要受理过，重复下发永远返回原指令：
        不产生第二条、不占容量、不推进序号、不改载荷/状态，与活动队列是否已回收
        该记录无关。返回 (指令, 是否本次新建)。
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
            # 幂等判定先于一切，且查的是终身台账：旧指令即便已确认/已回收也命中，
            # 返回的就是当初受理那条（同一对象、原载荷、原终态），不重新入队
            existing = self._idem.get((device_id, idempotency_key))
            if existing is not None:
                return existing, False  # 幂等命中：无任何新状态、不计数、不占额度
            # 容量检查在写状态之前；失败不推进 seq、不占额度
            per_device = self._count_active(device_id)
            if per_device >= self._per_device_capacity:
                raise HubError(ErrorCode.COMMAND_QUEUE_FULL, "该设备待投递队列已满")
            if self._count_active_global() >= self._global_capacity:
                raise HubError(ErrorCode.COMMAND_QUEUE_FULL, "全局待投递队列已满")

            queue = self._devices.get(device_id)
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
            self._idem[(device_id, idempotency_key)] = command
            return command, True

    # ---- 重连补投 ----

    def drain_report(self, device_id: str, now: float
                     ) -> tuple[list[Command], list[ExpiryConclusion]]:
        """设备重连：先剔除过期 QUEUED，再按 seq 顺序补投其余 QUEUED。

        返回 (delivered, expired_conclusions)：
        - delivered：本次由 QUEUED 提升为 PENDING 的指令（严格 FIFO，首次投递）；
        - expired_conclusions：补投前剔除的过期指令的可解释结论（哪一条、原因、时刻）。
        已处于 PENDING（在旧连接上投递但未确认）的指令不在此重复计数，调用方
        可通过 pending_for 重推；二者各自统计，绝不重复投递计数。
        剔除与结论落账在同一把锁内完成，多连接并发补投时每条过期指令只落一次结论。
        """
        with self._lock:
            queue = self._devices.get(device_id)
            if queue is None:
                return [], []
            delivered: list[Command] = []
            conclusions: list[ExpiryConclusion] = []
            # OrderedDict 顺序即发送先后；过期者先标记剔除并落结论，其余按序投递
            for command in list(queue.values()):
                if command.status != CommandStatus.QUEUED:
                    continue
                if now >= command.expires_at:
                    command.status = CommandStatus.EXPIRED
                    command.terminal_reason = "expired_on_drain"
                    self._register_terminal(device_id, command)
                    conclusion = self._record_expiry(
                        device_id, command, now, "expired_on_drain")
                    conclusions.append(conclusion)
                else:
                    command.status = CommandStatus.PENDING
                    command.delivered_at = now
                    command.delivery_count += 1
                    delivered.append(command)
            self._prune(queue, device_id)
            return delivered, conclusions

    def drain(self, device_id: str, now: float) -> tuple[list[Command], list[Command]]:
        """兼容封装：返回 (delivered, expired_commands)，结论同 drain_report。"""
        delivered, conclusions = self.drain_report(device_id, now)
        with self._lock:
            expired = [
                self._terminal[device_id][c.command_id]
                for c in conclusions
            ]
        return delivered, expired

    # ---- 确认 ----

    def ack(self, device_id: str, command_id: str, success: bool, now: float) -> Command:
        """设备确认：仅 PENDING 可确认；终态重复确认只给结论、不再生效。

        活动队列已回收的终态指令查终身台账：已确认过的无论隔多久都稳定返回
        COMMAND_ALREADY_ACKED（原终态不变），已过期的返回 COMMAND_EXPIRED，
        只有平台确实从未受理过的 command_id 才是 NOT_FOUND。
        """
        with self._lock:
            queue = self._devices.get(device_id)
            command = queue.get(command_id) if queue is not None else None
            if command is None:
                # 活动队列里没有：查终态台账，把“已终态后被回收”与“从未下发”区分开
                command = self._terminal.get(device_id, {}).get(command_id)
                if command is None:
                    raise HubError(ErrorCode.NOT_FOUND, "指令不存在")
                if command.status in ACKED_TERMINAL:
                    raise HubError(ErrorCode.COMMAND_ALREADY_ACKED,
                                   f"指令已确认（{command.status.value}），重复确认不生效")
                if command.status == CommandStatus.EXPIRED:
                    raise HubError(ErrorCode.COMMAND_EXPIRED, "指令已过期失效")
                raise HubError(ErrorCode.INTERNAL, "指令状态异常")
            if command.status in ACKED_TERMINAL:
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
            self._register_terminal(device_id, command)  # 先落终身台账再回收
            self._prune(queue, device_id)
            return command

    # ---- 过期清扫 ----

    def sweep_expired(self, now: float) -> list[Command]:
        """把已过有效期仍未走完的 QUEUED/PENDING 指令统一置 EXPIRED。

        补投前的 QUEUED 过期结论为 expired_on_drain；清扫发现的过期结论为
        expired_while_queued（从未投递）或 expired_while_pending（投递后未确认）。
        每条同时写入终态台账与过期结论台账。
        """
        expired_all: list[Command] = []
        with self._lock:
            for device_id, queue in list(self._devices.items()):
                for command in list(queue.values()):
                    if command.status in TERMINAL or now < command.expires_at:
                        continue
                    command.status = CommandStatus.EXPIRED
                    reason = (
                        "expired_while_pending" if command.delivery_count > 0
                        else "expired_while_queued"
                    )
                    command.terminal_reason = reason
                    self._register_terminal(device_id, command)
                    self._record_expiry(device_id, command, now, reason)
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
        """取指令：活动队列没有时回退终态台账，终态历史被回收也查得到原结论。"""
        with self._lock:
            queue = self._devices.get(device_id)
            command = queue.get(command_id) if queue is not None else None
            if command is None:
                command = self._terminal.get(device_id, {}).get(command_id)
            if command is None:
                raise HubError(ErrorCode.NOT_FOUND, "指令不存在")
            return command

    def expired_for(self, device_id: str, limit: int | None = None
                    ) -> list[ExpiryConclusion]:
        """该设备被剔除的过期指令结论，按发生先后返回（最早在前）。

        limit 给定时只返回最近 limit 条。台账有界（默认每设备 1000 条，FIFO），
        但与活动队列回收解耦：指令确认/回收不影响这里的历史可解释性。
        """
        with self._lock:
            journal = self._expiry.get(device_id)
            if journal is None:
                return []
            if limit is not None:
                if limit <= 0:
                    return []
                if limit < len(journal):
                    return list(journal)[-limit:]
            return list(journal)

    def count_active(self, device_id: str | None = None) -> int:
        """未走完（QUEUED+PENDING）的待投递数量；可按设备或全局统计。"""
        with self._lock:
            if device_id is not None:
                return self._count_active(device_id)
            return self._count_active_global()

    # ---- 内部：调用方必须持锁 ----

    def _register_terminal(self, device_id: str, command: Command) -> None:
        """进入终态即登记终身台账；之后活动队列如何回收都不影响终态可查。"""
        self._terminal.setdefault(device_id, {})[command.command_id] = command

    def _record_expiry(self, device_id: str, command: Command, now: float,
                       reason: str) -> ExpiryConclusion:
        journal = self._expiry.get(device_id)
        if journal is None:
            journal = deque(maxlen=self._expiry_history)
            self._expiry[device_id] = journal
        conclusion = ExpiryConclusion(
            device_id=device_id,
            command_id=command.command_id,
            idempotency_key=command.idempotency_key,
            kind=command.kind,
            seq=command.seq,
            created_at=command.created_at,
            expires_at=command.expires_at,
            expired_at=now,
            reason=reason,
        )
        journal.append(conclusion)
        return conclusion

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
        """只回收活动队列里超出保留窗口的终态记录，保持活动队列内存有界。

        绝不删除幂等台账与终态台账：回收只影响活动队列大小，不影响
        “同键重发返回原指令”和“历史终态可重复确认判别”这两个终身保证；
        seq 也保留，单设备序号始终单调，不受历史回收影响。
        """
        terminal_ids = [cid for cid, c in queue.items() if c.status in TERMINAL]
        stale = terminal_ids[:-self._terminal_keep] \
            if len(terminal_ids) > self._terminal_keep else []
        for cid in stale:
            queue.pop(cid)
        if not queue:
            self._devices.pop(device_id, None)
