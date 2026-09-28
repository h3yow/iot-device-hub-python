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

持久账本（进程生命周期内不回收）：
- _history：设备维度的受理账本，每条受理过的指令只存一份对象，进入终态、活动队列
  回收后仍可查；重复确认据此稳定给出 COMMAND_ALREADY_ACKED / COMMAND_EXPIRED，
  绝不退化成 NOT_FOUND，也不改写原终态；
- _idem：(device_id, idempotency_key) -> command_id，与活动队列解耦，不随终态回收
  删除——同一去重标识无论确认掉多少条、隔多久重发，都返回原指令（原 command_id、
  原载荷、原 seq），不新建、不改载荷、不重复排队、不重复计数，设备不执行第二次；
- _expired：设备维度的过期结论流水，记录具体哪条指令因何原因被剔除
  （expired_on_drain / expired_while_queued / expired_while_pending），业务侧随时
  可查，不再只有补投时刻的一个汇总数量；
- 账本严格按 device_id 隔离，绝不跨设备串用；活动队列仍只含 QUEUED/PENDING、受
  容量上限约束，账本只随“平台受理过的指令总量”增长（进程重启清空，与既有定位一致）。
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
        # device_id -> OrderedDict[command_id, Command]：仅活动指令（QUEUED/PENDING），
        # 按发送先后（seq）排列；进入终态即移出，故其规模恒受容量上限约束。
        self._devices: dict[str, OrderedDict[str, Command]] = {}
        # device_id -> {command_id: Command}：受理账本，平台受理过的每条指令在此只存
        # 一份（与活动队列中是同一对象），终态与回收都不删除，用于稳定幂等语义。
        self._history: dict[str, dict[str, Command]] = {}
        # (device_id, idempotency_key) -> command_id：持久去重索引，不随终态回收删除。
        self._idem: dict[tuple[str, str], str] = {}
        # device_id -> [Command]：过期结论流水（剔除先后顺序），业务侧可逐条查询。
        self._expired: dict[str, list[Command]] = {}
        # device_id -> [Command]：最近一次重连补投剔除的过期指令（connect 同步返回）。
        self._last_drain_expired: dict[str, list[Command]] = {}
        # device_id -> 已分配最大 seq：活动队列清空也不重置，保证 seq 单设备单调。
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
            # 去重索引独立于活动队列：原指令早已确认、活动记录被回收后依然命中。
            existing_id = self._idem.get((device_id, idempotency_key))
            if existing_id is not None:
                existing = self._history.get(device_id, {}).get(existing_id)
                if existing is not None:
                    return existing, False  # 幂等命中：返回原指令，无任何新状态
            # 容量检查在写状态之前；失败不推进 seq、不占额度
            per_device = self._count_active(device_id)
            if per_device >= self._per_device_capacity:
                raise HubError(ErrorCode.COMMAND_QUEUE_FULL, "该设备待投递队列已满")
            if self._count_active_global() >= self._global_capacity:
                raise HubError(ErrorCode.COMMAND_QUEUE_FULL, "全局待投递队列已满")

            history = self._history.setdefault(device_id, {})
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
            # 全部字段就绪后一次性提交：活动队列、受理账本、去重索引、序号
            queue[command.command_id] = command
            history[command.command_id] = command
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
                self._last_drain_expired.pop(device_id, None)
                return [], []
            delivered: list[Command] = []
            expired: list[Command] = []
            expired_log = self._expired.setdefault(device_id, [])
            # OrderedDict 顺序即发送先后；过期者先标记剔除，其余按序投递
            for command in list(queue.values()):
                if command.status != CommandStatus.QUEUED:
                    continue
                if now >= command.expires_at:
                    command.status = CommandStatus.EXPIRED
                    command.terminal_reason = "expired_on_drain"
                    expired.append(command)
                    expired_log.append(command)  # 持久结论流水：哪条、什么原因
                else:
                    command.status = CommandStatus.PENDING
                    command.delivered_at = now
                    command.delivery_count += 1
                    delivered.append(command)
            # 本次补投剔除结论同步给 connect；持久明细已并入 expired_log
            self._last_drain_expired[device_id] = list(expired)
            self._archive_terminal(queue, device_id)
            return delivered, expired

    # ---- 确认 ----

    def ack(self, device_id: str, command_id: str, success: bool, now: float) -> Command:
        """设备确认：仅 PENDING 可确认；终态重复确认只给结论、不再生效。

        受理账本保证：指令早已确认、活动记录被回收后再确认，仍稳定得到原来的终态
        结论（SUCCEEDED/FAILED -> COMMAND_ALREADY_ACKED；EXPIRED -> COMMAND_EXPIRED），
        不会退化成 NOT_FOUND，也不改写原终态；只有平台从未受理过的 command_id 才是
        NOT_FOUND，与“已确认过”严格区分。
        """
        with self._lock:
            history = self._history.get(device_id)
            command = history.get(command_id) if history is not None else None
            if command is None:
                raise HubError(ErrorCode.NOT_FOUND, "指令不存在")
            if command.status in (CommandStatus.SUCCEEDED, CommandStatus.FAILED):
                # 重复确认（哪怕记录已从活动队列回收）：原终态与统计保持不变，只生效一次
                raise HubError(ErrorCode.COMMAND_ALREADY_ACKED,
                               f"指令已确认（{command.status.value}），重复确认不生效")
            if command.status == CommandStatus.EXPIRED:
                raise HubError(ErrorCode.COMMAND_EXPIRED, "指令已过期失效")
            if command.status == CommandStatus.QUEUED:
                raise HubError(ErrorCode.COMMAND_NOT_PENDING, "指令尚未投递，不可确认")
            # PENDING：未确认前绝不算已执行；此刻才进入终态
            command.status = CommandStatus.SUCCEEDED if success else CommandStatus.FAILED
            command.acked_at = now
            queue = self._devices.get(device_id)
            if queue is not None:
                self._archive_terminal(queue, device_id)
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
                expired_log = self._expired.setdefault(device_id, [])
                for command in list(queue.values()):
                    if command.status in TERMINAL or now < command.expires_at:
                        continue
                    command.status = CommandStatus.EXPIRED
                    command.terminal_reason = (
                        "expired_while_pending" if command.delivery_count > 0
                        else "expired_while_queued"
                    )
                    expired_all.append(command)
                    expired_log.append(command)  # 持久结论流水：哪条、什么原因
                self._archive_terminal(queue, device_id)
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
        """查单条指令：含已终态、已从活动队列回收的历史指令（受理账本不回收）。"""
        with self._lock:
            history = self._history.get(device_id)
            command = history.get(command_id) if history is not None else None
            if command is None:
                raise HubError(ErrorCode.NOT_FOUND, "指令不存在")
            return command

    def expired_for(self, device_id: str) -> list[Command]:
        """该设备被剔除的过期指令流水（剔除先后顺序），含 command_id、幂等键、载荷
        与 terminal_reason——业务侧据此拿到“具体哪一条、因何被剔除”，而不只是数量。"""
        with self._lock:
            return list(self._expired.get(device_id, []))

    def last_drain_expired(self, device_id: str) -> list[Command]:
        """最近一次重连补投（drain）剔除的过期指令；未发生过补投则为空。

        供 connect 同步向业务侧返回本次连接的可解释剔除结论；明细同样落在
        expired_for 的持久流水中，稍后仍可逐条回查。
        """
        with self._lock:
            log = self._last_drain_expired.get(device_id)
            return list(log or [])

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

    def _archive_terminal(self, queue: OrderedDict[str, Command], device_id: str) -> None:
        """把终态记录移出活动队列，保持待投递规模严格等于 QUEUED+PENDING、受容量上限
        约束；指令对象仍保留在受理账本 _history 中，去重索引与过期流水均不删除，
        因此大批量确认、回收历史后，重复下发仍返回原指令、重复确认仍报原终态结论。
        """
        for cid in [cid for cid, c in queue.items() if c.status in TERMINAL]:
            queue.pop(cid, None)
        if not queue:
            self._devices.pop(device_id, None)
            # 注意：不删 _history/_idem/_expired/_seq——幂等与终态结论长期有效，
            # seq 继续单调，后续重发/再确认不会被当成全新请求。
