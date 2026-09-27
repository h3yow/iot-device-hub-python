"""离线指令/配置下发：每设备 FIFO 待投递队列，重连按序补投，确认幂等。

语义（确定且可解释）：
- QUEUED：已入队未投递；DELIVERED：已补投待确认，未确认绝不视为已执行；
- ACKED：设备确认，唯一生效终态；重复确认幂等，只生效一次；
- EXPIRED：超过有效期被剔除，投递前与清扫时统一剔除并给出结论。
容量：全局 + 单设备双上限，满时抛 QUEUE_FULL（与重复确认、设备不存在、
凭据不可用可区分）；任何被拒的入队不留半条记录、不消耗后续额度。
"""
from __future__ import annotations

import threading
from collections import deque

from .errors import ErrorCode, HubError
from .models import Command, CommandKind, CommandStatus


class CommandQueue:
    """离线指令队列：每设备 FIFO + 全局容量上限，全部状态变更持锁原子完成。"""

    def __init__(self, per_device_capacity: int = 100,
                 global_capacity: int = 10000,
                 concluded_capacity: int = 1000) -> None:
        if per_device_capacity <= 0 or global_capacity <= 0:
            raise ValueError("容量必须为正")
        self._lock = threading.RLock()
        self._per_device_capacity = per_device_capacity
        self._global_capacity = global_capacity
        self._concluded_capacity = concluded_capacity
        # device_id -> 活跃队列（QUEUED/DELIVERED，FIFO，按入队先后排序）
        self._queues: dict[str, deque[Command]] = {}
        # device_id -> {command_id: Command} 终态记录（有界，FIFO 淘汰）
        self._concluded: dict[str, dict[str, Command]] = {}
        self._total_active = 0

    # ---- 入队 ----

    def enqueue(self, device_id: str, command_id: str, kind: CommandKind,
                payload: dict, ttl_seconds: float, now: float) -> tuple[Command, bool]:
        """入队，返回 (指令, 是否新建)。幂等：同 command_id 重复入队返回既有记录。

        校验全部通过后才写入；任何拒绝路径不留半条记录、不消耗额度。
        """
        if not command_id:
            raise HubError(ErrorCode.MALFORMED_PAYLOAD, "command_id 不能为空")
        if not isinstance(payload, dict):
            raise HubError(ErrorCode.MALFORMED_PAYLOAD, "payload 必须为对象")
        if ttl_seconds <= 0:
            raise HubError(ErrorCode.MALFORMED_PAYLOAD, "ttl_seconds 必须为正")
        with self._lock:
            # 先剔除该设备已过期项，保证容量判定基于真实占用
            self._purge_device_locked(device_id, now)
            queue = self._queues.setdefault(device_id, deque())
            # 幂等：活跃队列或终态记录中已存在同 command_id，直接返回
            for cmd in queue:
                if cmd.command_id == command_id:
                    return cmd, False
            done = self._concluded.get(device_id, {})
            if command_id in done:
                return done[command_id], False
            # 容量判定：先单设备后全局；失败不写入、计数不变
            if len(queue) >= self._per_device_capacity:
                raise HubError(ErrorCode.QUEUE_FULL, "单设备待投递队列已满")
            if self._total_active >= self._global_capacity:
                raise HubError(ErrorCode.QUEUE_FULL, "全局待投递队列已满")
            cmd = Command(
                command_id=command_id, device_id=device_id, kind=kind,
                payload=dict(payload), created_at=now, expires_at=now + ttl_seconds,
            )
            queue.append(cmd)
            self._total_active += 1
            return cmd, True

    # ---- 补投 ----

    def deliver(self, device_id: str, now: float) -> list[Command]:
        """重连补投：剔除过期项后，按入队先后返回全部未确认指令。

        已 DELIVERED 未确认的会再次随补投返回（重复投递可能），
        生效与否由 ack 幂等保证，绝不重复生效。
        """
        with self._lock:
            self._purge_device_locked(device_id, now)
            queue = self._queues.get(device_id)
            if not queue:
                return []
            out = list(queue)  # FIFO 顺序即入队先后，不插队不漏投
            for cmd in out:
                if cmd.status == CommandStatus.QUEUED:
                    cmd.status = CommandStatus.DELIVERED
                    cmd.delivered_at = now
            return out

    # ---- 确认 ----

    def ack(self, device_id: str, command_id: str, now: float) -> tuple[Command, bool]:
        """确认。返回 (指令, 是否本次生效)；重复确认幂等，只生效一次。"""
        with self._lock:
            cmd = self._find_active(device_id, command_id)
            if cmd is None:
                done = self._concluded.get(device_id, {})
                concluded = done.get(command_id)
                if concluded is None:
                    raise HubError(ErrorCode.NOT_FOUND, "指令不存在")
                if concluded.status == CommandStatus.ACKED:
                    return concluded, False  # 重复确认：幂等返回，不重复生效
                if concluded.status == CommandStatus.EXPIRED:
                    raise HubError(ErrorCode.COMMAND_EXPIRED, "指令已过期，不可确认")
                raise HubError(ErrorCode.COMMAND_NOT_PENDING,
                               f"指令状态 {concluded.status.value} 不可确认")
            if cmd.status != CommandStatus.DELIVERED:
                raise HubError(ErrorCode.COMMAND_NOT_PENDING,
                               "指令尚未投递，不可确认")
            cmd.status = CommandStatus.ACKED
            cmd.acked_at = now
            self._remove_active_locked(device_id, cmd)
            self._record_concluded_locked(device_id, cmd)
            return cmd, True

    # ---- 过期清扫 ----

    def sweep_expired(self, now: float) -> list[Command]:
        """剔除全部设备已过期指令，标记 EXPIRED 并返回（可解释结论）。"""
        expired: list[Command] = []
        with self._lock:
            for device_id in list(self._queues):
                expired.extend(self._purge_device_locked(device_id, now))
        return expired

    # ---- 查询 ----

    def pending_count(self, device_id: str, now: float) -> int:
        """该设备剩余待投递数量（先剔除过期项，计数真实）。"""
        with self._lock:
            self._purge_device_locked(device_id, now)
            queue = self._queues.get(device_id)
            return len(queue) if queue else 0

    def get(self, device_id: str, command_id: str) -> Command:
        with self._lock:
            cmd = self._find_active(device_id, command_id)
            if cmd is not None:
                return cmd
            done = self._concluded.get(device_id, {})
            if command_id in done:
                return done[command_id]
            raise HubError(ErrorCode.NOT_FOUND, "指令不存在")

    def list_device(self, device_id: str, now: float) -> list[Command]:
        """该设备全部活跃指令（FIFO 序）+ 终态记录，供业务侧核对。"""
        with self._lock:
            self._purge_device_locked(device_id, now)
            active = list(self._queues.get(device_id, ()))
            concluded = list(self._concluded.get(device_id, {}).values())
            return active + concluded

    def __len__(self) -> int:
        with self._lock:
            return self._total_active

    # ---- 内部 ----

    def _find_active(self, device_id: str, command_id: str) -> Command | None:
        for cmd in self._queues.get(device_id, ()):
            if cmd.command_id == command_id:
                return cmd
        return None

    def _purge_device_locked(self, device_id: str, now: float) -> list[Command]:
        """把该设备已过期活跃项移入终态记录；返回被剔除项。"""
        queue = self._queues.get(device_id)
        if not queue:
            return []
        expired: list[Command] = []
        kept: deque[Command] = deque()
        for cmd in queue:
            if now >= cmd.expires_at:
                cmd.status = CommandStatus.EXPIRED
                expired.append(cmd)
                self._total_active -= 1
                self._record_concluded_locked(device_id, cmd)
            else:
                kept.append(cmd)
        self._queues[device_id] = kept
        return expired

    def _remove_active_locked(self, device_id: str, cmd: Command) -> None:
        queue = self._queues.get(device_id)
        if queue is not None:
            self._queues[device_id] = deque(c for c in queue if c is not cmd)
        self._total_active -= 1

    def _record_concluded_locked(self, device_id: str, cmd: Command) -> None:
        done = self._concluded.setdefault(device_id, {})
        done[cmd.command_id] = cmd
        while len(done) > self._concluded_capacity:  # 有界，FIFO 淘汰最旧
            done.pop(next(iter(done)))
