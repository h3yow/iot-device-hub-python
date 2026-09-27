"""离线指令/配置下发：排队、重连补投、幂等确认、过期剔除、容量与并发。"""
import logging
import threading

from iot_hub.commands import CommandQueue
from iot_hub.errors import ErrorCode
from iot_hub.models import CommandKind, CommandStatus, Scope
from iot_hub.ratelimit import RateLimiter
from iot_hub.service import DeviceHubService
from tests.test_auth import expect_error

log = logging.getLogger("test")

CMD_SCOPES = {Scope.SHADOW_READ, Scope.COMMAND_ACK}


def setup_device(hub, clock, device_id="dev-1", online=False):
    hub.registry.register(device_id, "sensor-A", "1.0.0", clock())
    token = hub.registry.issue_credential(device_id, CMD_SCOPES, clock()).token
    hub.registry.set_online(device_id, online, clock())
    log.info("注册设备 device_id=%s online=%s scopes=%s",
             device_id, online, [s.value for s in CMD_SCOPES])
    return token


def enqueue_n(hub, device_id, n, prefix="cmd", ttl=300.0):
    for i in range(n):
        hub.enqueue_command(device_id, f"{prefix}-{i}", CommandKind.COMMAND,
                            {"n": i}, ttl)


def test_offline_enqueue_and_reconnect_redeliver_in_order(hub, clock):
    """设备离线时入队，重连后按发送先后补投，不插队、不漏投。"""
    token = setup_device(hub, clock, online=False)
    enqueue_n(hub, "dev-1", 3)
    log.info("输入: 离线设备入队 3 条指令 cmd-0/1/2")
    assert hub.pending_command_count("dev-1") == 3

    hub.connect(token)  # 重连触发补投
    delivered = hub.commands.list_device("dev-1", clock())
    active = [c for c in delivered if c.status == CommandStatus.DELIVERED]
    log.info("判定依据: 重连后补投顺序 %s", [c.command_id for c in active])
    assert [c.command_id for c in active] == ["cmd-0", "cmd-1", "cmd-2"]  # FIFO
    assert hub.pending_command_count("dev-1") == 3  # 未确认前仍在队列


def test_unacked_command_not_effective(hub, clock):
    """设备没确认之前不能算已执行。"""
    setup_device(hub, clock)
    hub.enqueue_command("dev-1", "cmd-1", CommandKind.CONFIG, {"k": 1}, 300.0)
    hub.deliver_commands("dev-1")
    cmd = hub.commands.get("dev-1", "cmd-1")
    log.info("判定依据: 已投递未确认，状态=%s（非 ACKED）", cmd.status.value)
    assert cmd.status == CommandStatus.DELIVERED
    assert cmd.acked_at == 0.0


def test_duplicate_ack_applies_once(hub, clock):
    """同一条指令重复确认只生效一次，统计不重复累加。"""
    token = setup_device(hub, clock)
    hub.enqueue_command("dev-1", "cmd-1", CommandKind.COMMAND, {}, 300.0)
    hub.deliver_commands("dev-1")
    first = hub.ack_command(token, "dev-1", "cmd-1")
    second = hub.ack_command(token, "dev-1", "cmd-1")  # 重复确认：幂等
    snap = hub.stats.snapshot()
    log.info("输入: 同一指令确认两次 -> acked=%d ack_deduplicated=%d",
             snap.get("command/acked", 0), snap.get("command/ack_deduplicated", 0))
    assert first.status == second.status == CommandStatus.ACKED
    assert snap["command/acked"] == 1  # 只生效一次
    assert snap["command/ack_deduplicated"] == 1
    assert hub.pending_command_count("dev-1") == 0


def test_duplicate_enqueue_idempotent(hub, clock):
    """同 command_id 重复投递不产生第二条记录。"""
    setup_device(hub, clock)
    hub.enqueue_command("dev-1", "cmd-1", CommandKind.COMMAND, {"v": 1}, 300.0)
    cmd2 = hub.enqueue_command("dev-1", "cmd-1", CommandKind.COMMAND, {"v": 2}, 300.0)
    log.info("判定依据: 重复入队返回既有记录 payload=%s", cmd2.payload)
    assert cmd2.payload == {"v": 1}  # 保留首次内容
    assert hub.pending_command_count("dev-1") == 1
    assert len(hub.commands) == 1


def test_expired_commands_purged_before_deliver(hub, clock):
    """补投前剔除已过期指令，给出 EXPIRED 结论，不投递。"""
    setup_device(hub, clock)
    hub.enqueue_command("dev-1", "cmd-old", CommandKind.COMMAND, {}, 30.0)
    hub.enqueue_command("dev-1", "cmd-new", CommandKind.COMMAND, {}, 300.0)
    clock.advance(60)  # cmd-old 过期，cmd-new 仍有效
    delivered = hub.deliver_commands("dev-1")
    expired_cmd = hub.commands.get("dev-1", "cmd-old")
    log.info("输入: ttl=30s 指令过期后补投 -> 投递 %s，过期指令状态 %s",
             [c.command_id for c in delivered], expired_cmd.status.value)
    assert [c.command_id for c in delivered] == ["cmd-new"]
    assert expired_cmd.status == CommandStatus.EXPIRED
    assert hub.pending_command_count("dev-1") == 1


def test_sweep_expired_gives_explainable_outcome(hub, clock):
    setup_device(hub, clock)
    enqueue_n(hub, "dev-1", 2, ttl=30.0)
    enqueue_n(hub, "dev-1", 1, prefix="keep", ttl=300.0)
    clock.advance(31)
    expired = hub.sweep_expired_commands()
    log.info("输入: 2 条过期 1 条有效 -> 清扫剔除 %s",
             [c.command_id for c in expired])
    assert sorted(c.command_id for c in expired) == ["cmd-0", "cmd-1"]
    assert all(c.status == CommandStatus.EXPIRED for c in expired)
    assert hub.pending_command_count("dev-1") == 1
    assert len(hub.commands) == 1


def test_ack_expired_command_rejected(hub, clock):
    token = setup_device(hub, clock)
    hub.enqueue_command("dev-1", "cmd-1", CommandKind.COMMAND, {}, 30.0)
    clock.advance(31)
    hub.sweep_expired_commands()
    log.info("输入: 确认已过期指令")
    expect_error(ErrorCode.COMMAND_EXPIRED,
                 lambda: hub.ack_command(token, "dev-1", "cmd-1"),
                 "过期指令不可确认，应为 COMMAND_EXPIRED")


def test_ack_undelivered_command_rejected(hub, clock):
    token = setup_device(hub, clock)
    hub.enqueue_command("dev-1", "cmd-1", CommandKind.COMMAND, {}, 300.0)
    log.info("输入: 确认尚未投递的指令")
    expect_error(ErrorCode.COMMAND_NOT_PENDING,
                 lambda: hub.ack_command(token, "dev-1", "cmd-1"),
                 "QUEUED 未投递不可确认，应为 COMMAND_NOT_PENDING")


def test_per_device_queue_full_distinguishable(hub, clock):
    """单设备超限：QUEUE_FULL，与设备不存在/凭据不可用/重复确认可区分；
    被拒后不留半条记录、不消耗后续额度。"""
    setup_device(hub, clock)
    hub.commands = CommandQueue(per_device_capacity=2, global_capacity=100)
    enqueue_n(hub, "dev-1", 2)
    log.info("输入: 单设备容量 2，再入队第 3 条")
    expect_error(ErrorCode.QUEUE_FULL,
                 lambda: hub.enqueue_command("dev-1", "cmd-x", CommandKind.COMMAND, {}, 300.0),
                 "单设备队列已满，应为 QUEUE_FULL")
    assert hub.pending_command_count("dev-1") == 2  # 无半条记录
    assert len(hub.commands) == 2  # 全局计数未被失败吃掉
    # 确认一条释放额度后，后续入队立即可用（额度未被提前消耗）
    token = hub.registry.get_device("dev-1").credentials[0].token
    hub.deliver_commands("dev-1")
    hub.ack_command(token, "dev-1", "cmd-0")
    hub.enqueue_command("dev-1", "cmd-x", CommandKind.COMMAND, {}, 300.0)
    log.info("判定依据: 释放额度后重试成功，pending=%d",
             hub.pending_command_count("dev-1"))
    assert hub.pending_command_count("dev-1") == 2


def test_global_queue_full(hub, clock):
    setup_device(hub, clock, "dev-1")
    setup_device(hub, clock, "dev-2")
    hub.commands = CommandQueue(per_device_capacity=10, global_capacity=3)
    enqueue_n(hub, "dev-1", 2)
    enqueue_n(hub, "dev-2", 1)
    log.info("输入: 全局容量 3 已满，向 dev-2 再入队")
    expect_error(ErrorCode.QUEUE_FULL,
                 lambda: hub.enqueue_command("dev-2", "cmd-x", CommandKind.COMMAND, {}, 300.0),
                 "全局队列已满，应为 QUEUE_FULL")
    assert len(hub.commands) == 3  # 计数不变，无部分状态


def test_enqueue_unknown_device_and_bad_credential_distinguishable(hub, clock):
    setup_device(hub, clock)
    log.info("输入: 向未注册设备下发")
    expect_error(ErrorCode.UNKNOWN_DEVICE,
                 lambda: hub.enqueue_command("ghost", "c1", CommandKind.COMMAND, {}, 300.0),
                 "目标设备不存在，应为 UNKNOWN_DEVICE（区别于 QUEUE_FULL）")
    hub.enqueue_command("dev-1", "c1", CommandKind.COMMAND, {}, 300.0)
    hub.deliver_commands("dev-1")
    log.info("输入: 凭据不可用确认指令")
    expect_error(ErrorCode.CREDENTIAL_INVALID,
                 lambda: hub.ack_command("bad-token", "dev-1", "c1"),
                 "凭据不可用，应为 CREDENTIAL_INVALID（区别于 QUEUE_FULL）")


def test_ack_cross_device_scope_denied(hub, clock):
    setup_device(hub, clock, "dev-1")
    token2 = setup_device(hub, clock, "dev-2")
    hub.enqueue_command("dev-1", "c1", CommandKind.COMMAND, {}, 300.0)
    hub.deliver_commands("dev-1")
    log.info("输入: dev-2 凭据确认 dev-1 的指令")
    expect_error(ErrorCode.SCOPE_DENIED,
                 lambda: hub.ack_command(token2, "dev-1", "c1"),
                 "跨设备确认，应为 SCOPE_DENIED")


def test_invalid_enqueue_params_rejected(hub, clock):
    setup_device(hub, clock)
    log.info("输入: ttl<=0 / 空 command_id / 非对象 payload")
    expect_error(ErrorCode.MALFORMED_PAYLOAD,
                 lambda: hub.enqueue_command("dev-1", "c1", CommandKind.COMMAND, {}, 0),
                 "ttl_seconds 必须为正")
    expect_error(ErrorCode.MALFORMED_PAYLOAD,
                 lambda: hub.enqueue_command("dev-1", "", CommandKind.COMMAND, {}, 300.0),
                 "command_id 不能为空")
    expect_error(ErrorCode.MALFORMED_PAYLOAD,
                 lambda: hub.enqueue_command("dev-1", "c2", CommandKind.COMMAND, ["x"], 300.0),
                 "payload 必须为对象")
    assert len(hub.commands) == 0  # 全部拒绝，无任何残留


def test_multiple_connections_no_duplicate_effect(hub, clock):
    """同一设备多条连接并发补投：重复投递可能，生效仍只一次。"""
    token = setup_device(hub, clock)
    hub.enqueue_command("dev-1", "c1", CommandKind.COMMAND, {}, 300.0)
    d1 = hub.connect(token)  # 连接 1 补投
    d2 = hub.connect(token)  # 连接 2 再补投（重复投递场景）
    hub.ack_command(token, "dev-1", "c1")
    hub.ack_command(token, "dev-1", "c1")  # 重复确认
    snap = hub.stats.snapshot()
    log.info("判定依据: 双连接补投后 acked=%d（只生效一次）", snap["command/acked"])
    assert d1 == d2 == "dev-1"
    assert snap["command/acked"] == 1
    assert hub.pending_command_count("dev-1") == 0


def test_concurrent_ack_and_sweep_consistent(clock):
    """多条指令并发确认 + 过期清理同时发生：最终状态与计数一致。"""
    hub = DeviceHubService(now_fn=clock)
    hub.rate_limiter = RateLimiter(1e9, 1_000_000, 1e9, 1_000_000)
    hub.registry.register("dev-1", "m", "1.0.0", clock())
    token = hub.registry.issue_credential("dev-1", CMD_SCOPES, clock()).token
    n = 40
    enqueue_n(hub, "dev-1", n, ttl=300.0)
    hub.deliver_commands("dev-1")

    def ack_worker(i: int) -> None:
        for _ in range(3):  # 每条重复确认 3 次
            try:
                hub.ack_command(token, "dev-1", f"cmd-{i}")
            except Exception:  # noqa: BLE001,S112 - 幂等/终态拒绝都允许
                pass

    def sweep_worker() -> None:
        for _ in range(10):
            hub.sweep_expired_commands()  # 未过期，不应剔除任何指令

    threads = ([threading.Thread(target=ack_worker, args=(i,)) for i in range(n)]
               + [threading.Thread(target=sweep_worker) for _ in range(2)])
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    snap = hub.stats.snapshot()
    log.info("输入: %d 条指令并发确认 x3 + 并发清扫 -> acked=%d pending=%d",
             n, snap["command/acked"], hub.pending_command_count("dev-1"))
    assert snap["command/acked"] == n  # 每条恰好生效一次，不重复累加
    assert hub.pending_command_count("dev-1") == 0
    assert len(hub.commands) == 0
    assert "command/expired" not in snap  # 未过期不得被剔除


def test_concurrent_enqueue_never_exceeds_capacity(clock):
    """并发入队：不超过容量上限，成功数 + 拒绝数 = 总数，计数不串设备。"""
    hub = DeviceHubService(now_fn=clock)
    hub.rate_limiter = RateLimiter(1e9, 1_000_000, 1e9, 1_000_000)
    hub.commands = CommandQueue(per_device_capacity=25, global_capacity=25)
    for dev in ("dev-1", "dev-2"):
        hub.registry.register(dev, "m", "1.0.0", clock())
    results = {"ok": 0, "full": 0}
    lock = threading.Lock()

    def worker(dev: str) -> None:
        for i in range(20):
            try:
                hub.enqueue_command(dev, f"{dev}-{i}", CommandKind.COMMAND, {}, 300.0)
                with lock:
                    results["ok"] += 1
            except Exception as exc:  # noqa: BLE001
                from iot_hub.errors import HubError
                assert isinstance(exc, HubError) and exc.code == ErrorCode.QUEUE_FULL
                with lock:
                    results["full"] += 1

    threads = [threading.Thread(target=worker, args=(d,)) for d in ("dev-1", "dev-2")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    log.info("输入: 2 设备 x 20 条并发入队（全局容量 25）-> 成功 %d 拒绝 %d",
             results["ok"], results["full"])
    assert results["ok"] == 25 and results["full"] == 15
    assert len(hub.commands) == 25  # 与全局计数一致，不超上限
    assert (hub.pending_command_count("dev-1")
            + hub.pending_command_count("dev-2") == 25)  # 不跨设备串用
