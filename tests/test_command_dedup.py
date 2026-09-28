"""批量重复下发 / 历史回收后重复确认 / 补投过期结论查询 / 并发一致性回归。

针对“量级上去之后”才暴露的问题：
- 一台设备近上限批量下发 -> 重连逐条确认 -> 终态记录被活动队列回收后，
  拿最早那条的去重标识重发，必须返回原指令而不是新建；
- 早就确认过、且历史记录已被回收的指令再确认，必须稳定表示“已确认过”，
  不能退化成 NOT_FOUND；
- 重连补投剔除的过期指令，业务侧要拿到“具体哪一条、什么原因”，而不是汇总数量；
- 多连接重连、并发确认、过期清扫、同键重发同时发生时，上述保证同样成立。

每条用例用 log.info 打印输入与判定依据，便于本地定位。
"""
import logging
import threading

from iot_hub.errors import ErrorCode, HubError
from iot_hub.models import CommandStatus, Scope
from iot_hub.ratelimit import RateLimiter
from tests.test_auth import expect_error
from tests.test_command import register_with_cmd_scope

log = logging.getLogger("test")


def test_batch_resend_after_all_acked_and_pruned_returns_original(hub, clock):
    """缺陷复现：近上限批量下发、逐条确认、历史回收后，最早的键重发仍是原指令。"""
    # 活动队列终态只保留最近 2 条：确认几条后最早的记录必被回收
    from iot_hub.command import CommandQueue
    hub.commands = CommandQueue(
        per_device_capacity=100, global_capacity=10000, terminal_keep=2)
    token = register_with_cmd_scope(hub, clock)
    n = 20
    first_payload = {"v": "original", "n": 0}
    cmds = [hub.send_command("dev-1", "config",
                             first_payload if i == 0 else {"v": i}, f"k{i}")
            for i in range(n)]
    first = cmds[0]
    log.info("输入: 批量下发 %d 条（近上限），最早一条 key=k0 id=%s payload=%s",
             n, first.command_id[:6], first.payload)

    hub.connect(token)  # 重连补投全部
    for c in cmds:     # 逐条确认完
        hub.ack_command(token, "dev-1", c.command_id, True)
    log.info("判定依据: 全部确认后活动队列内容数=%d（终态仅保留最近 2 条，最早已回收）",
             len(hub.commands._devices.get("dev-1", {})))
    # 直接确认最早那条确实已经不在活动队列里（但必须仍可在终态台账查到）
    assert first.command_id not in hub.commands._devices.get("dev-1", {})

    # 业务侧拿最早那条的去重标识、用全新载荷重发
    resent = hub.send_command("dev-1", "command", {"v": "TAMPERED-NEW"}, "k0")
    log.info("重发 k0（载荷被换成 %s）-> id=%s status=%s payload=%s",
             {"v": "TAMPERED-NEW"}, resent.command_id[:6],
             resent.status.value, resent.payload)

    assert resent.command_id == first.command_id      # 返回原来那条，不新建
    assert resent is hub.commands.get("dev-1", first.command_id)
    assert resent.payload == first_payload            # 载荷不被新传内容改写
    assert resent.status is CommandStatus.SUCCEEDED   # 原终态不变，不会再执行
    assert resent.seq == first.seq                    # 不重新排队、不占新序号
    assert hub.commands.count_active("dev-1") == 0    # 不重复排队
    assert hub.commands.queued_for("dev-1") == []
    snap = hub.stats.snapshot()
    assert snap["command/queued"] == n                # 计数不重复累加
    assert snap["command/delivered"] == n
    assert snap["command/succeeded"] == n
    log.info("判定依据: 重发后 queued/delivered/succeeded 仍各=%d，active=0", n)


def test_resend_long_after_pruned_still_original_with_more_traffic(hub, clock):
    """回收之后又有新流量把窗口继续往前推，旧键重发依旧是原指令（隔多久都行）。"""
    from iot_hub.command import CommandQueue
    hub.commands = CommandQueue(per_device_capacity=100, global_capacity=10000,
                                terminal_keep=1)
    token = register_with_cmd_scope(hub, clock)
    old = hub.send_command("dev-1", "config", {"step": 1}, "old-key")
    hub.connect(token)
    hub.ack_command(token, "dev-1", old.command_id, True)
    # 再来 5 轮“下发-确认”，把保留窗口反复推远
    for i in range(5):
        c = hub.send_command("dev-1", "config", {"step": i + 2}, f"later-{i}")
        hub.commands.drain("dev-1", clock())
        hub.ack_command(token, "dev-1", c.command_id, True)
    log.info("输入: old-key 确认后又流过 5 条指令，活动队列=%s",
             list(hub.commands._devices.get("dev-1", {}).keys())[:4])

    again = hub.send_command("dev-1", "config", {"step": 999}, "old-key")
    assert again.command_id == old.command_id
    assert again.payload == {"step": 1}
    assert again.status is CommandStatus.SUCCEEDED
    log.info("判定依据: 5 轮流量后重发 old-key -> 原 id=%s 原载荷=%s 终态=%s",
             again.command_id[:6], again.payload, again.status.value)


def test_duplicate_ack_after_pruned_stays_already_acked_never_not_found(hub, clock):
    """缺陷复现：早就确认过、历史被回收后再确认，稳定 COMMAND_ALREADY_ACKED。"""
    from iot_hub.command import CommandQueue
    hub.commands = CommandQueue(per_device_capacity=100, global_capacity=10000,
                                terminal_keep=1)
    token = register_with_cmd_scope(hub, clock)
    target = hub.send_command("dev-1", "config", {"a": 1}, "k0")
    others = [hub.send_command("dev-1", "config", {"a": i + 1}, f"k{i+1}")
              for i in range(3)]
    hub.connect(token)
    hub.ack_command(token, "dev-1", target.command_id, True)
    for c in others:  # 确认后续指令，把 target 挤出活动队列保留窗口
        hub.ack_command(token, "dev-1", c.command_id, True)
    assert target.command_id not in hub.commands._devices.get("dev-1", {})
    log.info("输入: 指令 %s 早已确认且已被回收，再次确认（还带相反 success=False）",
             target.command_id[:6])

    # 不能退化成 NOT_FOUND；必须是“已确认过”，且不改写原终态
    expect_error(ErrorCode.COMMAND_ALREADY_ACKED,
                 lambda: hub.ack_command(token, "dev-1", target.command_id, False),
                 "回收后重复确认应为 COMMAND_ALREADY_ACKED，不能是 NOT_FOUND")
    expect_error(ErrorCode.COMMAND_ALREADY_ACKED,
                 lambda: hub.ack_command(token, "dev-1", target.command_id, True),
                 "再确认多少次结论都一致")
    assert hub.commands.get("dev-1", target.command_id).status \
        is CommandStatus.SUCCEEDED
    snap = hub.stats.snapshot()
    assert snap["command/succeeded"] == 4            # 原终态统计不被重复累加
    assert "command/not_found" not in snap
    log.info("判定依据: succeeded=%d（无 not_found），原指令终态仍为 SUCCEEDED",
             snap["command/succeeded"])

    # 与真正从未下发过的 command_id 必须可区分
    expect_error(ErrorCode.NOT_FOUND,
                 lambda: hub.commands.ack("dev-1", "never-existed", True, clock()),
                 "从未受理的指令才是 NOT_FOUND")


def test_failed_terminal_after_pruned_keeps_original_failure(hub, clock):
    """FAILED 的指令回收后重复确认同样稳定，且不能被成功确认翻案。"""
    from iot_hub.command import CommandQueue
    hub.commands = CommandQueue(per_device_capacity=100, global_capacity=10000,
                                terminal_keep=1)
    token = register_with_cmd_scope(hub, clock)
    target = hub.send_command("dev-1", "config", {"a": 1}, "k0")
    push = [hub.send_command("dev-1", "config", {"a": i + 1}, f"k{i+1}")
            for i in range(2)]
    hub.connect(token)
    hub.ack_command(token, "dev-1", target.command_id, False)  # 设备确认失败
    for c in push:
        hub.ack_command(token, "dev-1", c.command_id, True)
    expect_error(ErrorCode.COMMAND_ALREADY_ACKED,
                 lambda: hub.ack_command(token, "dev-1", target.command_id, True),
                 "FAILED 终态回收后再确认成功也不得改写")
    assert hub.commands.get("dev-1", target.command_id).status is CommandStatus.FAILED
    log.info("判定依据: FAILED 指令回收后重确仍 COMMAND_ALREADY_ACKED，终态保持 FAILED")


def test_drain_expired_conclusions_are_explainable_and_queryable(hub, clock):
    """缺陷复现：补投剔除的过期指令要能查到具体哪一条、什么原因，而非只有汇总数。"""
    register_with_cmd_scope(hub, clock)
    old1 = hub.send_command("dev-1", "config", {"n": 1}, "exp-1", ttl=10.0)
    old2 = hub.send_command("dev-1", "command", {"n": 2}, "exp-2", ttl=5.0)
    fresh = hub.send_command("dev-1", "config", {"n": 3}, "keep", ttl=100.0)
    clock.advance(11)  # old1/old2 均过期，fresh 仍有效
    delivered, expired = hub.commands.drain("dev-1", clock())
    log.info("输入: 补投时 %d 条过期、%d 条有效；过期 ids=%s",
             len(expired), len(delivered), [c.idempotency_key for c in expired])
    assert [c.command_id for c in delivered] == [fresh.command_id]

    conclusions = hub.commands.expired_for("dev-1")
    log.info("判定依据: 过期结论台账 %s",
             [(c.idempotency_key, c.reason, c.expired_at) for c in conclusions])
    assert len(conclusions) == 2  # 不再只是汇总数量
    by_id = {c.command_id: c for c in conclusions}
    for old in (old1, old2):
        concl = by_id[old.command_id]
        assert concl.reason == "expired_on_drain"       # 原因可解释
        assert concl.device_id == "dev-1"
        assert concl.seq == old.seq
        assert concl.expired_at == clock()
        assert concl.expires_at == old.expires_at
        # 指令本身也处于 EXPIRED 终态，不会被补投/执行
        assert hub.commands.get("dev-1", old.command_id).status is CommandStatus.EXPIRED

    # 门面查询同样可拿到（设备不存在仍明确拒绝）
    via_service = hub.expired_command_conclusions("dev-1")
    assert [c.command_id for c in via_service] == [old1.command_id, old2.command_id]
    expect_error(ErrorCode.__members__["UNKNOWN_DEVICE"],
                 lambda: hub.expired_command_conclusions("ghost"),
                 "查询不存在设备的过期结论应为 UNKNOWN_DEVICE")

    # limit 取最近 N 条
    recent = hub.commands.expired_for("dev-1", limit=1)
    assert [c.command_id for c in recent] == [old2.command_id]


def test_sweep_expired_conclusions_recorded_with_distinct_reasons(hub, clock):
    """清扫路径（从未投递 / 投递后未确认）同样落可解释结论。"""
    register_with_cmd_scope(hub, clock, "dev-1")
    q = hub.send_command("dev-1", "config", {"a": 1}, "q", ttl=10.0)
    hub.registry.register("dev-2", "m", "1.0.0", clock())
    tok2 = hub.registry.issue_credential(
        "dev-2", {Scope.SHADOW_READ, Scope.COMMAND_ACK}, clock()).token
    hub.registry.set_online("dev-2", True, clock())
    p = hub.send_command("dev-2", "config", {"a": 1}, "p", ttl=10.0)
    clock.advance(11)
    hub.sweep_expired_commands()
    c1 = {c.command_id: c for c in hub.commands.expired_for("dev-1")}
    c2 = {c.command_id: c for c in hub.commands.expired_for("dev-2")}
    log.info("判定依据: 清扫结论 dev-1=%s dev-2=%s",
             [(k, v.reason) for k, v in c1.items()],
             [(k, v.reason) for k, v in c2.items()])
    assert c1[q.command_id].reason == "expired_while_queued"
    assert c2[p.command_id].reason == "expired_while_pending"
    # 过期指令回收判别：再确认是 COMMAND_EXPIRED，而不是 NOT_FOUND
    expect_error(ErrorCode.COMMAND_EXPIRED,
                 lambda: hub.ack_command(tok2, "dev-2", p.command_id, True),
                 "过期后再确认应为 COMMAND_EXPIRED")


def test_idempotency_never_crosses_devices(hub, clock):
    """同键不同设备各自独立；一台回收历史不影响另一台，绝不串用。"""
    from iot_hub.command import CommandQueue
    hub.commands = CommandQueue(per_device_capacity=100, global_capacity=10000,
                                terminal_keep=1)
    t1 = register_with_cmd_scope(hub, clock, "dev-1")
    t2 = register_with_cmd_scope(hub, clock, "dev-2")
    c1 = hub.send_command("dev-1", "config", {"d": 1}, "shared")
    c2 = hub.send_command("dev-2", "config", {"d": 2}, "shared")
    assert c1.command_id != c2.command_id
    hub.connect(t1)
    hub.connect(t2)
    hub.ack_command(t1, "dev-1", c1.command_id, True)
    hub.ack_command(t2, "dev-2", c2.command_id, True)
    # dev-1 用更多流量把 shared 挤出活动队列
    for i in range(3):
        c = hub.send_command("dev-1", "config", {"x": i}, f"x{i}")
        hub.commands.drain("dev-1", clock())
        hub.ack_command(t1, "dev-1", c.command_id, True)
    r1 = hub.send_command("dev-1", "config", {"d": 999}, "shared")
    r2 = hub.send_command("dev-2", "config", {"d": 999}, "shared")
    log.info("判定依据: 同键跨设备重发 -> dev-1 原 id=%s，dev-2 原 id=%s",
             r1.command_id[:6], r2.command_id[:6])
    assert r1.command_id == c1.command_id and r1.payload == {"d": 1}
    assert r2.command_id == c2.command_id and r2.payload == {"d": 2}
    assert hub.commands.expired_for("dev-1") == []  # 无过期串台


def test_concurrent_drain_ack_sweep_resend_consistency(hub, clock):
    """多连接补投、并发确认、过期清扫、同键重发同时进行：终身保证不被破坏。"""
    hub.rate_limiter = RateLimiter(1e9, 1_000_000, 1e9, 1_000_000)
    from iot_hub.command import CommandQueue
    hub.commands = CommandQueue(per_device_capacity=1000, global_capacity=100000,
                                terminal_keep=4)
    token = register_with_cmd_scope(hub, clock)  # noqa: PLW0104
    n = 40
    cmds = [hub.send_command("dev-1", "config", {"i": i}, f"k{i}") for i in range(n)]
    first = cmds[0]
    hub.connect(token)  # 首次补投走门面：delivered 统计恰为 n
    barrier = threading.Barrier(4)
    errors: list[str] = []
    elock = threading.Lock()

    def run(fn):
        try:
            barrier.wait()
            fn()
        except Exception as exc:  # noqa: BLE001 - 记录下来统一断言
            with elock:
                errors.append(f"{type(exc).__name__}: {exc}")

    def drains():
        for _ in range(3):  # 模拟多连接反复重连补投
            hub.commands.drain("dev-1", clock())

    def acks():
        for c in cmds:
            for _ in range(2):  # 每条被重复确认
                try:
                    hub.ack_command(token, "dev-1", c.command_id, True)
                except HubError as exc:
                    assert exc.code is ErrorCode.COMMAND_ALREADY_ACKED

    def sweeps():
        for _ in range(10):
            hub.sweep_expired_commands()  # ttl 很长，不应误杀

    def resends():
        # 与确认/清扫并发地反复重发最早的键
        for _ in range(20):
            got = hub.send_command("dev-1", "command", {"i": -1}, "k0")
            assert got.command_id == first.command_id
            assert got.payload == {"i": 0}

    threads = [threading.Thread(target=run, args=(f,))
               for f in (drains, acks, sweeps, resends)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors

    snap = hub.stats.snapshot()
    log.info("输入: %d 条 + 3 轮多连接补投 + 每指令重复确认 + 10 轮清扫 + 20 次同键重发",
             n)
    log.info("判定依据: delivered=%d succeeded=%d already_acked=%d active=%d",
             snap.get("command/delivered", 0), snap["command/succeeded"],
             snap.get("command/command_already_acked", 0),
             hub.commands.count_active("dev-1"))
    assert snap["command/succeeded"] == n                 # 每条恰好生效一次
    assert snap.get("command/delivered", 0) == n          # 只首次投递一次
    assert snap.get("command/expired", 0) == 0            # 未过期，零误杀
    assert hub.commands.count_active("dev-1") == 0
    final = hub.send_command("dev-1", "command", {"i": -1}, "k0")
    assert final.command_id == first.command_id           # 风暴后仍是原指令
    assert final.status is CommandStatus.SUCCEEDED
    # 最早那条回收后重复确认仍是“已确认过”，不是 NOT_FOUND
    expect_error(ErrorCode.COMMAND_ALREADY_ACKED,
                 lambda: hub.ack_command(token, "dev-1", first.command_id, True),
                 "并发风暴后历史指令重复确认仍应稳定为 ALREADY_ACKED")


def test_concurrent_first_send_same_key_creates_exactly_one(hub, clock):
    """同一新键首并发：恰有一条新建，其余全部幂等命中，计数不重复累加。"""
    hub.rate_limiter = RateLimiter(1e9, 1_000_000, 1e9, 1_000_000)
    register_with_cmd_scope(hub, clock)
    results: list = []
    rlock = threading.Lock()
    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()
        cmd, created = hub.commands.enqueue(
            "dev-1", "config", {"w": 1}, "race-key", clock())
        with rlock:
            results.append((cmd.command_id, created))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    ids = {cid for cid, _ in results}
    created = sum(1 for _, c in results if c)
    log.info("判定依据: 8 线程首并发同键 -> 不同 id 数=%d 新建数=%d", len(ids), created)
    assert len(ids) == 1 and created == 1
    assert hub.commands.count_active("dev-1") == 1  # 直接调底层，只看队列状态
