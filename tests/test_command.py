"""离线指令/配置下发：排队、按序补投、确认幂等、过期剔除、超限与并发一致。

每条用例用 log.info 打印输入与判定依据，便于本地定位。
"""
import logging
import threading

from iot_hub.command import CommandQueue
from iot_hub.errors import ErrorCode, HubError
from iot_hub.models import CommandStatus, Scope
from iot_hub.ratelimit import RateLimiter
from tests.test_auth import expect_error

log = logging.getLogger("test")

CMD_SCOPE = {Scope.SHADOW_READ, Scope.COMMAND_ACK}


def make_queue(per_device=5, global_capacity=20, ttl=1000.0) -> CommandQueue:
    return CommandQueue(per_device_capacity=per_device,
                        global_capacity=global_capacity, default_ttl=ttl)


def register_with_cmd_scope(hub, clock, device_id="dev-1") -> str:
    hub.registry.register(device_id, "sensor-A", "1.0.0", clock())
    return hub.registry.issue_credential(device_id, CMD_SCOPE, clock()).token


# ---------- 离线排队与重连补投 ----------

def test_offline_enqueue_then_fifo_drain_on_reconnect(hub, clock):
    token = register_with_cmd_scope(hub, clock, "dev-1")
    ids = []
    for i in range(3):
        cmd = hub.send_command("dev-1", "config", {"i": i}, f"k{i}")
        assert cmd.status == CommandStatus.QUEUED
        ids.append(cmd.command_id)
    log.info("输入: 设备离线时按序下发 3 条 -> 均 QUEUED，seq=%s",
             [c.seq for c in hub.commands.queued_for("dev-1")])

    assert hub.connect(token) == "dev-1"  # 重连触发补投
    pending = hub.commands.pending_for("dev-1")
    log.info("判定依据: 重连后补投顺序 %s，投递时间 delivered_at=%s",
             [c.command_id[:6] for c in pending], [c.delivered_at for c in pending])
    assert [c.command_id for c in pending] == ids  # 严格按发送先后，不插队
    assert [c.delivery_count for c in pending] == [1, 1, 1]
    assert hub.stats.snapshot()["command/delivered"] == 3
    assert hub.commands.count_active("dev-1") == 3


def test_later_command_never_overtakes(hub, clock):
    register_with_cmd_scope(hub, clock, "dev-1")
    first = hub.send_command("dev-1", "command", {"v": 1}, "a")
    second = hub.send_command("dev-1", "command", {"v": 2}, "b")
    delivered, expired = hub.commands.drain("dev-1", clock())
    log.info("输入: 先发 a 后发 b -> 补投顺序 [%s, %s]",
             delivered[0].idempotency_key, delivered[1].idempotency_key)
    assert [c.idempotency_key for c in delivered] == ["a", "b"]
    assert first.seq < second.seq


def test_no_loss_or_duplicate_after_multiple_reconnects(hub, clock):
    token = register_with_cmd_scope(hub, clock, "dev-1")
    c1 = hub.send_command("dev-1", "config", {"x": 1}, "k1")
    hub.connect(token)                 # 第一次重连：c1 QUEUED -> PENDING
    hub.disconnect("dev-1")
    c2 = hub.send_command("dev-1", "config", {"x": 2}, "k2")  # 离线再发一条
    hub.connect(token)                 # 第二次重连：只补投 c2
    log.info("输入: 两次重连，第二次只补投新指令 -> %s", c2.command_id[:6])
    pending = hub.commands.pending_for("dev-1")
    assert {c.command_id for c in pending} == {c1.command_id, c2.command_id}
    assert hub.stats.snapshot()["command/delivered"] == 2  # 每条恰好投递一次


# ---------- 重复确认只生效一次 ----------

def test_duplicate_ack_applies_once(hub, clock):
    token = register_with_cmd_scope(hub, clock)
    cmd = hub.send_command("dev-1", "config", {"a": 1}, "k1")
    hub.connect(token)
    acked = hub.ack_command(token, "dev-1", cmd.command_id, True)
    assert acked.status == CommandStatus.SUCCEEDED and acked.acked_at == clock()
    log.info("输入: 对同一指令 %s 并发/重复确认多次", cmd.command_id[:6])
    expect_error(ErrorCode.COMMAND_ALREADY_ACKED,
                 lambda: hub.ack_command(token, "dev-1", cmd.command_id, True),
                 "已终态指令重复确认应为 COMMAND_ALREADY_ACKED")
    expect_error(ErrorCode.COMMAND_ALREADY_ACKED,
                 lambda: hub.ack_command(token, "dev-1", cmd.command_id, False),
                 "成功后再确认失败也不得改写终态")
    snap = hub.stats.snapshot()
    log.info("判定依据: succeeded=%d already_acked=%d（终态只生效一次）",
             snap["command/succeeded"], snap["command/command_already_acked"])
    assert snap["command/succeeded"] == 1
    assert hub.commands.get("dev-1", cmd.command_id).status == CommandStatus.SUCCEEDED


def test_unacked_command_not_effective(hub, clock):
    token = register_with_cmd_scope(hub, clock)
    cmd = hub.send_command("dev-1", "config", {"a": 1}, "k1")
    assert cmd.status == CommandStatus.QUEUED
    log.info("输入: 指令未投递/未确认")
    expect_error(ErrorCode.COMMAND_NOT_PENDING,
                 lambda: hub.commands.ack("dev-1", cmd.command_id, True, clock()),
                 "QUEUED 状态确认应为 COMMAND_NOT_PENDING")
    hub.connect(token)
    # PENDING 未确认前不算已执行：仍在待确认集合中
    assert hub.commands.get("dev-1", cmd.command_id).status == CommandStatus.PENDING
    log.info("判定依据: PENDING 未确认，指令未进入终态")


# ---------- 过期剔除 ----------

def test_expired_queued_command_pruned_before_drain(hub, clock):
    register_with_cmd_scope(hub, clock)
    expired_cmd = hub.send_command("dev-1", "config", {"a": 1}, "old", ttl=10.0)
    clock.advance(5)
    fresh_cmd = hub.send_command("dev-1", "config", {"b": 2}, "new", ttl=100.0)
    clock.advance(6)  # old 已过期（11s > 10s），new 仍有效
    delivered, expired = hub.commands.drain("dev-1", clock())
    log.info("输入: 补投时刻 old 过期/new 有效 -> 剔除 %s，投递 %s",
             [c.idempotency_key for c in expired],
             [c.idempotency_key for c in delivered])
    assert [c.command_id for c in expired] == [expired_cmd.command_id]
    assert expired[0].status == CommandStatus.EXPIRED
    assert expired[0].terminal_reason == "expired_on_drain"
    assert [c.command_id for c in delivered] == [fresh_cmd.command_id]
    assert hub.commands.count_active("dev-1") == 1  # 过期者不再占待投递额度


def test_sweep_expired_pending_and_queued(hub, clock):
    register_with_cmd_scope(hub, clock, "dev-1")
    queued = hub.send_command("dev-1", "config", {"a": 1}, "q", ttl=10.0)
    hub.registry.register("dev-2", "sensor-A", "1.0.0", clock())
    tok2 = hub.registry.issue_credential("dev-2", CMD_SCOPE, clock()).token
    hub.registry.set_online("dev-2", True, clock())
    pending = hub.send_command("dev-2", "config", {"a": 1}, "p", ttl=10.0)
    assert pending.status == CommandStatus.PENDING  # 在线下发即投递
    clock.advance(11)
    swept = hub.sweep_expired_commands()
    reasons = {c.command_id: c.terminal_reason for c in swept}
    log.info("输入: 过期清扫 -> 结论 %s", reasons)
    assert reasons[queued.command_id] == "expired_while_queued"
    assert reasons[pending.command_id] == "expired_while_pending"
    expect_error(ErrorCode.COMMAND_EXPIRED,
                 lambda: hub.ack_command(tok2, "dev-2", pending.command_id, True),
                 "过期指令再确认应为 COMMAND_EXPIRED")


# ---------- 队列超限：错误码可区分、拒绝不留痕、不提前吃额度 ----------

def test_per_device_capacity_rejected_distinct(hub, clock):
    register_with_cmd_scope(hub, clock)
    hub.commands._per_device_capacity = 3
    for i in range(3):
        hub.send_command("dev-1", "config", {"i": i}, f"k{i}")
    log.info("输入: 单设备容量 3，下发第 4 条")
    expect_error(ErrorCode.COMMAND_QUEUE_FULL,
                 lambda: hub.send_command("dev-1", "config", {"i": 3}, "k3"),
                 "单设备装满应为 COMMAND_QUEUE_FULL，区别于重复确认/设备不存在/凭据问题")
    assert hub.commands.count_active("dev-1") == 3
    # 被拒的一条没有推进 seq、没有留下记录
    assert all(c.idempotency_key != "k3" for c in hub.commands.queued_for("dev-1"))
    assert hub.commands._seq["dev-1"] == 3
    log.info("判定依据: 待投递=%d seq=%d，失败请求未留痕、未吃额度", 3, 3)


def test_global_capacity_rejected_does_not_starve_later_slot(hub, clock):
    hub.commands = make_queue(per_device=100, global_capacity=2)
    register_with_cmd_scope(hub, clock, "dev-1")
    register_with_cmd_scope(hub, clock, "dev-2")
    hub.send_command("dev-1", "config", {"a": 1}, "k1")
    hub.send_command("dev-2", "config", {"a": 1}, "k2")
    log.info("输入: 全局容量 2 已满，再下发应拒且不跨设备串用")
    expect_error(ErrorCode.COMMAND_QUEUE_FULL,
                 lambda: hub.send_command("dev-1", "config", {"a": 2}, "k3"),
                 "全局装满应为 COMMAND_QUEUE_FULL")
    assert hub.commands.count_active("dev-1") == 1
    assert hub.commands.count_active("dev-2") == 1
    # 确认一条释放额度后，新的下发可成功——失败没有提前吃掉后续额度
    hub.registry.set_online("dev-2", True, clock())
    delivered, _ = hub.commands.drain("dev-2", clock())
    hub.commands.ack("dev-2", delivered[0].command_id, True, clock())
    again = hub.send_command("dev-1", "config", {"a": 3}, "k4")
    log.info("判定依据: 释放 1 个全局额度后新下发 -> status=%s", again.status.value)
    assert again.status in (CommandStatus.QUEUED, CommandStatus.PENDING)


def test_capacity_full_distinct_from_other_errors(hub, clock):
    register_with_cmd_scope(hub, clock)
    hub.commands._per_device_capacity = 1
    hub.send_command("dev-1", "config", {"a": 1}, "k1")
    # 同一批语义不同的拒绝必须能互相区分
    try:
        hub.send_command("dev-1", "config", {"a": 2}, "k2")
    except HubError as exc:
        full_code = exc.code
    try:
        hub.send_command("ghost", "config", {"a": 2}, "kx")
    except HubError as exc:
        unknown_code = exc.code
    try:
        hub.registry.authenticate("bad-token", Scope.COMMAND_ACK, clock())
    except HubError as exc:
        cred_code = exc.code
    log.info("判定依据: 超限=%s 设备不存在=%s 凭据不可用=%s",
             full_code.value, unknown_code.value, cred_code.value)
    assert len({full_code, unknown_code, cred_code}) == 3


# ---------- 幂等下发：重复投递不产生第二条 ----------

def test_idempotent_send_returns_same_command(hub, clock):
    register_with_cmd_scope(hub, clock)
    c1 = hub.send_command("dev-1", "config", {"a": 1}, "same-key")
    c2 = hub.send_command("dev-1", "config", {"a": 999}, "same-key")  # 载荷不同也幂等
    log.info("判定依据: 相同幂等键返回同一 command_id=%s", c1.command_id[:6])
    assert c1.command_id == c2.command_id and c2.payload == {"a": 1}
    assert hub.commands.count_active("dev-1") == 1
    assert hub.stats.snapshot()["command/queued"] == 1  # 不重复计数、不重复占额度


def test_malformed_send_normalized(hub, clock):
    register_with_cmd_scope(hub, clock)
    log.info("输入: 非法 kind / 空 payload / 缺幂等键")
    expect_error(ErrorCode.MALFORMED_PAYLOAD,
                 lambda: hub.send_command("dev-1", "reset", {"a": 1}, "k"),
                 "kind 非法应为 MALFORMED_PAYLOAD")
    expect_error(ErrorCode.MALFORMED_PAYLOAD,
                 lambda: hub.send_command("dev-1", "config", {}, "k"),
                 "空 payload 应为 MALFORMED_PAYLOAD")
    expect_error(ErrorCode.MALFORMED_PAYLOAD,
                 lambda: hub.send_command("dev-1", "config", {"a": 1}, "", ttl=0),
                 "ttl 非正/缺幂等键应为 MALFORMED_PAYLOAD")
    assert hub.commands.count_active("dev-1") == 0  # 全部拒绝，无半条记录


def test_cross_device_ack_scope_denied(hub, clock):
    register_with_cmd_scope(hub, clock, "dev-1")
    token2 = register_with_cmd_scope(hub, clock, "dev-2")
    cmd = hub.send_command("dev-1", "config", {"a": 1}, "k1")
    log.info("输入: dev-2 凭据确认 dev-1 的指令")
    expect_error(ErrorCode.SCOPE_DENIED,
                 lambda: hub.ack_command(token2, "dev-1", cmd.command_id, True),
                 "跨设备确认应为 SCOPE_DENIED，不串用状态")


# ---------- 大批量：重复下发/重复确认在历史回收后仍稳定（本次修复核心） ----------

def test_resend_after_full_batch_acked_returns_original(hub, clock):
    """队列下到接近上限 -> 重连逐条确认完 -> 拿最早一条的去重标识重发：
    必须返回原指令（原 id/原载荷/原 seq），不新建、不改载荷、不重复排队。"""
    hub.rate_limiter = RateLimiter(1e9, 1_000_000, 1e9, 1_000_000)
    token = register_with_cmd_scope(hub, clock, "dev-1")
    n = 100  # 下到单设备上限
    cmds = [hub.send_command("dev-1", "config", {"i": i, "ver": "old"},
                             f"batch-{i}") for i in range(n)]
    first = cmds[0]
    log.info("输入: 设备 dev-1 待投递下到 %d 条（上限），最早一条 key=%s id=%s 载荷=%s",
             n, first.idempotency_key, first.command_id[:6], first.payload)
    hub.connect(token)  # 重连：全部 QUEUED -> PENDING
    for c in cmds:     # 逐条确认完，活动记录全部进入终态并被回收
        hub.ack_command(token, "dev-1", c.command_id, True)
    assert hub.commands.count_active("dev-1") == 0
    log.info("判定依据: 全部确认后活动队列条目=%d（终态已移出活动队列）",
             len(hub.commands._devices.get("dev-1", {})))

    clock.advance(3600)  # 隔多久都一样
    again = hub.send_command("dev-1", "command", {"ver": "NEW"}, "batch-0")  # 换 kind/载荷
    log.info("输入: 隔 3600s 用最早去重标识 batch-0 以新载荷重发 -> id=%s 载荷=%s seq=%d",
             again.command_id[:6], again.payload, again.seq)
    assert again.command_id == first.command_id   # 原来那条
    assert again.payload == {"i": 0, "ver": "old"}  # 载荷不被改写
    assert again.seq == first.seq and again.kind == "config"
    assert again.status == CommandStatus.SUCCEEDED  # 原终态不变
    assert hub.commands.count_active("dev-1") == 0  # 不重新排队
    snap_before = hub.stats.snapshot()
    # 再多重发若干次，任意统计键都不增加
    for _ in range(3):
        hub.send_command("dev-1", "command", {"ver": "NEW"}, "batch-0")
        hub.send_command("dev-1", "command", {"ver": "NEW2"}, "batch-99")
    assert hub.stats.snapshot() == snap_before
    log.info("判定依据: 重发未新建/未改载荷/未重排队，统计快照未变化 %s", snap_before)


def test_repeat_ack_after_history_recycled_still_already_acked(hub, clock):
    """早就确认过、历史活动记录已被回收的旧指令再确认：稳定 COMMAND_ALREADY_ACKED，
    不退化成 NOT_FOUND，不改写终态；与真正没下发过的 command_id 严格区分。"""
    hub.rate_limiter = RateLimiter(1e9, 1_000_000, 1e9, 1_000_000)
    token = register_with_cmd_scope(hub, clock, "dev-1")
    cmds = [hub.send_command("dev-1", "config", {"i": i}, f"k{i}") for i in range(80)]
    oldest = cmds[0]
    hub.connect(token)
    for c in cmds:
        hub.ack_command(token, "dev-1", c.command_id, True)
    log.info("输入: 80 条全部确认（最早 id=%s 早已移出活动队列），再隔很久重复确认",
             oldest.command_id[:6])
    # 直接走队列层验证“记录回收后再确认”的稳定结论，不受凭据有效期干扰
    expect_error(ErrorCode.COMMAND_ALREADY_ACKED,
                 lambda: hub.commands.ack("dev-1", oldest.command_id, False, clock()),
                 "回收后再确认旧指令仍应为 COMMAND_ALREADY_ACKED，不能退化为 NOT_FOUND")
    # 原终态不被失败确认改写
    assert hub.commands.get("dev-1", oldest.command_id).status == CommandStatus.SUCCEEDED
    expect_error(ErrorCode.NOT_FOUND,
                 lambda: hub.commands.ack("dev-1", "never-existed-id", True, clock()),
                 "真正没下发过的 command_id 才是 NOT_FOUND")
    # 隔很久（凭据过期后重签）走完整门面链路，结论依旧
    clock.advance(7200)
    new_token = hub.registry.issue_credential("dev-1", CMD_SCOPE, clock()).token
    expect_error(ErrorCode.COMMAND_ALREADY_ACKED,
                 lambda: hub.ack_command(new_token, "dev-1", oldest.command_id, True),
                 "重签凭据后再确认旧指令仍应为 COMMAND_ALREADY_ACKED")
    snap = hub.stats.snapshot()
    log.info("判定依据: 旧指令再确认 already_acked=%d，succeeded 仍为 %d（不重复累加）",
             snap.get("command/command_already_acked", 0), snap["command/succeeded"])
    assert snap["command/succeeded"] == 80  # 重复确认不重复累加终态计数


def test_resend_same_key_across_devices_never_mixes(hub, clock):
    """同一去重标识在不同设备上互不影响：不跨设备返回别人的指令。"""
    register_with_cmd_scope(hub, clock, "dev-A")
    register_with_cmd_scope(hub, clock, "dev-B")
    ca = hub.send_command("dev-A", "config", {"who": "A"}, "shared-key")
    cb = hub.send_command("dev-B", "config", {"who": "B"}, "shared-key")
    log.info("输入: dev-A/dev-B 用相同去重标识 shared-key 各发一条 -> %s / %s",
             ca.command_id[:6], cb.command_id[:6])
    assert ca.command_id != cb.command_id and ca.payload == {"who": "A"} \
        and cb.payload == {"who": "B"}
    ra = hub.send_command("dev-A", "config", {"who": "X"}, "shared-key")
    rb = hub.send_command("dev-B", "config", {"who": "Y"}, "shared-key")
    assert ra.command_id == ca.command_id and rb.command_id == cb.command_id
    assert ra.payload == {"who": "A"} and rb.payload == {"who": "B"}
    log.info("判定依据: 两设备重发各回本设备原指令，载荷互不串用")


def test_concurrent_resend_while_acks_sweep_and_reconnect(hub, clock):
    """多连接重连、并发确认、过期清扫与同键重发同时发生：
    同一去重标识只受理一次，计数不重复累加，状态不跨设备串用。"""
    hub.rate_limiter = RateLimiter(1e9, 1_000_000, 1e9, 1_000_000)
    token = register_with_cmd_scope(hub, clock, "dev-1")
    n = 40
    cmds = [hub.send_command("dev-1", "config", {"i": i}, f"k{i}") for i in range(n)]
    outcomes: list[str] = []
    returned_ids: set[str] = set()
    lock = threading.Lock()

    def connect_worker() -> None:  # 多连接反复重连补投
        for _ in range(4):
            hub.connect(token)

    def ack_worker() -> None:     # 并发逐条确认，每条重复确认多次
        for c in cmds:
            for _ in range(3):
                try:
                    hub.ack_command(token, "dev-1", c.command_id, True)
                    result = "ok"
                except HubError as exc:
                    result = exc.code.value
                with lock:
                    outcomes.append(result)

    def resend_worker() -> None:  # 与确认/重连并发：用旧键重发，载荷是新的
        for i in range(n):
            c = hub.send_command("dev-1", "command", {"i": i, "new": True}, f"k{i}")
            with lock:
                returned_ids.add(c.command_id)

    def sweep_worker() -> None:
        for _ in range(5):
            hub.sweep_expired_commands()

    threads = [threading.Thread(target=connect_worker),
               threading.Thread(target=ack_worker),
               threading.Thread(target=resend_worker),
               threading.Thread(target=sweep_worker)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    snap = hub.stats.snapshot()
    log.info("输入: %d 条 + 4 连接重连 x 3 次重复确认 + 同键并发重发 + 清扫", n)
    log.info("判定依据: succeeded=%d already_acked=%d 重发返回的不同 id 数=%d",
             snap["command/succeeded"], snap.get("command/command_already_acked", 0),
             len(returned_ids))
    assert snap["command/succeeded"] == n              # 终态计数每条恰好一次
    assert returned_ids == {c.command_id for c in cmds}  # 重发全部命中原指令
    assert len(hub.commands._idem) == n                # 去重索引无重复条目
    # 原载荷不被并发重发改写
    for i, c in enumerate(cmds):
        got = hub.commands.get("dev-1", c.command_id)
        assert got.payload == {"i": i} and got.status == CommandStatus.SUCCEEDED
    assert hub.commands.count_active() == 0


# ---------- 补投过期指令：业务侧可拿到逐条可解释结论 ----------

def test_expired_on_drain_explainable_via_report_and_query(hub, clock):
    token = register_with_cmd_scope(hub, clock, "dev-1")
    c_old1 = hub.send_command("dev-1", "config", {"a": 1}, "old1", ttl=10.0)
    c_old2 = hub.send_command("dev-1", "command", {"a": 2}, "old2", ttl=10.0)
    clock.advance(5)
    c_fresh = hub.send_command("dev-1", "config", {"b": 1}, "fresh", ttl=100.0)
    clock.advance(6)  # old1/old2 过期，fresh 有效
    device_id, delivered, expired = hub.connect_with_report(token)
    log.info("输入: 重连补投时刻 old1/old2 过期、fresh 有效")
    assert device_id == "dev-1"
    assert [c.command_id for c in expired] == [c_old1.command_id, c_old2.command_id]
    assert all(c.terminal_reason == "expired_on_drain" for c in expired)
    assert [c.command_id for c in delivered] == [c_fresh.command_id]

    # 当次结论可查（last_connect_expired），历史流水也可逐条回查（expired_for）
    last = hub.last_connect_expired("dev-1")
    ledger = hub.expired_commands("dev-1")
    log.info("判定依据: 当次补投剔除 %s；过期流水 %s（含 command_id/key/payload/reason）",
             [(c.idempotency_key, c.terminal_reason) for c in last],
             [(c.command_id[:6], c.idempotency_key, c.payload, c.terminal_reason)
              for c in ledger])
    assert [c.idempotency_key for c in last] == ["old1", "old2"]
    assert {c.idempotency_key for c in ledger} == {"old1", "old2"}
    assert all(c.status == CommandStatus.EXPIRED for c in ledger)
    # 后续清扫/再次重连不会让明细丢失或重复登记
    hub.sweep_expired_commands()
    hub.connect_with_report(token)
    ledger2 = hub.expired_commands("dev-1")
    assert [c.idempotency_key for c in ledger2] == ["old1", "old2"]


def test_swept_expired_commands_remain_explainable(hub, clock):
    register_with_cmd_scope(hub, clock, "dev-1")
    q = hub.send_command("dev-1", "config", {"a": 1}, "q", ttl=10.0)
    token2 = register_with_cmd_scope(hub, clock, "dev-2")
    hub.registry.set_online("dev-2", True, clock())  # 在线下发 -> PENDING（投递后未确认）
    p = hub.send_command("dev-2", "config", {"a": 1}, "p", ttl=10.0)
    assert p.status == CommandStatus.PENDING
    clock.advance(11)
    hub.sweep_expired_commands()
    ledger1 = {(c.idempotency_key, c.terminal_reason) for c in hub.expired_commands("dev-1")}
    ledger2 = {(c.idempotency_key, c.terminal_reason) for c in hub.expired_commands("dev-2")}
    log.info("判定依据: 清扫后过期流水 dev-1=%s dev-2=%s", ledger1, ledger2)
    assert ledger1 == {("q", "expired_while_queued")}
    assert ledger2 == {("p", "expired_while_pending")}
    # 过期指令再确认仍是 COMMAND_EXPIRED（从账本稳定给出），且终态不改写
    expect_error(ErrorCode.COMMAND_EXPIRED,
                 lambda: hub.ack_command(token2, "dev-2", p.command_id, True),
                 "已回收的过期指令再确认仍应为 COMMAND_EXPIRED")
    assert hub.commands.get("dev-1", q.command_id).terminal_reason == "expired_while_queued"


# ---------- 并发：多连接、并发确认、清理同时进行 ----------

def test_concurrent_acks_and_sweep_consistent(hub, clock):
    # 排除限速干扰
    hub.rate_limiter = RateLimiter(1e9, 1_000_000, 1e9, 1_000_000)
    token = register_with_cmd_scope(hub, clock)
    n = 20
    cmds = [hub.send_command("dev-1", "config", {"i": i}, f"k{i}") for i in range(n)]
    delivered, _ = hub.commands.drain("dev-1", clock())
    assert len(delivered) == n
    outcomes: list[str] = []
    lock = threading.Lock()

    def ack_worker(command) -> None:
        for _ in range(3):  # 每条指令被 3 个线程重复确认
            try:
                hub.ack_command(token, "dev-1", command.command_id, True)
                result = "ok"
            except HubError as exc:
                result = exc.code.value
            with lock:
                outcomes.append(result)

    def sweep_worker() -> None:
        for _ in range(5):
            hub.sweep_expired_commands()  # 与确认并发；无指令过期，不应误删

    threads = [threading.Thread(target=ack_worker, args=(c,)) for c in cmds]
    threads.append(threading.Thread(target=sweep_worker))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    snap = hub.stats.snapshot()
    log.info("输入: %d 条指令 x 3 次重复确认 + 并发清扫 -> succeeded=%d already_acked=%d",
             n, snap["command/succeeded"], snap.get("command/command_already_acked", 0))
    assert snap["command/succeeded"] == n                      # 每条恰好生效一次
    assert snap.get("command/command_already_acked", 0) == 2 * n
    assert hub.commands.count_active("dev-1") == 0            # 全部进入终态
    for c in cmds:
        assert hub.commands.get("dev-1", c.command_id).status == CommandStatus.SUCCEEDED


def test_concurrent_connect_multi_connection_fifo_consistent(hub, clock):
    hub.rate_limiter = RateLimiter(1e9, 1_000_000, 1e9, 1_000_000)
    token = register_with_cmd_scope(hub, clock)
    for i in range(10):
        hub.send_command("dev-1", "config", {"i": i}, f"k{i}")

    delivered_orders: list[list[str]] = []
    lock = threading.Lock()

    def connect_worker() -> None:
        delivered, _ = hub.commands.drain("dev-1", clock())
        with lock:
            delivered_orders.append([c.idempotency_key for c in delivered])

    threads = [threading.Thread(target=connect_worker) for _ in range(4)]  # 模拟多连接
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    total_delivered = sum(len(o) for o in delivered_orders)
    log.info("输入: 4 条并发连接同时补投 10 条排队指令 -> 各连接投递数 %s",
             [len(o) for o in delivered_orders])
    assert total_delivered == 10                 # 10 条指令合计只首次投递一次
    flat = [k for order in delivered_orders for k in order]
    assert flat == sorted(flat, key=lambda k: int(k[1:]))  # 合起来仍严格按发送先后
    assert hub.stats is not None
    assert hub.commands.count_active("dev-1") == 10       # 全部 PENDING，未确认不终态


def test_concurrent_enqueue_and_ack_no_cross_device(hub, clock):
    hub.rate_limiter = RateLimiter(1e9, 1_000_000, 1e9, 1_000_000)
    tokens = {}
    for d in ("dev-A", "dev-B"):
        hub.registry.register(d, "m", "1.0.0", clock())
        tokens[d] = hub.registry.issue_credential(d, CMD_SCOPE, clock()).token
    hub.registry.set_online("dev-A", True, clock())
    hub.registry.set_online("dev-B", True, clock())

    def worker(dev: str) -> None:
        for i in range(20):
            cmd = hub.send_command(dev, "config", {"i": i}, f"{dev}-{i}")
            # 在线即 PENDING，直接确认
            hub.ack_command(tokens[dev], dev, cmd.command_id, True)

    threads = [threading.Thread(target=worker, args=(d,)) for d in ("dev-A", "dev-B")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    snap = hub.stats.snapshot()
    log.info("判定依据: 两设备并发下发+确认 succeeded=%d delivered=%d，无跨设备串用",
             snap["command/succeeded"], snap["command/delivered"])
    assert snap["command/succeeded"] == 40
    assert hub.commands.count_active() == 0
