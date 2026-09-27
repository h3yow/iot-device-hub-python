# IoT Device Hub (Python)

一套**完全本地可运行**的 IoT 设备接入平台：设备注册与鉴权、遥测接入、设备影子、
OTA 分发、**离线指令/配置下发**。不依赖外部消息代理、云端服务或真实设备，全部状态
在进程内维护，接口层为 FastAPI，业务逻辑在 `iot_hub/` 包内，可独立单测。

## 快速开始

```bash
uv sync                 # 安装依赖
uv run pytest tests/ -v # 运行全部测试（日志打印输入与判定依据）
uv run uvicorn main:app # 启动本地服务，默认 http://127.0.0.1:8000
```

本地验证一条完整链路（无需任何真实设备）：

```bash
# 1. 注册设备并签发凭据
curl -s -X POST localhost:8000/devices -H 'content-type: application/json' -d \
  '{"device_id":"dev-1","model":"sensor-A","firmware_version":"1.0.0"}'
# 2. 用返回的 token 连接、上报遥测、写影子、参与 OTA（见下文接口）
```

## 接入与鉴权模型

- **注册**：`POST /devices` 登记 `device_id / model / firmware_version`，重复注册返回 `CONFLICT`。
- **凭据**：注册即签发 token，携带授权范围（scope）：`telemetry:write`、`shadow:read`、
  `shadow:write`、`ota:ack`、`command:ack`。每个受保护操作先限流、再鉴权、再校验设备归属。
- **吊销**：`POST /devices/{id}/credentials/revoke` 立即生效，之后该 token 一律
  返回 `CREDENTIAL_REVOKED`（与 `CREDENTIAL_INVALID` / `CREDENTIAL_EXPIRED` 可区分）。
- **轮换宽限（确定语义）**：`POST /devices/{id}/credentials` 轮换后，旧凭据有效窗口
  收窄为 `min(原过期时间, 轮换时刻 + 300s 宽限)`；宽限结束后按 `CREDENTIAL_EXPIRED`
  拒绝；宽限期内显式吊销优先于宽限，按 `CREDENTIAL_REVOKED` 拒绝。
- **越权**：凭据缺少所需 scope 返回 `SCOPE_DENIED`；用 A 设备凭据操作 B 设备资源
  同样返回 `SCOPE_DENIED`，杜绝跨设备串用。

## 遥测语义

每条遥测携带 `message_id`（幂等标识）、`seq`（设备侧单调序号）、`timestamp`（采样时间）。
按序判定，拒绝原因可区分：

| 情形 | 结论 |
| --- | --- |
| 协议版本 ≠ 1 | `PROTOCOL_VERSION_UNSUPPORTED` |
| 缺字段 / 类型错误 | `MALFORMED_PAYLOAD` |
| `message_id` 已处理 | `DUPLICATE_MESSAGE`，不重复生效、不重复计数 |
| `seq` ≤ 已应用最大序号 | `STALE_SEQUENCE`，不覆盖较新状态 |
| 采样时间超出 `[now-3600s, now+300s]` | `EXPIRED_DATA` |
| 全部通过 | `applied`：记录幂等标识、推进序号、更新最新状态，一次性提交 |

**断线续传**：设备重连后重发已送达消息得到 `DUPLICATE_MESSAGE`，补发未送达消息按序
生效——不丢、不重。去重表容量有界（默认 10000，FIFO 淘汰）。

## 设备影子语义

- 每台设备一份 `{reported, desired, version, fresh}`，持久保存于进程内。
- `version` **单调递增**：每次生效写入恰好 +1，不倒退、不重复计数。
- 写入为"校验 → 副本合并 → 原子替换"，任何失败路径不留半更新。
- 可选 `base_version` 乐观并发控制：不匹配返回 `VERSION_CONFLICT`，调用方读新版本重试；
  并发写同一版本时恰有一个生效。
- **新鲜度**：设备在线写入的影子 `fresh=true`；设备断连后状态保留但 `fresh=false`，
  业务侧不得将陈旧值当作最新值参与判定。标记不改变版本号。

## OTA 语义

- **批次任务**：`POST /ota/campaigns` 指定 `model / target_version / batch_size / ack_timeout`。
- **下发**：型号不匹配或设备已处于目标版本 → `INCOMPATIBLE_FIRMWARE` 明确拒绝；
  设备离线或当前批次已满 → `QUEUED` 排队；否则 `PENDING` 并启动确认计时。
  重复下发幂等，返回同一任务。
- **状态机**：`QUEUED → PENDING → SUCCEEDED / FAILED / ROLLED_BACK`。
  - 设备确认成功（`SUCCEEDED`）**才**推进设备固件版本；未确认绝不视为已生效。
  - 设备上报失败（`FAILED`）：固件保持升级前的可用版本（回退结论）。
  - 确认超时（`ROLLED_BACK`）：`POST /ota/sweep` 或定时清扫强制回退，固件不变。
  - 终态任务再确认 → `OTA_NOT_PENDING`。
- **重连提升**：离线排队设备上线后 `POST /ota/campaigns/{id}/promote` 按批次空位
  提升为 `PENDING`。

## 离线指令/配置下发语义

设备不在线时，业务侧仍可把配置/指令交给平台；内容进入**该设备自己的**待投递队列，
设备重连时严格按发送先后补投。能力在 `iot_hub/command.py`，HTTP 接口如下：

- **下发**：`POST /devices/{id}/commands`，字段 `kind`（`config`/`command`）、
  `payload`（非空对象）、`idempotency_key`（必填，幂等标识）、可选 `ttl`（秒，默认
  86400）。设备离线 -> `QUEUED`；设备在线 -> 直接 `PENDING`（已投递）。
- **重连补投**：`POST /connect` 成功后自动执行——先剔除已过期的 `QUEUED`，再把
  其余排队指令按 `seq`（单设备单调递增，即发送先后）逐条置 `PENDING`，
  **后发的不能插队、不漏投、不重复投递**。
- **确认**：`POST /commands/ack`（需设备凭据的 `command:ack` scope，默认随注册签发）。
  未确认**绝不视为已执行**。
- **查询**：`GET /devices/{id}/commands` 返回 `queued` 与 `pending` 两列；
  `POST /commands/sweep` 清扫过期指令。

**状态机**：`QUEUED -> PENDING -> SUCCEEDED / FAILED / EXPIRED`

| 情形 | 结论 |
| --- | --- |
| 同一条指令重复确认 / 已终态再确认 | `COMMAND_ALREADY_ACKED`，状态与统计只生效一次 |
| 对 `QUEUED`（尚未投递）的指令确认 | `COMMAND_NOT_PENDING` |
| 补投前已过期的排队指令 | `EXPIRED`，结论 `expired_on_drain`，从待投递剔除 |
| 清扫发现的过期指令 | `expired_while_queued`（从未投递）/ `expired_while_pending`（投递后未确认） |
| 过期指令再确认 | `COMMAND_EXPIRED` |
| 单设备队列满（默认 100）/ 全局满（默认 10000） | `COMMAND_QUEUE_FULL`（HTTP 429），不静默丢弃 |
| 目标设备未注册 | `UNKNOWN_DEVICE`（404），与超限/凭据问题可区分 |
| 凭据缺失/无效/过期/越权 | 既有 `CREDENTIAL_*` / `SCOPE_DENIED`，与队列结论可区分 |
| 字段缺失 / 类型不对 / 取值非法 | `MALFORMED_PAYLOAD`（400），不透出框架校验细节或原始请求 |

**容量与一致性约束**：

- 双上限（单设备 + 全局）都按"未走完的指令"（`QUEUED`+`PENDING`）计数；任一装满即
  拒绝，拒绝在分配序号/写状态**之前**完成——不留半条记录、不推进 `seq`、不提前吃掉
  后续可用额度；腾出额度后新的下发立即可成功。
- `idempotency_key` 相同的重复下发返回同一条指令，不产生第二条、不重复占额度/计数。
- 同一设备挂多条连接、多条指令并发确认、过期清扫与重连补投同时发生时，所有状态变更
  在同一把队列锁内"校验-提交"原子完成：每条指令最终状态一致、每台设备剩余待投递数
  一致、`GET /stats` 中 `command/*` 计数不重复累加、不跨设备串用。
- 终态记录保留最近 64 条/设备（超出回收），既保证内存有界，又能把"重复确认"与
  "指令不存在（`NOT_FOUND`）"区分开。

### 与设备影子 / OTA 的关系与能力边界

- 三者**互补、状态各自独立**：影子表达"设备期望/上报的**最新状态**"（同字段后写覆盖
  前写，只关心终值）；离线指令表达"**有先后顺序的一次性动作/配置批次**"（必须按序、
  必须确认、过期即失效）；OTA 是面向固件的**分批升级**状态机（带批次空位与回退）。
  离线指令不修改影子版本、不推进固件版本。
- 典型用法：用离线指令把"请应用这份配置"作为一次有序操作补投给设备，设备执行后再把
  结果写回影子 `reported`——顺序与确认由指令通道保证，最新值由影子承载。
- 边界：指令在进程内内存中保存，**进程重启不保留**（与影子/OTA 的既有定位一致）；
  不做服务端主动推送（无长连接），补投发生在设备 `connect` 时刻；已投递未确认
  （`PENDING`）的指令不会被重复计为新投递，可通过 `GET .../commands` 的 `pending`
  在新连接上重推，设备凭 `command_id` 去重确认。

## 限流与积压

- **令牌桶双维度**：每设备（默认 10/s、突发 20）+ 全局（默认 1000/s、突发 2000），
  任一超限返回 `RATE_LIMITED`；被拒请求不消耗另一维度令牌、不写任何业务状态。
- **积压上限**：`Backlog` 容量固定，满时返回 `BACKLOG_FULL`；去重表、设备桶表均有
  容量上限——任何情况下内存有界。
- 失败操作（鉴权失败、版本冲突、限流等）在状态提交前抛出，**无部分状态残留**。

## 错误归一化

所有失败统一为 `HubError(code, message)`，HTTP 层映射为
`{"error": {"code": ..., "message": ...}}` 与对应状态码
（401/403/404/409/422/429/500）。底层异常一律归一化为 `INTERNAL`，
堆栈、路径等内部细节不透出接口。完整 code 列表见 `iot_hub/errors.py`。

## 并发一致性

- 每个子系统内部持锁，状态变更"校验-提交"原子完成；
- 统计计数（`GET /stats`）仅在操作最终生效/拒绝时各累加一次，并发下不重复累加；
- 大量设备并发接入时，鉴权结论、影子版本、OTA 进度、统计计数各自一致，
  无跨设备状态串用（见 `tests/test_concurrency.py`）。

## 本地验证方法

无需外部消息代理、云端服务或真实设备：

1. **单元/集成测试**：`uv run pytest tests/ -v -s`，覆盖重复上报、乱序与过期数据、
   凭据吊销与轮换宽限、升级失败回退、确认超时、离线排队、并发接入与限流；以及离线
   指令的排队与重连按序补投、重复确认只生效一次、过期剔除、单设备/全局超限、
   并发确认/清理/多连接一致性（见 `tests/test_command.py`）；每条用例日志打印输入
   与判定依据。
2. **HTTP 冒烟**：`uv run uvicorn main:app` 后用 curl 走注册 → 连接 → 遥测 →
   影子 → OTA 全链路（`tests/test_api.py` 即为例证）。离线指令本地冒烟（无需真实
   设备，先在设备**未连接**时下发，再 connect 观察自动补投与确认）：

   ```bash
   # 注册（返回 token，默认含 command:ack）
   TOK=$(curl -s -X POST localhost:8000/devices -H 'content-type: application/json' \
     -d '{"device_id":"dev-1","model":"sensor-A","firmware_version":"1.0.0"}' \
     | python3 -c 'import sys,json;print(json.load(sys.stdin)["credential"]["token"])')
   # 设备离线时下发两条（按序排队）
   curl -s -X POST localhost:8000/devices/dev-1/commands -H 'content-type: application/json' \
     -d '{"kind":"config","idempotency_key":"k1","payload":{"led":1},"ttl":300}'
   curl -s -X POST localhost:8000/devices/dev-1/commands -H 'content-type: application/json' \
     -d '{"kind":"command","idempotency_key":"k2","payload":{"reboot":true},"ttl":300}'
   # 重连：自动按 k1 -> k2 补投
   curl -s -X POST localhost:8000/connect -H 'content-type: application/json' \
     -d "{\"token\":\"$TOK\"}"
   curl -s localhost:8000/devices/dev-1/commands   # 两条均在 pending
   ```
3. 测试使用注入时钟（`DeviceHubService(now_fn=...)`），超时/过期场景确定性复现。

## 目录结构

```
iot_hub/
  errors.py     # 统一错误码与 HubError
  models.py     # 纯数据模型
  auth.py       # 注册、凭据签发/吊销/轮换、鉴权
  telemetry.py  # 幂等、序号、过期判定
  shadow.py     # 影子：单调版本、原子提交、新鲜度
  ota.py        # 分批下发、确认、回退、离线排队
  command.py    # 离线指令/配置：FIFO 待投递队列、按序补投、确认幂等、有效期/容量
  ratelimit.py  # 令牌桶限速 + 有界积压
  service.py    # 门面：组合子系统、统计计数
main.py         # FastAPI 接口层与错误映射
tests/          # 以异常链路为主的测试
```
