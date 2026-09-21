# IoT Device Hub (Python)

一套**完全本地可运行**的 IoT 设备接入平台：设备注册与鉴权、遥测接入、设备影子、
OTA 分发。不依赖外部消息代理、云端服务或真实设备，全部状态在进程内维护，
接口层为 FastAPI，业务逻辑在 `iot_hub/` 包内，可独立单测。

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
  `shadow:write`、`ota:ack`。每个受保护操作先限流、再鉴权、再校验设备归属。
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
   凭据吊销与轮换宽限、升级失败回退、确认超时、离线排队、并发接入与限流；
   每条用例日志打印输入与判定依据。
2. **HTTP 冒烟**：`uv run uvicorn main:app` 后用 curl 走注册 → 连接 → 遥测 →
   影子 → OTA 全链路（`tests/test_api.py` 即为例证）。
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
  ratelimit.py  # 令牌桶限速 + 有界积压
  service.py    # 门面：组合子系统、统计计数
main.py         # FastAPI 接口层与错误映射
tests/          # 以异常链路为主的测试
```
