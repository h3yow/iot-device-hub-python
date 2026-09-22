# 本地 IoT 设备接入平台（Local IoT Device Hub）

一套**完全本地、零外部依赖**（仅 FastAPI，无需消息代理/云服务/真实设备）的 IoT 设备接入平台，
覆盖：设备注册与鉴权、带幂等/序号语义的遥测接入、持久化设备影子、OTA 分批下发与失败回退、
设备/全局两级限速与有界积压。所有异常链路的结论都归一化为**稳定可区分的错误码**，
并发下结论一致、可解释。

## 快速开始

```bash
uv sync                       # 安装依赖
uv run pytest                 # 运行全部测试（49 个用例）
uv run python scripts/demo.py # 端到端演示（注册/遥测/轮换/影子/OTA 回退/快照）
uv run uvicorn main:app --reload   # 启动 HTTP 服务
```

无需任何外部消息代理、数据库服务、云端或真实设备。状态默认保存在进程内存中，
也可通过 `save_snapshot/load_snapshot`（或管理快照文件）持久化为本地 JSON。

## 架构与模块

| 模块 | 职责 |
|---|---|
| `iot_hub/errors.py` | 归一化错误码体系（`HubError`），内部异常不外泄 |
| `iot_hub/models.py` | 领域数据类型（凭据/设备/影子/固件/rollout/序号状态） |
| `iot_hub/storage.py` | 线程安全有界内存存储、每设备 RLock、序号窗口、幂等 LRU、JSON 快照 |
| `iot_hub/auth.py` | 注册、凭据签发/轮换/吊销、可区分原因的鉴权、会话纪元 |
| `iot_hub/telemetry.py` | 幂等 + 单调序号 + 新鲜度判定，乱序缓冲按序应用 |
| `iot_hub/shadow.py` | 影子版本、乐观并发、reported/desired/delta、新鲜度 |
| `iot_hub/ota.py` | 固件/批次、QUEUED→DELIVERED→CONFIRMED、超时/失败回退 |
| `iot_hub/ratelimit.py` | 设备/全局令牌桶 + 全局有界在途信号量 |
| `iot_hub/service.py` | 用例门面：鉴权→限速→积压→业务的固定编排 |
| `iot_hub/api.py` | FastAPI 边界：协议版本、载荷校验、错误归一化 |

## 1. 接入与鉴权模型

### 凭据模型
- 设备注册时签发第一代凭据（`credential_id` + 一次性明文 `secret`，服务端只存 SHA-256 哈希）。
- 每个凭据携带授权范围 `scopes`：`telemetry:write`、`shadow:read`、`shadow:write`、
  `ota:read`、`ota:admin`。注册默认携带 telemetry/shadow-read/ota-read。
- 密钥比较使用恒定时间 `hmac.compare_digest`。

### 拒绝原因（错误码互斥、稳定）
| 场景 | 错误码 | HTTP |
|---|---|---|
| 设备未注册（含已注销） | `DEVICE_NOT_REGISTERED` | 404 |
| 凭据 ID 不存在 / 不属于该设备 / 密钥错误 | `CREDENTIAL_INVALID` | 401 |
| 凭据已吊销 | `CREDENTIAL_REVOKED` | 401 |
| 旧凭据超过轮换宽限期 | `CREDENTIAL_GRACE_EXPIRED` | 401 |
| scope 不满足操作要求 | `SCOPE_DENIED` | 403 |
| 同一设备已有更新的连接会话 | `SESSION_STALE` | 409 |
| 协议头缺失/版本不受支持 | `PROTOCOL_INCOMPATIBLE` | 400 |
| 载荷不是合法 JSON / 字段畸形 | `MALFORMED_PAYLOAD` | 422 |

未注册、吊销、宽限过期、越权是**四类不同结论**，调用方可据 code 区分处理，
不会笼统地得到 401/403。

### 凭据轮换宽限（确定语义）
- `rotate_credential(grace_period_s=300)`：签发新一代凭据；旧凭据设置
  `grace_until = now + 300s`。
- 宽限期内：新旧凭据都可接入；旧凭据的鉴权结论 `AuthContext.grace=true`，
  响应头/字段显式标注，客户端据此触发凭据更新。
- 宽限期外：旧凭据固定得到 `CREDENTIAL_GRACE_EXPIRED`（不是 REVOKED，也不是 INVALID）。
- `grace_period_s=0` 表示不设宽限，旧凭据立即吊销。
- 吊销（revoke）立即生效，之后一律 `CREDENTIAL_REVOKED`。

### 会话纪元（同一设备多连接的一致性）
- 每次 `POST /devices/connect` 成功后，服务端为该设备分配单调递增的 `epoch` 并返回。
- 设备后续遥测/OTA 请求在 `x-iot-epoch` 头携带该值。
- 设备断线重连会产生更大的 epoch；**旧连接（僵尸连接）携带旧 epoch 的写入一律得到
  `SESSION_STALE`**，旧会话数据不可能污染新会话。
- 新会话重置"会话序号空间"（见下），但影子版本、reported 内容、幂等表跨会话保留。

## 2. 遥测：幂等、序号与新鲜度

遥测请求：`POST /telemetry`，头 `x-iot-device-id / x-iot-credential / x-iot-secret /
x-iot-protocol: 1.0 / x-iot-epoch: <n>`，体：

```json
{ "seq": 12, "ts": 1700000000.5, "payload": {"temp": 21.5}, "idempotency_key": "uuid-..." }
```

判定顺序（在该设备的同一把 RLock 内原子完成）：
1. 载荷/字段类型 → `MALFORMED_PAYLOAD`；
2. 时间戳新鲜窗口（默认过去 300s 内、未来 60s 内）→ `TELEMETRY_EXPIRED`；
3. 幂等键：同键不同请求指纹 → `IDEMPOTENCY_CONFLICT`；同键同体重放 → 回放首次结论，
   `applied=false`（绝不重复生效）；
4. 序号窗口（每个会话独立）：
   - 首条到达的序号建立会话连续水位（设备须从约定起始序号发起）；
   - 水位之上先到的乱序序号被**缓冲**（载荷不进影子），空洞按序补齐后按 seq 顺序合并；
   - 因此乱序/重复数据**不会覆盖较新状态**（如 seq=3 的 `temp=23` 不会被迟到的
     seq=2 `temp=22` 覆盖）；
   - 水位之下/已接收序号重发 → `TELEMETRY_DUPLICATE`（accepted 但 applied=false）；
   - 超过序号窗口（默认 10000）的巨跳：中间缺口显式放弃，迟到序号 → `TELEMETRY_OUT_OF_ORDER`；
5. 生效：序号登记与影子合并、影子版本 +1 在同一原子段，任何序号最多应用一次。

断线重连续传：设备重放未确认消息——已接收者得到幂等/重复结论，空洞者补齐，
既不丢失生效机会，也不重复生效。被拒绝（过期/乱序）的写入不残留任何部分状态。

## 3. 设备影子语义

```
GET /devices/{id}/shadow 或 GET /admin/devices/{id}/shadow
PUT /admin/devices/{id}/shadow/desired
```

- `reported`：只由遥测推进；`desired`：服务端期望值；`delta`：两者不一致的键。
- **版本单调**：任何一次有效写入（遥测合并或 desired 更新）版本 +1，
  并发/回退写入不会产生版本倒退或重复计数。
- **乐观并发**：`PUT desired` 可带 `expected_version`；不匹配 →
  `SHADOW_VERSION_CONFLICT`（409），调用方可重读重试；冲突写入不留半更新。
- **新鲜度显式标记**：视图返回 `freshness`：
  - `FRESH`：reported 在阈值内（默认 60s）；
  - `STALE`：设备离线/数据超龄——值仍然可见，但**明确不是最新值**，
    业务方不得把它当最新状态参与判定；
  - `UNKNOWN`：从未上报。
- 影子状态可随 JSON 快照持久化，版本跨重启单调。

## 4. OTA 状态语义

### 生命周期（每个设备一台有限状态机）

```
NONE ──分配(离线)──> QUEUED ──上线 pull──> DELIVERED ──设备确认──> CONFIRMED
                         │                    │
                         └──── 设备报失败 ─────┴──> FAILED/ROLLED_BACK
                                   确认截止超时 ──> ROLLED_BACK
```

- `QUEUED`：设备离线，更新进入**按 rollout 有界**的队列（默认每 rollout 10000 台），
  超限 → `BACKLOG_FULL`，状态不残留为半 QUEUED。
- `DELIVERED`：已下发，`deadline = now + confirm_timeout_s`；未确认前
  **绝不改变 `current_fw_version` / `last_known_good_version`**。
- `CONFIRMED`：只有设备确认的版本才生效，同时成为新的"已知良好版本"（LKG）。
- 超时或失败：当前版本**原子回退**到 `last_known_good_version`，状态 `ROLLED_BACK`，
  `failure_reason` 为 `CONFIRM_TIMEOUT` 或 `DEVICE_REPORTED:<原因>`。
  从未有过已确认版本时 → 确定性结论 `OTA_NO_AVAILABLE_VERSION`。
- 超时由后台扫描线程（`auto_sweep`，可关闭）与每次查询/拉取前的惰性扫描双重保证。

### 兼容性与分批
- 型号不匹配 → `OTA_INCOMPATIBLE`（`reason=MODEL_MISMATCH`）；
- 当前版本低于固件要求的 `min_from_version` → `OTA_INCOMPATIBLE`
  （`reason=VERSION_TOO_OLD`）；
- 确认版本与下发目标不一致 → `OTA_INCOMPATIBLE`（`VERSION_MISMATCH`），状态保持 DELIVERED；
- 分批按 `sha256(device_id)` 稳定分桶（结论跨重启不变）+ `percent`（1–100）；
  不在批次 → `OTA_INCOMPATIBLE`（`NOT_IN_BATCH`），绝不静默排队。
- 同型号多 rollout：按固件版本号高者优先自动选择；已在某 rollout 上失败/回退过的设备
  不会被同一 rollout 自动重复选择。

## 5. 限速与积压（内存始终有界）

- **两级令牌桶**：每次遥测/确认先过"设备桶"（默认 10/s，突发 20），再过"全局桶"
  （默认 10000/s，突发 20000）。
  - 设备维度超限 → `RATE_LIMITED`（`dimension=device`）；
  - 全局维度超限 → `RATE_LIMITED`（`dimension=global`），并归还已预扣的设备令牌，
    使拒绝请求不产生副作用。
  - 每设备桶表本身是 LRU 有界（默认 100000），未知设备 ID 洪水也撑不爆内存。
- **全局有界在途**：`BoundedBacklog`（默认 10000）以上下文管理器保护每个用例，
  成功、业务拒绝、内部异常都会释放占用；超限 → `BACKLOG_FULL`，绝不无界堆积。
- 序号窗口、遥测缓冲、幂等表（默认 4096/设备 LRU）、OTA 队列均有硬上限；
  巨跳时缓冲随窗口一起压缩，失败操作不残留部分状态。

## 6. 错误归一化与对外接口

- 所有业务失败均为 `{"error": <CODE>, "message": ..., "details": {...}}`，
  错误码字符串是对外契约，不随内部重构改名。
- FastAPI 边界捕获一切非 `HubError` 异常 → 500 `INTERNAL_ERROR`，
  仅记录服务端日志，不向调用方透出堆栈、内部路径、密钥等。
- 统计计数 `GET /admin/metrics`（如 `auth.ok`、`auth.denied.revoked`、
  `telemetry.duplicate`、`ota.rolled_back` 等）全部在锁内增减，
  并发下不重复累加、不丢失。

## 7. 本地验证方法（无外部依赖）

```bash
# 1) 全部单测（重点覆盖：重复/乱序/过期、凭据吊销与宽限、升级失败回退、并发接入）
uv run pytest -v

# 2) 端到端演示脚本（含打印每一步输入与判定结论）
uv run python scripts/demo.py

# 3) 手工 HTTP 演练
uv run uvicorn main:app --port 8000 &
curl -s localhost:8000/health
# 所有受管/设备接口都需要头: x-iot-protocol: 1.0
```

测试分布：

| 文件 | 重点 |
|---|---|
| `tests/test_auth.py` | 未注册/无效/吊销/宽限/越权区分、轮换语义 |
| `tests/test_telemetry.py` | 重复、乱序空洞、过期（过去/未来）、幂等冲突、拒绝后重试 |
| `tests/test_shadow.py` | 版本单调、乐观冲突、并发写入、STALE/UNKNOWN 新鲜度 |
| `tests/test_ota.py` | 型号/版本不兼容、分批、离线排队、超时与失败回退、未确认不生效 |
| `tests/test_ratelimit.py` | 设备/全局限速区分、积压上限、异常路径释放 |
| `tests/test_concurrency.py` | 200 设备并发、吊销竞态、僵尸连接 SESSION_STALE、确认只生效一次、跨设备隔离 |
| `tests/test_api.py` | 协议/载荷归一化、端到端错误码、内部异常不泄漏 |
| `tests/test_persistence.py` | JSON 快照恢复后影子/序号/凭据/OTA 结论一致 |

测试日志（pytest 捕获输出）会打印每次关键操作的输入与判定依据（错误码、版本、
新鲜度、回退目标等），便于复核异常链路结论。

## 关键设计约定小结

1. 每设备一把 `RLock`：鉴权结论、序号、影子版本、OTA 状态在同一把锁下原子推进。
2. 会话纪元 epoch 单调：旧连接写入一律 `SESSION_STALE`，杜绝多连接串用。
3. 序号/版本严格单调：旧数据不覆盖新状态，重复不重复计数。
4. 未确认的 OTA 永不生效；失败必回退到确定的 LKG。
5. 一切集合/队列/缓冲有硬上限；任何异常路径释放资源。
