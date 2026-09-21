# 余震告警服务

市级余震告警后端：把带台站时间、震级、深度和区域的观测，按可配置的**空间网格 × 时间窗口**归并定级，
各辖区维护自己的**订阅阈值**；告警升级通过 webhook 幂等投递，值班人员经鉴权接口查看区域级别、
演变过程与未确认订阅。

观测时间统一使用带时区的 ISO 8601 字符串，区域使用稳定编码；业务数据与订阅密钥不得写入源码。

- 运行环境：Node.js ≥ 20（仅使用内置模块，无第三方依赖）
- 测试：`npm test`
- 启动：`npm start`（默认端口 3000，可用 `PORT` 调整）

## 核心语义

1. **归并**：同一区域、同一网格单元（默认 0.5° × 0.5°）、同一滚动时间窗（默认 10 分钟）
   内的观测聚合为一个决策；窗口从 Unix 纪元对齐，边界确定可复现。
2. **各辖区阈值**：每个订阅自带级别规则 `rank / minMagnitude / minCount / maxDepthKm`
   （级别 1 蓝Ⅳ → 2 黄Ⅲ → 3 橙Ⅱ → 4 红Ⅰ），同一观测对不同订阅分别定级。
3. **不可变告警 + 追加修正**：每个决策的演变为 `OPEN → ESCALATION → …`；
   同级聚合变化与降级只追加 `CORRECTION`/`DOWNGRADE`，**已发送的通知绝不改写、不重发**。
4. **迟到观测**：窗口结束并经过 `LATENESS_WAIT_MS` 宽限后关闭；之后到达的观测只追加
   `LATE_CORRECTION`，不改变当前级别，也不会新开或重发告警。
5. **幂等通知**：幂等键 = `决策ID:R修订号`（如 `dec_abc:R2`）。状态机：
   `QUEUED → IN_FLIGHT → SENT`，失败转 `WAITING` 指数退避重试，超最大次数转 `DEAD`。
   请求携带固定的 `Idempotency-Key` 头，接收方可去重；`IN_FLIGHT` 不落盘，
   进程崩溃恢复后凭同一幂等键补投——**重复消息、投递失败、进程恢复都不会产生第二次通知**。
6. **事件溯源**：所有状态变更先追加到 JSONL 事件日志再更新内存；启动时重放恢复。
   终态 `SENT/DEAD` 在恢复时绝不重新投递。
7. **固定时钟**：时钟可注入（`ManualClock`），用固定步长重放可精确核对窗口边界处
   每个幂等键与每次重试的最终状态。

## 配置（环境变量）

| 变量 | 说明 | 默认 |
| --- | --- | --- |
| `PORT` | 监听端口 | `3000` |
| `EVENT_LOG_PATH` | 事件日志 JSONL 路径 | `data/events.jsonl` |
| `ALERT_API_TOKENS` | 令牌清单：`令牌:作用域[:辖区ID]`，逗号分隔；作用域 `ingest`/`operator`/`admin` | 空（注入开发令牌并告警） |
| `CELL_SIZE_DEG` | 网格边长（度） | `0.5` |
| `WINDOW_MS` | 归并时间窗（毫秒） | `600000` |
| `LATENESS_WAIT_MS` | 窗口关闭后的迟到宽限（毫秒） | `120000` |

令牌示例：

```
ALERT_API_TOKENS="station-ingest:ingest,duty-li:operator:J01,ops-admin:admin"
```

作用域层级：`admin` > `operator` > `ingest`（高隐含低）。绑定辖区的 operator 只能访问本辖区数据。

## HTTP 接口

鉴权：`Authorization: Bearer <令牌>`（`GET /health` 除外）。

### 观测接入（ingest）

`POST /v1/observations`

```json
{
  "stationId": "S1",
  "region": "R01",
  "eventTime": "2026-09-21T03:00:05+08:00",
  "magnitude": 5.4,
  "depthKm": 11,
  "latitude": 30.12,
  "longitude": 120.17,
  "id": "可选：调用方幂等ID，缺省按内容指纹生成"
}
```

- 首次受理返回 `202`；重复观测（同 id 或同内容指纹）返回 `200 {"duplicated":true}`，不产生告警。
- 响应中的 `effects` 列出每个订阅的 `OPEN/ESCALATION/CORRECTION/...`。

### 订阅管理（admin）

- `POST /v1/subscriptions`：创建（`jurisdictionId`、`name`、`region`、`webhookUrl`、`levels`）。
- `PUT /v1/subscriptions/:id`：更新（可只改 `levels` / `webhookUrl` / `active`）。
- `GET /v1/subscriptions`：列出（operator 可查）。

`levels` 缺省继承内置阈值（M3/M4/M5/M6 对应 1/2/3/4 级）。自定义示例：

```json
{ "levels": [
  { "rank": 2, "minMagnitude": 4.0, "minCount": 2, "maxDepthKm": 30 }
] }
```

含义：窗口内至少 2 次震级 ≥4.0 且深度 ≤30km 才定 2 级（可压制深源小震刷屏）。

### 值班查询（operator）

- `GET /v1/regions/:region/level`：区域当前级别与活跃单元。
- `GET /v1/decisions?region=&cellId=&status=ACTIVE|CLOSED`：决策列表。
- `GET /v1/decisions/:id`：单个决策的完整**演变过程**（OPEN/ESCALATION/CORRECTION/DOWNGRADE/LATE_CORRECTION）及其通知状态。
- `POST /v1/decisions/:id/acknowledge`：确认（body 可带 `{"by":"张值班"}`）。
- `GET /v1/unacknowledged`：存在已送达但未确认升级的订阅。
- `GET /v1/notifications?decisionId=&status=QUEUED|WAITING|SENT|DEAD`、`GET /v1/notifications/:key`：通知台账。
- `POST /v1/notifications/:key/retry`：人工重试（已 SENT 返回 `409` 拒绝，杜绝二次通知）。
- `POST /v1/notifications/:key/resurrect`（admin）：复活 DEAD 通知，沿用同一幂等键。
- `GET /v1/events?since=&types=`（admin）：原始事件审计。

## 固定时钟重放核对

```bash
# 离线重放（不触网），查看每个决策演变与幂等键终态
node scripts/replay.mjs data/events.jsonl

# 把固定时钟设到指定时刻，观察窗口关闭/重试到点后的状态
node scripts/replay.mjs data/events.jsonl --at 2026-09-21T03:30:00Z
```

## 目录结构

```
src/
  clock.js    时钟抽象（SystemClock / ManualClock）
  grid.js     空间网格与时间窗分桶
  levels.js   级别规则与定级
  store.js    只追加事件日志与重放
  sender.js   webhook 投递器（Idempotency-Key）
  engine.js   归并决策、不可变告警、通知状态机、恢复
  auth.js     Bearer 令牌与作用域
  api.js      HTTP 路由
  server.js   装配与启动
scripts/replay.mjs  离线重放核对工具
test/                 node:test 测试（34 个）
```
