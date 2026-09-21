# 余震告警服务

市级余震告警后端：把带台站时间、震级、深度和区域的观测，按可配置空间网格与时间窗口归并，
按各辖区自维护的订阅阈值投递告警，并保证重复消息、投递失败与进程恢复都不会产生第二次通知。

## 运行

- 需要 Node.js 20 或更高版本；`npm test` 运行全部测试。
- 复制 `config/example.json` 为 `config/local.json`（已被 gitignore），填入真实令牌、网格与等级阈值。
- `AAS_CONFIG=config/local.json npm start` 启动，端口由 `PORT` 或配置 `port` 决定（默认 3000）。

## 数据与配置

- 观测时间统一使用带时区的 ISO 8601 字符串，区域使用稳定编码；业务数据与订阅密钥不得写入源码。
- **空间网格**：`grid.cells` 把区域编码映射到归并单元；可选 `defaultCellId` 接收未映射区域（缺省则拒绝上报）。
- **时间窗口**：`windowMs`（毫秒），半开区间 `[start, end)`，恰好落在边界上的观测归入下一窗口。
- **告警等级**：`levelThresholds` 按窗口内最大震级匹配，未达任何阈值时为 0 级。
- **令牌**：`tokens` 区分 `station`（台站上报）与 `duty`（值班查询/管理）两种角色。
- **投递**：`delivery.maxAttempts` 次尝试、指数退避（`baseBackoffMs` 起，上限 1 小时），耗尽进入 `dead`。

## 接口

| 方法 | 路径 | 角色 | 说明 |
| --- | --- | --- | --- |
| GET | `/health` | 公开 | 健康检查 |
| POST | `/v1/observations` | station | 上报观测：`idempotencyKey, stationId, region, magnitude, depthKm, observedAt` |
| PUT | `/v1/subscriptions` | duty | 维护辖区订阅阈值：`jurisdictionId, cellId, minLevel, endpoint` |
| GET | `/v1/subscriptions` | duty | 列出订阅（可按 `jurisdictionId` 过滤） |
| GET | `/v1/cells/:cellId` | duty | 网格单元当前级别与窗口概况 |
| GET | `/v1/cells/:cellId/history` | duty | 演变过程：告警、修正、关窗事件时间线 |
| GET | `/v1/cells/:cellId/windows/:index` | duty | 单个窗口的聚合结果与修正记录 |
| GET | `/v1/regions/:region` | duty | 区域当前级别（经网格解析） |
| GET | `/v1/jurisdictions/:id/unacknowledged` | duty | 辖区未确认的通知 |
| POST | `/v1/notifications/:id/ack` | duty | 确认通知（重复确认返回首次结果） |
| GET | `/v1/notifications/:id` | duty | 通知状态与每一次重试的落点 |
| GET | `/v1/idempotency/:key` | duty | 幂等键的处理状态与首个结果 |

鉴权方式：`Authorization: Bearer <令牌>`。错误响应统一为 `{ "error": { "code", "message" } }`。

## 一致性与幂等语义

- 每次状态变化先追加到 `dataFile` 事件日志再应用到内存，重启后按序重放即可完整恢复。
- 观测必须携带 `idempotencyKey`：重复提交返回首个结果（HTTP 200，`duplicate: true`）；
  同一键提交不同内容返回 409 `idempotency_conflict`。
- 告警事件（`alert.raised` / `alert.escalated`）一旦落盘不可改写。接收时间越过窗口结束时刻的
  迟到观测只追加 `alert.corrected` 修正记录；若修正揭示出更高等级，会以新事件触发新通知，
  而不是改写已发送的历史。
- 通知投递只更新同一条通知记录：失败按退避重排，成功进入 `delivered`，耗尽进入 `dead`；
  重试与进程恢复都不会新建通知。投递头携带 `x-notification-key` 与 `x-notification-attempt`，
  订阅端可据此去重，以吸收"投递成功但确认前进程重启"造成的边界重发。
- 进程恢复时：崩溃残留的幂等键若观测已落盘则补做评估并补全结果（已发出的告警因
  `notifiedLevel` 已持久化而不会重发），未落盘的释放该键允许客户端安全重试。

## 固定时钟重放

领域服务的时间全部来自注入时钟。测试以 `FixedClock` 与假通知器重放窗口边界，
验证幂等键与每次重试的最终落点，参见 `test/replay.test.js`、`test/recovery.test.js`。
值班接口 `GET /v1/idempotency/:key` 与 `GET /v1/notifications/:id` 提供同样的线上可观测性。
