// 服务入口：装配事件日志、时钟、投递器、鉴权与 HTTP 服务。
//
// 环境变量：
//   PORT               监听端口（默认 3000）
//   EVENT_LOG_PATH     事件日志 JSONL 路径（默认 data/events.jsonl）
//   ALERT_API_TOKENS   令牌清单：令牌:作用域[:辖区ID]，逗号分隔
//                      作用域为 ingest / operator / admin，例如
//                      ingest-token:ingest,duty-token:operator:J01,admin-token:admin
//   CELL_SIZE_DEG      网格边长（度，默认 0.5）
//   WINDOW_MS          归并时间窗（毫秒，默认 600000）
//   LATENESS_WAIT_MS   窗口关闭后的迟到宽限（毫秒，默认 120000）
import { pathToFileURL } from "node:url";
import { resolve } from "node:path";
import { AlertEngine } from "./engine.js";
import { SystemClock } from "./clock.js";
import { createWebhookSender } from "./sender.js";
import { TokenRegistry } from "./auth.js";
import { createApiServer } from "./api.js";

export function buildApp({
  path = process.env.EVENT_LOG_PATH ?? resolve("data/events.jsonl"),
  tokens = TokenRegistry.fromEnv(),
  clock = new SystemClock(),
  config,
} = {}) {
  const envConfig = {};
  if (process.env.CELL_SIZE_DEG) envConfig.cellSizeDeg = Number(process.env.CELL_SIZE_DEG);
  if (process.env.WINDOW_MS) envConfig.windowMs = Number(process.env.WINDOW_MS);
  if (process.env.LATENESS_WAIT_MS) envConfig.latenessWaitMs = Number(process.env.LATENESS_WAIT_MS);
  const engine = new AlertEngine({
    path,
    clock,
    sender: createWebhookSender(),
    config: { ...envConfig, ...config },
  });

  // 无令牌时注入开发占位令牌并打印警告；生产必须显式配置 ALERT_API_TOKENS。
  if (tokens.size === 0) {
    tokens.add("dev-admin-token", "admin");
    console.warn("[警告] 未配置任何令牌，已注入开发用 admin 令牌，请勿用于生产。");
  }

  const app = createApiServer({ engine, tokens });

  // 恢复在途/待重试通知；真实时钟下重试由各自定时器驱动。
  const rearmed = engine.rearm();
  // 每分钟清扫一次到期窗口。
  const sweepTimer = setInterval(() => engine.sweep(), 60_000);
  if (typeof sweepTimer.unref === "function") sweepTimer.unref();

  return { app, engine, rearmed };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const { app } = buildApp();
  const port = Number(process.env.PORT ?? 3000);
  app.listen(port, () => {
    console.log(`余震告警服务监听端口 ${port}`);
  });
}
