import { pathToFileURL } from "node:url";
import { createApp } from "./app.js";
import { SystemClock } from "./clock.js";
import { loadConfig } from "./config.js";
import { Grid } from "./grid.js";
import { HttpNotifier } from "./notifier.js";
import { AlertService } from "./service.js";
import { Store } from "./store.js";

/**
 * 组装存储、领域服务与 HTTP 层。
 * 测试可注入 FixedClock 与假通知器，以确定性地重放窗口边界。
 */
export function buildServer(config, { clock = new SystemClock(), notifier } = {}) {
  const store = Store.open(config.dataFile);
  const service = new AlertService({
    store,
    clock,
    grid: new Grid(config.grid),
    windowMs: config.windowMs,
    levelThresholds: config.levelThresholds,
    notifier: notifier ?? new HttpNotifier({ timeoutMs: config.delivery?.timeoutMs }),
    delivery: config.delivery,
    maxFutureSkewMs: config.maxFutureSkewMs,
  });
  service.recover();
  const app = createApp({ service, tokens: config.tokens });
  let timer = null;
  if (config.worker?.enabled !== false) {
    const pollMs = config.worker?.pollMs ?? 1000;
    timer = setInterval(() => {
      service.tick().catch((error) => console.error("窗口推进/投递失败", error));
    }, pollMs);
    timer.unref();
  }
  return {
    app,
    service,
    store,
    stop() {
      if (timer) clearInterval(timer);
    },
  };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const config = loadConfig();
  const { app, stop } = buildServer(config);
  app.listen(config.port, () => {
    console.log(`余震告警服务已启动，端口 ${config.port}`);
  });
  const shutdown = () => {
    stop();
    app.close(() => process.exit(0));
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}
