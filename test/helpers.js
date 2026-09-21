import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { FixedClock } from "../src/clock.js";
import { buildServer } from "../src/server.js";

export const DUTY_TOKEN = "duty-token";
export const STATION_TOKEN = "station-token";
export const WINDOW_MS = 60_000;

/** 可脚本化成败的通知器，记录每一次投递调用。 */
export class FakeNotifier {
  constructor() {
    this.calls = [];
    this.failuresBeforeSuccess = 0;
    this.alwaysFail = false;
  }

  async deliver(call) {
    this.calls.push(call);
    if (this.alwaysFail || this.failuresBeforeSuccess > 0) {
      if (this.failuresBeforeSuccess > 0) this.failuresBeforeSuccess -= 1;
      throw new Error("订阅端不可达");
    }
  }
}

export function testConfig(dir, overrides = {}) {
  return {
    port: 0,
    dataFile: join(dir, "events.jsonl"),
    windowMs: WINDOW_MS,
    maxFutureSkewMs: 300_000,
    levelThresholds: [
      { level: 1, minMagnitude: 3.0 },
      { level: 2, minMagnitude: 4.0 },
      { level: 3, minMagnitude: 5.0 },
    ],
    grid: {
      cells: [
        { cellId: "C1", regions: ["R1", "R2"] },
        { cellId: "C2", regions: ["R3"] },
      ],
    },
    tokens: new Map([
      [DUTY_TOKEN, { role: "duty", name: "duty-officer" }],
      [STATION_TOKEN, { role: "station", name: "station-gateway" }],
    ]),
    delivery: { maxAttempts: 3, baseBackoffMs: 1000, timeoutMs: 1000 },
    worker: { enabled: false },
    ...overrides,
  };
}

/**
 * 启动一个使用固定时钟与假通知器的测试服务。
 * 传入相同 dir 可模拟进程重启（重放同一事件日志）。
 */
export async function startTestServer({
  start = "2026-09-21T08:00:30Z",
  configOverrides = {},
  notifier,
  dir,
} = {}) {
  const dataDir = dir ?? mkdtempSync(join(tmpdir(), "aas-test-"));
  const clock = new FixedClock(start);
  const fakeNotifier = notifier ?? new FakeNotifier();
  const config = testConfig(dataDir, configOverrides);
  const server = buildServer(config, { clock, notifier: fakeNotifier });
  await new Promise((resolve) => server.app.listen(0, resolve));
  const base = `http://127.0.0.1:${server.app.address().port}`;
  return {
    base,
    clock,
    notifier: fakeNotifier,
    config,
    dir: dataDir,
    service: server.service,
    store: server.store,
    async close() {
      server.stop();
      await new Promise((resolve) => server.app.close(resolve));
    },
  };
}

export async function api(base, method, path, { token, body } = {}) {
  const headers = {};
  if (body !== undefined) headers["content-type"] = "application/json";
  if (token) headers.authorization = `Bearer ${token}`;
  const response = await fetch(`${base}${path}`, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return { status: response.status, body: await response.json() };
}

export function observation(overrides = {}) {
  return {
    idempotencyKey: "key-1",
    stationId: "S1",
    region: "R1",
    magnitude: 3.5,
    depthKm: 10,
    observedAt: "2026-09-21T08:00:10Z",
    ...overrides,
  };
}

export async function subscribe(base, overrides = {}) {
  return api(base, "PUT", "/v1/subscriptions", {
    token: DUTY_TOKEN,
    body: {
      jurisdictionId: "J1",
      cellId: "C1",
      minLevel: 1,
      endpoint: "http://subscriber.example/hook",
      ...overrides,
    },
  });
}

export async function ingest(base, overrides = {}) {
  return api(base, "POST", "/v1/observations", {
    token: STATION_TOKEN,
    body: observation(overrides),
  });
}
