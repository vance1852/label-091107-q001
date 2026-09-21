// 测试辅助：临时事件日志、固定时钟、按幂等键脚本化的投递器。
import { join } from "node:path";
import { tmpdir } from "node:os";
import { randomUUID } from "node:crypto";
import { ManualClock } from "../src/clock.js";
import { AlertEngine } from "../src/engine.js";

export const BASE_TIME = 60_000_000_000; // 与 60s 窗口对齐的固定时刻

export function tmpLog() {
  return join(tmpdir(), `aftershock-${randomUUID()}.jsonl`);
}

export function testClock(at = BASE_TIME) {
  return new ManualClock(at);
}

export const TEST_CONFIG = {
  cellSizeDeg: 0.5,
  windowMs: 60_000,
  latenessWaitMs: 10_000,
  retry: { maxAttempts: 4, baseDelayMs: 10_000, maxDelayMs: 60_000 },
};

/**
 * 脚本化投递器：fail(key, attempt) 返回
 *   'retryable'（可重试失败）/ 'permanent'（永久失败）/ 'success'（成功）。
 * calls 记录每一次真实网络调用，用于断言“同一幂等键只投递一次/重试次数”。
 */
export function scriptedSender({ fail = () => "success" } = {}) {
  const calls = [];
  const counts = new Map();
  const sender = async (notification) => {
    const attempt = (counts.get(notification.key) ?? 0) + 1;
    counts.set(notification.key, attempt);
    calls.push({ key: notification.key, attempt, rank: notification.rank });
    const outcome = await fail(notification.key, attempt, notification);
    if (outcome === "permanent") {
      return { ok: false, retryable: false, status: 400, error: "永久失败" };
    }
    if (outcome === "retryable") {
      return { ok: false, retryable: true, status: 503, error: "可重试失败" };
    }
    return { ok: true, status: 200 };
  };
  return { sender, calls, counts };
}

export function createEngine({ path = tmpLog(), clock = testClock(), sender, config = TEST_CONFIG } = {}) {
  const scripted = scriptedSender();
  const engine = new AlertEngine({ path, clock, sender: sender ?? scripted.sender, config });
  return { engine, path, clock, scripted, config: TEST_CONFIG };
}

export async function seedSubscription(engine, overrides = {}) {
  return engine.upsertSubscription({
    jurisdictionId: "J01",
    name: "市应急办订阅",
    region: "R01",
    webhookUrl: "http://receiver.invalid/hook",
    ...overrides,
  });
}

export function observation(overrides = {}) {
  return {
    stationId: "S1",
    region: "R01",
    eventTime: new Date(BASE_TIME + 5_000).toISOString(),
    magnitude: 3.2,
    depthKm: 12,
    latitude: 30.1,
    longitude: 120.1,
    ...overrides,
  };
}
