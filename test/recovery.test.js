import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { observationRequestHash } from "../src/service.js";
import { Store } from "../src/store.js";
import {
  api,
  DUTY_TOKEN,
  ingest,
  startTestServer,
  STATION_TOKEN,
  subscribe,
  WINDOW_MS,
} from "./helpers.js";

test("进程恢复后继续未完成的投递，且不产生第二次通知", async (t) => {
  const first = await startTestServer({ start: "2026-09-21T08:00:30Z" });
  await subscribe(first.base);
  const ingested = await ingest(first.base, { idempotencyKey: "k-1", magnitude: 4.2 });
  const notificationId = ingested.body.notificationsCreated[0];
  await first.close(); // 模拟进程退出：通知已创建但尚未投递

  // 同一数据文件重启：恢复后补做投递
  const second = await startTestServer({ start: "2026-09-21T08:00:40Z", dir: first.dir });
  await second.service.tick();
  assert.equal(second.notifier.calls.length, 1);
  const view = await api(second.base, "GET", `/v1/notifications/${notificationId}`, {
    token: DUTY_TOKEN,
  });
  assert.equal(view.body.status, "delivered");
  await second.close();

  // 再次重启：已投递的不会重发
  const third = await startTestServer({ start: "2026-09-21T08:00:50Z", dir: first.dir });
  t.after(third.close);
  await third.service.tick();
  assert.equal(third.notifier.calls.length, 0);
  assert.equal(third.store.state.notifications.size, 1);
});

test("崩溃残留的幂等键在恢复时被补全或释放", async (t) => {
  const dir = mkdtempSync(join(tmpdir(), "aas-crash-"));
  const dataFile = join(dir, "events.jsonl");
  const t0 = Date.parse("2026-09-21T08:00:00Z");
  const index = Math.floor(t0 / WINDOW_MS);

  // 手工构造崩溃现场一：键已开始、观测已落盘，但结果未写完
  const retryBody = {
    idempotencyKey: "k-crash",
    stationId: "S1",
    region: "R1",
    magnitude: 5.1,
    depthKm: 12,
    observedAt: "2026-09-21T08:00:15Z",
  };
  const store = Store.open(dataFile);
  store.record("idempotency.started", {
    at: "2026-09-21T08:00:20.000Z",
    key: "k-crash",
    requestHash: observationRequestHash(retryBody),
  });
  store.record("observation.recorded", {
    at: "2026-09-21T08:00:20.000Z",
    observation: {
      observationId: "obs_2",
      key: "k-crash",
      stationId: "S1",
      region: "R1",
      cellId: "C1",
      magnitude: 5.1,
      depthKm: 12,
      observedAt: "2026-09-21T08:00:15.000Z",
      receivedAt: "2026-09-21T08:00:20.000Z",
      windowIndex: index,
      windowStartMs: index * WINDOW_MS,
      windowEndMs: (index + 1) * WINDOW_MS,
      late: false,
    },
  });
  // 崩溃现场二：只有键开始、没有观测
  store.record("idempotency.started", {
    at: "2026-09-21T08:00:21.000Z",
    key: "k-half",
    requestHash: "whatever",
  });

  const ctx = await startTestServer({ start: "2026-09-21T08:00:30Z", dir });
  t.after(ctx.close);

  // 现场一：恢复补做了评估，5.1 级触发告警，键被补全
  const history = await api(ctx.base, "GET", "/v1/cells/C1/history", { token: DUTY_TOKEN });
  assert.deepEqual(
    history.body.events.map((e) => e.type),
    ["alert.raised"],
  );
  assert.equal(history.body.events[0].level, 3);
  const crashed = await api(ctx.base, "GET", "/v1/idempotency/k-crash", { token: DUTY_TOKEN });
  assert.equal(crashed.body.status, "processed");
  assert.equal(crashed.body.result.recovered, true);

  // 客户端用同一键重试：返回恢复后的结果，不会二次处理
  const retry = await api(ctx.base, "POST", "/v1/observations", {
    token: STATION_TOKEN,
    body: retryBody,
  });
  assert.equal(retry.status, 200);
  assert.equal(retry.body.duplicate, true);
  assert.equal(retry.body.observationId, "obs_2");
  assert.equal(ctx.store.state.observations.size, 1);

  // 现场二：键被释放，客户端可安全重试
  const half = await api(ctx.base, "GET", "/v1/idempotency/k-half", { token: DUTY_TOKEN });
  assert.equal(half.status, 404);
  const reingested = await api(ctx.base, "POST", "/v1/observations", {
    token: STATION_TOKEN,
    body: { ...retryBody, idempotencyKey: "k-half" },
  });
  assert.equal(reingested.status, 201);
});
