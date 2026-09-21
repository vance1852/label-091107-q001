import assert from "node:assert/strict";
import test from "node:test";
import {
  api,
  DUTY_TOKEN,
  ingest,
  observation,
  startTestServer,
  STATION_TOKEN,
  subscribe,
} from "./helpers.js";

test("同一网格与窗口内的观测被归并并计算当前级别", async (t) => {
  const ctx = await startTestServer();
  t.after(ctx.close);
  await subscribe(ctx.base);

  const first = await ingest(ctx.base, { idempotencyKey: "k-1", magnitude: 3.2 });
  assert.equal(first.status, 201);
  assert.equal(first.body.level, 1);
  assert.equal(first.body.late, false);
  assert.equal(first.body.alert.type, "alert.raised");
  assert.equal(first.body.notificationsCreated.length, 1);

  // 同一网格单元的另一区域、同一窗口：归并到一起
  const second = await ingest(ctx.base, {
    idempotencyKey: "k-2",
    region: "R2",
    magnitude: 3.6,
    observedAt: "2026-09-21T08:00:20Z",
  });
  assert.equal(second.body.windowIndex, first.body.windowIndex);
  assert.equal(second.body.level, 1);
  assert.equal(second.body.notificationsCreated.length, 0); // 未超过已通知等级

  const state = await api(ctx.base, "GET", "/v1/cells/C1", { token: DUTY_TOKEN });
  assert.equal(state.body.currentLevel, 1);
  assert.equal(state.body.currentWindow.count, 2);
  assert.equal(state.body.currentWindow.maxMagnitude, 3.6);
  assert.equal(state.body.currentWindow.minDepthKm, 10);
  assert.equal(state.body.lastNotifiedLevel, 1);

  // 区域视角经网格解析到同一单元
  const region = await api(ctx.base, "GET", "/v1/regions/R2", { token: DUTY_TOKEN });
  assert.equal(region.body.cellId, "C1");
  assert.equal(region.body.currentLevel, 1);
});

test("窗口内震级升级会追加升级告警并再次通知", async (t) => {
  const ctx = await startTestServer();
  t.after(ctx.close);
  await subscribe(ctx.base);
  await ingest(ctx.base, { idempotencyKey: "k-1", magnitude: 3.2 });

  const up = await ingest(ctx.base, {
    idempotencyKey: "k-2",
    magnitude: 4.4,
    observedAt: "2026-09-21T08:00:20Z",
  });
  assert.equal(up.body.level, 2);
  assert.equal(up.body.alert.type, "alert.escalated");
  assert.equal(up.body.notificationsCreated.length, 1);

  const history = await api(ctx.base, "GET", "/v1/cells/C1/history", { token: DUTY_TOKEN });
  assert.deepEqual(
    history.body.events.map((e) => e.type),
    ["alert.raised", "alert.escalated"],
  );
  assert.deepEqual(
    history.body.events.map((e) => e.level),
    [1, 2],
  );
});

test("订阅阈值过滤：低于 minLevel 的告警不通知该辖区", async (t) => {
  const ctx = await startTestServer();
  t.after(ctx.close);
  await subscribe(ctx.base, { jurisdictionId: "J-high", minLevel: 3 });

  const low = await ingest(ctx.base, { idempotencyKey: "k-1", magnitude: 4.4 });
  assert.equal(low.body.level, 2);
  assert.equal(low.body.notificationsCreated.length, 0);

  const high = await ingest(ctx.base, {
    idempotencyKey: "k-2",
    magnitude: 5.2,
    observedAt: "2026-09-21T08:00:20Z",
  });
  assert.equal(high.body.level, 3);
  assert.equal(high.body.notificationsCreated.length, 1);
});

test("不同网格单元的观测互不干扰", async (t) => {
  const ctx = await startTestServer();
  t.after(ctx.close);
  await subscribe(ctx.base); // J1 订阅 C1

  const other = await api(ctx.base, "POST", "/v1/observations", {
    token: STATION_TOKEN,
    body: observation({ idempotencyKey: "k-1", region: "R3", magnitude: 5.5 }),
  });
  assert.equal(other.body.cellId, "C2");
  assert.equal(other.body.level, 3);
  assert.equal(other.body.notificationsCreated.length, 0); // C2 无人订阅

  const c1 = await api(ctx.base, "GET", "/v1/cells/C1", { token: DUTY_TOKEN });
  assert.equal(c1.body.currentLevel, 0);
  assert.equal(c1.body.currentWindow, null);
});
