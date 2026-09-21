import assert from "node:assert/strict";
import test from "node:test";
import { api, DUTY_TOKEN, ingest, observation, startTestServer, STATION_TOKEN, subscribe } from "./helpers.js";

test("重复消息返回首个结果且不会产生第二次通知", async (t) => {
  const ctx = await startTestServer();
  t.after(ctx.close);
  await subscribe(ctx.base);
  const body = observation({ idempotencyKey: "dup-1", magnitude: 4.2 });

  const first = await api(ctx.base, "POST", "/v1/observations", { token: STATION_TOKEN, body });
  const second = await api(ctx.base, "POST", "/v1/observations", { token: STATION_TOKEN, body });

  assert.equal(first.status, 201);
  assert.equal(second.status, 200);
  assert.equal(second.body.duplicate, true);
  assert.equal(second.body.observationId, first.body.observationId);
  assert.deepEqual(second.body.notificationsCreated, first.body.notificationsCreated);
  assert.equal(ctx.store.state.observations.size, 1);
  assert.equal(ctx.store.state.notifications.size, 1); // 没有第二次通知
});

test("同一幂等键提交不同内容返回 409", async (t) => {
  const ctx = await startTestServer();
  t.after(ctx.close);
  await subscribe(ctx.base);
  await ingest(ctx.base, { idempotencyKey: "dup-2", magnitude: 4.2 });

  const conflict = await api(ctx.base, "POST", "/v1/observations", {
    token: STATION_TOKEN,
    body: observation({ idempotencyKey: "dup-2", magnitude: 4.3 }),
  });
  assert.equal(conflict.status, 409);
  assert.equal(conflict.body.error.code, "idempotency_conflict");
});

test("值班接口可查看幂等键的最终落点", async (t) => {
  const ctx = await startTestServer();
  t.after(ctx.close);
  await subscribe(ctx.base);
  const ingested = await ingest(ctx.base, { idempotencyKey: "k-view", magnitude: 4.2 });

  const view = await api(ctx.base, "GET", "/v1/idempotency/k-view", { token: DUTY_TOKEN });
  assert.equal(view.status, 200);
  assert.equal(view.body.status, "processed");
  assert.equal(view.body.observationId, ingested.body.observationId);
  assert.equal(view.body.result.level, 2);
  assert.equal(typeof view.body.requestHash, "string");

  const missing = await api(ctx.base, "GET", "/v1/idempotency/no-such-key", { token: DUTY_TOKEN });
  assert.equal(missing.status, 404);
});
