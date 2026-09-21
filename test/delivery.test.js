import assert from "node:assert/strict";
import test from "node:test";
import { api, DUTY_TOKEN, ingest, startTestServer, subscribe } from "./helpers.js";

test("投递失败按指数退避重试，最终只投递一次成功", async (t) => {
  const ctx = await startTestServer({ start: "2026-09-21T08:00:30Z" });
  t.after(ctx.close);
  await subscribe(ctx.base);
  const ingested = await ingest(ctx.base, { idempotencyKey: "k-1", magnitude: 4.2 });
  const notificationId = ingested.body.notificationsCreated[0];
  ctx.notifier.failuresBeforeSuccess = 2;

  await ctx.service.tick(); // 第 1 次尝试失败
  assert.equal(ctx.notifier.calls.length, 1);
  let view = await api(ctx.base, "GET", `/v1/notifications/${notificationId}`, {
    token: DUTY_TOKEN,
  });
  assert.equal(view.body.status, "retrying");
  assert.equal(view.body.attempts.length, 1);
  assert.equal(view.body.attempts[0].outcome, "retry");
  assert.equal(view.body.attempts[0].nextAttemptAt, "2026-09-21T08:00:31.000Z");

  ctx.clock.advance(500); // 未到重试时间
  await ctx.service.tick();
  assert.equal(ctx.notifier.calls.length, 1);

  ctx.clock.advance(600); // 08:00:31.100，第 2 次尝试失败，退避 2000ms
  await ctx.service.tick();
  assert.equal(ctx.notifier.calls.length, 2);
  view = await api(ctx.base, "GET", `/v1/notifications/${notificationId}`, { token: DUTY_TOKEN });
  assert.equal(view.body.attempts[1].outcome, "retry");
  assert.equal(view.body.attempts[1].nextAttemptAt, "2026-09-21T08:00:33.100Z");

  ctx.clock.advance(2100); // 08:00:33.200，第 3 次尝试成功
  await ctx.service.tick();
  assert.equal(ctx.notifier.calls.length, 3);
  view = await api(ctx.base, "GET", `/v1/notifications/${notificationId}`, { token: DUTY_TOKEN });
  assert.equal(view.body.status, "delivered");
  assert.equal(view.body.deliveredAt, "2026-09-21T08:00:33.200Z");
  assert.deepEqual(
    view.body.attempts.map((a) => a.outcome),
    ["retry", "retry", "delivered"],
  );
  assert.equal(ctx.store.state.notifications.size, 1); // 始终是同一条通知

  // 投递头携带幂等键与尝试序号，供订阅端去重
  assert.equal(ctx.notifier.calls[0].headers["x-notification-key"], notificationId);
  assert.equal(ctx.notifier.calls[2].headers["x-notification-attempt"], "3");
});

test("确认后从未确认列表移除，重复确认不产生新事件", async (t) => {
  const ctx = await startTestServer();
  t.after(ctx.close);
  await subscribe(ctx.base);
  const ingested = await ingest(ctx.base, { idempotencyKey: "k-1", magnitude: 4.2 });
  const notificationId = ingested.body.notificationsCreated[0];
  await ctx.service.tick(); // 投递成功

  let unacked = await api(ctx.base, "GET", "/v1/jurisdictions/J1/unacknowledged", {
    token: DUTY_TOKEN,
  });
  assert.equal(unacked.body.notifications.length, 1);
  assert.equal(unacked.body.notifications[0].notificationId, notificationId);

  const ack = await api(ctx.base, "POST", `/v1/notifications/${notificationId}/ack`, {
    token: DUTY_TOKEN,
    body: { by: "值班员甲" },
  });
  assert.equal(ack.body.alreadyAcknowledged, false);
  assert.equal(ack.body.ackedBy, "值班员甲");

  unacked = await api(ctx.base, "GET", "/v1/jurisdictions/J1/unacknowledged", {
    token: DUTY_TOKEN,
  });
  assert.equal(unacked.body.notifications.length, 0);

  const again = await api(ctx.base, "POST", `/v1/notifications/${notificationId}/ack`, {
    token: DUTY_TOKEN,
    body: { by: "值班员乙" },
  });
  assert.equal(again.body.alreadyAcknowledged, true);
  assert.equal(again.body.ackedBy, "值班员甲"); // 首次确认未被覆盖
});

test("重试耗尽进入 dead，仍列于未确认且可人工确认", async (t) => {
  const ctx = await startTestServer({ start: "2026-09-21T08:00:30Z" });
  t.after(ctx.close);
  ctx.notifier.alwaysFail = true;
  await subscribe(ctx.base);
  const ingested = await ingest(ctx.base, { idempotencyKey: "k-1", magnitude: 4.2 });
  const notificationId = ingested.body.notificationsCreated[0];

  await ctx.service.tick(); // 尝试 1
  ctx.clock.advance(1100);
  await ctx.service.tick(); // 尝试 2
  ctx.clock.advance(2100);
  await ctx.service.tick(); // 尝试 3，达到上限

  const view = await api(ctx.base, "GET", `/v1/notifications/${notificationId}`, {
    token: DUTY_TOKEN,
  });
  assert.equal(view.body.status, "dead");
  assert.deepEqual(
    view.body.attempts.map((a) => a.outcome),
    ["retry", "retry", "dead"],
  );

  const unacked = await api(ctx.base, "GET", "/v1/jurisdictions/J1/unacknowledged", {
    token: DUTY_TOKEN,
  });
  assert.equal(unacked.body.notifications.length, 1);
  assert.equal(unacked.body.notifications[0].status, "dead");

  const ack = await api(ctx.base, "POST", `/v1/notifications/${notificationId}/ack`, {
    token: DUTY_TOKEN,
    body: {},
  });
  assert.equal(ack.body.alreadyAcknowledged, false);
});

test("未投递完成的通知不能确认", async (t) => {
  const ctx = await startTestServer();
  t.after(ctx.close);
  await subscribe(ctx.base);
  const ingested = await ingest(ctx.base, { idempotencyKey: "k-1", magnitude: 4.2 });
  const notificationId = ingested.body.notificationsCreated[0];

  const ack = await api(ctx.base, "POST", `/v1/notifications/${notificationId}/ack`, {
    token: DUTY_TOKEN,
    body: {},
  });
  assert.equal(ack.status, 409);
  assert.equal(ack.body.error.code, "notification_not_delivered");
});
