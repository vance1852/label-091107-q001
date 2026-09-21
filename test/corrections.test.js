import assert from "node:assert/strict";
import test from "node:test";
import { api, DUTY_TOKEN, ingest, startTestServer, subscribe } from "./helpers.js";

test("迟到观测只追加修正记录，不改写已发送的告警历史", async (t) => {
  const ctx = await startTestServer({ start: "2026-09-21T08:00:30Z" });
  t.after(ctx.close);
  await subscribe(ctx.base);

  const raised = await ingest(ctx.base, { idempotencyKey: "k-1", magnitude: 4.2 });
  assert.equal(raised.body.alert.type, "alert.raised");
  assert.equal(raised.body.level, 2);

  // 时钟越过窗口边界，窗口关闭
  ctx.clock.set("2026-09-21T08:01:05Z");
  await ctx.service.tick();

  // 属于已关闭窗口的迟到观测，震级更高
  const late = await ingest(ctx.base, {
    idempotencyKey: "k-2",
    magnitude: 5.3,
    observedAt: "2026-09-21T08:00:50Z",
  });
  assert.equal(late.status, 201);
  assert.equal(late.body.late, true);
  assert.equal(late.body.level, 3);
  assert.equal(late.body.alert.type, "alert.corrected");
  assert.equal(late.body.notificationsCreated.length, 1); // 危险升级仍会通知

  const history = await api(ctx.base, "GET", "/v1/cells/C1/history", { token: DUTY_TOKEN });
  assert.deepEqual(
    history.body.events.map((e) => e.type),
    ["alert.raised", "window.closed", "alert.corrected"],
  );
  // 已发送的告警保持原样：级别 2、最大震级 4.2
  assert.equal(history.body.events[0].level, 2);
  assert.equal(history.body.events[0].maxMagnitude, 4.2);
  // 修正记录追加在后
  const corrected = history.body.events[2];
  assert.equal(corrected.level, 3);
  assert.equal(corrected.previousLevel, 2);

  // 不提升等级的迟到观测只留修正记录，不再通知
  const lateSmall = await ingest(ctx.base, {
    idempotencyKey: "k-3",
    magnitude: 3.1,
    observedAt: "2026-09-21T08:00:55Z",
  });
  assert.equal(lateSmall.body.late, true);
  assert.equal(lateSmall.body.notificationsCreated.length, 0);

  const window = await api(ctx.base, "GET", `/v1/cells/C1/windows/${raised.body.windowIndex}`, {
    token: DUTY_TOKEN,
  });
  assert.equal(window.body.count, 3);
  assert.equal(window.body.corrections.length, 2);
  assert.equal(window.body.maxMagnitude, 5.3); // 聚合反映全部数据
  assert.equal(ctx.store.state.notifications.size, 2); // raised 与 corrected 各一条
});
