import assert from "node:assert/strict";
import test from "node:test";
import { api, DUTY_TOKEN, ingest, startTestServer, subscribe } from "./helpers.js";

test("固定时钟重放窗口边界：幂等键与重试落点清晰可查", async (t) => {
  const start = "2026-09-21T08:00:59.999Z";
  const first = await startTestServer({ start });
  await subscribe(first.base);

  // 边界前 1ms 的观测归入当前窗口
  const before = await ingest(first.base, {
    idempotencyKey: "k-edge-1",
    magnitude: 4.2,
    observedAt: "2026-09-21T08:00:59.999Z",
  });
  // 恰好落在边界上的观测归入下一个窗口
  const at = await ingest(first.base, {
    idempotencyKey: "k-edge-2",
    magnitude: 4.8,
    observedAt: "2026-09-21T08:01:00.000Z",
  });
  assert.equal(at.body.windowIndex, before.body.windowIndex + 1);
  assert.equal(before.body.late, false);
  assert.equal(at.body.late, false);

  first.clock.set("2026-09-21T08:01:30Z");
  await first.service.tick(); // 关闭前一窗口并投递两条通知
  assert.equal(first.notifier.calls.length, 2);
  await first.close();

  // 用同一数据文件与同一固定时钟重放整个流程
  const replay = await startTestServer({ start, dir: first.dir });
  t.after(replay.close);
  const before2 = await ingest(replay.base, {
    idempotencyKey: "k-edge-1",
    magnitude: 4.2,
    observedAt: "2026-09-21T08:00:59.999Z",
  });
  assert.equal(before2.status, 200);
  assert.equal(before2.body.duplicate, true);
  assert.equal(before2.body.observationId, before.body.observationId);
  const at2 = await ingest(replay.base, {
    idempotencyKey: "k-edge-2",
    magnitude: 4.8,
    observedAt: "2026-09-21T08:01:00.000Z",
  });
  assert.equal(at2.body.duplicate, true);

  replay.clock.set("2026-09-21T08:01:30Z");
  await replay.service.tick();
  assert.equal(replay.notifier.calls.length, 0); // 重放不产生第二次通知
  assert.equal(replay.store.state.notifications.size, 2);

  // 每个幂等键的最终落点可分辨
  const keyView = await api(replay.base, "GET", "/v1/idempotency/k-edge-1", { token: DUTY_TOKEN });
  assert.equal(keyView.body.status, "processed");
  assert.equal(keyView.body.observationId, before.body.observationId);

  // 每次重试的最终落点可分辨
  const notification = await api(
    replay.base,
    "GET",
    `/v1/notifications/${before.body.notificationsCreated[0]}`,
    { token: DUTY_TOKEN },
  );
  assert.equal(notification.body.status, "delivered");
  assert.deepEqual(
    notification.body.attempts.map((a) => a.outcome),
    ["delivered"],
  );
  assert.equal(notification.body.attempts[0].at, "2026-09-21T08:01:30.000Z");

  // 两个窗口的归并结果在重放后保持原样
  const w1 = await api(replay.base, "GET", `/v1/cells/C1/windows/${before.body.windowIndex}`, {
    token: DUTY_TOKEN,
  });
  assert.equal(w1.body.closed, true);
  assert.equal(w1.body.maxMagnitude, 4.2);
  const w2 = await api(replay.base, "GET", `/v1/cells/C1/windows/${at.body.windowIndex}`, {
    token: DUTY_TOKEN,
  });
  assert.equal(w2.body.maxMagnitude, 4.8);
});
