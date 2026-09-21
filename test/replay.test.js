import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import { AlertEngine } from "../src/engine.js";
import { BASE_TIME, TEST_CONFIG, observation, scriptedSender, testClock, tmpLog } from "./helpers.js";

// 固定时钟重放：以固定步长推进时钟，完整跨过窗口边界与重试退避，
// 然后新起进程重放同一份事件日志，核对每个幂等键的最终状态。
test("固定时钟重放窗口边界：幂等键与重试最终状态在重放后完全一致", async () => {
  const path = tmpLog();
  const clock = testClock();
  const sender = scriptedSender({
    fail: (key, attempt) => {
      // 每个幂等键的前两次尝试失败，第三次成功。
      return attempt < 3 ? "retryable" : "success";
    },
  });
  let engine = new AlertEngine({ path, clock, sender: sender.sender, config: TEST_CONFIG });
  await engine.upsertSubscription({
    jurisdictionId: "J01",
    name: "订阅",
    region: "R01",
    webhookUrl: "http://receiver.invalid/hook",
  });

  // 第 1 个窗口：一次升级（R1 → R2）。
  await engine.ingestObservation(observation({ magnitude: 3.2, stationId: "S1" }));
  await engine.ingestObservation(
    observation({ magnitude: 4.5, stationId: "S2", eventTime: new Date(BASE_TIME + 20_000).toISOString() }),
  );

  // 以固定步长推进，让两个通知各重试两次后成功。
  for (const step of [10_000, 20_000]) {
    clock.advance(step);
    await engine.pump();
    await engine.idle();
  }

  // 跨过窗口边界：第 2 个窗口产生新的决策与幂等键。
  clock.advance(40_000); // now = BASE_TIME+90s，窗口 1（含宽限）已关闭
  await engine.pump();
  await engine.ingestObservation(
    observation({
      magnitude: 6.1,
      stationId: "S3",
      eventTime: new Date(BASE_TIME + 65_000).toISOString(),
    }),
  );
  await engine.idle();
  for (const step of [10_000, 20_000]) {
    clock.advance(step);
    await engine.pump();
    await engine.idle();
  }

  await engine.idle();
  const beforeKeys = engine
    .listNotifications()
    .map((n) => [n.key, n.status, n.attempts.length])
    .sort((a, b) => a[0].localeCompare(b[0]));
  assert.deepEqual(
    beforeKeys.map(([, status]) => status),
    ["SENT", "SENT", "SENT"],
  );
  assert.deepEqual(
    beforeKeys.map(([, , attempts]) => attempts),
    [3, 3, 3],
  );

  // 模拟进程恢复：全新引擎实例重放日志。
  const replayClock = testClock(clock.now());
  const replaySender = scriptedSender(); // 重放时不应再有任何真实投递
  engine = new AlertEngine({ path, clock: replayClock, sender: replaySender.sender, config: TEST_CONFIG });
  await engine.rearm();
  await engine.idle();

  const afterKeys = engine
    .listNotifications()
    .map((n) => [n.key, n.status, n.attempts.length])
    .sort((a, b) => a[0].localeCompare(b[0]));
  assert.deepEqual(afterKeys, beforeKeys); // 幂等键与最终状态逐一对齐
  assert.equal(replaySender.calls.length, 0); // 重放不发生第二次通知

  // 区域级别只反映活跃窗口（窗口 1 已关闭，只剩窗口 2 的 4 级）。
  const level = engine.getRegionLevel("R01");
  assert.equal(level.currentRank, 4);
  assert.equal(level.activeCells.length, 1);

  // 已关闭窗口的演变过程仍完整可查。
  const closed = engine.listDecisions({ status: "CLOSED" });
  assert.equal(closed.length, 1);
  assert.deepEqual(
    closed[0].history.map((e) => e.kind),
    ["OPEN", "ESCALATION"],
  );
});

test("事件日志可独立审计：同一幂等键有唯一的 Queued 与唯一终态", async () => {
  const path = tmpLog();
  const clock = testClock();
  const sender = scriptedSender({ fail: (_key, attempt) => (attempt < 2 ? "retryable" : "success") });
  const engine = new AlertEngine({ path, clock, sender: sender.sender, config: TEST_CONFIG });
  await engine.upsertSubscription({
    jurisdictionId: "J01",
    name: "订阅",
    region: "R01",
    webhookUrl: "http://receiver.invalid/hook",
  });
  await engine.ingestObservation(observation({ magnitude: 3.2 }));
  await engine.idle();
  clock.advance(10_000);
  await engine.pump();
  await engine.idle();

  const lines = readFileSync(path, "utf8").trim().split("\n").map(JSON.parse);
  const queuedKeys = lines.filter((e) => e.type === "NotificationQueued").map((e) => e.notification.key);
  assert.equal(new Set(queuedKeys).size, queuedKeys.length); // 幂等键只入队一次
  const key = queuedKeys[0];
  const failures = lines.filter((e) => e.type === "DeliveryFailed" && e.key === key);
  const successes = lines.filter((e) => e.type === "DeliverySucceeded" && e.key === key);
  assert.equal(failures.length, 1);
  assert.equal(successes.length, 1);
  // 成功事件排在最后一次失败之后，之后没有任何回退事件。
  assert.ok(failures[0].seq < successes[0].seq);
  assert.equal(
    lines.filter((e) => e.key === key).at(-1).type,
    "DeliverySucceeded",
  );
});
