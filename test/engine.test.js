import assert from "node:assert/strict";
import test from "node:test";
import { AlertEngine } from "../src/engine.js";
import {
  BASE_TIME,
  TEST_CONFIG,
  createEngine,
  observation,
  scriptedSender,
  seedSubscription,
  testClock,
  tmpLog,
} from "./helpers.js";

// ---------------------------------------------------------------
// 归并、去重与升级/修正
// ---------------------------------------------------------------

test("同窗口观测归并定级并投递一次，重复观测不产生第二次通知", async (t) => {
  const { engine, scripted } = createEngine();
  await seedSubscription(engine);

  const first = await engine.ingestObservation(observation());
  assert.equal(first.duplicated, false);
  assert.equal(first.effects[0].kind, "OPEN");
  const key = first.effects[0].notificationKey;

  const repeat = await engine.ingestObservation(observation()); // 内容指纹相同
  assert.equal(repeat.duplicated, true);
  assert.deepEqual(repeat.effects, []);

  const repeatById = await engine.ingestObservation(
    observation({ id: first.observation.id, magnitude: 9.9 }), // 显式同 id 也视为重复
  );
  assert.equal(repeatById.duplicated, true);

  await engine.idle();
  assert.equal(scripted.calls.length, 1);
  assert.equal(scripted.calls[0].key, key);
  assert.equal(engine.getNotification(key).status, "SENT");
});

test("窗口内升级产生新幂等键通知；同级变化只追加修正不重发", async () => {
  const { engine, scripted } = createEngine();
  await seedSubscription(engine);

  const r1 = await engine.ingestObservation(observation({ magnitude: 3.2 }));
  assert.equal(r1.effects[0].rank, 1);

  const r2 = await engine.ingestObservation(
    observation({ stationId: "S2", magnitude: 4.6, eventTime: new Date(BASE_TIME + 20_000).toISOString() }),
  );
  assert.equal(r2.effects[0].kind, "ESCALATION");
  assert.equal(r2.effects[0].rank, 2);

  const r3 = await engine.ingestObservation(
    observation({ stationId: "S3", magnitude: 2.1, eventTime: new Date(BASE_TIME + 30_000).toISOString() }),
  );
  assert.equal(r3.effects[0].kind, "CORRECTION");
  assert.equal(r3.effects[0].rank, 2);
  assert.equal(r3.effects[0].notificationKey, undefined);

  await engine.idle();
  assert.equal(scripted.calls.length, 2);
  assert.notEqual(scripted.calls[0].key, scripted.calls[1].key);
  assert.ok(scripted.calls[1].key.endsWith(":R2"));

  const decision = engine.getDecision(r1.effects[0].decisionId);
  assert.deepEqual(
    decision.history.map((e) => e.kind),
    ["OPEN", "ESCALATION", "CORRECTION"],
  );
  assert.equal(decision.currentRank, 2);
});

test("不同网格/窗口分别归并为不同决策", async () => {
  const { engine, clock } = createEngine();
  await seedSubscription(engine);

  const a = await engine.ingestObservation(observation({ latitude: 30.1, stationId: "S1" }));
  const b = await engine.ingestObservation(observation({ latitude: 30.9, stationId: "S2" }));
  clock.advance(70_000); // 进入第 2 个窗口（不越过其迟到宽限）
  const c = await engine.ingestObservation(
    observation({ eventTime: new Date(BASE_TIME + 65_000).toISOString(), stationId: "S3" }),
  );
  assert.notEqual(a.cellId, b.cellId);
  assert.notEqual(a.windowStart, c.windowStart);
  assert.equal(engine.listDecisions().length, 3);
});

// ---------------------------------------------------------------
// 迟到观测：只追加修正，不改写历史、不发通知
// ---------------------------------------------------------------

test("窗口关闭后迟到观测追加 LATE_CORRECTION，当前级别与已发通知不变", async () => {
  const { engine, scripted, clock } = createEngine();
  await seedSubscription(engine);

  const opened = await engine.ingestObservation(observation({ magnitude: 3.2 }));
  const decisionId = opened.effects[0].decisionId;
  const key = opened.effects[0].notificationKey;
  await engine.idle();
  const callsAtClose = scripted.calls.length;

  clock.advance(71_000); // 越过窗口 60s + 迟到宽限 10s
  await engine.pump();
  assert.equal(engine.getDecision(decisionId).status, "CLOSED");

  // 迟到的 7 级强震落在同一已关闭窗口。
  const late = await engine.ingestObservation(
    observation({ stationId: "S9", magnitude: 7.2, eventTime: new Date(BASE_TIME + 50_000).toISOString() }),
  );
  assert.equal(late.effects[0].kind, "LATE_CORRECTION");
  assert.equal(late.effects[0].notificationKey, undefined);

  await engine.idle();
  const decision = engine.getDecision(decisionId);
  assert.equal(decision.currentRank, 1); // 当前级别不被迟到观测改写
  assert.equal(decision.history.at(-1).late, true);
  assert.equal(engine.getNotification(key).status, "SENT"); // 已发历史不变
  assert.equal(scripted.calls.length, callsAtClose); // 没有任何新投递
});

test("已关闭窗口才首次达标的迟到观测不得新开告警", async () => {
  const clock = testClock();
  const scripted = scriptedSender();
  const engine = new AlertEngine({ path: tmpLog(), clock, sender: scripted.sender, config: TEST_CONFIG });
  await seedSubscription(engine);

  await engine.ingestObservation(
    observation({ magnitude: 1.2, stationId: "S1", eventTime: new Date(BASE_TIME + 5_000).toISOString() }),
  );
  assert.equal(engine.listDecisions().length, 0);

  clock.advance(71_000);
  await engine.pump();
  const late = await engine.ingestObservation(
    observation({ magnitude: 7.0, stationId: "S9", eventTime: new Date(BASE_TIME + 40_000).toISOString() }),
  );
  assert.deepEqual(late.effects, []);
  assert.equal(engine.listDecisions().length, 0);
  assert.equal(engine.listNotifications().length, 0);
});

// ---------------------------------------------------------------
// 失败重试、终止、人工重试与复活
// ---------------------------------------------------------------

test("可重试失败按退避重试，固定时钟推进后成功；全程同一幂等键", async () => {
  const clock = testClock();
  const scripted = scriptedSender({ fail: (_key, attempt) => (attempt < 3 ? "retryable" : "success") });
  const engine = new AlertEngine({ path: tmpLog(), clock, sender: scripted.sender, config: TEST_CONFIG });
  await seedSubscription(engine);

  const opened = await engine.ingestObservation(observation({ magnitude: 4.2 }));
  await engine.idle();
  const key = opened.effects[0].notificationKey;
  assert.equal(engine.getNotification(key).status, "WAITING");
  assert.equal(engine.getNotification(key).attempts.length, 1);

  clock.advance(10_000); // baseDelayMs
  await engine.pump();
  assert.equal(engine.getNotification(key).status, "WAITING");
  assert.equal(engine.getNotification(key).attempts.length, 2);

  clock.advance(20_000); // 2 × baseDelayMs
  await engine.pump();
  assert.equal(engine.getNotification(key).status, "SENT");
  assert.equal(engine.getNotification(key).attempts.length, 3);
  assert.deepEqual(
    scripted.calls.map((c) => c.key),
    [key, key, key],
  );

  const retry = await engine.retryNotification(key);
  assert.equal(retry.accepted, false); // 已 SENT 的人工重试必须拒绝
  assert.equal(retry.status, "SENT");
  assert.equal(scripted.calls.length, 3);
});

test("超过最大尝试次数后 DEAD，复活沿用同一幂等键成功", async () => {
  const clock = testClock();
  const failing = scriptedSender({ fail: () => "retryable" });
  const engine = new AlertEngine({ path: tmpLog(), clock, sender: failing.sender, config: TEST_CONFIG });
  await seedSubscription(engine);
  const opened = await engine.ingestObservation(observation());
  const key = opened.effects[0].notificationKey;
  await engine.idle();

  for (const delay of [10_000, 20_000, 40_000]) {
    clock.advance(delay);
    await engine.pump();
    await engine.idle();
  }
  assert.equal(engine.getNotification(key).status, "DEAD");
  assert.equal(failing.calls.length, 4); // maxAttempts

  const retry = await engine.retryNotification(key);
  assert.equal(retry.accepted, false);
  assert.equal(failing.calls.length, 4);

  const ok = scriptedSender();
  engine.setSender(ok.sender);
  const resurrected = await engine.resurrect(key);
  assert.equal(resurrected.accepted, true);
  await engine.idle();
  assert.equal(engine.getNotification(key).status, "SENT");
  assert.equal(ok.calls[0].key, key); // 幂等键不变，接收方按键去重
  assert.equal(failing.calls.length, 4); // 旧投递器没有被再次调用
});

test("永久失败一次即 DEAD，不做无意义重试", async () => {
  const clock = testClock();
  const scripted = scriptedSender({ fail: () => "permanent" });
  const engine = new AlertEngine({ path: tmpLog(), clock, sender: scripted.sender, config: TEST_CONFIG });
  await seedSubscription(engine);
  await engine.ingestObservation(observation());
  await engine.idle();
  const key = engine.listNotifications()[0].key;
  assert.equal(engine.getNotification(key).status, "DEAD");
  assert.equal(scripted.calls.length, 1);
});

// ---------------------------------------------------------------
// 进程恢复：重放事件日志，不重复通知
// ---------------------------------------------------------------

test("SENT 状态在进程重启重放后保持，绝不二次投递", async () => {
  const path = tmpLog();
  const clock = testClock();
  const first = scriptedSender();
  let engine = new AlertEngine({ path, clock, sender: first.sender, config: TEST_CONFIG });
  await seedSubscription(engine);
  await engine.ingestObservation(observation({ magnitude: 5.1 }));
  await engine.idle();
  const key = first.calls[0].key;
  assert.equal(engine.getNotification(key).status, "SENT");

  const second = scriptedSender();
  engine = new AlertEngine({ path, clock, sender: second.sender, config: TEST_CONFIG });
  await engine.rearm();
  assert.equal(engine.getNotification(key).status, "SENT");
  assert.equal(second.calls.length, 0);
});

test("崩溃发生在投递在途时：恢复后凭同一幂等键补投", async () => {
  const path = tmpLog();
  const clock = testClock();
  const hanging = scriptedSender({ fail: () => new Promise(() => {}) });
  let engine = new AlertEngine({ path, clock, sender: hanging.sender, config: TEST_CONFIG });
  await seedSubscription(engine);
  const opened = await engine.ingestObservation(observation());
  const key = opened.effects[0].notificationKey;
  await Promise.resolve();
  assert.equal(engine.getNotification(key).status, "IN_FLIGHT");

  // 丢弃实例即模拟进程被杀：日志中最后状态是 NotificationQueued。
  const second = scriptedSender();
  engine = new AlertEngine({ path, clock, sender: second.sender, config: TEST_CONFIG });
  await engine.rearm();
  assert.equal(engine.getNotification(key).status, "SENT");
  assert.equal(second.calls.length, 1);
  assert.equal(second.calls[0].key, key);
});

test("WAITING 的重试在重启后按日志中的 nextAttemptAt 恢复", async () => {
  const path = tmpLog();
  const clock = testClock();
  const failing = scriptedSender({ fail: (_key, attempt) => (attempt < 2 ? "retryable" : "success") });
  let engine = new AlertEngine({ path, clock, sender: failing.sender, config: TEST_CONFIG });
  await seedSubscription(engine);
  await engine.ingestObservation(observation());
  await engine.idle();
  const key = engine.listNotifications()[0].key;
  assert.equal(engine.getNotification(key).status, "WAITING");

  const second = scriptedSender({ fail: () => "success" });
  engine = new AlertEngine({ path, clock, sender: second.sender, config: TEST_CONFIG });
  await engine.rearm(); // 未到重试时刻：不立即投递
  assert.equal(engine.getNotification(key).status, "WAITING");
  assert.equal(second.calls.length, 0);

  clock.advance(10_000);
  await engine.pump();
  await engine.idle();
  assert.equal(engine.getNotification(key).status, "SENT");
  assert.equal(second.calls[0].key, key);
});

// ---------------------------------------------------------------
// 值班视图与辖区阈值
// ---------------------------------------------------------------

test("区域当前级别随活跃单元聚合，窗口关闭后归零但历史保留", async () => {
  const { engine, clock } = createEngine();
  await seedSubscription(engine);
  await engine.ingestObservation(observation({ magnitude: 3.2 }));
  await engine.idle();

  const level = engine.getRegionLevel("R01");
  assert.equal(level.currentRank, 1);
  assert.equal(level.activeCells.length, 1);

  const unacked = engine.listUnacknowledged();
  assert.equal(unacked.length, 1);
  assert.equal(unacked[0].sentRank, 1);

  clock.advance(71_000);
  await engine.pump();
  assert.equal(engine.getRegionLevel("R01").currentRank, 0);
  assert.equal(engine.listDecisions({ status: "CLOSED" }).length, 1);
  assert.equal(engine.listUnacknowledged().length, 1); // 已发升级仍待确认

  await engine.acknowledge(level.activeCells[0].decisionId, "张值班");
  assert.equal(engine.listUnacknowledged().length, 0);
  const decision = engine.getDecision(level.activeCells[0].decisionId);
  assert.equal(decision.acknowledgedBy, "张值班");
});

test("各辖区维护不同阈值，同一观测按订阅分别定级", async () => {
  const { engine } = createEngine();
  await seedSubscription(engine, {
    jurisdictionId: "J01",
    levels: [{ rank: 2, minMagnitude: 4.0, minCount: 1 }],
  });
  await engine.upsertSubscription({
    jurisdictionId: "J02",
    name: "邻区订阅",
    region: "R01",
    webhookUrl: "http://other.invalid/hook",
    levels: [{ rank: 1, minMagnitude: 3.0, minCount: 1 }],
  });

  const result = await engine.ingestObservation(observation({ magnitude: 3.5 }));
  await engine.idle();
  assert.deepEqual(result.effects.map((e) => e.rank), [1]); // J01 不达标，仅 J02 定级
  assert.equal(engine.listUnacknowledged({ jurisdictionId: "J02" }).length, 1);
  assert.equal(engine.listUnacknowledged({ jurisdictionId: "J01" }).length, 0);
});

test("升级后再确认：只确认到当时已送达的最高修订", async () => {
  const { engine } = createEngine();
  await seedSubscription(engine);
  const r1 = await engine.ingestObservation(observation({ magnitude: 3.2 }));
  await engine.ingestObservation(
    observation({ stationId: "S2", magnitude: 5.5, eventTime: new Date(BASE_TIME + 20_000).toISOString() }),
  );
  await engine.idle();
  const decisionId = r1.effects[0].decisionId;
  assert.equal(engine.listUnacknowledged()[0].sentRank, 3);

  await engine.acknowledge(decisionId);
  assert.equal(engine.listUnacknowledged().length, 0);
});

test("并发重复上报同一观测只产生一次 OPEN 与一次通知", async () => {
  const { engine, scripted } = createEngine();
  await seedSubscription(engine);

  const results = await Promise.all(
    Array.from({ length: 5 }, () => engine.ingestObservation(observation())),
  );
  await engine.idle();
  assert.equal(results.filter((r) => !r.duplicated).length, 1);
  assert.equal(scripted.calls.length, 1);
  assert.equal(engine.listDecisions().length, 1);
});
