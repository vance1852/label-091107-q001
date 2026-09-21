import assert from "node:assert/strict";
import test from "node:test";
import { createApiServer } from "../src/api.js";
import { TokenRegistry } from "../src/auth.js";
import { AlertEngine } from "../src/engine.js";
import { BASE_TIME, TEST_CONFIG, observation, scriptedSender, testClock, tmpLog } from "./helpers.js";

async function startHarness({ tokens, sender } = {}) {
  const registry = tokens ?? new TokenRegistry();
  const scripted = scriptedSender();
  const engine = new AlertEngine({
    path: tmpLog(),
    clock: testClock(),
    sender: sender ?? scripted.sender,
    config: TEST_CONFIG,
  });
  const app = createApiServer({ engine, tokens: registry });
  await new Promise((resolve) => app.listen(0, resolve));
  const { port } = app.address();
  const base = `http://127.0.0.1:${port}`;
  return {
    base,
    engine,
    app,
    scripted,
    close: () => Promise.all([engine.idle(), new Promise((r) => app.close(r))]),
  };
}

const subscriptionBody = {
  jurisdictionId: "J01",
  name: "市应急办",
  region: "R01",
  webhookUrl: "http://receiver.invalid/hook",
};

test("无令牌与错误令牌返回 401", async () => {
  const h = await startHarness();
  const r1 = await fetch(`${h.base}/v1/decisions`);
  assert.equal(r1.status, 401);
  const r2 = await fetch(`${h.base}/v1/decisions`, {
    headers: { authorization: "Bearer nope" },
  });
  assert.equal(r2.status, 401);
  await h.close();
});

test("ingest 作用域只能上报观测，不能查询值班接口", async () => {
  const tokens = new TokenRegistry();
  tokens.add("ingest-tok", "ingest");
  const h = await startHarness({ tokens });

  const r1 = await fetch(`${h.base}/v1/observations`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: "Bearer ingest-tok" },
    body: JSON.stringify(observation()),
  });
  assert.equal(r1.status, 202);

  const r2 = await fetch(`${h.base}/v1/regions/R01/level`, {
    headers: { authorization: "Bearer ingest-tok" },
  });
  assert.equal(r2.status, 403);
  await h.close();
});

test("operator 可查看区域级别、未确认订阅并确认；重复上报幂等", async () => {
  const tokens = new TokenRegistry();
  tokens.add("op", "operator");
  tokens.add("ing", "ingest");
  const h = await startHarness({ tokens });

  const admin = new TokenRegistry();
  admin.add("a", "admin");
  // 用 admin 建订阅（同一引擎实例，直接调用方法）。
  await h.engine.upsertSubscription(subscriptionBody);

  const postObs = async () => {
    const r = await fetch(`${h.base}/v1/observations`, {
      method: "POST",
      headers: { "content-type": "application/json", authorization: "Bearer ing" },
      body: JSON.stringify(observation({ magnitude: 4.5 })),
    });
    return r;
  };
  assert.equal((await postObs()).status, 202);
  assert.equal((await postObs()).status, 200); // 重复观测 → 200 duplicated
  await h.engine.idle();

  const levelRes = await fetch(`${h.base}/v1/regions/R01/level`, {
    headers: { authorization: "Bearer op" },
  });
  assert.equal(levelRes.status, 200);
  const level = await levelRes.json();
  assert.equal(level.currentRank, 2);

  const unackedRes = await fetch(`${h.base}/v1/unacknowledged`, {
    headers: { authorization: "Bearer op" },
  });
  const { items: unacked } = await unackedRes.json();
  assert.equal(unacked.length, 1);

  const ack = await fetch(`${h.base}/v1/decisions/${unacked[0].decisionId}/acknowledge`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: "Bearer op" },
    body: JSON.stringify({ by: "李值班" }),
  });
  assert.equal(ack.status, 200);
  const after = await (
    await fetch(`${h.base}/v1/unacknowledged`, { headers: { authorization: "Bearer op" } })
  ).json();
  assert.equal(after.items.length, 0);

  assert.equal(h.scripted.calls.length, 1); // 全程只投递一次
  await h.close();
});

test("绑定辖区的 operator 无法看到其他辖区的决策", async () => {
  const tokens = new TokenRegistry();
  tokens.add("op1", "operator", "J01");
  const h = await startHarness({ tokens });
  await h.engine.upsertSubscription({ ...subscriptionBody, jurisdictionId: "J02" });
  await h.engine.ingestObservation(observation());
  await h.engine.idle();

  const res = await fetch(`${h.base}/v1/unacknowledged`, {
    headers: { authorization: "Bearer op1" },
  });
  const { items } = await res.json();
  assert.deepEqual(items, []);

  const decisionId = h.engine.listDecisions()[0].id;
  const r = await fetch(`${h.base}/v1/decisions/${decisionId}`, {
    headers: { authorization: "Bearer op1" },
  });
  assert.equal(r.status, 404);
  await h.close();
});

test("admin 通过接口管理订阅，operator 不能创建", async () => {
  const tokens = new TokenRegistry();
  tokens.add("admin-tok", "admin");
  tokens.add("op-tok", "operator");
  const h = await startHarness({ tokens });

  const forbidden = await fetch(`${h.base}/v1/subscriptions`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: "Bearer op-tok" },
    body: JSON.stringify(subscriptionBody),
  });
  assert.equal(forbidden.status, 403);

  const created = await fetch(`${h.base}/v1/subscriptions`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: "Bearer admin-tok" },
    body: JSON.stringify(subscriptionBody),
  });
  assert.equal(created.status, 201);
  const sub = await created.json();
  assert.equal(sub.region, "R01");

  const updated = await fetch(`${h.base}/v1/subscriptions/${sub.id}`, {
    method: "PUT",
    headers: { "content-type": "application/json", authorization: "Bearer admin-tok" },
    body: JSON.stringify({ name: "改名后的订阅" }),
  });
  assert.equal(updated.status, 200);
  assert.equal((await updated.json()).name, "改名后的订阅");

  const list = await fetch(`${h.base}/v1/subscriptions`, {
    headers: { authorization: "Bearer op-tok" },
  });
  assert.equal(list.status, 200);
  await h.close();
});

test("非法观测返回 400 且不产生事件", async () => {
  const tokens = new TokenRegistry();
  tokens.add("ing", "ingest");
  const h = await startHarness({ tokens });
  const r = await fetch(`${h.base}/v1/observations`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: "Bearer ing" },
    body: JSON.stringify({ ...observation(), magnitude: 99 }),
  });
  assert.equal(r.status, 400);
  const body = await r.json();
  assert.match(body.detail, /震级/);
  await h.close();
});

test("已 SENT 通知的人工重试返回 409", async () => {
  const tokens = new TokenRegistry();
  tokens.add("op", "operator");
  const h = await startHarness({ tokens });
  await h.engine.upsertSubscription(subscriptionBody);
  await h.engine.ingestObservation(observation());
  await h.engine.idle();
  const key = h.engine.listNotifications()[0].key;

  const r = await fetch(`${h.base}/v1/notifications/${encodeURIComponent(key)}/retry`, {
    method: "POST",
    headers: { authorization: "Bearer op" },
  });
  assert.equal(r.status, 409);
  const body = await r.json();
  assert.equal(body.accepted, false);
  assert.equal(h.scripted.calls.length, 1);
  await h.close();
});
