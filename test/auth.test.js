import assert from "node:assert/strict";
import test from "node:test";
import { api, DUTY_TOKEN, ingest, observation, startTestServer, STATION_TOKEN } from "./helpers.js";

test("未携带令牌访问值班接口返回 401", async (t) => {
  const ctx = await startTestServer();
  t.after(ctx.close);
  const res = await api(ctx.base, "GET", "/v1/cells/C1");
  assert.equal(res.status, 401);
  assert.equal(res.body.error.code, "unauthenticated");
});

test("错误令牌返回 401", async (t) => {
  const ctx = await startTestServer();
  t.after(ctx.close);
  const res = await api(ctx.base, "GET", "/v1/cells/C1", { token: "wrong-token" });
  assert.equal(res.status, 401);
});

test("台站令牌访问值班接口返回 403", async (t) => {
  const ctx = await startTestServer();
  t.after(ctx.close);
  const res = await api(ctx.base, "GET", "/v1/cells/C1", { token: STATION_TOKEN });
  assert.equal(res.status, 403);
  assert.equal(res.body.error.code, "forbidden");
});

test("值班令牌调用上报接口返回 403", async (t) => {
  const ctx = await startTestServer();
  t.after(ctx.close);
  const res = await api(ctx.base, "POST", "/v1/observations", {
    token: DUTY_TOKEN,
    body: observation(),
  });
  assert.equal(res.status, 403);
});

test("台站令牌可上报，值班令牌可查询", async (t) => {
  const ctx = await startTestServer();
  t.after(ctx.close);
  const ingested = await ingest(ctx.base, { idempotencyKey: "k-1" });
  assert.equal(ingested.status, 201);
  const state = await api(ctx.base, "GET", "/v1/cells/C1", { token: DUTY_TOKEN });
  assert.equal(state.status, 200);
});
