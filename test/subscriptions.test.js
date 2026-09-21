import assert from "node:assert/strict";
import test from "node:test";
import { api, DUTY_TOKEN, startTestServer, subscribe } from "./helpers.js";

test("辖区可维护自己的订阅阈值，重复维护保持同一订阅", async (t) => {
  const ctx = await startTestServer();
  t.after(ctx.close);

  const created = await subscribe(ctx.base, { minLevel: 2 });
  assert.equal(created.status, 200);
  assert.equal(created.body.subscriptionId, "J1@C1");
  assert.equal(created.body.minLevel, 2);

  const updated = await subscribe(ctx.base, { minLevel: 3 });
  assert.equal(updated.body.subscriptionId, "J1@C1");
  assert.equal(updated.body.minLevel, 3);

  const list = await api(ctx.base, "GET", "/v1/subscriptions?jurisdictionId=J1", {
    token: DUTY_TOKEN,
  });
  assert.equal(list.body.subscriptions.length, 1);
  assert.equal(list.body.subscriptions[0].minLevel, 3);

  const all = await api(ctx.base, "GET", "/v1/subscriptions", { token: DUTY_TOKEN });
  assert.equal(all.body.subscriptions.length, 1);
});

test("订阅参数校验", async (t) => {
  const ctx = await startTestServer();
  t.after(ctx.close);

  const unknownCell = await subscribe(ctx.base, { cellId: "NOPE" });
  assert.equal(unknownCell.status, 422);
  assert.equal(unknownCell.body.error.code, "unknown_cell");

  const badLevel = await subscribe(ctx.base, { minLevel: 0 });
  assert.equal(badLevel.status, 422);
  assert.equal(badLevel.body.error.code, "invalid_min_level");

  const badEndpoint = await subscribe(ctx.base, { endpoint: "ftp://x" });
  assert.equal(badEndpoint.status, 422);
  assert.equal(badEndpoint.body.error.code, "invalid_endpoint");

  const noJurisdiction = await subscribe(ctx.base, { jurisdictionId: "" });
  assert.equal(noJurisdiction.status, 422);
});
