import assert from "node:assert/strict";
import test from "node:test";
import { api, DUTY_TOKEN, ingest, observation, startTestServer, STATION_TOKEN } from "./helpers.js";

test("缺少幂等键返回 422", async (t) => {
  const ctx = await startTestServer();
  t.after(ctx.close);
  const body = observation();
  delete body.idempotencyKey;
  const res = await api(ctx.base, "POST", "/v1/observations", { token: STATION_TOKEN, body });
  assert.equal(res.status, 422);
  assert.equal(res.body.error.code, "invalid_idempotency_key");
});

test("震级与深度越界返回 422", async (t) => {
  const ctx = await startTestServer();
  t.after(ctx.close);
  const mag = await ingest(ctx.base, { idempotencyKey: "k-1", magnitude: 11 });
  assert.equal(mag.status, 422);
  assert.equal(mag.body.error.code, "invalid_magnitude");
  const depth = await ingest(ctx.base, { idempotencyKey: "k-2", depthKm: -1 });
  assert.equal(depth.status, 422);
  assert.equal(depth.body.error.code, "invalid_depth");
});

test("台站时间必须带时区", async (t) => {
  const ctx = await startTestServer();
  t.after(ctx.close);
  const res = await ingest(ctx.base, { idempotencyKey: "k-1", observedAt: "2026-09-21T08:00:10" });
  assert.equal(res.status, 422);
  assert.equal(res.body.error.code, "invalid_observed_at");
});

test("超出允许偏移的未来观测返回 422", async (t) => {
  const ctx = await startTestServer({ start: "2026-09-21T08:00:30Z" });
  t.after(ctx.close);
  const res = await ingest(ctx.base, { idempotencyKey: "k-1", observedAt: "2026-09-21T09:00:00Z" });
  assert.equal(res.status, 422);
  assert.equal(res.body.error.code, "observation_in_future");
});

test("未映射区域在无默认单元时返回 422，有默认单元时落入默认单元", async (t) => {
  const strict = await startTestServer();
  t.after(strict.close);
  const rejected = await ingest(strict.base, { idempotencyKey: "k-1", region: "R-UNKNOWN" });
  assert.equal(rejected.status, 422);
  assert.equal(rejected.body.error.code, "unknown_region");
  const regionView = await api(strict.base, "GET", "/v1/regions/R-UNKNOWN", { token: DUTY_TOKEN });
  assert.equal(regionView.status, 404);

  const lenient = await startTestServer({
    configOverrides: {
      grid: {
        cells: [
          { cellId: "C1", regions: ["R1", "R2"] },
          { cellId: "C2", regions: ["R3"] },
        ],
        defaultCellId: "C2",
      },
    },
  });
  t.after(lenient.close);
  const accepted = await ingest(lenient.base, { idempotencyKey: "k-1", region: "R-UNKNOWN" });
  assert.equal(accepted.status, 201);
  assert.equal(accepted.body.cellId, "C2");
});

test("非法 JSON 请求体返回 400", async (t) => {
  const ctx = await startTestServer();
  t.after(ctx.close);
  const response = await fetch(`${ctx.base}/v1/observations`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${STATION_TOKEN}` },
    body: "{not json",
  });
  assert.equal(response.status, 400);
  assert.equal((await response.json()).error.code, "invalid_json");
});
