import assert from "node:assert/strict";
import test from "node:test";
import { api, startTestServer } from "./helpers.js";

test("健康检查返回服务状态", async (t) => {
  const ctx = await startTestServer();
  t.after(ctx.close);
  const res = await api(ctx.base, "GET", "/health");
  assert.equal(res.status, 200);
  assert.deepEqual(res.body, { service: "aftershock-alert", status: "ok" });
});
