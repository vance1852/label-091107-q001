import assert from "node:assert/strict";
import test from "node:test";
import { createApiServer } from "../src/api.js";
import { TokenRegistry } from "../src/auth.js";
import { AlertEngine } from "../src/engine.js";
import { ManualClock } from "../src/clock.js";

// 健康检查不依赖鉴权与持久化，用纯内存引擎装配。
function makeApp() {
  const engine = new AlertEngine({
    path: null,
    clock: new ManualClock("2026-01-01T00:00:00Z"),
    sender: async () => ({ ok: true, status: 200 }),
  });
  return createApiServer({ engine, tokens: new TokenRegistry() });
}

test("健康检查返回服务状态", async () => {
  const app = makeApp();
  await new Promise((resolve) => app.listen(0, resolve));
  const { port } = app.address();
  const response = await fetch(`http://127.0.0.1:${port}/health`);
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { service: "aftershock-alert", status: "ok" });
  await new Promise((resolve) => app.close(resolve));
});
