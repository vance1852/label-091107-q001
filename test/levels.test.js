import assert from "node:assert/strict";
import test from "node:test";
import { aggregateObservations, normalizeLevels, rankForObservations } from "../src/levels.js";

const obs = (magnitude, depthKm = 10) => ({ magnitude, depthKm });

test("内置阈值按最高达标级别定级", () => {
  const levels = normalizeLevels();
  assert.equal(rankForObservations(levels, [obs(2.9)]), 0);
  assert.equal(rankForObservations(levels, [obs(3.0)]), 1);
  assert.equal(rankForObservations(levels, [obs(4.9)]), 2);
  assert.equal(rankForObservations(levels, [obs(6.2)]), 4);
});

test("minCount 需要窗口内足够数量的达标观测", () => {
  const levels = normalizeLevels([{ rank: 2, minMagnitude: 4.0, minCount: 2 }]);
  assert.equal(rankForObservations(levels, [obs(4.1), obs(3.9)]), 0);
  assert.equal(rankForObservations(levels, [obs(4.1), obs(4.0)]), 2);
});

test("maxDepthKm 可排除深源小震的刷屏告警", () => {
  const levels = normalizeLevels([{ rank: 1, minMagnitude: 3.0, maxDepthKm: 30 }]);
  assert.equal(rankForObservations(levels, [obs(3.5, 50)]), 0);
  assert.equal(rankForObservations(levels, [obs(3.5, 20)]), 1);
});

test("聚合结果随观测内容变化，供签名判定", () => {
  const a = aggregateObservations([obs(3.2, 12), obs(4.1, 8)]);
  assert.equal(a.count, 2);
  assert.equal(a.maxMagnitude, 4.1);
  assert.equal(a.minDepthKm, 8);
});
