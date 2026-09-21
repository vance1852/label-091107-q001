import assert from "node:assert/strict";
import test from "node:test";
import { cellIdOf, gridBins, windowStartOf } from "../src/grid.js";

test("同一边界内坐标归入同一网格单元", () => {
  assert.deepEqual(gridBins(30.1, 120.1, 0.5), gridBins(30.4, 120.4, 0.5));
  assert.notDeepEqual(gridBins(30.1, 120.1, 0.5), gridBins(30.6, 120.1, 0.5));
});

test("负坐标按 floor 分桶，零点两侧不合并", () => {
  assert.equal(gridBins(-0.1, -0.1, 0.5).latBin, -1);
  assert.notDeepEqual(gridBins(0.1, 0.1, 0.5), gridBins(-0.1, -0.1, 0.5));
});

test("单元标识带区域前缀，跨辖区坐标不互相归并", () => {
  assert.notEqual(
    cellIdOf("R01", 30.1, 120.1, 0.5),
    cellIdOf("R02", 30.1, 120.1, 0.5),
  );
});

test("时间窗按 epoch 对齐，边界观测落入各自窗口", () => {
  const w = 60_000;
  assert.equal(windowStartOf(65_000, w), 60_000);
  assert.equal(windowStartOf(59_999, w), 0);
  assert.equal(windowStartOf(60_000, w), 60_000);
  assert.notEqual(windowStartOf(59_999, w), windowStartOf(60_000, w));
});
