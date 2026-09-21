/**
 * 按窗口内最大震级匹配等级阈值。
 * thresholds 形如 [{ level: 1, minMagnitude: 3.0 }, ...]，未达任何阈值时为 0 级。
 */
export function levelFor(maxMagnitude, thresholds) {
  if (maxMagnitude === null || maxMagnitude === undefined) return 0;
  let level = 0;
  for (const threshold of thresholds) {
    if (maxMagnitude >= threshold.minMagnitude && threshold.level > level) {
      level = threshold.level;
    }
  }
  return level;
}
