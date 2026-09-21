/**
 * 时间窗口为半开区间 [start, end)：
 * 台站时间恰好等于 end 的观测归入下一个窗口。
 */
export function windowIndexFor(epochMs, windowMs) {
  return Math.floor(epochMs / windowMs);
}

export function windowBounds(index, windowMs) {
  const startMs = index * windowMs;
  return { startMs, endMs: startMs + windowMs };
}

export function windowKey(cellId, index) {
  return `${cellId}|${index}`;
}
