// 空间网格与时间窗口的确定性分桶工具。
// 所有分桶结果只依赖观测本身与配置，保证固定时钟重放时结果可复现。

/**
 * 计算观测落入的网格单元编号（整数索引，避免浮点字符串不稳定）。
 * @param {number} latitude
 * @param {number} longitude
 * @param {number} cellSizeDeg 网格边长（度）
 * @returns {{latBin:number, lonBin:number}}
 */
export function gridBins(latitude, longitude, cellSizeDeg) {
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
    throw new Error("经纬度必须为有限数值");
  }
  if (latitude < -90 || latitude > 90) {
    throw new Error("纬度超出 [-90, 90] 范围");
  }
  if (longitude < -180 || longitude > 180) {
    throw new Error("经度超出 [-180, 180] 范围");
  }
  if (!(cellSizeDeg > 0)) {
    throw new Error("网格边长必须为正数");
  }
  return {
    latBin: Math.floor(latitude / cellSizeDeg),
    lonBin: Math.floor(longitude / cellSizeDeg),
  };
}

/**
 * 稳定单元标识：区域编码 + 网格行列。
 * 带上区域前缀，避免两个辖区边界处坐标相同而被错误归并。
 */
export function cellIdOf(region, latitude, longitude, cellSizeDeg) {
  const { latBin, lonBin } = gridBins(latitude, longitude, cellSizeDeg);
  return `${region}@${latBin},${lonBin}`;
}

/**
 * 时间窗起点：从 epoch 起对齐的固定滚动窗口（tumbling window）。
 * @param {number} eventTimeMs 台站观测时间（毫秒）
 * @param {number} durationMs 窗口长度（毫秒）
 * @param {number} epochMs 对齐原点（毫秒），默认为 Unix 纪元
 */
export function windowStartOf(eventTimeMs, durationMs, epochMs = 0) {
  if (!(durationMs > 0)) {
    throw new Error("时间窗口长度必须为正数");
  }
  return Math.floor((eventTimeMs - epochMs) / durationMs) * durationMs + epochMs;
}

export function cellWindowKey(cellId, windowStart) {
  return `${cellId}|${windowStart}`;
}
