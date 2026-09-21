// 响应级别与订阅阈值判定。
// 数字秩越大越危险：1 蓝色(Ⅳ) → 2 黄色(Ⅲ) → 3 橙色(Ⅱ) → 4 红色(Ⅰ)，0 为正常。

export const RANK_LABELS = {
  0: "正常",
  1: "蓝色Ⅳ级",
  2: "黄色Ⅲ级",
  3: "橙色Ⅱ级",
  4: "红色Ⅰ级",
};

export const MAX_RANK = 4;

/**
 * 内置默认阈值，订阅未自带 levels 时继承。
 * minCount/maxDepthKm 缺省分别为 1 与不限（越深越弱，浅源更危险）。
 */
export const DEFAULT_LEVELS = [
  { rank: 1, minMagnitude: 3.0, minCount: 1 },
  { rank: 2, minMagnitude: 4.0, minCount: 1 },
  { rank: 3, minMagnitude: 5.0, minCount: 1 },
  { rank: 4, minMagnitude: 6.0, minCount: 1 },
];

export function normalizeLevels(levels) {
  const list = (levels && levels.length ? levels : DEFAULT_LEVELS)
    .map((rule) => {
      const rank = Number(rule.rank);
      if (!Number.isInteger(rank) || rank < 1 || rank > MAX_RANK) {
        throw new Error(`级别秩必须为 1..${MAX_RANK} 的整数`);
      }
      const minMagnitude = Number(rule.minMagnitude);
      if (!Number.isFinite(minMagnitude)) {
        throw new Error("minMagnitude 必须为数值");
      }
      const minCount = rule.minCount === undefined ? 1 : Number(rule.minCount);
      if (!Number.isInteger(minCount) || minCount < 1) {
        throw new Error("minCount 必须为正整数");
      }
      const maxDepthKm =
        rule.maxDepthKm === undefined ? null : Number(rule.maxDepthKm);
      if (maxDepthKm !== null && !Number.isFinite(maxDepthKm)) {
        throw new Error("maxDepthKm 必须为数值");
      }
      return { rank, minMagnitude, minCount, maxDepthKm };
    })
    .sort((a, b) => b.rank - a.rank);
  return list;
}

/**
 * 依据窗口内全部观测判定可达的最高级别。
 * 规则：达到 minMagnitude 且深度不超过 maxDepthKm（如有）的观测数不少于 minCount。
 * @returns {number} 0..4
 */
export function rankForObservations(levels, observations) {
  let rank = 0;
  for (const rule of levels) {
    let qualifying = 0;
    for (const obs of observations) {
      if (obs.magnitude < rule.minMagnitude) continue;
      if (rule.maxDepthKm !== null && obs.depthKm > rule.maxDepthKm) continue;
      qualifying += 1;
    }
    if (qualifying >= rule.minCount) {
      rank = rule.rank;
      break; // levels 已按秩降序
    }
  }
  return rank;
}

/**
 * 汇总一个已归并窗口内的观测，用于定级与不可变记录。
 */
export function aggregateObservations(observations) {
  let count = 0;
  let maxMagnitude = null;
  let minDepthKm = null;
  let maxMagnitudeObservationId = null;
  let firstEventAt = null;
  let lastEventAt = null;
  for (const obs of observations) {
    count += 1;
    if (maxMagnitude === null || obs.magnitude > maxMagnitude) {
      maxMagnitude = obs.magnitude;
      maxMagnitudeObservationId = obs.id;
    }
    if (minDepthKm === null || obs.depthKm < minDepthKm) {
      minDepthKm = obs.depthKm;
    }
    if (firstEventAt === null || obs.eventTime < firstEventAt) {
      firstEventAt = obs.eventTime;
    }
    if (lastEventAt === null || obs.eventTime > lastEventAt) {
      lastEventAt = obs.eventTime;
    }
  }
  return {
    count,
    maxMagnitude,
    minDepthKm,
    maxMagnitudeObservationId,
    firstEventAt,
    lastEventAt,
  };
}

export function aggregateSignature(agg) {
  return `${agg.count}:${agg.maxMagnitude}:${agg.minDepthKm}`;
}
