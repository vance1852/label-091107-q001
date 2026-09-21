import { readFileSync } from "node:fs";

const ROLES = new Set(["duty", "station"]);

/**
 * 从 JSON 文件加载服务配置。
 * 令牌、订阅端点等敏感与业务数据只能来自配置文件或环境变量，不写进源码。
 */
export function loadConfig(env = process.env) {
  const path = env.AAS_CONFIG;
  if (!path) {
    throw new Error("请通过环境变量 AAS_CONFIG 指定服务配置文件（可参考 config/example.json）");
  }
  return normalizeConfig(JSON.parse(readFileSync(path, "utf8")), env);
}

export function normalizeConfig(raw, env = process.env) {
  const problems = [];
  if (!Number.isInteger(raw.windowMs) || raw.windowMs <= 0) {
    problems.push("windowMs 必须是正整数（毫秒）");
  }
  if (!Array.isArray(raw.levelThresholds) || raw.levelThresholds.length === 0) {
    problems.push("levelThresholds 至少需要一个等级阈值");
  } else {
    for (const t of raw.levelThresholds) {
      if (!Number.isInteger(t?.level) || t.level < 1 || typeof t?.minMagnitude !== "number") {
        problems.push("levelThresholds 的每一项必须是 { level: 正整数, minMagnitude: 数字 }");
      }
    }
  }
  if (!raw.grid || !Array.isArray(raw.grid.cells) || raw.grid.cells.length === 0) {
    problems.push("grid.cells 至少需要一个网格单元");
  }
  if (!Array.isArray(raw.tokens) || raw.tokens.length === 0) {
    problems.push("tokens 至少需要一个访问令牌");
  } else {
    for (const t of raw.tokens) {
      if (typeof t?.token !== "string" || t.token.length < 8) {
        problems.push("每个令牌必须是长度不少于 8 的字符串");
      }
      if (!ROLES.has(t?.role)) {
        problems.push(`令牌角色必须是 ${[...ROLES].join(" 或 ")}`);
      }
    }
    if (!raw.tokens.some((t) => t.role === "duty")) problems.push("至少配置一个 duty 角色令牌");
    if (!raw.tokens.some((t) => t.role === "station")) problems.push("至少配置一个 station 角色令牌");
  }
  if (problems.length > 0) {
    throw new Error(`配置无效：\n- ${problems.join("\n- ")}`);
  }
  return {
    port: Number(env.PORT ?? raw.port ?? 3000),
    dataFile: raw.dataFile ?? "data/events.jsonl",
    windowMs: raw.windowMs,
    levelThresholds: raw.levelThresholds,
    grid: raw.grid,
    tokens: new Map(raw.tokens.map((t) => [t.token, { role: t.role, name: t.name ?? t.role }])),
    delivery: {
      maxAttempts: raw.delivery?.maxAttempts ?? 5,
      baseBackoffMs: raw.delivery?.baseBackoffMs ?? 2000,
      timeoutMs: raw.delivery?.timeoutMs ?? 5000,
    },
    maxFutureSkewMs: raw.maxFutureSkewMs ?? 300000,
    worker: { enabled: raw.worker?.enabled !== false, pollMs: raw.worker?.pollMs ?? 1000 },
  };
}
