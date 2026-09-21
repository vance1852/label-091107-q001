// Bearer Token 鉴权。
// 令牌只来自环境变量/运行时注册，绝不写入源码或日志。
// 作用域：admin（全部） > operator（值班查询、确认、重试） > ingest（仅上报观测）。
import { timingSafeEqual } from "node:crypto";

const SCOPES = ["ingest", "operator", "admin"];

export class TokenRegistry {
  #tokens = new Map();

  /**
   * 从环境变量装载。
   * ALERT_API_TOKENS=令牌:作用域[:辖区ID],令牌:作用域[:辖区ID]
   */
  static fromEnv(value = process.env.ALERT_API_TOKENS ?? "") {
    const registry = new TokenRegistry();
    for (const entry of value.split(",")) {
      const piece = entry.trim();
      if (!piece) continue;
      const [token, scope, jurisdictionId] = piece.split(":");
      if (!token || !SCOPES.includes(scope)) {
        throw new Error(`ALERT_API_TOKENS 条目无效，作用域须为 ${SCOPES.join("/")}`);
      }
      registry.add(token, scope, jurisdictionId || null);
    }
    return registry;
  }

  add(token, scope, jurisdictionId = null) {
    if (!SCOPES.includes(scope)) throw new Error(`未知作用域: ${scope}`);
    this.#tokens.set(token, { scope, jurisdictionId });
  }

  get size() {
    return this.#tokens.size;
  }

  authenticate(authorization) {
    if (typeof authorization !== "string" || !authorization.startsWith("Bearer ")) {
      return null;
    }
    const presented = authorization.slice("Bearer ".length).trim();
    if (!presented) return null;
    for (const [token, principal] of this.#tokens) {
      const a = Buffer.from(presented);
      const b = Buffer.from(token);
      if (a.length === b.length && timingSafeEqual(a, b)) {
        return { token: presented, ...principal };
      }
    }
    return null;
  }

  /** 层级作用域：高作用域隐含低作用域。 */
  static allows(principal, scope) {
    if (!principal) return false;
    return SCOPES.indexOf(principal.scope) >= SCOPES.indexOf(scope);
  }
}
