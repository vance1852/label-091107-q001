/**
 * 基于静态令牌表的鉴权。
 * 令牌只来自配置文件或环境变量，绝不写入源码；
 * 角色区分 station（台站上报）与 duty（值班查询/管理）。
 */
export function authenticate(request, tokens) {
  const header = request.headers.authorization;
  if (typeof header !== "string") return null;
  const match = /^Bearer\s+(.+)$/.exec(header.trim());
  if (!match) return null;
  return tokens.get(match[1]) ?? null;
}
