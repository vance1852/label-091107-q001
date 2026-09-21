// HTTP 接口：鉴权、JSON 校验与引擎方法的薄封装。
//
// 作用域：ingest（仅观测上报）、operator（值班查询/确认/重试，辖区受限）、admin（订阅管理/审计）。
import { createServer } from "node:http";
import { TokenRegistry } from "./auth.js";

const MAX_BODY_BYTES = 1_048_576;

export function createApiServer({ engine, tokens }) {
  return createServer(async (request, response) => {
    try {
      await route(request, response, { engine, tokens });
    } catch (error) {
      const status = error?.statusCode ?? 400; // 引擎对输入校验抛出的错误统一为 400
      sendJson(response, status >= 400 && status < 600 ? status : 400, {
        error: status >= 500 ? "服务器内部错误" : "请求被拒绝",
        detail: String(error?.message ?? error),
      });
    }
  });
}

async function route(request, response, deps) {
  const url = new URL(request.url, "http://localhost");
  const { pathname } = url;

  if (request.method === "GET" && pathname === "/health") {
    sendJson(response, 200, { service: "aftershock-alert", status: "ok" });
    return;
  }

  const principal = deps.tokens.authenticate(request.headers.authorization);
  if (!principal) {
    sendJson(response, 401, { error: "未鉴权：请提供有效的 Bearer 令牌" });
    return;
  }
  const requireScope = (scope) => TokenRegistry.allows(principal, scope);
  // operator 若绑定辖区，只能访问本辖区数据。
  const jurisdictionScope = principal.scope === "operator" ? principal.jurisdictionId : null;

  // ---------- 观测接入 ----------
  if (request.method === "POST" && pathname === "/v1/observations") {
    if (!requireScope("ingest")) return forbidden(response);
    const body = await readJson(request);
    const result = await deps.engine.ingestObservation(body);
    sendJson(response, result.duplicated ? 200 : 202, result);
    return;
  }

  // ---------- 订阅管理（admin） ----------
  if (request.method === "POST" && pathname === "/v1/subscriptions") {
    if (!requireScope("admin")) return forbidden(response);
    const body = await readJson(request);
    const subscription = await deps.engine.upsertSubscription(body);
    sendJson(response, 201, subscription);
    return;
  }

  let match;
  if (request.method === "PUT" && (match = pathname.match(/^\/v1\/subscriptions\/([^/]+)$/))) {
    if (!requireScope("admin")) return forbidden(response);
    const body = await readJson(request);
    const subscription = await deps.engine.upsertSubscription({ ...body, id: decode(match[1]) });
    sendJson(response, 200, subscription);
    return;
  }

  if (request.method === "GET" && pathname === "/v1/subscriptions") {
    if (!requireScope("operator")) return forbidden(response);
    const list = deps.engine.listSubscriptions({
      jurisdictionId: jurisdictionScope ?? optional(url.searchParams.get("jurisdictionId")),
      active: url.searchParams.has("active") ? url.searchParams.get("active") === "true" : undefined,
    });
    sendJson(response, 200, { items: list });
    return;
  }

  // ---------- 值班查询（operator） ----------
  if (request.method === "GET" && (match = pathname.match(/^\/v1\/regions\/([^/]+)\/level$/))) {
    if (!requireScope("operator")) return forbidden(response);
    const region = decode(match[1]);
    const level = deps.engine.getRegionLevel(region, { jurisdictionId: jurisdictionScope });
    sendJson(response, 200, level);
    return;
  }

  if (request.method === "GET" && pathname === "/v1/decisions") {
    if (!requireScope("operator")) return forbidden(response);
    const items = deps.engine.listDecisions({
      region: optional(url.searchParams.get("region")),
      cellId: optional(url.searchParams.get("cellId")),
      jurisdictionId: jurisdictionScope ?? optional(url.searchParams.get("jurisdictionId")),
      status: optional(url.searchParams.get("status")),
    });
    sendJson(response, 200, { items });
    return;
  }

  if (request.method === "GET" && (match = pathname.match(/^\/v1\/decisions\/([^/]+)$/))) {
    if (!requireScope("operator")) return forbidden(response);
    const decision = deps.engine.getDecision(decode(match[1]));
    if (!decision) return notFound(response);
    if (jurisdictionScope && decision.jurisdictionId !== jurisdictionScope) return notFound(response);
    sendJson(response, 200, decision);
    return;
  }

  if (request.method === "POST" && (match = pathname.match(/^\/v1\/decisions\/([^/]+)\/acknowledge$/))) {
    if (!requireScope("operator")) return forbidden(response);
    const id = decode(match[1]);
    const existing = deps.engine.getDecision(id);
    if (!existing) return notFound(response);
    if (jurisdictionScope && existing.jurisdictionId !== jurisdictionScope) return notFound(response);
    const body = await readJson(request, true);
    const decision = await deps.engine.acknowledge(id, body?.by ?? principal.scope);
    sendJson(response, 200, decision);
    return;
  }

  if (request.method === "GET" && pathname === "/v1/unacknowledged") {
    if (!requireScope("operator")) return forbidden(response);
    const items = deps.engine.listUnacknowledged({ jurisdictionId: jurisdictionScope });
    sendJson(response, 200, { items });
    return;
  }

  // ---------- 通知与重试 ----------
  if (request.method === "GET" && pathname === "/v1/notifications") {
    if (!requireScope("operator")) return forbidden(response);
    const items = deps.engine.listNotifications({
      decisionId: optional(url.searchParams.get("decisionId")),
      jurisdictionId: jurisdictionScope ?? optional(url.searchParams.get("jurisdictionId")),
      status: optional(url.searchParams.get("status")),
    });
    sendJson(response, 200, { items });
    return;
  }

  if (request.method === "GET" && (match = pathname.match(/^\/v1\/notifications\/([^/]+)$/))) {
    if (!requireScope("operator")) return forbidden(response);
    const notification = deps.engine.getNotification(decode(match[1]));
    if (!notification) return notFound(response);
    if (jurisdictionScope && notification.jurisdictionId !== jurisdictionScope) return notFound(response);
    sendJson(response, 200, notification);
    return;
  }

  if (request.method === "POST" && (match = pathname.match(/^\/v1\/notifications\/([^/]+)\/retry$/))) {
    if (!requireScope("operator")) return forbidden(response);
    const key = decode(match[1]);
    const notification = deps.engine.getNotification(key);
    if (!notification) return notFound(response);
    if (jurisdictionScope && notification.jurisdictionId !== jurisdictionScope) return notFound(response);
    const result = await deps.engine.retryNotification(key);
    sendJson(response, result.accepted ? 202 : 409, result);
    return;
  }

  if (request.method === "POST" && (match = pathname.match(/^\/v1\/notifications\/([^/]+)\/resurrect$/))) {
    if (!requireScope("admin")) return forbidden(response);
    const result = await deps.engine.resurrect(decode(match[1]));
    sendJson(response, result.accepted ? 202 : 409, result);
    return;
  }

  // ---------- 事件审计（admin，重放核对用） ----------
  if (request.method === "GET" && pathname === "/v1/events") {
    if (!requireScope("admin")) return forbidden(response);
    const since = Number(url.searchParams.get("since") ?? 0);
    const typesParam = optional(url.searchParams.get("types"));
    const items = deps.engine.listEvents({
      since: Number.isFinite(since) ? since : 0,
      types: typesParam ? typesParam.split(",") : undefined,
    });
    sendJson(response, 200, { items });
    return;
  }

  sendJson(response, 404, { error: "接口不存在" });
}

function optional(value) {
  return value === null ? undefined : value;
}

function decode(segment) {
  return decodeURIComponent(segment);
}

function forbidden(response) {
  sendJson(response, 403, { error: "权限不足" });
}

function notFound(response) {
  sendJson(response, 404, { error: "资源不存在" });
}

function sendJson(response, status, payload) {
  response.writeHead(status, { "content-type": "application/json; charset=utf-8" });
  response.end(JSON.stringify(payload));
}

async function readJson(request, allowEmpty = false) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > MAX_BODY_BYTES) {
      throw Object.assign(new Error("请求体超过 1MB 限制"), { statusCode: 413 });
    }
    chunks.push(chunk);
  }
  const raw = Buffer.concat(chunks).toString("utf8");
  if (!raw) {
    if (allowEmpty) return {};
    throw Object.assign(new Error("请求体不能为空"), { statusCode: 400 });
  }
  try {
    return JSON.parse(raw);
  } catch {
    throw Object.assign(new Error("请求体不是合法 JSON"), { statusCode: 400 });
  }
}
