import { createServer } from "node:http";
import { authenticate } from "./auth.js";
import { ServiceError } from "./errors.js";

const MAX_BODY_BYTES = 1024 * 1024;

const ROUTES = [
  {
    method: "GET",
    pattern: ["health"],
    role: null,
    handler: () => ({ data: { service: "aftershock-alert", status: "ok" } }),
  },
  {
    method: "POST",
    pattern: ["v1", "observations"],
    role: "station",
    handler: ({ service, body }) => {
      const result = service.ingestObservation(body);
      return { status: result.duplicate ? 200 : 201, data: result };
    },
  },
  {
    method: "PUT",
    pattern: ["v1", "subscriptions"],
    role: "duty",
    handler: ({ service, body }) => ({ data: service.upsertSubscription(body) }),
  },
  {
    method: "GET",
    pattern: ["v1", "subscriptions"],
    role: "duty",
    handler: ({ service, query }) => ({
      data: { subscriptions: service.listSubscriptions(query.get("jurisdictionId")) },
    }),
  },
  {
    method: "GET",
    pattern: ["v1", "cells", ":cellId"],
    role: "duty",
    handler: ({ service, params }) => ({ data: service.cellState(params.cellId) }),
  },
  {
    method: "GET",
    pattern: ["v1", "cells", ":cellId", "history"],
    role: "duty",
    handler: ({ service, params }) => ({ data: { events: service.cellHistory(params.cellId) } }),
  },
  {
    method: "GET",
    pattern: ["v1", "cells", ":cellId", "windows", ":index"],
    role: "duty",
    handler: ({ service, params }) => ({
      data: service.windowView(params.cellId, Number(params.index)),
    }),
  },
  {
    method: "GET",
    pattern: ["v1", "regions", ":region"],
    role: "duty",
    handler: ({ service, params }) => ({ data: service.regionView(params.region) }),
  },
  {
    method: "GET",
    pattern: ["v1", "jurisdictions", ":jurisdictionId", "unacknowledged"],
    role: "duty",
    handler: ({ service, params }) => ({
      data: { notifications: service.unacknowledged(params.jurisdictionId) },
    }),
  },
  {
    method: "POST",
    pattern: ["v1", "notifications", ":notificationId", "ack"],
    role: "duty",
    handler: ({ service, params, body, principal }) => ({
      data: service.ackNotification(params.notificationId, body?.by ?? principal.name),
    }),
  },
  {
    method: "GET",
    pattern: ["v1", "notifications", ":notificationId"],
    role: "duty",
    handler: ({ service, params }) => ({ data: service.notificationView(params.notificationId) }),
  },
  {
    method: "GET",
    pattern: ["v1", "idempotency", ":key"],
    role: "duty",
    handler: ({ service, params }) => ({ data: service.idempotencyView(params.key) }),
  },
];

export function createApp({ service, tokens }) {
  return createServer(async (request, response) => {
    try {
      const url = new URL(request.url, "http://aftershock-alert.local");
      const segments = url.pathname.split("/").filter(Boolean).map(decodeURIComponent);
      const route = matchRoute(request.method, segments);
      if (!route) throw new ServiceError(404, "not_found", "接口不存在");
      let principal = null;
      if (route.role) {
        principal = authenticate(request, tokens);
        if (!principal) throw new ServiceError(401, "unauthenticated", "缺少或无效的访问令牌");
        if (principal.role !== route.role) {
          throw new ServiceError(403, "forbidden", "该令牌的角色无权访问此接口");
        }
      }
      const body = await readJsonBody(request);
      const result = await route.handler({
        service,
        params: route.params,
        query: url.searchParams,
        body,
        principal,
      });
      sendJson(response, result.status ?? 200, result.data);
    } catch (error) {
      if (error instanceof ServiceError) {
        sendJson(response, error.status, { error: { code: error.code, message: error.message } });
      } else {
        console.error("未处理的服务错误", error);
        sendJson(response, 500, { error: { code: "internal_error", message: "服务内部错误" } });
      }
    }
  });
}

function matchRoute(method, segments) {
  for (const route of ROUTES) {
    if (route.method !== method || route.pattern.length !== segments.length) continue;
    const params = {};
    let matched = true;
    for (let i = 0; i < route.pattern.length; i += 1) {
      const part = route.pattern[i];
      if (part.startsWith(":")) {
        params[part.slice(1)] = segments[i];
      } else if (part !== segments[i]) {
        matched = false;
        break;
      }
    }
    if (matched) return { ...route, params };
  }
  return null;
}

function readJsonBody(request) {
  return new Promise((resolve, reject) => {
    if (request.method === "GET" || request.method === "HEAD") {
      resolve(null);
      return;
    }
    const chunks = [];
    let size = 0;
    request.on("data", (chunk) => {
      size += chunk.length;
      if (size > MAX_BODY_BYTES) {
        reject(new ServiceError(413, "body_too_large", "请求体超过 1MB 限制"));
        request.destroy();
        return;
      }
      chunks.push(chunk);
    });
    request.on("end", () => {
      if (chunks.length === 0) {
        resolve(null);
        return;
      }
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString("utf8")));
      } catch {
        reject(new ServiceError(400, "invalid_json", "请求体不是合法 JSON"));
      }
    });
    request.on("error", reject);
  });
}

function sendJson(response, status, data) {
  response.writeHead(status, { "content-type": "application/json; charset=utf-8" });
  response.end(JSON.stringify(data));
}
