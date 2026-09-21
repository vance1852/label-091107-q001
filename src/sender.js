// Webhook 投递器：把同一份告警快照按幂等键投递给订阅地址。
// 重试时请求体与 Idempotency-Key 头保持不变，接收方可据此去重——
// 这是“进程恢复 / 投递失败不产生第二次通知”在跨进程语义上的保证。

const DEFAULT_TIMEOUT_MS = 5000;

/**
 * @param {{timeoutMs?:number, fetchImpl?:typeof fetch}} [opts]
 * @returns {(notification:object)=>Promise<object>}
 */
export function createWebhookSender({ timeoutMs = DEFAULT_TIMEOUT_MS, fetchImpl = fetch } = {}) {
  return async function send(notification) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    let response;
    try {
      response = await fetchImpl(notification.webhookUrl, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "idempotency-key": notification.key,
          "x-alert-decision": notification.decisionId,
        },
        body: JSON.stringify(buildWebhookBody(notification)),
        signal: controller.signal,
      });
    } catch (error) {
      clearTimeout(timer);
      return {
        ok: false,
        retryable: true,
        error: error?.name === "AbortError" ? "请求超时" : `网络错误: ${error?.message ?? error}`,
      };
    }
    clearTimeout(timer);
    const status = response.status;
    if (response.ok) {
      return { ok: true, status };
    }
    return {
      ok: false,
      status,
      // 429 与 5xx 可重试；其余 4xx 视为永久失败，避免无意义重试。
      retryable: status === 429 || status >= 500,
      error: `HTTP ${status}`,
    };
  };
}

export function buildWebhookBody(notification) {
  return {
    type: "aftershock.alert",
    idempotencyKey: notification.key,
    decisionId: notification.decisionId,
    subscriptionId: notification.subscriptionId,
    jurisdictionId: notification.jurisdictionId,
    region: notification.region,
    cellId: notification.cellId,
    windowStart: new Date(notification.windowStart).toISOString(),
    windowEnd: new Date(notification.windowEnd).toISOString(),
    rank: notification.rank,
    level: notification.level,
    escalatedFrom: notification.escalatedFrom,
    aggregate: notification.aggregate,
    queuedAt: new Date(notification.queuedAt).toISOString(),
  };
}
