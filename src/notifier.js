/**
 * 通过 HTTP Webhook 向辖区订阅端点投递告警通知。
 * 投递头携带 x-notification-key，订阅端可据此去重，
 * 以吸收"投递成功但确认前进程重启"造成的边界重发。
 */
export class HttpNotifier {
  constructor({ timeoutMs = 5000, fetchImpl } = {}) {
    this.timeoutMs = timeoutMs;
    this.fetchImpl = fetchImpl ?? fetch;
  }

  async deliver({ endpoint, payload, headers }) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      const response = await this.fetchImpl(endpoint, {
        method: "POST",
        headers: { "content-type": "application/json", ...headers },
        body: JSON.stringify(payload),
        signal: controller.signal,
      });
      if (!response.ok) {
        throw new Error(`订阅端返回 HTTP ${response.status}`);
      }
    } finally {
      clearTimeout(timer);
    }
  }
}
