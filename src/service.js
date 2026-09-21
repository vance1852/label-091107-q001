import { createHash } from "node:crypto";
import { ServiceError } from "./errors.js";
import { levelFor } from "./levels.js";
import { windowBounds, windowIndexFor, windowKey } from "./windows.js";

const OBSERVED_AT_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})$/;

/** 观测请求内容的稳定指纹，用于识别"同一幂等键、不同内容"的冲突提交。 */
export function observationRequestHash(input) {
  return createHash("sha256")
    .update(
      JSON.stringify({
        stationId: input.stationId,
        region: input.region,
        magnitude: input.magnitude,
        depthKm: input.depthKm,
        observedAt: input.observedAt,
      }),
    )
    .digest("hex");
}

/**
 * 余震告警领域服务。
 * 所有时间都来自注入的时钟，所有状态变化都先写入事件存储，
 * 因此固定时钟重放、进程恢复都不会改变已落盘的历史。
 */
export class AlertService {
  constructor({
    store,
    clock,
    grid,
    windowMs,
    levelThresholds,
    notifier,
    delivery = {},
    maxFutureSkewMs = 300000,
  }) {
    if (!Number.isFinite(windowMs) || windowMs <= 0) {
      throw new Error("windowMs 必须是正数（毫秒）");
    }
    this.store = store;
    this.clock = clock;
    this.grid = grid;
    this.windowMs = windowMs;
    this.levelThresholds = levelThresholds;
    this.notifier = notifier;
    this.maxFutureSkewMs = maxFutureSkewMs;
    this.delivery = {
      maxAttempts: delivery.maxAttempts ?? 5,
      baseBackoffMs: delivery.baseBackoffMs ?? 2000,
      maxBackoffMs: delivery.maxBackoffMs ?? 3600000,
    };
  }

  // ---- 写入路径 ----

  /**
   * 归并一条台站观测。
   * 同一 idempotencyKey 重复提交返回首个处理结果；同键不同内容返回 409。
   */
  ingestObservation(input = {}) {
    const parsed = this.#validateObservation(input);
    const requestHash = observationRequestHash(parsed);
    const existing = this.store.state.idempotency.get(parsed.key);
    if (existing) {
      if (existing.requestHash !== requestHash) {
        throw new ServiceError(409, "idempotency_conflict", "同一幂等键提交了不同的观测内容");
      }
      if (existing.status === "processed") {
        return { ...existing.result, duplicate: true };
      }
      throw new ServiceError(409, "idempotency_in_flight", "相同幂等键的请求尚未完成，请稍后查询该键状态");
    }

    const nowMs = this.clock.now().getTime();
    const observedAtMs = Date.parse(parsed.observedAt);
    if (observedAtMs > nowMs + this.maxFutureSkewMs) {
      throw new ServiceError(422, "observation_in_future", "台站时间超出允许的未来偏移");
    }
    const cellId = this.grid.cellForRegion(parsed.region);
    if (!cellId) {
      throw new ServiceError(422, "unknown_region", `区域 ${parsed.region} 未配置到任何网格单元`);
    }
    const index = windowIndexFor(observedAtMs, this.windowMs);
    const { startMs, endMs } = windowBounds(index, this.windowMs);
    // 接收时间已越过窗口结束时刻的观测属于迟到观测
    const late = nowMs >= endMs;
    const priorWindow = this.store.state.windows.get(windowKey(cellId, index));
    const priorMaxMagnitude = priorWindow ? priorWindow.maxMagnitude : null;

    this.#record("idempotency.started", { key: parsed.key, requestHash });
    const observationId = `obs_${this.store.seq + 1}`;
    this.#record("observation.recorded", {
      observation: {
        observationId,
        key: parsed.key,
        stationId: parsed.stationId,
        region: parsed.region,
        cellId,
        magnitude: parsed.magnitude,
        depthKm: parsed.depthKm,
        observedAt: new Date(observedAtMs).toISOString(),
        receivedAt: new Date(nowMs).toISOString(),
        windowIndex: index,
        windowStartMs: startMs,
        windowEndMs: endMs,
        late,
      },
    });

    const window = this.store.state.windows.get(windowKey(cellId, index));
    const evaluation = this.#evaluateWindow(window, { observationId, late, priorMaxMagnitude });
    const result = {
      observationId,
      cellId,
      windowIndex: index,
      windowStart: new Date(startMs).toISOString(),
      windowEnd: new Date(endMs).toISOString(),
      late,
      level: evaluation.level,
      notifiedLevel: window.notifiedLevel,
      alert: evaluation.alert
        ? { seq: evaluation.alert.seq, type: evaluation.alert.type, level: evaluation.alert.level }
        : null,
      notificationsCreated: evaluation.notificationIds,
      duplicate: false,
    };
    this.#record("idempotency.completed", { key: parsed.key, observationId, result });
    return result;
  }

  /** 维护（新建或更新）辖区对某网格单元的订阅阈值。 */
  upsertSubscription(input = {}) {
    const { jurisdictionId, cellId, minLevel, endpoint } = input;
    if (typeof jurisdictionId !== "string" || jurisdictionId.trim() === "") {
      throw new ServiceError(422, "invalid_jurisdiction", "jurisdictionId 不能为空");
    }
    if (typeof cellId !== "string" || !this.grid.has(cellId)) {
      throw new ServiceError(422, "unknown_cell", `网格单元 ${cellId} 不存在`);
    }
    if (!Number.isInteger(minLevel) || minLevel < 1 || minLevel > 10) {
      throw new ServiceError(422, "invalid_min_level", "minLevel 必须是 1 到 10 的整数");
    }
    if (typeof endpoint !== "string" || !/^https?:\/\//.test(endpoint)) {
      throw new ServiceError(422, "invalid_endpoint", "endpoint 必须是 http(s) 地址");
    }
    const subscription = {
      subscriptionId: `${jurisdictionId}@${cellId}`,
      jurisdictionId,
      cellId,
      minLevel,
      endpoint,
      updatedAt: this.clock.now().toISOString(),
    };
    this.#record("subscription.upserted", { subscription });
    return subscription;
  }

  /** 确认一条通知；重复确认返回已有结果，不产生新事件。 */
  ackNotification(notificationId, by) {
    const notification = this.store.state.notifications.get(notificationId);
    if (!notification) {
      throw new ServiceError(404, "notification_not_found", "通知不存在");
    }
    if (notification.ackedAt) {
      return {
        notificationId,
        ackedAt: notification.ackedAt,
        ackedBy: notification.ackedBy,
        alreadyAcknowledged: true,
      };
    }
    if (notification.status !== "delivered" && notification.status !== "dead") {
      throw new ServiceError(409, "notification_not_delivered", "通知尚未投递完成，不能确认");
    }
    this.#record("notification.acked", { notificationId, by: by ?? "unknown" });
    const updated = this.store.state.notifications.get(notificationId);
    return {
      notificationId,
      ackedAt: updated.ackedAt,
      ackedBy: updated.ackedBy,
      alreadyAcknowledged: false,
    };
  }

  /**
   * 推进时钟：关闭已到期的窗口并执行到期的投递。
   * 生产环境由定时器驱动，测试用固定时钟手动调用。
   */
  async tick() {
    const nowMs = this.clock.now().getTime();
    for (const window of this.store.state.windows.values()) {
      if (!window.closed && nowMs >= window.endMs) {
        this.#record("window.closed", {
          cellId: window.cellId,
          windowIndex: window.index,
          count: window.count,
          maxMagnitude: window.maxMagnitude,
          minDepthKm: window.minDepthKm,
          level: levelFor(window.maxMagnitude, this.levelThresholds),
          notifiedLevel: window.notifiedLevel,
        });
      }
    }
    await this.runDueDeliveries();
  }

  /**
   * 执行到期的投递尝试。重试只更新同一条通知记录，
   * 失败按指数退避重排，达到上限进入 dead，绝不新建通知。
   */
  async runDueDeliveries() {
    const nowMs = this.clock.now().getTime();
    for (const notification of this.store.state.notifications.values()) {
      if (notification.status !== "pending" && notification.status !== "retrying") continue;
      if (Date.parse(notification.nextAttemptAt) > nowMs) continue;
      const attempt = notification.attempts.length + 1;
      const subscription = this.store.state.subscriptions.get(notification.subscriptionId);
      let failure = null;
      if (!subscription) {
        failure = "订阅已不存在";
      } else {
        try {
          await this.notifier.deliver({
            endpoint: subscription.endpoint,
            headers: {
              "x-notification-key": notification.notificationId,
              "x-notification-attempt": String(attempt),
            },
            payload: this.#notificationPayload(notification),
          });
        } catch (error) {
          failure = error instanceof Error ? error.message : String(error);
        }
      }
      if (failure === null) {
        this.#record("delivery.attempted", {
          notificationId: notification.notificationId,
          attempt,
          outcome: "delivered",
        });
      } else if (attempt >= this.delivery.maxAttempts) {
        this.#record("delivery.attempted", {
          notificationId: notification.notificationId,
          attempt,
          outcome: "dead",
          error: failure,
        });
      } else {
        const backoffMs = Math.min(
          this.delivery.baseBackoffMs * 2 ** (attempt - 1),
          this.delivery.maxBackoffMs,
        );
        this.#record("delivery.attempted", {
          notificationId: notification.notificationId,
          attempt,
          outcome: "retry",
          error: failure,
          nextAttemptAt: new Date(nowMs + backoffMs).toISOString(),
        });
      }
    }
  }

  /**
   * 进程恢复：重放日志后调用。
   * 崩溃时处于 processing 的幂等键——观测已落盘的补做评估并补全结果
   * （已发出的告警因 notifiedLevel 已持久化而不会重发），
   * 观测未落盘的释放该键允许客户端重试。
   */
  recover() {
    for (const entry of [...this.store.state.idempotency.values()]) {
      if (entry.status !== "processing") continue;
      const observation = [...this.store.state.observations.values()].find(
        (o) => o.key === entry.key,
      );
      if (!observation) {
        this.#record("idempotency.abandoned", {
          key: entry.key,
          reason: "进程在记录观测前退出，键已释放允许重试",
        });
        continue;
      }
      const window = this.store.state.windows.get(
        windowKey(observation.cellId, observation.windowIndex),
      );
      let alert = null;
      let notificationIds = [];
      const alreadyHandled = observation.late
        ? window.corrections.some((c) => c.observationId === observation.observationId)
        : false;
      if (!alreadyHandled) {
        const evaluation = this.#evaluateWindow(window, {
          observationId: observation.observationId,
          late: observation.late,
          priorMaxMagnitude: window.maxMagnitude,
        });
        alert = evaluation.alert;
        notificationIds = evaluation.notificationIds;
      }
      const result = {
        observationId: observation.observationId,
        cellId: observation.cellId,
        windowIndex: observation.windowIndex,
        windowStart: new Date(window.startMs).toISOString(),
        windowEnd: new Date(window.endMs).toISOString(),
        late: observation.late,
        level: levelFor(window.maxMagnitude, this.levelThresholds),
        notifiedLevel: window.notifiedLevel,
        alert: alert ? { seq: alert.seq, type: alert.type, level: alert.level } : null,
        notificationsCreated: notificationIds,
        duplicate: false,
        recovered: true,
      };
      this.#record("idempotency.completed", {
        key: entry.key,
        observationId: observation.observationId,
        result,
      });
    }
  }

  // ---- 查询路径 ----

  /** 网格单元当前级别与窗口概况。 */
  cellState(cellId) {
    if (!this.grid.has(cellId)) {
      throw new ServiceError(404, "unknown_cell", `网格单元 ${cellId} 不存在`);
    }
    const nowMs = this.clock.now().getTime();
    const windows = [...this.store.state.windows.values()]
      .filter((w) => w.cellId === cellId)
      .sort((a, b) => a.index - b.index);
    const current = windows.find((w) => w.startMs <= nowMs && nowMs < w.endMs) ?? null;
    const lastClosed = [...windows].reverse().find((w) => w.closed) ?? null;
    const latest = windows.length > 0 ? windows[windows.length - 1] : null;
    return {
      cellId,
      now: new Date(nowMs).toISOString(),
      currentLevel: current ? levelFor(current.maxMagnitude, this.levelThresholds) : 0,
      lastNotifiedLevel: latest ? latest.notifiedLevel : 0,
      currentWindow: current ? this.#windowView(current) : null,
      lastClosedWindow: lastClosed ? this.#windowView(lastClosed) : null,
    };
  }

  /** 区域当前级别：经网格解析到归并单元。 */
  regionView(region) {
    const cellId = this.grid.cellForRegion(region);
    if (!cellId) {
      throw new ServiceError(404, "unknown_region", `区域 ${region} 未配置到任何网格单元`);
    }
    return { region, cellId, ...this.cellState(cellId) };
  }

  /** 演变过程：该单元的告警、修正与关窗事件时间线。 */
  cellHistory(cellId) {
    if (!this.grid.has(cellId)) {
      throw new ServiceError(404, "unknown_cell", `网格单元 ${cellId} 不存在`);
    }
    return this.store.state.timeline.filter((e) => e.cellId === cellId).map((e) => ({ ...e }));
  }

  /** 单个窗口的聚合结果、修正记录与相关事件。 */
  windowView(cellId, index) {
    if (!this.grid.has(cellId)) {
      throw new ServiceError(404, "unknown_cell", `网格单元 ${cellId} 不存在`);
    }
    const window = this.store.state.windows.get(windowKey(cellId, index));
    if (!window) {
      throw new ServiceError(404, "window_not_found", "该窗口没有观测记录");
    }
    return {
      ...this.#windowView(window),
      events: this.store.state.timeline
        .filter((e) => e.cellId === cellId && e.windowIndex === index)
        .map((e) => ({ ...e })),
    };
  }

  /** 辖区未确认的通知（未 ack 的全部状态，含投递中与 dead）。 */
  unacknowledged(jurisdictionId) {
    return [...this.store.state.notifications.values()]
      .filter((n) => n.jurisdictionId === jurisdictionId && n.ackedAt === null)
      .sort((a, b) => (a.createdAt < b.createdAt ? -1 : 1))
      .map((n) => ({
        notificationId: n.notificationId,
        subscriptionId: n.subscriptionId,
        cellId: n.cellId,
        windowIndex: n.windowIndex,
        level: n.level,
        sourceType: n.sourceType,
        status: n.status,
        createdAt: n.createdAt,
        deliveredAt: n.deliveredAt,
        attemptCount: n.attempts.length,
      }));
  }

  /** 通知详情：状态与每一次重试的落点。 */
  notificationView(notificationId) {
    const n = this.store.state.notifications.get(notificationId);
    if (!n) {
      throw new ServiceError(404, "notification_not_found", "通知不存在");
    }
    return {
      notificationId: n.notificationId,
      subscriptionId: n.subscriptionId,
      jurisdictionId: n.jurisdictionId,
      cellId: n.cellId,
      windowIndex: n.windowIndex,
      level: n.level,
      sourceType: n.sourceType,
      sourceSeq: n.sourceSeq,
      status: n.status,
      createdAt: n.createdAt,
      nextAttemptAt: n.nextAttemptAt,
      deliveredAt: n.deliveredAt,
      ackedAt: n.ackedAt,
      ackedBy: n.ackedBy,
      attempts: n.attempts.map((a) => ({ ...a })),
    };
  }

  /** 幂等键详情：处理状态与首个处理结果。 */
  idempotencyView(key) {
    const entry = this.store.state.idempotency.get(key);
    if (!entry) {
      throw new ServiceError(404, "key_not_found", "幂等键不存在");
    }
    return {
      key: entry.key,
      status: entry.status,
      observationId: entry.observationId,
      requestHash: entry.requestHash,
      createdAt: entry.createdAt,
      updatedAt: entry.updatedAt,
      result: entry.result,
    };
  }

  listSubscriptions(jurisdictionId = null) {
    return [...this.store.state.subscriptions.values()]
      .filter((s) => !jurisdictionId || s.jurisdictionId === jurisdictionId)
      .map((s) => ({ ...s }));
  }

  // ---- 内部 ----

  #evaluateWindow(window, { observationId, late, priorMaxMagnitude }) {
    const level = levelFor(window.maxMagnitude, this.levelThresholds);
    if (!late) {
      // 开放窗口：等级上升即发出/升级告警，下降不惊动辖区
      if (level <= window.notifiedLevel) {
        return { level, alert: null, notificationIds: [] };
      }
      const type = window.notifiedLevel === 0 ? "alert.raised" : "alert.escalated";
      const alert = this.#record(type, {
        cellId: window.cellId,
        windowIndex: window.index,
        level,
        previousLevel: window.notifiedLevel,
        maxMagnitude: window.maxMagnitude,
        observationId,
      });
      return { level, alert, notificationIds: this.#notify(alert) };
    }
    // 迟到观测：已发出的告警历史不可改写，只追加修正记录
    const notifiedBefore = window.notifiedLevel;
    const previousLevel =
      priorMaxMagnitude === null ? 0 : levelFor(priorMaxMagnitude, this.levelThresholds);
    const alert = this.#record("alert.corrected", {
      cellId: window.cellId,
      windowIndex: window.index,
      observationId,
      previousLevel,
      level,
      notifiedLevelAfter: Math.max(notifiedBefore, level),
      maxMagnitude: window.maxMagnitude,
    });
    // 修正揭示出更高等级时仍触发新通知（新事件，而非改写旧告警）
    const notificationIds = level > notifiedBefore ? this.#notify(alert) : [];
    return { level, alert, notificationIds };
  }

  #notify(alert) {
    const ids = [];
    for (const sub of this.store.state.subscriptions.values()) {
      if (sub.cellId !== alert.cellId || alert.level < sub.minLevel) continue;
      const notificationId = `ntf_${this.store.seq + 1}`;
      this.#record("notification.created", {
        notificationId,
        subscriptionId: sub.subscriptionId,
        jurisdictionId: sub.jurisdictionId,
        cellId: sub.cellId,
        windowIndex: alert.windowIndex,
        level: alert.level,
        sourceSeq: alert.seq,
        sourceType: alert.type,
      });
      ids.push(notificationId);
    }
    return ids;
  }

  #notificationPayload(notification) {
    const { startMs, endMs } = windowBounds(notification.windowIndex, this.windowMs);
    return {
      notificationId: notification.notificationId,
      jurisdictionId: notification.jurisdictionId,
      cellId: notification.cellId,
      windowIndex: notification.windowIndex,
      windowStart: new Date(startMs).toISOString(),
      windowEnd: new Date(endMs).toISOString(),
      level: notification.level,
      sourceType: notification.sourceType,
      createdAt: notification.createdAt,
    };
  }

  #windowView(window) {
    return {
      cellId: window.cellId,
      index: window.index,
      start: new Date(window.startMs).toISOString(),
      end: new Date(window.endMs).toISOString(),
      count: window.count,
      maxMagnitude: window.maxMagnitude,
      minDepthKm: window.minDepthKm,
      averageMagnitude:
        window.count > 0 ? Number((window.sumMagnitude / window.count).toFixed(2)) : null,
      level: levelFor(window.maxMagnitude, this.levelThresholds),
      notifiedLevel: window.notifiedLevel,
      closed: window.closed,
      closedAt: window.closedAt,
      corrections: window.corrections.map((c) => ({ ...c })),
      observationIds: [...window.observationIds],
    };
  }

  #validateObservation(input) {
    if (typeof input !== "object" || input === null) {
      throw new ServiceError(422, "invalid_body", "请求体必须是 JSON 对象");
    }
    const { idempotencyKey, stationId, region, magnitude, depthKm, observedAt } = input;
    if (
      typeof idempotencyKey !== "string" ||
      idempotencyKey.length === 0 ||
      idempotencyKey.length > 200
    ) {
      throw new ServiceError(422, "invalid_idempotency_key", "idempotencyKey 必须是 1 到 200 字符的字符串");
    }
    if (typeof stationId !== "string" || stationId.trim() === "") {
      throw new ServiceError(422, "invalid_station", "stationId 不能为空");
    }
    if (typeof region !== "string" || region.trim() === "") {
      throw new ServiceError(422, "invalid_region", "region 不能为空");
    }
    if (typeof magnitude !== "number" || !Number.isFinite(magnitude) || magnitude < -2 || magnitude > 10) {
      throw new ServiceError(422, "invalid_magnitude", "magnitude 必须是 -2 到 10 的数字");
    }
    if (typeof depthKm !== "number" || !Number.isFinite(depthKm) || depthKm < 0 || depthKm > 800) {
      throw new ServiceError(422, "invalid_depth", "depthKm 必须是 0 到 800 的数字");
    }
    if (
      typeof observedAt !== "string" ||
      !OBSERVED_AT_PATTERN.test(observedAt) ||
      Number.isNaN(Date.parse(observedAt))
    ) {
      throw new ServiceError(422, "invalid_observed_at", "observedAt 必须是带时区的 ISO 8601 时间");
    }
    return { key: idempotencyKey, stationId, region, magnitude, depthKm, observedAt };
  }

  #record(type, payload) {
    return this.store.record(type, { at: this.clock.now().toISOString(), ...payload });
  }
}
