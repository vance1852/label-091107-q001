// 告警引擎：观测归并、阈值决策、不可变告警与幂等通知状态机。
//
// 设计要点：
// 1. 所有状态变化都是事件日志里的不可变事件；内存状态由 reducer 重放得到。
// 2. 同一决策（网格单元 × 时间窗 × 订阅）首次定级产生 OPEN，之后只有
//    ESCALATION（升级，触发新通知）/ CORRECTION / DOWNGRADE / LATE_CORRECTION
//    （仅追加修正，绝不重发、绝不改写已发历史）。
// 3. 通知幂等键 = 决策ID + 修订号；状态机为
//    QUEUED → IN_FLIGHT → SENT
//                          ↘ WAITING →（重试）… → SENT / DEAD
//    IN_FLIGHT 只是内存态：崩溃恢复后回到 QUEUED/WAITING，凭幂等键由接收方去重。
import { randomUUID, createHash } from "node:crypto";
import { cellIdOf, windowStartOf } from "./grid.js";
import { RANK_LABELS, aggregateObservations, aggregateSignature, normalizeLevels, rankForObservations } from "./levels.js";
import { toMillis } from "./clock.js";
import { EventStore } from "./store.js";

export const DEFAULT_CONFIG = {
  cellSizeDeg: 0.5,
  windowMs: 10 * 60 * 1000,
  windowEpochMs: 0,
  latenessWaitMs: 2 * 60 * 1000,
  retry: { maxAttempts: 4, baseDelayMs: 1000, maxDelayMs: 30_000 },
};

const ACTIVE = "ACTIVE";
const CLOSED = "CLOSED";
const TERMINAL = new Set(["SENT", "DEAD"]);

function shortHash(value) {
  return createHash("sha256").update(value).digest("hex").slice(0, 16);
}

export class AlertEngine {
  #store;
  #clock;
  #sender;
  #config;
  #timers = new Map();
  #inflight = new Map(); // key → 在途投递 Promise
  #chain = Promise.resolve();

  state = {
    configured: false,
    config: null,
    subscriptions: new Map(), // id → subscription
    observations: new Map(), // id → observation（内容指纹去重）
    windows: new Map(), // cellWindowKey → {cellId, region, windowStart, windowEnd, observationIds}
    decisions: new Map(), // decisionId → decision
    decisionByKey: new Map(), // cellWindowKey#subscriptionId → decisionId
    notifications: new Map(), // idempotencyKey → notification
  };

  /**
   * @param {object} args
   * @param {string|null} args.path 事件日志路径；null 为纯内存（测试用）
   * @param {{now():number}} args.clock
   * @param {(notification:object)=>Promise<object>} args.sender 投递器
   * @param {object} [args.config] 仅当日志中没有配置事件时生效（配置以日志为准）
   */
  constructor({ path = null, clock, sender, config = {} }) {
    this.#clock = clock;
    this.#sender = sender;
    this.#config = {
      ...DEFAULT_CONFIG,
      ...config,
      retry: { ...DEFAULT_CONFIG.retry, ...(config.retry ?? {}) },
    };
    // 重放与写入共用 reducer：apply 在构造期间即可安全调用。
    this.#store = EventStore.open(path, (event) => this.apply(event));
    if (!this.state.configured) {
      this.#store.append("ConfigSet", { config: this.#config });
    }
  }

  get config() {
    return this.#config;
  }

  /** 更换投递器（主要用于测试与运维侧热修复）。 */
  setSender(sender) {
    this.#sender = sender;
  }

  // ---------- 进程恢复：重新装载未完成的投递，终态绝不重投 ----------
  rearm() {
    const now = this.#clock.now();
    const promises = [];
    for (const notification of this.state.notifications.values()) {
      if (notification.status === "QUEUED") {
        promises.push(this.#dispatch(notification.key));
      } else if (notification.status === "WAITING") {
        if (notification.nextAttemptAt !== null && notification.nextAttemptAt <= now) {
          promises.push(this.#dispatch(notification.key));
        } else {
          this.#schedule(notification);
        }
      }
    }
    return Promise.all(promises);
  }

  // ---------- 互斥：所有事件提交按序进行，避免并发 ingest 交错 ----------
  #exclusive(fn) {
    const run = this.#chain.then(() => fn());
    this.#chain = run.catch(() => {});
    return run;
  }

  // =========================================================
  // 订阅管理（各辖区维护自己的阈值）
  // =========================================================

  upsertSubscription(input) {
    return this.#exclusive(() => {
      const existing = input.id ? this.state.subscriptions.get(input.id) : undefined;
      if (input.id && !existing) throw new Error(`订阅不存在: ${input.id}`);
      const merged = { ...(existing ?? {}), ...input };
      const subscription = {
        id: existing?.id ?? `sub_${randomUUID()}`,
        jurisdictionId: requireText(merged.jurisdictionId, "jurisdictionId"),
        name: requireText(merged.name ?? "未命名订阅", "name"),
        region: normalizeRegion(merged.region),
        webhookUrl: requireText(merged.webhookUrl, "webhookUrl"),
        levels: normalizeLevels(merged.levels),
        active: merged.active ?? true,
        createdAt: existing?.createdAt ?? this.#clock.now(),
        updatedAt: this.#clock.now(),
      };
      if (!/^https?:\/\//.test(subscription.webhookUrl)) {
        throw new Error("webhookUrl 必须是 http(s) 地址");
      }
      this.#store.append("SubscriptionUpserted", { subscription });
      return this.serializeSubscription(subscription);
    });
  }

  serializeSubscription(s) {
    return { ...s };
  }

  listSubscriptions({ jurisdictionId, active } = {}) {
    return [...this.state.subscriptions.values()]
      .filter((s) => (jurisdictionId ? s.jurisdictionId === jurisdictionId : true))
      .filter((s) => (active === undefined ? true : s.active === active))
      .map((s) => this.serializeSubscription(s));
  }

  // =========================================================
  // 观测接入（去重 + 归并 + 定级）
  // =========================================================

  /**
   * 接入一条台站观测。重复观测（同一 id 或同一内容指纹）安全返回，不产生新告警。
   */
  ingestObservation(input) {
    return (async () => {
      let outcome;
      await this.#exclusive(async () => {
        const now = this.#clock.now();
        this.#closeExpiredWindows(now);

        const observation = normalizeObservation(input, now);
        const existed = this.state.observations.get(observation.id);
        if (existed) {
          outcome = { duplicated: true, observation: existed, effects: [] };
          return;
        }

        const { cellSizeDeg, windowMs, windowEpochMs, latenessWaitMs } = this.#config;
        const cellId = cellIdOf(
          observation.region,
          observation.latitude,
          observation.longitude,
          cellSizeDeg,
        );
        const windowStart = windowStartOf(observation.eventTime, windowMs, windowEpochMs);
        const windowEnd = windowStart + windowMs;
        const cwKey = `${cellId}|${windowStart}`;

        this.#store.append("ObservationAccepted", { observation, cellId, windowStart, windowEnd });

        const effects = [];
        const keys = [];
        const windowClosed = windowEnd + latenessWaitMs <= now;
        for (const subscription of this.state.subscriptions.values()) {
          if (!subscription.active || subscription.region !== observation.region) continue;
          const effect = this.#evaluate(subscription, cwKey, now, windowClosed);
          if (effect) {
            effects.push(effect);
            if (effect.notificationKey) keys.push(effect.notificationKey);
          }
        }
        outcome = { duplicated: false, observation, cellId, windowStart, windowEnd, effects, keys };
      });
      // 通知异步投递（202 语义：已受理，投递结果经事件状态机跟踪，崩溃可恢复）。
      for (const key of outcome.keys ?? []) {
        this.#dispatch(key).catch(() => {});
      }
      delete outcome.keys;
      return outcome;
    })();
  }

  /**
   * 依据某订阅阈值评估一个 单元×窗口，追加 OPEN / ESCALATION / 修正记录。
   */
  #evaluate(subscription, cwKey, now, windowClosed) {
    const window = this.state.windows.get(cwKey);
    const observations = window.observationIds
      .map((id) => this.state.observations.get(id))
      .sort((a, b) => a.eventTime - b.eventTime || a.id.localeCompare(b.id));
    const rank = rankForObservations(subscription.levels, observations);
    const aggregate = aggregateObservations(observations);
    const signature = aggregateSignature(aggregate);

    const decisionKey = `${cwKey}#${subscription.id}`;
    const decisionId = this.state.decisionByKey.get(decisionKey) ?? `dec_${shortHash(decisionKey)}`;
    const decision = this.state.decisions.get(decisionId);

    if (!decision) {
      // 窗口（含迟到宽限）已关闭才到达的观测，不得再开新告警。
      if (rank === 0 || windowClosed) return null;
      const opened = {
        id: decisionId,
        key: decisionKey,
        jurisdictionId: subscription.jurisdictionId,
        subscriptionId: subscription.id,
        region: subscription.region,
        cellId: window.cellId,
        windowStart: window.windowStart,
        windowEnd: window.windowEnd,
        status: ACTIVE,
        currentRank: rank,
        revision: 1,
        openedAt: now,
        closedAt: null,
        acknowledgedRevision: 0,
        acknowledgedAt: null,
        acknowledgedBy: null,
      };
      const event = {
        kind: "OPEN",
        revision: 1,
        at: now,
        rank,
        fromRank: 0,
        aggregate,
        signature,
        late: false,
      };
      this.#store.append("AlertOpened", { decision: opened, event });
      const notificationKey = this.#enqueue(opened, event, now);
      return { decisionId, kind: "OPEN", rank, notificationKey };
    }

    const lastEvent = decision.events[decision.events.length - 1];
    if (lastEvent && lastEvent.signature === signature && lastEvent.rank === rank) {
      return { decisionId, kind: "UNCHANGED", rank: decision.currentRank };
    }

    if (decision.status === CLOSED) {
      // 窗口关闭后的迟到观测：只追加修正记录，不触发任何通知，也不改变当前级别。
      const correction = {
        kind: "LATE_CORRECTION",
        revision: decision.revision + 1,
        at: now,
        rank,
        fromRank: decision.currentRank,
        aggregate,
        signature,
        late: true,
      };
      this.#store.append("AlertAppended", { decisionId, event: correction });
      return { decisionId, kind: "LATE_CORRECTION", rank };
    }

    if (rank > decision.currentRank) {
      const escalation = {
        kind: "ESCALATION",
        revision: decision.revision + 1,
        at: now,
        rank,
        fromRank: decision.currentRank,
        aggregate,
        signature,
        late: false,
      };
      this.#store.append("AlertAppended", { decisionId, event: escalation });
      const notificationKey = this.#enqueue(decision, escalation, now);
      return { decisionId, kind: "ESCALATION", rank, notificationKey };
    }

    // 同级（聚合变了）或降级：追加修正记录，不重发、不改写已发历史。
    const kind = rank < decision.currentRank ? "DOWNGRADE" : "CORRECTION";
    const correction = {
      kind,
      revision: decision.revision + 1,
      at: now,
      rank,
      fromRank: decision.currentRank,
      aggregate,
      signature,
      late: false,
    };
    this.#store.append("AlertAppended", { decisionId, event: correction });
    return { decisionId, kind, rank };
  }

  // =========================================================
  // 通知状态机
  // =========================================================

  #enqueue(decision, decisionEvent, now) {
    const key = `${decision.id}:R${decisionEvent.revision}`;
    if (this.state.notifications.has(key)) return key; // 双重保护
    const subscription = this.state.subscriptions.get(decision.subscriptionId);
    const notification = {
      key,
      decisionId: decision.id,
      revision: decisionEvent.revision,
      subscriptionId: decision.subscriptionId,
      jurisdictionId: decision.jurisdictionId,
      region: decision.region,
      cellId: decision.cellId,
      windowStart: decision.windowStart,
      windowEnd: decision.windowEnd,
      rank: decisionEvent.rank,
      level: RANK_LABELS[decisionEvent.rank],
      escalatedFrom: decisionEvent.fromRank,
      aggregate: decisionEvent.aggregate,
      trigger: {
        kind: decisionEvent.kind,
        maxMagnitudeObservationId: decisionEvent.aggregate.maxMagnitudeObservationId,
      },
      webhookUrl: subscription.webhookUrl,
      status: "QUEUED",
      queuedAt: now,
      sentAt: null,
      attempts: [],
      nextAttemptAt: null,
      lastError: null,
    };
    this.#store.append("NotificationQueued", { notification });
    return key;
  }

  /**
   * 发起一次投递尝试。前置状态检查与结果回写各自进入互斥区，
   * 网络往返发生在锁外；终态（SENT/DEAD）永不重新投递。
   */
  #dispatch(key) {
    const existing = this.#inflight.get(key);
    if (existing) return existing;

    const promise = (async () => {
      let notification;
      try {
        await this.#exclusive(() => {
          notification = this.state.notifications.get(key);
          if (!notification) throw new Error(`通知不存在: ${key}`);
          if (TERMINAL.has(notification.status)) return;
          if (this.#timers.has(key)) {
            clearTimeout(this.#timers.get(key));
            this.#timers.delete(key);
          }
          notification.status = "IN_FLIGHT"; // 仅内存态，不落事件
        });
      } catch (error) {
        return;
      }
      if (!notification || TERMINAL.has(notification.status)) return;

      let result;
      try {
        result = await this.#sender(notification);
      } catch (error) {
        result = { ok: false, retryable: true, error: `投递器异常: ${error?.message ?? error}` };
      }

      await this.#exclusive(() => this.#recordResult(key, result));
    })().finally(() => this.#inflight.delete(key));

    this.#inflight.set(key, promise);
    return promise;
  }

  #recordResult(key, result) {
    const notification = this.state.notifications.get(key);
    if (!notification) return;
    if (TERMINAL.has(notification.status)) return; // 终态不可翻转

    const now = this.#clock.now();
    const attempt = {
      at: now,
      ok: Boolean(result.ok),
      status: result.status ?? null,
      error: result.error ?? null,
      retryable: result.ok ? false : Boolean(result.retryable),
    };

    if (result.ok) {
      this.#store.append("DeliverySucceeded", { key, attempt, sentAt: now });
      return;
    }

    const { maxAttempts, baseDelayMs, maxDelayMs } = this.#config.retry;
    const failedAttempts = notification.attempts.length + 1;
    const giveUp = !result.retryable || failedAttempts >= maxAttempts;
    const nextAttemptAt = giveUp
      ? null
      : now + Math.min(baseDelayMs * 2 ** (failedAttempts - 1), maxDelayMs);
    this.#store.append("DeliveryFailed", {
      key,
      attempt,
      failedAttempts,
      nextAttemptAt,
      dead: giveUp,
    });
    if (!giveUp) this.#schedule(notification);
  }

  #schedule(notification) {
    if (this.#timers.has(notification.key)) return;
    if (notification.nextAttemptAt === null) return;
    const delay = Math.max(0, notification.nextAttemptAt - this.#clock.now());
    const timer = setTimeout(() => {
      this.#timers.delete(notification.key);
      this.#dispatch(notification.key);
    }, delay);
    if (typeof timer.unref === "function") timer.unref();
    this.#timers.set(notification.key, timer);
  }

  /** 人工重试：只有 QUEUED/IN_FLIGHT/WAITING 可触发，SENT 永不重投，DEAD 需先复活。 */
  retryNotification(key) {
    return this.#exclusive(() => {
      const notification = this.state.notifications.get(key);
      if (!notification) throw new Error(`通知不存在: ${key}`);
      if (notification.status === "SENT") {
        return { status: "SENT", accepted: false, reason: "已成功投递，拒绝重复通知" };
      }
      if (notification.status === "DEAD") {
        return { status: "DEAD", accepted: false, reason: "已终止，需先调用复活接口" };
      }
      this.#dispatch(key);
      return { status: notification.status, accepted: true };
    });
  }

  /** 复活 DEAD 通知并重新排队（留下 NotificationResurrected 审计事件）。 */
  resurrect(key) {
    return this.#exclusive(() => {
      const notification = this.state.notifications.get(key);
      if (!notification) throw new Error(`通知不存在: ${key}`);
      if (notification.status !== "DEAD") {
        return { status: notification.status, accepted: false };
      }
      this.#store.append("NotificationResurrected", { key, at: this.#clock.now() });
      this.#dispatch(key);
      return { status: "QUEUED", accepted: true };
    });
  }

  /**
   * 固定时钟推进：关闭到期窗口，投递到点的重试。手动时钟的测试/重放循环调用并等待。
   */
  async pump() {
    let due = [];
    await this.#exclusive(() => {
      const now = this.#clock.now();
      this.#closeExpiredWindows(now);
      for (const notification of this.state.notifications.values()) {
        if (notification.status === "QUEUED") {
          due.push(notification.key);
        } else if (
          notification.status === "WAITING" &&
          notification.nextAttemptAt !== null &&
          notification.nextAttemptAt <= now
        ) {
          due.push(notification.key);
        }
      }
    });
    due = [...new Set(due)].filter((key) => !this.#inflight.has(key));
    await Promise.all(due.map((key) => this.#dispatch(key)));
    return { now: this.#clock.now(), dispatched: due.length };
  }

  /** 周期清扫：只关闭到期窗口（真实时钟下由定时器驱动；重试另有定时器）。 */
  sweep() {
    return this.#exclusive(() => this.#closeExpiredWindows(this.#clock.now()));
  }

  /** 等待所有已排队的状态变更与在途投递落定（测试/优雅停机用）。 */
  async idle() {
    await this.#chain;
    while (this.#inflight.size > 0) {
      await Promise.all([...this.#inflight.values()]);
      await this.#chain;
    }
  }

  #closeExpiredWindows(now) {
    for (const decision of this.state.decisions.values()) {
      if (decision.status === ACTIVE && decision.windowEnd + this.#config.latenessWaitMs <= now) {
        this.#store.append("AlertClosed", { decisionId: decision.id, at: now });
      }
    }
  }

  // =========================================================
  // 值班查询接口
  // =========================================================

  /** 审计视图：不可变事件日志（重放窗口边界时核对每个幂等键的最终状态）。 */
  listEvents({ since = 0, types } = {}) {
    const allow = types ? new Set(types) : null;
    return this.#store.events
      .filter((e) => e.seq >= since)
      .filter((e) => (allow ? allow.has(e.type) : true));
  }

  acknowledge(decisionId, by = "值班员") {
    return this.#exclusive(() => {
      const decision = this.state.decisions.get(decisionId);
      if (!decision) throw new Error(`决策不存在: ${decisionId}`);
      this.#store.append("DecisionAcknowledged", {
        decisionId,
        revision: decision.revision,
        at: this.#clock.now(),
        by,
      });
      return this.serializeDecision(decision);
    });
  }

  /** 区域当前级别：取该区域所有未关闭决策的最高级别（可按辖区过滤）。 */
  getRegionLevel(region, { jurisdictionId } = {}) {
    const decisions = [...this.state.decisions.values()].filter(
      (d) => d.region === region && (jurisdictionId ? d.jurisdictionId === jurisdictionId : true),
    );
    const active = decisions.filter((d) => d.status === ACTIVE);
    const currentRank = active.reduce((max, d) => Math.max(max, d.currentRank), 0);
    return {
      region,
      currentRank,
      level: RANK_LABELS[currentRank],
      activeCells: active
        .map((d) => ({
          cellId: d.cellId,
          rank: d.currentRank,
          windowStart: d.windowStart,
          windowEnd: d.windowEnd,
          decisionId: d.id,
          jurisdictionId: d.jurisdictionId,
        }))
        .sort((a, b) => b.rank - a.rank || a.cellId.localeCompare(b.cellId)),
      evaluatedAt: this.#clock.now(),
    };
  }

  listDecisions({ region, cellId, jurisdictionId, status } = {}) {
    return [...this.state.decisions.values()]
      .filter((d) => (region ? d.region === region : true))
      .filter((d) => (cellId ? d.cellId === cellId : true))
      .filter((d) => (jurisdictionId ? d.jurisdictionId === jurisdictionId : true))
      .filter((d) => (status ? d.status === status : true))
      .sort((a, b) => b.openedAt - a.openedAt)
      .map((d) => this.serializeDecision(d));
  }

  getDecision(decisionId) {
    const decision = this.state.decisions.get(decisionId);
    return decision ? this.serializeDecision(decision) : null;
  }

  serializeDecision(decision) {
    return {
      id: decision.id,
      jurisdictionId: decision.jurisdictionId,
      subscriptionId: decision.subscriptionId,
      region: decision.region,
      cellId: decision.cellId,
      windowStart: decision.windowStart,
      windowEnd: decision.windowEnd,
      status: decision.status,
      currentRank: decision.currentRank,
      currentLevel: RANK_LABELS[decision.currentRank],
      revision: decision.revision,
      openedAt: decision.openedAt,
      closedAt: decision.closedAt,
      acknowledgedRevision: decision.acknowledgedRevision,
      acknowledgedAt: decision.acknowledgedAt,
      acknowledgedBy: decision.acknowledgedBy,
      history: decision.events.map((e) => ({
        kind: e.kind,
        revision: e.revision,
        at: e.at,
        rank: e.rank,
        level: RANK_LABELS[e.rank],
        fromRank: e.fromRank,
        late: e.late,
        aggregate: e.aggregate,
      })),
      notifications: decision.events
        .filter((e) => e.kind === "OPEN" || e.kind === "ESCALATION")
        .map((e) => {
          const key = `${decision.id}:R${e.revision}`;
          const n = this.state.notifications.get(key);
          return n ? this.serializeNotification(n) : { key, status: "MISSING" };
        }),
    };
  }

  #maxSentRevision(decision) {
    let revision = 0;
    for (const e of decision.events) {
      if (e.kind !== "OPEN" && e.kind !== "ESCALATION") continue;
      const n = this.state.notifications.get(`${decision.id}:R${e.revision}`);
      if (n && n.status === "SENT") revision = e.revision;
    }
    return revision;
  }

  /** 未确认订阅：存在已成功投递但尚未确认的升级（OPEN/ESCALATION）的订阅。 */
  listUnacknowledged({ jurisdictionId } = {}) {
    const result = [];
    for (const decision of this.state.decisions.values()) {
      if (jurisdictionId && decision.jurisdictionId !== jurisdictionId) continue;
      const maxSent = this.#maxSentRevision(decision);
      if (maxSent > decision.acknowledgedRevision) {
        const subscription = this.state.subscriptions.get(decision.subscriptionId);
        const pendingNotification = this.state.notifications.get(`${decision.id}:R${maxSent}`);
        result.push({
          subscriptionId: decision.subscriptionId,
          subscriptionName: subscription?.name ?? null,
          jurisdictionId: decision.jurisdictionId,
          region: decision.region,
          cellId: decision.cellId,
          decisionId: decision.id,
          sentRank: pendingNotification?.rank ?? decision.currentRank,
          sentAt: pendingNotification?.sentAt ?? null,
          windowEnd: decision.windowEnd,
          status: decision.status,
        });
      }
    }
    return result.sort((a, b) => (b.sentAt ?? 0) - (a.sentAt ?? 0));
  }

  getNotification(key) {
    const notification = this.state.notifications.get(key);
    return notification ? this.serializeNotification(notification) : null;
  }

  listNotifications({ decisionId, jurisdictionId, status } = {}) {
    return [...this.state.notifications.values()]
      .filter((n) => (decisionId ? n.decisionId === decisionId : true))
      .filter((n) => (jurisdictionId ? n.jurisdictionId === jurisdictionId : true))
      .filter((n) => (status ? n.status === status : true))
      .sort((a, b) => a.queuedAt - b.queuedAt || a.key.localeCompare(b.key))
      .map((n) => this.serializeNotification(n));
  }

  serializeNotification(n) {
    return {
      key: n.key,
      decisionId: n.decisionId,
      revision: n.revision,
      subscriptionId: n.subscriptionId,
      jurisdictionId: n.jurisdictionId,
      region: n.region,
      cellId: n.cellId,
      windowStart: n.windowStart,
      windowEnd: n.windowEnd,
      rank: n.rank,
      level: n.level,
      escalatedFrom: n.escalatedFrom,
      aggregate: n.aggregate,
      trigger: n.trigger,
      status: n.status,
      queuedAt: n.queuedAt,
      sentAt: n.sentAt,
      nextAttemptAt: n.nextAttemptAt,
      lastError: n.lastError,
      attempts: n.attempts,
    };
  }

  // =========================================================
  // Reducer：事件 → 内存状态（启动重放与正常写入共用同一份逻辑）
  // =========================================================

  apply = (event) => {
    const s = this.state;
    switch (event.type) {
      case "ConfigSet":
        s.configured = true;
        s.config = event.config;
        this.#config = event.config;
        break;

      case "SubscriptionUpserted":
        s.subscriptions.set(event.subscription.id, event.subscription);
        break;

      case "ObservationAccepted": {
        s.observations.set(event.observation.id, event.observation);
        const key = `${event.cellId}|${event.windowStart}`;
        let window = s.windows.get(key);
        if (!window) {
          window = {
            cellId: event.cellId,
            region: event.observation.region,
            windowStart: event.windowStart,
            windowEnd: event.windowEnd,
            observationIds: [],
          };
          s.windows.set(key, window);
        }
        window.observationIds.push(event.observation.id);
        break;
      }

      case "AlertOpened": {
        const d = event.decision;
        s.decisions.set(d.id, { ...d, events: [event.event] });
        s.decisionByKey.set(d.key, d.id);
        break;
      }

      case "AlertAppended": {
        const d = s.decisions.get(event.decisionId);
        d.events.push(event.event);
        d.revision = event.event.revision;
        if (event.event.kind !== "LATE_CORRECTION") {
          d.currentRank = event.event.rank;
        }
        break;
      }

      case "AlertClosed": {
        const d = s.decisions.get(event.decisionId);
        d.status = CLOSED;
        d.closedAt = event.at;
        break;
      }

      case "DecisionAcknowledged": {
        const d = s.decisions.get(event.decisionId);
        d.acknowledgedRevision = event.revision;
        d.acknowledgedAt = event.at;
        d.acknowledgedBy = event.by;
        break;
      }

      case "NotificationQueued":
        s.notifications.set(event.notification.key, { ...event.notification });
        break;

      case "DeliverySucceeded": {
        const n = s.notifications.get(event.key);
        n.attempts.push(event.attempt);
        n.status = "SENT";
        n.sentAt = event.sentAt;
        n.nextAttemptAt = null;
        n.lastError = null;
        break;
      }

      case "DeliveryFailed": {
        const n = s.notifications.get(event.key);
        n.attempts.push(event.attempt);
        n.lastError = event.attempt.error;
        n.nextAttemptAt = event.nextAttemptAt;
        n.status = event.dead ? "DEAD" : "WAITING";
        break;
      }

      case "NotificationResurrected": {
        const n = s.notifications.get(event.key);
        n.status = "QUEUED";
        n.nextAttemptAt = null;
        break;
      }

      default:
        // 未知事件保留在日志中但不影响状态，前向兼容。
        break;
    }
  };
}

// =========================================================
// 输入校验
// =========================================================

function requireText(value, field) {
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`${field} 不能为空`);
  }
  return value.trim();
}

function normalizeRegion(value) {
  const region = requireText(value, "region");
  if (!/^[A-Za-z0-9_-]{1,32}$/.test(region)) {
    throw new Error("区域编码只能包含字母、数字、下划线与连字符（1-32 位）");
  }
  return region;
}

function normalizeObservation(input, now) {
  const stationId = requireText(input.stationId, "stationId");
  const region = normalizeRegion(input.region);
  const eventTime = toMillis(input.eventTime);
  const magnitude = Number(input.magnitude);
  const depthKm = Number(input.depthKm);
  const latitude = Number(input.latitude);
  const longitude = Number(input.longitude);
  if (!Number.isFinite(magnitude) || magnitude < -2 || magnitude > 10) {
    throw new Error("震级必须为 [-2, 10] 内的数值");
  }
  if (!Number.isFinite(depthKm) || depthKm < 0 || depthKm > 700) {
    throw new Error("深度必须为 [0, 700] 公里内的数值");
  }
  if (!Number.isFinite(latitude) || latitude < -90 || latitude > 90) {
    throw new Error("纬度必须为 [-90, 90] 内的数值");
  }
  if (!Number.isFinite(longitude) || longitude < -180 || longitude > 180) {
    throw new Error("经度必须为 [-180, 180] 内的数值");
  }
  if (eventTime > now + 60_000) {
    throw new Error("观测时间不能晚于当前时间 1 分钟以上");
  }
  const id =
    input.id ??
    `obs_${shortHash(`${stationId}|${eventTime}|${magnitude}|${depthKm}|${latitude}|${longitude}`)}`;
  return {
    id: String(id),
    stationId,
    region,
    eventTime,
    receivedAt: now,
    magnitude,
    depthKm,
    latitude,
    longitude,
  };
}

export { RANK_LABELS };
