import { appendFileSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import { dirname } from "node:path";
import { windowKey } from "./windows.js";

/**
 * 追加式事件存储。
 * 每次状态变化先以一条 JSON 事件落盘，再应用到内存状态；
 * 进程重启后按 seq 顺序重放日志即可完整恢复，
 * 已投递、已确认的告警因此不会在恢复后重演。
 */
export class Store {
  constructor(filePath) {
    this.filePath = filePath;
    this.seq = 0;
    this.state = freshState();
  }

  static open(filePath) {
    const store = new Store(filePath);
    if (existsSync(filePath)) {
      const content = readFileSync(filePath, "utf8");
      for (const line of content.split("\n")) {
        if (!line.trim()) continue;
        const event = JSON.parse(line);
        store.seq = event.seq;
        apply(store.state, event);
      }
    }
    return store;
  }

  /** 追加一条事件并应用到内存状态，返回带 seq 的完整事件。 */
  record(type, payload) {
    const event = { seq: this.seq + 1, type, ...payload };
    mkdirSync(dirname(this.filePath), { recursive: true });
    appendFileSync(this.filePath, `${JSON.stringify(event)}\n`);
    this.seq = event.seq;
    apply(this.state, event);
    return event;
  }
}

function freshState() {
  return {
    observations: new Map(), // observationId → 观测
    idempotency: new Map(), // key → { status, requestHash, observationId, result, ... }
    windows: new Map(), // cellId|index → 窗口聚合
    timeline: [], // 告警 / 修正 / 关窗事件（按 seq 排序，供演变过程查询）
    subscriptions: new Map(), // subscriptionId → 订阅
    notifications: new Map(), // notificationId → 通知（含每次重试落点）
  };
}

function apply(state, event) {
  switch (event.type) {
    case "subscription.upserted": {
      state.subscriptions.set(event.subscription.subscriptionId, event.subscription);
      break;
    }
    case "idempotency.started": {
      state.idempotency.set(event.key, {
        key: event.key,
        status: "processing",
        requestHash: event.requestHash,
        observationId: null,
        result: null,
        createdAt: event.at,
        updatedAt: event.at,
      });
      break;
    }
    case "idempotency.completed": {
      const entry = state.idempotency.get(event.key);
      if (entry) {
        entry.status = "processed";
        entry.observationId = event.observationId;
        entry.result = event.result;
        entry.updatedAt = event.at;
      }
      break;
    }
    case "idempotency.abandoned": {
      // 进程在记录观测前退出：释放该键，允许客户端安全重试
      state.idempotency.delete(event.key);
      break;
    }
    case "observation.recorded": {
      const o = event.observation;
      state.observations.set(o.observationId, o);
      const window = ensureWindow(state, o.cellId, o.windowIndex, o.windowStartMs, o.windowEndMs);
      window.count += 1;
      window.maxMagnitude =
        window.maxMagnitude === null ? o.magnitude : Math.max(window.maxMagnitude, o.magnitude);
      window.minDepthKm =
        window.minDepthKm === null ? o.depthKm : Math.min(window.minDepthKm, o.depthKm);
      window.sumMagnitude += o.magnitude;
      window.observationIds.push(o.observationId);
      break;
    }
    case "alert.raised":
    case "alert.escalated": {
      const window = state.windows.get(windowKey(event.cellId, event.windowIndex));
      if (window) window.notifiedLevel = event.level;
      state.timeline.push({ ...event });
      break;
    }
    case "alert.corrected": {
      // 迟到观测的修正记录：追加到窗口，已发出的告警事件本身不可改写
      const window = state.windows.get(windowKey(event.cellId, event.windowIndex));
      if (window) {
        window.corrections.push({
          seq: event.seq,
          at: event.at,
          observationId: event.observationId,
          previousLevel: event.previousLevel,
          level: event.level,
          maxMagnitude: event.maxMagnitude,
        });
        window.notifiedLevel = event.notifiedLevelAfter;
      }
      state.timeline.push({ ...event });
      break;
    }
    case "window.closed": {
      const window = state.windows.get(windowKey(event.cellId, event.windowIndex));
      if (window) {
        window.closed = true;
        window.closedAt = event.at;
      }
      state.timeline.push({ ...event });
      break;
    }
    case "notification.created": {
      state.notifications.set(event.notificationId, {
        notificationId: event.notificationId,
        subscriptionId: event.subscriptionId,
        jurisdictionId: event.jurisdictionId,
        cellId: event.cellId,
        windowIndex: event.windowIndex,
        level: event.level,
        sourceSeq: event.sourceSeq,
        sourceType: event.sourceType,
        status: "pending",
        createdAt: event.at,
        nextAttemptAt: event.at,
        deliveredAt: null,
        ackedAt: null,
        ackedBy: null,
        attempts: [],
      });
      break;
    }
    case "delivery.attempted": {
      const notification = state.notifications.get(event.notificationId);
      if (!notification) break;
      notification.attempts.push({
        attempt: event.attempt,
        at: event.at,
        outcome: event.outcome,
        error: event.error ?? null,
        nextAttemptAt: event.nextAttemptAt ?? null,
      });
      if (event.outcome === "delivered") {
        notification.status = "delivered";
        notification.deliveredAt = event.at;
      } else if (event.outcome === "dead") {
        notification.status = "dead";
      } else {
        notification.status = "retrying";
        notification.nextAttemptAt = event.nextAttemptAt;
      }
      break;
    }
    case "notification.acked": {
      const notification = state.notifications.get(event.notificationId);
      if (!notification) break;
      notification.ackedAt = event.at;
      notification.ackedBy = event.by;
      break;
    }
    default:
      throw new Error(`未知事件类型: ${event.type}`);
  }
}

function ensureWindow(state, cellId, index, startMs, endMs) {
  const key = windowKey(cellId, index);
  let window = state.windows.get(key);
  if (!window) {
    window = {
      cellId,
      index,
      startMs,
      endMs,
      count: 0,
      maxMagnitude: null,
      minDepthKm: null,
      sumMagnitude: 0,
      observationIds: [],
      notifiedLevel: 0,
      corrections: [],
      closed: false,
      closedAt: null,
    };
    state.windows.set(key, window);
  }
  return window;
}
