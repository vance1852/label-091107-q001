// 时钟抽象：生产环境使用系统时钟，测试与重放使用可手动设定/推进的固定时钟。

export class SystemClock {
  now() {
    return Date.now();
  }
}

export class ManualClock {
  #current;

  /** @param {number|string|Date} initial 初始时刻 */
  constructor(initial) {
    this.#current = toMillis(initial);
  }

  now() {
    return this.#current;
  }

  /** 设定到某个绝对时刻 */
  setTo(time) {
    this.#current = toMillis(time);
    return this.#current;
  }

  /** 向前推进若干毫秒 */
  advance(ms) {
    if (!Number.isFinite(ms) || ms < 0) {
      throw new Error("推进时间必须为非负毫秒数");
    }
    this.#current += ms;
    return this.#current;
  }
}

export function toMillis(value) {
  let ms;
  if (value instanceof Date) {
    ms = value.getTime();
  } else if (typeof value === "number") {
    ms = value;
  } else if (typeof value === "string") {
    ms = Date.parse(value);
  } else {
    ms = NaN;
  }
  if (!Number.isFinite(ms)) {
    throw new Error(`无法识别的时间值: ${String(value)}`);
  }
  return ms;
}
