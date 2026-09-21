/** 系统时钟：生产环境使用，返回真实当前时间。 */
export class SystemClock {
  now() {
    return new Date();
  }
}

/**
 * 固定时钟：测试与窗口边界重放时使用。
 * 时间只能显式推进或设置，保证重放过程完全确定。
 */
export class FixedClock {
  constructor(startIso) {
    const ms = Date.parse(startIso);
    if (Number.isNaN(ms)) throw new Error(`非法起始时间: ${startIso}`);
    this.current = ms;
  }

  now() {
    return new Date(this.current);
  }

  set(iso) {
    const ms = Date.parse(iso);
    if (Number.isNaN(ms)) throw new Error(`非法时间: ${iso}`);
    this.current = ms;
  }

  advance(ms) {
    this.current += ms;
  }
}
