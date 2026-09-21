// 只追加（append-only）事件日志。
// 所有状态变更先序列化为不可变事件落盘，再交给 reducer 更新内存状态；
// 进程重启时逐条重放，保证已 SENT 的通知不会被再次投递。
import { appendFileSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import { dirname } from "node:path";

export class EventStore {
  #path;
  #apply;
  events = [];
  #nextSeq = 1;

  constructor(path, apply, events = [], nextSeq = 1) {
    this.#path = path;
    this.#apply = apply;
    this.events = events;
    this.#nextSeq = nextSeq;
  }

  /**
   * 打开日志并重放全部历史事件。
   * @param {string|null} path JSONL 文件路径；null 表示纯内存（测试用）
   * @param {(event:object)=>void} apply 事件重放函数
   */
  static open(path, apply) {
    const events = [];
    let nextSeq = 1;
    if (path && existsSync(path)) {
      const content = readFileSync(path, "utf8");
      for (const line of content.split("\n")) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        const event = JSON.parse(trimmed);
        events.push(event);
        apply(event);
        nextSeq = event.seq + 1;
      }
    }
    return new EventStore(path, apply, events, nextSeq);
  }

  /**
   * 追加事件：先补 seq，再落盘，最后应用到内存。
   * 落盘失败会抛出，内存状态不会被部分更新。
   */
  append(type, payload = {}) {
    const event = { seq: this.#nextSeq, type, ...payload };
    if (this.#path) {
      mkdirSync(dirname(this.#path), { recursive: true });
      appendFileSync(this.#path, JSON.stringify(event) + "\n", { flag: "a" });
    }
    this.events.push(event);
    this.#nextSeq += 1;
    this.#apply(event);
    return event;
  }
}
