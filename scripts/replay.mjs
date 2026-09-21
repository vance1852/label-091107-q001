#!/usr/bin/env node
// 固定时钟重放核对工具：离线重放事件日志，打印每个决策的演变与每个幂等键的最终状态。
//
// 用法：
//   node scripts/replay.mjs [事件日志路径] [--at ISO时间] [--send]
//
// 默认只重放不触网（--send 缺省）：QUEUED/WAITING 保持日志中的最后状态，
// 用来核对“如果进程在这里崩溃，哪些键会在恢复后补投”。
// 给定 --at 可把固定时钟设到指定时刻，重算该时刻各区域级别与到期重试。
import { resolve } from "node:path";
import { AlertEngine } from "../src/engine.js";
import { ManualClock, toMillis } from "../src/clock.js";

const args = process.argv.slice(2);
const path = resolve(args.find((a) => !a.startsWith("--")) ?? "data/events.jsonl");
const atIndex = args.indexOf("--at");
const atArg = atIndex >= 0 ? args[atIndex + 1] : undefined;
const maySend = args.includes("--send");

const clock = new ManualClock(atArg ? toMillis(atArg) : Date.now());
const refused = [];
const engine = new AlertEngine({
  path,
  clock,
  sender: async (n) => {
    refused.push(n.key);
    return { ok: false, retryable: true, error: "离线重放：禁止真实投递" };
  },
});

if (maySend) {
  await engine.pump();
}

const pad = (s, n) => String(s).padEnd(n);
console.log(`\n事件日志: ${path}`);
console.log(`固定时钟: ${new Date(clock.now()).toISOString()}   触网投递: ${maySend ? "是" : "否"}\n`);

console.log("== 决策与演变 ==");
for (const d of engine.listDecisions()) {
  console.log(
    `${d.id}  ${d.region} ${d.cellId}  状态=${pad(d.status, 6)} 当前=${d.currentLevel} 修订=R${d.revision} 确认=R${d.acknowledgedRevision}`,
  );
  for (const e of d.history) {
    console.log(
      `   R${e.revision} ${pad(e.kind, 15)} ${pad(e.level, 10)} ${new Date(e.at).toISOString()}${e.late ? "  [迟到-仅修正]" : ""}`,
    );
  }
}

console.log("\n== 幂等键最终状态 ==");
for (const n of engine.listNotifications()) {
  const due =
    n.nextAttemptAt !== null && n.nextAttemptAt <= clock.now() ? "  ← 已到点，恢复即补投" : "";
  console.log(
    `${n.key}  ${pad(n.status, 8)} 尝试=${n.attempts.length} 下次=${n.nextAttemptAt ? new Date(n.nextAttemptAt).toISOString() : "-"}${due}`,
  );
  for (const a of n.attempts) {
    console.log(`     - ${a.ok ? "成功" : `失败(${a.error ?? a.status})`} @ ${new Date(a.at).toISOString()}`);
  }
}

console.log("\n== 各区域当前级别 ==");
const regions = new Set(engine.listDecisions().map((d) => d.region));
for (const region of [...regions].sort()) {
  const level = engine.getRegionLevel(region);
  console.log(`${region}: ${level.level}（${level.currentRank}）活跃单元=${level.activeCells.length}`);
}

console.log("\n== 未确认订阅 ==");
const unacked = engine.listUnacknowledged();
if (!unacked.length) console.log("(无)");
for (const u of unacked) {
  console.log(`${u.subscriptionId} 辖区=${u.jurisdictionId} ${u.region}/${u.cellId} 已发级别=${u.sentRank}`);
}

if (!maySend && refused.length) {
  console.log(`\n（离线模式未触网；${refused.length} 个通知保持日志状态，不会产生第二次通知）`);
}
