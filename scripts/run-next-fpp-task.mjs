/**
 * Pick the next Founder Project Plan task for agent workers (Today's Focus queue).
 *
 *   node scripts/run-next-fpp-task.mjs
 *   node scripts/run-next-fpp-task.mjs --record-id recXXX
 *   node scripts/run-next-fpp-task.mjs --worker cursor
 *   node scripts/run-next-fpp-task.mjs --list 5
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { fetchAllRecords, getGtmConfig } from "../lib/dealality-master-todo/master-todo-airtable-io.js";
import {
  MAP_MASTER_TODO,
  MASTER_TODO_DEFAULT_TABLE_ID,
} from "../lib/dealality-master-todo/master-todo-field-map.js";
import { parseSeedId } from "../lib/dealality-master-todo/dealality-airtable-field-fill.js";
import {
  FPP_WORKERS,
  compareFppTaskQueue,
  isTodaysFocusTask,
  resolveTaskPlaybook,
  selectNextFppTask,
} from "../lib/dealality-master-todo/fpp-agent-workflow-config.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const F = MAP_MASTER_TODO;

const recordIdArg = argValue("--record-id");
const workerFilter = argValue("--worker");
const listN = Number(argValue("--list") || "0");
const REPORT_PATH = path.resolve(ROOT, "reports/fpp-agent-next-task.json");

function argValue(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}

function taskSnapshot(rec) {
  const f = rec.fields || {};
  const playbook = resolveTaskPlaybook(f);
  return {
    recordId: rec.id,
    seedId: parseSeedId(f),
    task: f[F.task],
    phase: f[F.phase],
    workstream: f[F.workstream],
    status: f[F.status],
    priority: f[F.priority],
    progress: f[F.progress],
    start: f[F.startDate],
    end: f[F.dueDate],
    nextAction: f[F.nextAction],
    deliverables: f["Deliverables"],
    roadmapSort: f["Roadmap Sort"],
    recommendedWorker: playbook.worker,
    playbookId: playbook.playbookId,
    agentEligible: playbook.agentEligible !== false,
    workerProfile: FPP_WORKERS[playbook.worker],
    playbook: {
      title: playbook.title,
      steps: playbook.steps,
      deliverables: playbook.deliverables,
      approvalChecklist: playbook.approvalChecklist,
      repoPaths: playbook.repoPaths,
      airtableNotes: playbook.airtableNotes,
    },
    approvalGate:
      "Stop at Needs Review. Joan approves → apply-fpp-agent-task-update --status Completed --approved-by \"Joan D.\" --execute",
  };
}

async function main() {
  const { token, baseId } = getGtmConfig();
  const records = await fetchAllRecords(baseId, token, MASTER_TODO_DEFAULT_TABLE_ID);

  const focus = records
    .filter((r) => isTodaysFocusTask(r.fields))
    .sort(compareFppTaskQueue);

  let queue = focus;
  if (workerFilter) {
    queue = focus.filter((r) => resolveTaskPlaybook(r.fields).worker === workerFilter);
  }

  const next = selectNextFppTask(records, { recordId: recordIdArg });

  const report = {
    generatedAt: new Date().toISOString(),
    tableId: MASTER_TODO_DEFAULT_TABLE_ID,
    todaysFocusCount: focus.length,
    queueCount: queue.length,
    workerFilter,
    next: next ? taskSnapshot(next) : null,
    queuePreview: (listN > 0 ? queue : queue.slice(0, 10)).slice(0, listN || 10).map(taskSnapshot),
    workers: FPP_WORKERS,
    docs: "docs/fpp-agent-task-runner.md",
  };

  fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
  fs.writeFileSync(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`);

  console.log("\nFPP Agent — next task");
  console.log(`Today's Focus: ${report.todaysFocusCount} tasks`);
  if (workerFilter) console.log(`Filtered worker: ${workerFilter} → ${report.queueCount} tasks`);
  console.log(`Report: ${REPORT_PATH}\n`);

  if (!next) {
    console.log("No eligible task in Today's Focus queue.");
    process.exit(0);
  }

  const s = report.next;
  console.log(`NEXT: [${s.recommendedWorker}] ${s.task}`);
  console.log(`  Record: ${s.recordId} | Seed: ${s.seedId || "—"} | Status: ${s.status}`);
  console.log(`  Phase: ${s.phase} | Due: ${s.end} | Playbook: ${s.playbookId}`);
  console.log(`\nSteps:`);
  s.playbook.steps.forEach((step, i) => console.log(`  ${i + 1}. ${step}`));
  console.log(`\nJoan approval checklist:`);
  s.playbook.approvalChecklist.forEach((c) => console.log(`  - ${c}`));
  console.log(`\n${s.approvalGate}`);

  if (listN > 0 || process.argv.includes("--list")) {
    console.log("\n--- Queue preview ---");
    report.queuePreview.forEach((t, i) => {
      console.log(
        `${i + 1}. [${t.recommendedWorker}] ${t.status} | ${t.end} | ${t.task?.slice(0, 55)}`
      );
    });
  }
}

main().catch((err) => {
  console.error("[run-next-fpp-task]", err.message || err);
  process.exit(1);
});
