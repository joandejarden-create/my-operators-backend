/**
 * Phase completion summary for Founder Project Plan.
 *
 *   node scripts/report-founder-project-plan-phase-progress.mjs
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  fetchAllRecords,
  getGtmConfig,
} from "../lib/dealality-master-todo/master-todo-airtable-io.js";
import { MASTER_TODO_DEFAULT_TABLE_ID } from "../lib/dealality-master-todo/master-todo-field-map.js";
import { FPP_EXCLUDE_PHASE_ROLLUP_FILTER } from "../lib/dealality-master-todo/founder-project-plan-view-config.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORT_JSON = path.resolve(ROOT, "reports/founder-project-plan-phase-progress.json");
const REPORT_MD = path.resolve(ROOT, "reports/founder-project-plan-phase-progress.md");

function isPhaseRollup(task) {
  return String(task || "").startsWith("[Phase rollup]");
}

function isCompleted(status) {
  return String(status || "").trim() === "Completed";
}

function pct(completed, total) {
  if (!total) return 0;
  return Math.round((completed / total) * 1000) / 10;
}

function matchesTodaysFocus(fields) {
  if (["Completed", "Deferred", "Not Needed"].includes(fields.Status)) return false;
  if (isPhaseRollup(fields.Task)) return false;
  const p = fields.Priority || "";
  const isP0P1 =
    p === "P0 = Urgent / Launch-Critical" || p === "P1 = Important Near-Term";
  if (fields.Source === "ChatGPT Master To-Do") return isP0P1;
  if (fields.Status === "In Progress" && isP0P1) return true;
  if (!isP0P1 || !fields.End) return false;
  const today = new Date();
  const end = new Date(`${fields.End}T12:00:00`);
  const startOfWeek = new Date(today);
  startOfWeek.setDate(today.getDate() - ((today.getDay() + 6) % 7));
  const endOfWeek = new Date(startOfWeek);
  endOfWeek.setDate(startOfWeek.getDate() + 6);
  return end >= startOfWeek && end <= endOfWeek;
}

async function main() {
  const { token, baseId } = getGtmConfig();
  const records = await fetchAllRecords(baseId, token, MASTER_TODO_DEFAULT_TABLE_ID);
  const tasks = records.filter((r) => !isPhaseRollup(r.fields?.Task));

  const byPhase = new Map();
  for (const rec of tasks) {
    const phase = rec.fields?.Phase || "(No Phase)";
    if (!byPhase.has(phase)) {
      byPhase.set(phase, {
        phase,
        total: 0,
        completed: 0,
        inProgress: 0,
        notStarted: 0,
        other: 0,
        byStatus: {},
      });
    }
    const bucket = byPhase.get(phase);
    bucket.total += 1;
    const status = rec.fields?.Status || "(blank)";
    bucket.byStatus[status] = (bucket.byStatus[status] || 0) + 1;
    if (isCompleted(status)) bucket.completed += 1;
    else if (status === "In Progress") bucket.inProgress += 1;
    else if (status === "Not Started") bucket.notStarted += 1;
    else bucket.other += 1;
  }

  const phases = [...byPhase.values()]
    .map((p) => ({
      ...p,
      percentCompleted: pct(p.completed, p.total),
    }))
    .sort((a, b) => b.total - a.total);

  const totals = phases.reduce(
    (acc, p) => {
      acc.total += p.total;
      acc.completed += p.completed;
      acc.inProgress += p.inProgress;
      acc.notStarted += p.notStarted;
      acc.other += p.other;
      return acc;
    },
    { total: 0, completed: 0, inProgress: 0, notStarted: 0, other: 0 }
  );
  totals.percentCompleted = pct(totals.completed, totals.total);

  const todaysFocus = tasks.filter((r) => matchesTodaysFocus(r.fields || {}));

  const report = {
    generatedAt: new Date().toISOString(),
    baseId,
    tableId: MASTER_TODO_DEFAULT_TABLE_ID,
    excludePhaseRollups: true,
    rollupFilter: FPP_EXCLUDE_PHASE_ROLLUP_FILTER,
    totals,
    phases,
    todaysFocusCount: todaysFocus.length,
    todaysFocusPreview: todaysFocus.map((r) => ({
      id: r.id,
      task: r.fields?.Task,
      priority: r.fields?.Priority,
      status: r.fields?.Status,
      end: r.fields?.End,
      nextAction: r.fields?.["Next Action"],
      source: r.fields?.Source,
    })),
  };

  const mdLines = [
    "# Founder Project Plan — phase progress",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    "## Overall",
    "",
    "| Metric | Count |",
    "|--------|------:|",
    `| Total tasks (excl. rollups) | ${totals.total} |`,
    `| Completed | ${totals.completed} (${totals.percentCompleted}%) |`,
    `| In Progress | ${totals.inProgress} |`,
    `| Not Started | ${totals.notStarted} |`,
    `| Other status | ${totals.other} |`,
    "",
    "## By phase",
    "",
    "| Phase | Total | Completed | % Done | In Progress | Not Started |",
    "|-------|------:|----------:|-------:|------------:|------------:|",
  ];

  for (const p of phases) {
    mdLines.push(
      `| ${p.phase} | ${p.total} | ${p.completed} | ${p.percentCompleted}% | ${p.inProgress} | ${p.notStarted} |`
    );
  }

  mdLines.push("");
  mdLines.push("## Today's focus preview");
  mdLines.push("");
  mdLines.push(`Tasks in Today's Focus view: **${todaysFocus.length}**`);
  mdLines.push("");
  for (const t of report.todaysFocusPreview) {
    mdLines.push(`- **${t.task}** — ${t.priority} | ${t.status} | End: ${t.end || "—"}`);
  }
  mdLines.push("");

  fs.mkdirSync(path.dirname(REPORT_JSON), { recursive: true });
  fs.writeFileSync(REPORT_JSON, `${JSON.stringify(report, null, 2)}\n`);
  fs.writeFileSync(REPORT_MD, `${mdLines.join("\n")}\n`);

  console.log("\nFounder Project Plan phase progress");
  console.log(`Total tasks: ${totals.total} | Completed: ${totals.completed} (${totals.percentCompleted}%)`);
  console.log(`Today's focus preview: ${todaysFocus.length} tasks`);
  console.log(`JSON: ${REPORT_JSON}`);
  console.log(`Markdown: ${REPORT_MD}`);
}

main().catch((err) => {
  console.error("[report-founder-project-plan-phase-progress]", err.message || err);
  process.exit(1);
});
