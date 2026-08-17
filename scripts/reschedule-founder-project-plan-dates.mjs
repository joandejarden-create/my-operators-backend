/**
 * Reschedule Founder Project Plan Start / End for non-completed tasks.
 * Kickoff assumption: next week (Mon Jul 7, 2026).
 *
 *   node scripts/reschedule-founder-project-plan-dates.mjs --dry-run
 *   node scripts/reschedule-founder-project-plan-dates.mjs --execute
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import {
  fetchAllRecords,
  getGtmConfig,
} from "../lib/dealality-master-todo/master-todo-airtable-io.js";
import {
  MAP_MASTER_TODO,
  MASTER_TODO_DEFAULT_TABLE_ID,
} from "../lib/dealality-master-todo/master-todo-field-map.js";
import {
  SCHEDULE_KICKOFF,
  computeFounderProjectPlanSchedule,
  scheduleDiff,
} from "../lib/dealality-master-todo/founder-project-plan-schedule.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const EXECUTE = process.argv.includes("--execute");
const REPORT_PATH = path.resolve(ROOT, "reports/founder-project-plan-schedule-report.json");

const F = MAP_MASTER_TODO;

async function main() {
  const { token, baseId } = getGtmConfig();
  const records = await fetchAllRecords(baseId, token, MASTER_TODO_DEFAULT_TABLE_ID);
  const schedule = computeFounderProjectPlanSchedule(records);

  const toUpdate = [];
  for (const rec of records) {
    const proposed = schedule.get(rec.id);
    if (!proposed) continue;
    const diff = scheduleDiff(rec.fields, proposed);
    if (!diff) continue;
    toUpdate.push({
      id: rec.id,
      task: rec.fields?.[F.task],
      phase: rec.fields?.[F.phase],
      status: rec.fields?.[F.status],
      priority: rec.fields?.[F.priority],
      scheduleSource: proposed.source,
      ...diff,
    });
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: EXECUTE ? "execute" : "dry-run",
    kickoff: SCHEDULE_KICKOFF,
    tableId: MASTER_TODO_DEFAULT_TABLE_ID,
    scanned: records.length,
    toUpdateCount: toUpdate.length,
    toUpdate,
    updated: [],
    errors: [],
  };

  if (EXECUTE && toUpdate.length) {
    const base = new Airtable({ apiKey: token }).base(baseId);
    for (let i = 0; i < toUpdate.length; i += 10) {
      const batch = toUpdate.slice(i, i + 10).map((u) => {
        const fields = {
          [F.startDate]: u.after.start,
          [F.dueDate]: u.after.end,
        };
        if (u.after.completedDate !== undefined && u.after.completedDate !== u.before.completedDate) {
          fields[F.completedDate] = u.after.completedDate;
        }
        return { id: u.id, fields };
      });
      try {
        const updated = await base(MASTER_TODO_DEFAULT_TABLE_ID).update(batch, { typecast: true });
        report.updated.push(
          ...updated.map((r) => ({
            id: r.id,
            start: r.fields?.[F.startDate],
            end: r.fields?.[F.dueDate],
          }))
        );
      } catch (err) {
        report.errors.push({ batch: i / 10, message: err.message || String(err) });
      }
    }
  }

  fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
  fs.writeFileSync(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`);

  console.log(`\nFPP Schedule (${report.mode}) — kickoff ${SCHEDULE_KICKOFF}`);
  console.log(`Scanned: ${report.scanned} | To update: ${report.toUpdateCount}`);
  if (EXECUTE) {
    console.log(`Updated: ${report.updated.length} | Errors: ${report.errors.length}`);
  }
  console.log(`Report: ${REPORT_PATH}`);

  const byPhase = {};
  for (const u of toUpdate) {
    const p = u.phase || "(none)";
    if (!byPhase[p]) byPhase[p] = 0;
    byPhase[p] += 1;
  }
  console.log("\nUpdates by phase:");
  Object.entries(byPhase)
    .sort((a, b) => b[1] - a[1])
    .forEach(([p, n]) => console.log(`  ${p}: ${n}`));

  if (report.errors.length) process.exitCode = 1;
}

main().catch((err) => {
  console.error("[reschedule-founder-project-plan-dates]", err.message || err);
  process.exit(1);
});
