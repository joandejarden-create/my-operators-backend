/**
 * Sync Founder Project Plan Status / Progress / Next Action across all phases
 * (aligned to shipped Dealality platform + GTM work).
 *
 *   node scripts/sync-founder-project-plan-phase-status.mjs --dry-run
 *   node scripts/sync-founder-project-plan-phase-status.mjs --execute
 *   node scripts/sync-founder-project-plan-phase-status.mjs --execute --phase "Platform Build"
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
  buildPhaseStatusPatch,
  isPhaseStatusTaskRow,
} from "../lib/dealality-master-todo/founder-project-plan-phase-status-updates.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const EXECUTE = process.argv.includes("--execute");
const phaseArgIdx = process.argv.indexOf("--phase");
const PHASE_FILTER = phaseArgIdx >= 0 ? process.argv[phaseArgIdx + 1] : null;
const REPORT_PATH = path.resolve(ROOT, "reports/founder-project-plan-phase-status-report.json");

const F = MAP_MASTER_TODO;

async function main() {
  const { token, baseId } = getGtmConfig();
  const records = await fetchAllRecords(baseId, token, MASTER_TODO_DEFAULT_TABLE_ID);

  const toUpdate = [];
  for (const rec of records) {
    if (!isPhaseStatusTaskRow(rec.fields)) continue;
    if (PHASE_FILTER && rec.fields?.[F.phase] !== PHASE_FILTER) continue;
    const built = buildPhaseStatusPatch(rec.fields);
    if (!built) continue;
    toUpdate.push({
      id: rec.id,
      phase: built.phase,
      step: built.step,
      task: rec.fields?.[F.task],
      workstream: rec.fields?.[F.workstream],
      before: {
        status: rec.fields?.[F.status],
        progress: rec.fields?.[F.progress],
        completedDate: rec.fields?.[F.completedDate],
        nextAction: rec.fields?.[F.nextAction],
      },
      after: built.target,
      patch: built.patch,
    });
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: EXECUTE ? "execute" : "dry-run",
    phaseFilter: PHASE_FILTER,
    tableId: MASTER_TODO_DEFAULT_TABLE_ID,
    scanned: records.length,
    updateCount: toUpdate.length,
    toUpdate,
    updated: [],
    errors: [],
  };

  if (EXECUTE && toUpdate.length) {
    const base = new Airtable({ apiKey: token }).base(baseId);
    for (let i = 0; i < toUpdate.length; i += 10) {
      const batch = toUpdate.slice(i, i + 10).map((u) => ({
        id: u.id,
        fields: u.patch,
      }));
      try {
        const updated = await base(MASTER_TODO_DEFAULT_TABLE_ID).update(batch, { typecast: true });
        report.updated.push(
          ...updated.map((r) => ({
            id: r.id,
            phase: r.fields?.[F.phase],
            task: r.fields?.[F.task],
            status: r.fields?.[F.status],
            progress: r.fields?.[F.progress],
          }))
        );
      } catch (err) {
        report.errors.push({ batch: i / 10, message: err.message || String(err) });
      }
    }
  }

  fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
  fs.writeFileSync(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`);

  const byPhase = {};
  const byTransition = {};
  for (const u of toUpdate) {
    byPhase[u.phase] = (byPhase[u.phase] || 0) + 1;
    const key = `${u.before.status} → ${u.after.status}`;
    byTransition[key] = (byTransition[key] || 0) + 1;
  }

  console.log(`\nFPP phase status sync (${report.mode})${PHASE_FILTER ? ` — ${PHASE_FILTER}` : ""}`);
  console.log(`Rows to update: ${report.updateCount}`);
  if (EXECUTE) {
    console.log(`Updated: ${report.updated.length} | Errors: ${report.errors.length}`);
  }
  console.log(`Report: ${REPORT_PATH}`);
  console.log("\nBy phase:");
  Object.entries(byPhase)
    .sort((a, b) => b[1] - a[1])
    .forEach(([p, n]) => console.log(`  ${p}: ${n}`));
  console.log("\nTransitions:");
  Object.entries(byTransition).forEach(([k, n]) => console.log(`  ${k}: ${n}`));

  if (report.errors.length) process.exitCode = 1;
}

main().catch((err) => {
  console.error("[sync-founder-project-plan-phase-status]", err.message || err);
  process.exit(1);
});
