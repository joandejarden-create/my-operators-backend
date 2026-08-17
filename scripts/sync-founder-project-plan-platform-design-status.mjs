/**
 * Sync Platform Design task Status / Progress / Next Action from shipped platform work.
 *
 *   node scripts/sync-founder-project-plan-platform-design-status.mjs --dry-run
 *   node scripts/sync-founder-project-plan-platform-design-status.mjs --execute
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
  buildPlatformDesignStatusPatch,
  isPlatformDesignTaskRow,
} from "../lib/dealality-master-todo/founder-project-plan-platform-design-status.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const EXECUTE = process.argv.includes("--execute");
const REPORT_PATH = path.resolve(
  ROOT,
  "reports/founder-project-plan-platform-design-status-report.json"
);

const F = MAP_MASTER_TODO;

async function main() {
  const { token, baseId } = getGtmConfig();
  const records = await fetchAllRecords(baseId, token, MASTER_TODO_DEFAULT_TABLE_ID);

  const toUpdate = [];
  for (const rec of records) {
    if (!isPlatformDesignTaskRow(rec.fields)) continue;
    const built = buildPlatformDesignStatusPatch(rec.fields);
    if (!built) continue;
    toUpdate.push({
      id: rec.id,
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
    tableId: MASTER_TODO_DEFAULT_TABLE_ID,
    scanned: records.length,
    platformDesignUpdates: toUpdate.length,
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

  const byStatus = {};
  for (const u of toUpdate) {
    const key = `${u.before.status} → ${u.after.status}`;
    byStatus[key] = (byStatus[key] || 0) + 1;
  }

  console.log(`\nPlatform Design status sync (${report.mode})`);
  console.log(`Rows to update: ${report.platformDesignUpdates}`);
  if (EXECUTE) {
    console.log(`Updated: ${report.updated.length} | Errors: ${report.errors.length}`);
  }
  console.log(`Report: ${REPORT_PATH}`);
  console.log("\nTransitions:");
  Object.entries(byStatus).forEach(([k, n]) => console.log(`  ${k}: ${n}`));

  if (report.errors.length) process.exitCode = 1;
}

main().catch((err) => {
  console.error("[sync-founder-project-plan-platform-design-status]", err.message || err);
  process.exit(1);
});
