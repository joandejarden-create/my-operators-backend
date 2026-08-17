/**
 * Migrate Founder Project Plan Priority values to descriptive Airtable labels.
 *
 *   node scripts/migrate-founder-project-plan-priority.mjs --dry-run
 *   node scripts/migrate-founder-project-plan-priority.mjs --execute
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
  VAL_FPP_PRIORITY,
  mapPriorityForWrite,
} from "../lib/dealality-master-todo/master-todo-field-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const EXECUTE = process.argv.includes("--execute");
const REPORT_PATH = path.resolve(ROOT, "reports/founder-project-plan-priority-migration.json");

async function main() {
  const { token, baseId } = getGtmConfig();
  const records = await fetchAllRecords(baseId, token, MASTER_TODO_DEFAULT_TABLE_ID);
  const field = MAP_MASTER_TODO.priority;
  const toUpdate = [];

  for (const rec of records) {
    const current = rec.fields?.[field];
    if (!current) continue;
    const mapped = mapPriorityForWrite(current);
    if (mapped && mapped !== current) {
      toUpdate.push({
        id: rec.id,
        task: rec.fields?.[MAP_MASTER_TODO.task] || rec.fields?.Phase,
        before: current,
        after: mapped,
      });
    }
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: EXECUTE ? "execute" : "dry-run",
    targetOptions: VAL_FPP_PRIORITY,
    recordCount: records.length,
    toUpdate,
    updated: [],
    errors: [],
  };

  if (EXECUTE && toUpdate.length) {
    const base = new Airtable({ apiKey: token }).base(baseId);
    for (let i = 0; i < toUpdate.length; i += 10) {
      const batch = toUpdate.slice(i, i + 10).map((u) => ({
        id: u.id,
        fields: { [field]: u.after },
      }));
      try {
        const updated = await base(MASTER_TODO_DEFAULT_TABLE_ID).update(batch, { typecast: true });
        report.updated.push(...updated.map((r) => ({ id: r.id, priority: r.fields?.[field] })));
      } catch (err) {
        report.errors.push({ batch: i / 10, message: err.message || String(err) });
      }
    }
  }

  fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
  fs.writeFileSync(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`);

  console.log(`\nFounder Project Plan priority migration (${report.mode})`);
  console.log(`Records scanned: ${report.recordCount}`);
  console.log(`To update: ${toUpdate.length}`);
  if (EXECUTE) {
    console.log(`Updated: ${report.updated.length}`);
    console.log(`Errors: ${report.errors.length}`);
  }
  console.log(`Report: ${REPORT_PATH}`);
  if (report.errors.length) process.exitCode = 1;
}

main().catch((err) => {
  console.error("[migrate-founder-project-plan-priority]", err.message || err);
  process.exit(1);
});
