/**
 * Update Founder Project Plan Workstream to Proper Case.
 *
 *   node scripts/fix-founder-project-plan-workstream-case.mjs --dry-run
 *   node scripts/fix-founder-project-plan-workstream-case.mjs --execute
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
  properCaseNeedsUpdate,
  toProperCaseText,
} from "../lib/dealality-master-todo/deliverables-proper-case.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const EXECUTE = process.argv.includes("--execute");
const FIELD = MAP_MASTER_TODO.workstream;
const REPORT_PATH = path.resolve(ROOT, "reports/founder-project-plan-workstream-case-report.json");

async function main() {
  const { token, baseId } = getGtmConfig();
  const records = await fetchAllRecords(baseId, token, MASTER_TODO_DEFAULT_TABLE_ID);

  const toUpdate = [];
  const valueMap = new Map();

  for (const rec of records) {
    const current = rec.fields?.[FIELD];
    if (!current || typeof current !== "string") continue;
    if (!properCaseNeedsUpdate(current)) continue;
    const next = toProperCaseText(current);
    toUpdate.push({
      id: rec.id,
      task: rec.fields?.[MAP_MASTER_TODO.task],
      phase: rec.fields?.[MAP_MASTER_TODO.phase],
      before: current,
      after: next,
    });
    if (!valueMap.has(current)) valueMap.set(current, next);
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: EXECUTE ? "execute" : "dry-run",
    tableId: MASTER_TODO_DEFAULT_TABLE_ID,
    field: FIELD,
    scanned: records.length,
    withWorkstream: records.filter((r) => r.fields?.[FIELD]).length,
    uniqueValuesChanged: valueMap.size,
    valueMap: Object.fromEntries(valueMap),
    toUpdateCount: toUpdate.length,
    toUpdate,
    updated: [],
    errors: [],
  };

  if (EXECUTE && toUpdate.length) {
    const base = new Airtable({ apiKey: token }).base(baseId);
    for (let i = 0; i < toUpdate.length; i += 10) {
      const batch = toUpdate.slice(i, i + 10).map((u) => ({
        id: u.id,
        fields: { [FIELD]: u.after },
      }));
      try {
        const updated = await base(MASTER_TODO_DEFAULT_TABLE_ID).update(batch, { typecast: true });
        report.updated.push(
          ...updated.map((r) => ({
            id: r.id,
            workstream: r.fields?.[FIELD],
          }))
        );
      } catch (err) {
        report.errors.push({ batch: i / 10, message: err.message || String(err) });
      }
    }
  }

  fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
  fs.writeFileSync(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`);

  console.log(`\nWorkstream Proper Case (${report.mode})`);
  console.log(`Scanned: ${report.scanned} | With Workstream: ${report.withWorkstream}`);
  console.log(`Unique values to change: ${valueMap.size} | Records to update: ${report.toUpdateCount}`);
  if (EXECUTE) {
    console.log(`Updated: ${report.updated.length} | Errors: ${report.errors.length}`);
  }
  console.log(`Report: ${REPORT_PATH}`);
  if (report.errors.length) process.exitCode = 1;
}

main().catch((err) => {
  console.error("[fix-founder-project-plan-workstream-case]", err.message || err);
  process.exit(1);
});
