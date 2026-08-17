/**
 * Update Founder Project Plan Deliverables to Proper Case.
 *
 *   node scripts/fix-founder-project-plan-deliverables-case.mjs --dry-run
 *   node scripts/fix-founder-project-plan-deliverables-case.mjs --execute
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
import { MASTER_TODO_DEFAULT_TABLE_ID } from "../lib/dealality-master-todo/master-todo-field-map.js";
import {
  deliverablesNeedsUpdate,
  toDeliverablesProperCase,
} from "../lib/dealality-master-todo/deliverables-proper-case.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const EXECUTE = process.argv.includes("--execute");
const FIELD = "Deliverables";
const REPORT_PATH = path.resolve(ROOT, "reports/founder-project-plan-deliverables-case-report.json");

async function main() {
  const { token, baseId } = getGtmConfig();
  const records = await fetchAllRecords(baseId, token, MASTER_TODO_DEFAULT_TABLE_ID);

  const toUpdate = [];
  for (const rec of records) {
    const current = rec.fields?.[FIELD];
    if (!current || typeof current !== "string") continue;
    if (!deliverablesNeedsUpdate(current)) continue;
    const next = toDeliverablesProperCase(current);
    toUpdate.push({
      id: rec.id,
      task: rec.fields?.Task || rec.fields?.Phase,
      before: current,
      after: next,
    });
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: EXECUTE ? "execute" : "dry-run",
    tableId: MASTER_TODO_DEFAULT_TABLE_ID,
    field: FIELD,
    scanned: records.length,
    withDeliverables: records.filter((r) => r.fields?.[FIELD]).length,
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
            deliverables: r.fields?.[FIELD],
          }))
        );
      } catch (err) {
        report.errors.push({ batch: i / 10, message: err.message || String(err) });
      }
    }
  }

  fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
  fs.writeFileSync(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`);

  console.log(`\nDeliverables Proper Case (${report.mode})`);
  console.log(`Scanned: ${report.scanned} | With Deliverables: ${report.withDeliverables}`);
  console.log(`To update: ${report.toUpdateCount}`);
  if (EXECUTE) {
    console.log(`Updated: ${report.updated.length} | Errors: ${report.errors.length}`);
  }
  console.log(`Report: ${REPORT_PATH}`);
  if (report.errors.length) process.exitCode = 1;
}

main().catch((err) => {
  console.error("[fix-founder-project-plan-deliverables-case]", err.message || err);
  process.exit(1);
});
