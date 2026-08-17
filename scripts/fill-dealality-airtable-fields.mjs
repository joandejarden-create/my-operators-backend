/**
 * Fill empty fields on Founder Project Plan and Pilot Target List.
 *
 *   node scripts/fill-dealality-airtable-fields.mjs --dry-run
 *   node scripts/fill-dealality-airtable-fields.mjs --execute
 *   node scripts/fill-dealality-airtable-fields.mjs --execute --fpp-only
 *   node scripts/fill-dealality-airtable-fields.mjs --execute --ptl-only
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import {
  fetchAllRecords,
  fetchAllTables,
  getGtmConfig,
} from "../lib/dealality-master-todo/master-todo-airtable-io.js";
import { MASTER_TODO_DEFAULT_TABLE_ID } from "../lib/dealality-master-todo/master-todo-field-map.js";
import {
  PILOT_TARGET_LIST_TABLE_ID,
  buildFounderFillPatch,
  buildMasterFillPatch,
  buildPilotTargetFillPatch,
} from "../lib/dealality-master-todo/dealality-airtable-field-fill.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const EXECUTE = process.argv.includes("--execute");
const DRY_RUN = !EXECUTE;
const FPP_ONLY = process.argv.includes("--fpp-only");
const PTL_ONLY = process.argv.includes("--ptl-only");
const RUN_FPP = !PTL_ONLY;
const RUN_PTL = !FPP_ONLY;
const REPORT_PATH = path.resolve(ROOT, "reports/dealality-airtable-field-fill-report.json");

function diffPatch(before, patch) {
  const changes = {};
  for (const [k, v] of Object.entries(patch)) {
    if (JSON.stringify(before?.[k]) !== JSON.stringify(v)) {
      changes[k] = { before: before?.[k] ?? null, after: v };
    }
  }
  return changes;
}

async function fillTable({ baseId, token, tableId, buildPatch, label }) {
  const tables = await fetchAllTables(baseId, token);
  const meta = tables.find((t) => t.id === tableId);
  const schemaFieldNames = new Set((meta?.fields || []).map((f) => f.name.toLowerCase()));
  const records = await fetchAllRecords(baseId, token, tableId);

  const toUpdate = [];
  for (const rec of records) {
    const patch = buildPatch(rec, schemaFieldNames);
    if (!patch) continue;
    const changes = diffPatch(rec.fields, patch);
    if (!Object.keys(changes).length) continue;
    toUpdate.push({
      id: rec.id,
      label: rec.fields?.Task || rec.fields?.Name || rec.fields?.Phase || rec.id,
      changes,
      fields: patch,
    });
  }

  const result = { tableId, label, recordCount: records.length, toUpdate, updated: [], errors: [] };

  if (!DRY_RUN && toUpdate.length) {
    const base = new Airtable({ apiKey: token }).base(baseId);
    for (let i = 0; i < toUpdate.length; i += 10) {
      const batch = toUpdate.slice(i, i + 10).map((u) => ({ id: u.id, fields: u.fields }));
      try {
        const updated = await base(tableId).update(batch, { typecast: true });
        result.updated.push(...updated.map((r) => ({ id: r.id })));
      } catch (err) {
        result.errors.push({ batch: i / 10, message: err.message || String(err) });
      }
    }
  }

  return result;
}

async function main() {
  const { token, baseId } = getGtmConfig();
  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "dry-run" : "execute",
    baseId,
    tables: [],
  };

  if (RUN_FPP) {
    report.tables.push(
      await fillTable({
        baseId,
        token,
        tableId: MASTER_TODO_DEFAULT_TABLE_ID,
        label: "Founder Project Plan",
        buildPatch: (rec, schema) => buildMasterFillPatch(rec, schema) || buildFounderFillPatch(rec, schema),
      })
    );
  }

  if (RUN_PTL) {
    report.tables.push(
      await fillTable({
        baseId,
        token,
        tableId: PILOT_TARGET_LIST_TABLE_ID,
        label: "Pilot Target List",
        buildPatch: buildPilotTargetFillPatch,
      })
    );
  }

  fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
  fs.writeFileSync(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`);

  for (const t of report.tables) {
    console.log(`\n${t.label} (${t.tableId})`);
    console.log(`  Records: ${t.recordCount}`);
    console.log(`  Proposed updates: ${t.toUpdate.length}`);
    if (!DRY_RUN) {
      console.log(`  Updated: ${t.updated.length}`);
      console.log(`  Errors: ${t.errors.length}`);
    }
  }
  console.log(`\nReport: ${REPORT_PATH}`);
  if (report.tables.some((t) => t.errors?.length)) process.exitCode = 1;
}

main().catch((err) => {
  console.error("[fill-dealality-airtable-fields]", err.message || err);
  process.exit(1);
});
