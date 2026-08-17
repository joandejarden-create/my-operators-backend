/**
 * Apply strategy-aligned updates to Founder Project Plan (master to-do + phase rollups).
 *
 *   node scripts/sync-founder-project-plan-state.mjs --dry-run
 *   node scripts/sync-founder-project-plan-state.mjs --execute
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
  COMPLETED_STATUS_VALUES,
  MAP_MASTER_TODO,
  MASTER_TODO_DEFAULT_TABLE_ID,
  VAL_MASTER_TODO_STATUS,
} from "../lib/dealality-master-todo/master-todo-field-map.js";
import {
  MASTER_TODO_SYNC_UPDATES,
  PHASE_ROLLUP_SYNC_UPDATES,
} from "../lib/dealality-master-todo/founder-project-plan-sync-updates.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const TABLE_ID = process.argv.includes("--table-id")
  ? process.argv[process.argv.indexOf("--table-id") + 1]
  : MASTER_TODO_DEFAULT_TABLE_ID;
const EXECUTE = process.argv.includes("--execute");
const DRY_RUN = !EXECUTE;
const REPORT_PATH = path.resolve(ROOT, "reports/founder-project-plan-sync-report.json");

function parseSeedId(fields) {
  const sm = String(fields?.[MAP_MASTER_TODO.successMetric] || "");
  const m = sm.match(/Seed ID: (mt-\d+)/);
  return m?.[1] || null;
}

function isCompleted(fields) {
  return COMPLETED_STATUS_VALUES.has(
    String(fields?.[MAP_MASTER_TODO.status] || "").trim().toLowerCase()
  );
}

function validateStatus(value) {
  if (!value) return { ok: true };
  if (!VAL_MASTER_TODO_STATUS.includes(value)) {
    return { ok: false, error: `Invalid Status "${value}"` };
  }
  return { ok: true };
}

function diffFields(before, after) {
  const changes = {};
  for (const [k, v] of Object.entries(after)) {
    if (JSON.stringify(before?.[k]) !== JSON.stringify(v)) {
      changes[k] = { before: before?.[k] ?? null, after: v };
    }
  }
  return changes;
}

async function main() {
  const { token, baseId } = getGtmConfig();
  const records = await fetchAllRecords(baseId, token, TABLE_ID);
  const byId = new Map(records.map((r) => [r.id, r]));
  const bySeedId = new Map();
  for (const rec of records) {
    const seedId = parseSeedId(rec.fields);
    if (seedId) bySeedId.set(seedId, rec);
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "dry-run" : "execute",
    baseId,
    tableId: TABLE_ID,
    toUpdate: [],
    skipped: [],
    errors: [],
    updated: [],
  };

  const pending = [];

  for (const item of MASTER_TODO_SYNC_UPDATES) {
    const rec = bySeedId.get(item.seedId);
    if (!rec) {
      report.skipped.push({ seedId: item.seedId, reason: "record not found" });
      continue;
    }
    if (isCompleted(rec.fields)) {
      report.skipped.push({
        seedId: item.seedId,
        recordId: rec.id,
        reason: "completed row not modified",
      });
      continue;
    }
    const statusCheck = validateStatus(item.fields[MAP_MASTER_TODO.status]);
    if (!statusCheck.ok) {
      report.errors.push({ seedId: item.seedId, error: statusCheck.error });
      continue;
    }
    const changes = diffFields(rec.fields, item.fields);
    if (!Object.keys(changes).length) {
      report.skipped.push({
        seedId: item.seedId,
        recordId: rec.id,
        reason: "no field changes",
      });
      continue;
    }
    report.toUpdate.push({
      type: "master-todo",
      seedId: item.seedId,
      recordId: rec.id,
      task: rec.fields?.[MAP_MASTER_TODO.task],
      changes,
      fields: item.fields,
    });
    pending.push({ id: rec.id, fields: item.fields });
  }

  for (const item of PHASE_ROLLUP_SYNC_UPDATES) {
    const rec = byId.get(item.recordId);
    if (!rec) {
      report.skipped.push({ recordId: item.recordId, reason: "phase rollup not found" });
      continue;
    }
    const statusCheck = validateStatus(item.fields[MAP_MASTER_TODO.status]);
    if (!statusCheck.ok) {
      report.errors.push({ recordId: item.recordId, error: statusCheck.error });
      continue;
    }
    const changes = diffFields(rec.fields, item.fields);
    if (!Object.keys(changes).length) {
      report.skipped.push({
        recordId: item.recordId,
        phase: rec.fields?.Phase,
        reason: "no field changes",
      });
      continue;
    }
    report.toUpdate.push({
      type: "phase-rollup",
      recordId: item.recordId,
      phase: rec.fields?.Phase,
      changes,
      fields: item.fields,
    });
    pending.push({ id: item.recordId, fields: item.fields });
  }

  if (!DRY_RUN && pending.length) {
    const base = new Airtable({ apiKey: token }).base(baseId);
    for (let i = 0; i < pending.length; i += 10) {
      const batch = pending.slice(i, i + 10);
      try {
        const updated = await base(TABLE_ID).update(batch, { typecast: true });
        report.updated.push(
          ...updated.map((r) => ({
            id: r.id,
            task: r.fields?.[MAP_MASTER_TODO.task] || r.fields?.Phase,
            status: r.fields?.[MAP_MASTER_TODO.status],
          }))
        );
      } catch (err) {
        report.errors.push({ action: "update", message: err.message || String(err) });
      }
    }
  }

  fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
  fs.writeFileSync(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`);

  console.log(`\nFounder Project Plan sync (${report.mode})`);
  console.log(`Table: ${TABLE_ID}`);
  console.log(`Proposed updates: ${report.toUpdate.length}`);
  console.log(`Skipped: ${report.skipped.length}`);
  if (!DRY_RUN) {
    console.log(`Updated: ${report.updated.length}`);
    console.log(`Errors: ${report.errors.length}`);
  }
  console.log(`Report: ${REPORT_PATH}`);

  if (report.errors.length) process.exitCode = 1;
}

main().catch((err) => {
  console.error("[sync-founder-project-plan-state]", err.message || err);
  process.exit(1);
});
