/**
 * Sync Phase Progress Summary table from Founder Project Plan counts.
 * Gives you a simple Airtable grid: Phase | Total | Completed | % Done
 *
 *   node scripts/sync-phase-progress-summary.mjs --dry-run
 *   node scripts/sync-phase-progress-summary.mjs --execute
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
  metaFetch,
} from "../lib/dealality-master-todo/master-todo-airtable-io.js";
import { MASTER_TODO_DEFAULT_TABLE_ID } from "../lib/dealality-master-todo/master-todo-field-map.js";
import { computePhaseProgress } from "../lib/dealality-master-todo/phase-progress-compute.js";
import {
  MAP_PHASE_PROGRESS_SUMMARY,
  PHASE_PROGRESS_SUMMARY_FIELDS,
  PHASE_PROGRESS_SUMMARY_TABLE_ALIASES,
  PHASE_PROGRESS_SUMMARY_TABLE_ID,
  PHASE_PROGRESS_SUMMARY_TABLE_NAME,
  toAirtablePercent,
} from "../lib/dealality-master-todo/phase-progress-summary-config.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const EXECUTE = process.argv.includes("--execute");
const REPORT_PATH = path.resolve(ROOT, "reports/phase-progress-summary-sync-report.json");

const F = MAP_PHASE_PROGRESS_SUMMARY;

function phaseToFields(phaseRow, syncedAt) {
  return {
    [F.phase]: phaseRow.phase,
    [F.totalTasks]: phaseRow.total,
    [F.completed]: phaseRow.completed,
    [F.inProgress]: phaseRow.inProgress,
    [F.notStarted]: phaseRow.notStarted,
    [F.otherStatus]: phaseRow.other,
    [F.percentDone]: toAirtablePercent(phaseRow.percentCompleted),
    [F.lastSynced]: syncedAt,
  };
}

async function ensureSummaryTable(baseId, token) {
  const tables = await fetchAllTables(baseId, token);
  let table = tables.find((t) => t.id === PHASE_PROGRESS_SUMMARY_TABLE_ID);
  if (!table) {
    table = tables.find((t) => PHASE_PROGRESS_SUMMARY_TABLE_ALIASES.includes(t.name));
  }

  if (table) return { tableId: table.id, tableName: table.name, created: false };

  if (!EXECUTE) {
    return { tableId: null, tableName: PHASE_PROGRESS_SUMMARY_TABLE_NAME, created: false, wouldCreate: true };
  }

  const { res, json } = await metaFetch(baseId, token, "/tables", {
    method: "POST",
    body: JSON.stringify({
      name: PHASE_PROGRESS_SUMMARY_TABLE_NAME,
      fields: PHASE_PROGRESS_SUMMARY_FIELDS,
    }),
  });

  if (!res.ok) {
    throw new Error(`Create table failed (${res.status}): ${JSON.stringify(json)}`);
  }

  return { tableId: json.id, tableName: json.name, created: true };
}

async function main() {
  const { token, baseId } = getGtmConfig();
  const syncedAt = new Date().toISOString();
  const fppRecords = await fetchAllRecords(baseId, token, MASTER_TODO_DEFAULT_TABLE_ID);
  const { phases, totals } = computePhaseProgress(fppRecords);

  const tableInfo = await ensureSummaryTable(baseId, token);
  const report = {
    generatedAt: syncedAt,
    mode: EXECUTE ? "execute" : "dry-run",
    baseId,
    sourceTableId: MASTER_TODO_DEFAULT_TABLE_ID,
    summaryTable: tableInfo,
    totals,
    phases,
    toCreate: [],
    toUpdate: [],
    created: [],
    updated: [],
    errors: [],
  };

  const allRows = [
    ...phases.map((p) => ({ ...p, phase: p.phase })),
    {
      phase: "— ALL PHASES —",
      total: totals.total,
      completed: totals.completed,
      inProgress: totals.inProgress,
      notStarted: totals.notStarted,
      other: totals.other,
      percentCompleted: totals.percentCompleted,
    },
  ];

  if (!tableInfo.tableId) {
    report.toCreate = allRows.map((p) => phaseToFields(p, syncedAt));
    fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
    fs.writeFileSync(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`);
    console.log("\nPhase Progress Summary sync (dry-run)");
    console.log(`Would create table: ${PHASE_PROGRESS_SUMMARY_TABLE_NAME}`);
    console.log(`Rows to sync: ${allRows.length}`);
    console.log(`Report: ${REPORT_PATH}`);
    console.log("\nRun: node scripts/sync-phase-progress-summary.mjs --execute");
    return;
  }

  const existing = await fetchAllRecords(baseId, token, tableInfo.tableId);
  const byPhase = new Map(existing.map((r) => [String(r.fields?.[F.phase] || ""), r]));

  const creates = [];
  const updates = [];

  for (const row of allRows) {
    const fields = phaseToFields(row, syncedAt);
    const hit = byPhase.get(row.phase);
    if (!hit) {
      report.toCreate.push({ phase: row.phase, fields });
      if (EXECUTE) creates.push({ fields });
    } else {
      report.toUpdate.push({ id: hit.id, phase: row.phase, fields });
      if (EXECUTE) updates.push({ id: hit.id, fields });
    }
  }

  if (EXECUTE) {
    const base = new Airtable({ apiKey: token }).base(baseId);
    const tableId = tableInfo.tableId;
    for (let i = 0; i < creates.length; i += 10) {
      try {
        const batch = creates.slice(i, i + 10);
        const created = await base(tableId).create(batch, { typecast: true });
        report.created.push(...created.map((r) => ({ id: r.id, phase: r.fields?.[F.phase] })));
      } catch (err) {
        report.errors.push({ action: "create", message: err.message || String(err) });
      }
    }
    for (let i = 0; i < updates.length; i += 10) {
      try {
        const batch = updates.slice(i, i + 10);
        const updated = await base(tableId).update(batch, { typecast: true });
        report.updated.push(...updated.map((r) => ({ id: r.id, phase: r.fields?.[F.phase] })));
      } catch (err) {
        report.errors.push({ action: "update", message: err.message || String(err) });
      }
    }
  }

  fs.mkdirSync(path.dirname(REPORT_PATH), { recursive: true });
  fs.writeFileSync(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`);

  console.log(`\nPhase Progress Summary sync (${report.mode})`);
  console.log(`Table: ${tableInfo.tableName} (${tableInfo.tableId})${tableInfo.created ? " [created]" : ""}`);
  console.log(`Phases: ${phases.length} + 1 totals row`);
  console.log(`Creates: ${report.toCreate.length} | Updates: ${report.toUpdate.length}`);
  if (EXECUTE) {
    console.log(`Written: ${report.created.length} created, ${report.updated.length} updated`);
    console.log(`Errors: ${report.errors.length}`);
  }
  console.log(`Report: ${REPORT_PATH}`);
  console.log("\nOpen the Phase Progress Summary table in Airtable for your dashboard grid.");
  if (report.errors.length) process.exitCode = 1;
}

main().catch((err) => {
  console.error("[sync-phase-progress-summary]", err.message || err);
  process.exit(1);
});
