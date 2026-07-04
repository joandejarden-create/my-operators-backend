#!/usr/bin/env node
/**
 * Phase 5C — Backfill Operator Setup alignment fields (live options only).
 *
 *   node scripts/backfill-operator-setup-alignment-fields.mjs --active-operators
 *   node scripts/backfill-operator-setup-alignment-fields.mjs --operator-id recq3NiRxOerg4kZU --apply
 *   node scripts/backfill-operator-setup-alignment-fields.mjs --active-operators --apply --overwrite
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getLiveOperatorAlignmentOptions } from "../lib/operator-alignment-airtable-options-loader.js";
import {
  loadActiveOperatorBackfillPlans,
  validateOperatorProposal,
} from "../lib/operator-alignment-operator-backfill-plans.js";
import {
  OPERATOR_FIELD_TO_TABLE_KEY,
  OPERATOR_TABLE_NAMES,
} from "../lib/operator-alignment-operator-field-map.js";
import { loadActiveOperatorCandidatesForAlignment } from "../lib/operator-alignment-company-utils.js";
import {
  NEW_BASE_MASTER_TABLE,
  NEW_BASE_PROFILE_TABLE,
  NEW_BASE_PLATFORM_TABLE,
  NEW_BASE_COMMERCIAL_TABLE,
  NEW_BASE_GOVERNANCE_TABLE,
  fetchRecordsLinkedToMaster,
} from "../api/lib/operator-setup-new-base-read.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const args = process.argv.slice(2);
const APPLY = args.includes("--apply");
const OVERWRITE = args.includes("--overwrite");
const ACTIVE = args.includes("--active-operators");
const operatorIdArg = (() => {
  const i = args.indexOf("--operator-id");
  return i >= 0 ? args[i + 1] : null;
})();

function isEmpty(v) {
  if (v == null) return true;
  if (Array.isArray(v)) return v.length === 0;
  return String(v).trim() === "";
}

function enc(t) {
  return encodeURIComponent(String(t));
}

async function patchRecord(tableName, recordId, fields) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const url = `https://api.airtable.com/v0/${baseId}/${enc(tableName)}/${enc(recordId)}`;
  const r = await fetch(url, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields }),
  });
  const j = await r.json();
  if (!r.ok) throw new Error(j.error?.message || `patch ${tableName} ${r.status}`);
  return j;
}

async function getLinkedRecordId(masterId, tableName) {
  const rows = await fetchRecordsLinkedToMaster(tableName, masterId);
  return rows[0]?.id || null;
}

const DATE_FIELDS = new Set(["Last Updated Date"]);

function planToPatches(plan, liveIndex) {
  const patches = { master: {}, profile: {}, platform: {}, commercial: {}, governance: {} };
  const validation = [];

  for (const [fieldName, prop] of Object.entries(plan.fields || {})) {
    let normalized = prop.value;
    let ok = true;
    let warnings = [];
    if (DATE_FIELDS.has(fieldName)) {
      normalized = prop.value;
    } else {
      const v = validateOperatorProposal(fieldName, prop.value, liveIndex);
      ok = v.ok;
      warnings = v.warnings || [];
      normalized = v.value;
      if (!ok) throw new Error(`Invalid live option for ${fieldName}: ${warnings.join("; ")}`);
    }
    validation.push({ field: fieldName, ok, warnings, proposed: prop.value, normalized });
    const tableKey = OPERATOR_FIELD_TO_TABLE_KEY[fieldName];
    if (!tableKey) continue;
    patches[tableKey][fieldName] = normalized;
  }
  return { patches, validation };
}

async function processOperator(candidate, plan, report, liveIndex) {
  const masterId = candidate.operatorId;
  const [profileId, platformId, commercialId, governanceId] = await Promise.all([
    getLinkedRecordId(masterId, NEW_BASE_PROFILE_TABLE),
    getLinkedRecordId(masterId, NEW_BASE_PLATFORM_TABLE),
    getLinkedRecordId(masterId, NEW_BASE_COMMERCIAL_TABLE),
    getLinkedRecordId(masterId, NEW_BASE_GOVERNANCE_TABLE),
  ]);

  const before = {
    master: candidate.master?.fields || {},
    profile: candidate.profile?.fields || {},
    platform: candidate.platform?.fields || {},
    commercial: candidate.commercial?.fields || {},
    governance: candidate.governance?.fields || {},
  };

  const { patches, validation } = planToPatches(plan, liveIndex);
  const changes = [];
  const tableIds = {
    master: masterId,
    profile: profileId,
    platform: platformId,
    commercial: commercialId,
    governance: governanceId,
  };

  for (const [tableKey, fields] of Object.entries(patches)) {
    const recordId = tableIds[tableKey];
    if (!recordId) {
      for (const [field, after] of Object.entries(fields)) {
        changes.push({ table: tableKey, field, action: "error_no_linked_row", before: null, after });
      }
      continue;
    }
    const tableName = OPERATOR_TABLE_NAMES[tableKey];
    const prior = before[tableKey] || {};
    for (const [field, after] of Object.entries(fields)) {
      const prev = prior[field];
      if (!OVERWRITE && !isEmpty(prev)) {
        changes.push({ table: tableKey, field, action: "skip_existing", before: prev, after });
        continue;
      }
      changes.push({
        table: tableKey,
        field,
        action: isEmpty(prev) ? "set" : "overwrite",
        before: prev,
        after,
      });
    }
  }

  const row = {
    operatorId: masterId,
    companyName: plan.companyName,
    status: APPLY ? "applied" : "dry_run",
    notes: plan.notes,
    validation,
    changes,
    linkedRows: { profileId, platformId, commercialId, governanceId },
  };
  report.rows.push(row);

  if (!APPLY) return;

  for (const [tableKey, fields] of Object.entries(patches)) {
    const recordId = tableIds[tableKey];
    if (!recordId) continue;
    const patch = {};
    for (const c of changes) {
      if (c.table !== tableKey || c.action === "skip_existing" || c.action === "error_no_linked_row") continue;
      patch[c.field] = c.after;
    }
    if (Object.keys(patch).length) {
      await patchRecord(OPERATOR_TABLE_NAMES[tableKey], recordId, patch);
    }
  }
}

async function main() {
  if (!process.env.AIRTABLE_BASE_ID || !process.env.AIRTABLE_API_KEY) {
    throw new Error("AIRTABLE credentials required");
  }
  if (!ACTIVE && !operatorIdArg) {
    console.error("Use --active-operators or --operator-id <rec...>");
    process.exit(1);
  }

  const liveIndex = await getLiveOperatorAlignmentOptions({ refresh: true });
  const { candidates } = await loadActiveOperatorCandidatesForAlignment();
  const plans = loadActiveOperatorBackfillPlans(liveIndex, candidates);

  const selected = operatorIdArg
    ? candidates.filter((c) => c.operatorId === operatorIdArg)
    : candidates;

  if (!selected.length) throw new Error("No matching active operators");

  const report = {
    mode: APPLY ? "apply" : "dry_run",
    overwrite: OVERWRITE,
    at: new Date().toISOString(),
    operatorCount: selected.length,
    rows: [],
  };

  for (const c of selected) {
    const plan = plans.get(c.operatorId);
    if (!plan) {
      report.rows.push({ operatorId: c.operatorId, status: "error", error: "No plan" });
      continue;
    }
    console.log("\n---", c.companyName, c.operatorId, "---");
    await processOperator(c, plan, report, liveIndex);
    const last = report.rows[report.rows.length - 1];
    for (const ch of last.changes || []) {
      console.log(`  ${ch.action}\t${ch.table}\t${ch.field}`);
    }
    if (last.notes?.length) console.log("  notes:", last.notes.join("; "));
  }

  const stamp = new Date().toISOString().slice(0, 19).replace(/:/g, "");
  const jsonPath = path.join(ROOT, "reports", `operator-setup-alignment-backfill-${stamp}.json`);
  const csvPath = path.join(ROOT, "reports", `operator-setup-alignment-backfill-${stamp}.csv`);
  fs.mkdirSync(path.join(ROOT, "reports"), { recursive: true });
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const csvRows = [];
  for (const r of report.rows) {
    for (const ch of r.changes || []) {
      csvRows.push({
        operatorId: r.operatorId,
        company: r.companyName,
        table: ch.table,
        field: ch.field,
        action: ch.action,
        before: JSON.stringify(ch.before),
        after: JSON.stringify(ch.after),
      });
    }
  }
  if (csvRows.length) {
    const headers = Object.keys(csvRows[0]);
    fs.writeFileSync(
      csvPath,
      [headers.join(","), ...csvRows.map((row) => headers.map((h) => `"${String(row[h] || "").replace(/"/g, '""')}"`).join(","))].join("\n")
    );
  }

  console.log("\nReport:", jsonPath);
  if (csvRows.length) console.log("CSV:", csvPath);
  if (!APPLY) console.log("Dry run only. Re-run with --apply to write.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
