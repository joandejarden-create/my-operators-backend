#!/usr/bin/env node
/**
 * Migrate Engagement & Reporting from Commercial JSON + cards → Operator Setup - Engagement & Reporting.
 *
 *   node scripts/migrate-operator-engagement-to-reporting-table.mjs
 *   node scripts/migrate-operator-engagement-to-reporting-table.mjs --master recWPKu5laVZxsvpn
 *   node scripts/migrate-operator-engagement-to-reporting-table.mjs --apply
 *   node scripts/migrate-operator-engagement-to-reporting-table.mjs --apply --force
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import {
  ENGAGEMENT_REPORTING_TABLE,
  buildEngagementReportingAirtableRowsFromLegacy,
} from "../api/lib/operator-engagement-reporting-map.js";
import {
  NEW_BASE_COMMERCIAL_TABLE,
  NEW_BASE_GOVERNANCE_TABLE,
  NEW_BASE_MASTER_TABLE,
  fetchAllRecordsRest,
} from "../api/lib/operator-setup-new-base-read.js";
import { replaceOperatorEngagementReportingRows } from "../api/lib/operator-setup-new-base-writer.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORTS = path.join(ROOT, "reports");

const APPLY = process.argv.includes("--apply");
const FORCE = process.argv.includes("--force");
const masterArg = process.argv.find((a, i) => process.argv[i - 1] === "--master");

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function masterIdFromLinked(fields) {
  const op = fields && fields.Operator;
  return Array.isArray(op) && op[0] ? String(op[0]) : "";
}

function stamp() {
  return new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
}

function csvEscape(v) {
  const s = String(v == null ? "" : v);
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

async function loadExistingByMaster() {
  let rows = [];
  try {
    rows = await fetchAllRecordsRest(ENGAGEMENT_REPORTING_TABLE);
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    if (/not found|could not find|404|INVALID|UNKNOWN/i.test(msg)) return new Map();
    throw e;
  }
  const byMaster = new Map();
  for (const r of rows) {
    const mid = masterIdFromLinked(r.fields || {});
    if (!mid) continue;
    if (!byMaster.has(mid)) byMaster.set(mid, []);
    byMaster.get(mid).push(r);
  }
  return byMaster;
}

async function main() {
  const [masters, commercialRows, governanceRows, existingByMaster] = await Promise.all([
    fetchAllRecordsRest(NEW_BASE_MASTER_TABLE),
    fetchAllRecordsRest(NEW_BASE_COMMERCIAL_TABLE),
    fetchAllRecordsRest(NEW_BASE_GOVERNANCE_TABLE),
    loadExistingByMaster(),
  ]);

  const masterNameById = new Map();
  for (const m of masters) {
    const f = m.fields || {};
    masterNameById.set(
      m.id,
      nz(f.company_name) || nz(f["Company Name"]) || nz(f.companyName) || m.id
    );
  }

  const govByMaster = new Map();
  for (const g of governanceRows) {
    const mid = masterIdFromLinked(g.fields);
    if (mid) govByMaster.set(mid, g.fields || {});
  }

  let targets = commercialRows.filter((r) => masterIdFromLinked(r.fields));
  if (masterArg) {
    targets = targets.filter((r) => masterIdFromLinked(r.fields) === masterArg);
    if (!targets.length) {
      throw new Error(`No Commercial row linked to Master ${masterArg}`);
    }
  }

  const plan = [];
  let wouldMigrate = 0;
  let wouldSkip = 0;

  for (const commRow of targets) {
    const fields = commRow.fields || {};
    const masterId = masterIdFromLinked(fields);
    const companyName = masterNameById.get(masterId) || masterId;
    const existing = existingByMaster.get(masterId) || [];
    const govFields = govByMaster.get(masterId) || {};
    const built = buildEngagementReportingAirtableRowsFromLegacy(fields, govFields);

    let action = "migrate";
    let reason = "";

    if (!built.rows.length) {
      action = "skip";
      reason = "no_migratable_data";
      wouldSkip++;
    } else if (existing.length && !FORCE) {
      action = "skip";
      reason = `existing_child_rows_${existing.length}`;
      wouldSkip++;
    } else {
      wouldMigrate++;
    }

    plan.push({
      masterId,
      companyName,
      action,
      reason,
      rowCount: built.rows.length,
      sources: built.sources,
      countsBySection: built.countsBySection,
    });
  }

  const ts = stamp();
  if (!fs.existsSync(REPORTS)) fs.mkdirSync(REPORTS, { recursive: true });

  const jsonPath = path.join(REPORTS, `operator-engagement-reporting-migration-${ts}.json`);
  fs.writeFileSync(
    jsonPath,
    JSON.stringify({ generatedAt: new Date().toISOString(), apply: APPLY, force: FORCE, plan }, null, 2)
  );

  const csvPath = path.join(REPORTS, `operator-engagement-reporting-migration-${ts}.csv`);
  const header = ["masterId", "companyName", "action", "reason", "rowCount", "sources"].join(",");
  const lines = plan.map((p) =>
    [
      p.masterId,
      csvEscape(p.companyName),
      p.action,
      p.reason,
      p.rowCount,
      csvEscape((p.sources || []).join(";")),
    ].join(",")
  );
  fs.writeFileSync(csvPath, [header, ...lines].join("\n"));

  console.log("Plan:", jsonPath);
  console.log("CSV:", csvPath);
  console.log(`Would migrate: ${wouldMigrate}, skip: ${wouldSkip}`);

  if (!APPLY) {
    console.log("\nDry run. Re-run with --apply (and --force to replace existing child rows).");
    return;
  }

  let applied = 0;
  let failed = 0;
  for (const entry of plan) {
    if (entry.action !== "migrate") continue;
    const built = buildEngagementReportingAirtableRowsFromLegacy(
      commercialRows.find((r) => masterIdFromLinked(r.fields) === entry.masterId)?.fields || {},
      govByMaster.get(entry.masterId) || {}
    );
    try {
      await replaceOperatorEngagementReportingRows(entry.masterId, built.rows);
      applied++;
      console.log("APPLIED", entry.companyName, entry.rowCount, "rows");
    } catch (e) {
      failed++;
      console.error("FAILED", entry.masterId, e.message || e);
    }
    await new Promise((r) => setTimeout(r, 280));
  }

  console.log(`\nApplied: ${applied}, failed: ${failed}`);
  if (failed) process.exit(1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
