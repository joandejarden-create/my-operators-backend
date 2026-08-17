#!/usr/bin/env node
/**
 * Migrate Operating Platform from Platform + Governance + Commercial → child table.
 *
 *   node scripts/migrate-operator-operating-to-platform-table.mjs --apply
 *   node scripts/migrate-operator-operating-to-platform-table.mjs --apply --master recWPKu5laVZxsvpn
 *   node scripts/migrate-operator-operating-to-platform-table.mjs --apply --force
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import {
  OPERATING_PLATFORM_TABLE,
  buildOperatingPlatformAirtableRowsFromLegacy,
} from "../api/lib/operator-operating-platform-map.js";
import {
  NEW_BASE_MASTER_TABLE,
  NEW_BASE_PLATFORM_TABLE,
  NEW_BASE_GOVERNANCE_TABLE,
  NEW_BASE_COMMERCIAL_TABLE,
  fetchAllRecordsRest,
} from "../api/lib/operator-setup-new-base-read.js";
import { replaceOperatorOperatingPlatformRows } from "../api/lib/operator-setup-new-base-writer.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORTS = path.join(ROOT, "reports");

const APPLY = process.argv.includes("--apply");
const FORCE = process.argv.includes("--force");
const masterArg = process.argv.find((a, i) => process.argv[i - 1] === "--master");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function masterIdFromLinked(fields) {
  const op = fields?.Operator;
  return Array.isArray(op) && op[0] ? String(op[0]) : "";
}

function stamp() {
  return new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
}

async function loadExistingByMaster() {
  try {
    const rows = await fetchAllRecordsRest(OPERATING_PLATFORM_TABLE);
    const byMaster = new Map();
    for (const r of rows) {
      const mid = masterIdFromLinked(r.fields || {});
      if (!mid) continue;
      if (!byMaster.has(mid)) byMaster.set(mid, []);
      byMaster.get(mid).push(r);
    }
    return byMaster;
  } catch (e) {
    const msg = e?.message || String(e);
    if (/not found|could not find|404|INVALID|UNKNOWN/i.test(msg)) return new Map();
    throw e;
  }
}

async function main() {
  const [masters, platformRows, govRows, commRows, existingByMaster] = await Promise.all([
    fetchAllRecordsRest(NEW_BASE_MASTER_TABLE),
    fetchAllRecordsRest(NEW_BASE_PLATFORM_TABLE),
    fetchAllRecordsRest(NEW_BASE_GOVERNANCE_TABLE),
    fetchAllRecordsRest(NEW_BASE_COMMERCIAL_TABLE),
    loadExistingByMaster(),
  ]);

  const masterNameById = new Map();
  for (const m of masters) {
    const f = m.fields || {};
    masterNameById.set(m.id, nz(f.company_name) || nz(f["Company Name"]) || m.id);
  }

  const govByMaster = new Map();
  for (const g of govRows) {
    const mid = masterIdFromLinked(g.fields);
    if (mid) govByMaster.set(mid, g.fields || {});
  }
  const commByMaster = new Map();
  for (const c of commRows) {
    const mid = masterIdFromLinked(c.fields);
    if (mid) commByMaster.set(mid, c.fields || {});
  }

  let targets = platformRows.filter((r) => masterIdFromLinked(r.fields));
  if (masterArg) {
    targets = targets.filter((r) => masterIdFromLinked(r.fields) === masterArg);
    if (!targets.length) throw new Error(`No Platform row for Master ${masterArg}`);
  }

  const plan = [];
  let wouldMigrate = 0;
  let wouldSkip = 0;

  for (const platRow of targets) {
    const platformFields = platRow.fields || {};
    const masterId = masterIdFromLinked(platformFields);
    const built = buildOperatingPlatformAirtableRowsFromLegacy(
      platformFields,
      govByMaster.get(masterId) || {},
      commByMaster.get(masterId) || {}
    );
    const existing = existingByMaster.get(masterId) || [];
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
      companyName: masterNameById.get(masterId) || masterId,
      action,
      reason,
      rowCount: built.rows.length,
      pillarCounts: Object.fromEntries(
        Object.entries(built.payload?.pillars || {}).map(([k, v]) => [k, (v.items || []).length])
      ),
    });
  }

  const ts = stamp();
  const jsonPath = path.join(REPORTS, `operator-operating-platform-migration-${ts}.json`);
  fs.mkdirSync(REPORTS, { recursive: true });
  fs.writeFileSync(jsonPath, JSON.stringify({ apply: APPLY, force: FORCE, plan }, null, 2));
  console.log("Plan:", jsonPath);
  console.log(`Would migrate: ${wouldMigrate}, skip: ${wouldSkip}`);

  if (!APPLY) {
    console.log("\nDry run. Re-run with --apply");
    return;
  }

  let applied = 0;
  let failed = 0;
  for (const entry of plan) {
    if (entry.action !== "migrate") continue;
    const platRow = targets.find((r) => masterIdFromLinked(r.fields) === entry.masterId);
    const built = buildOperatingPlatformAirtableRowsFromLegacy(
      platRow?.fields || {},
      govByMaster.get(entry.masterId) || {},
      commByMaster.get(entry.masterId) || {}
    );
    try {
      await replaceOperatorOperatingPlatformRows(entry.masterId, built.rows);
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
