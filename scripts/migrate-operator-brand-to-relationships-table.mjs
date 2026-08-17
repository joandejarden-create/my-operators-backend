#!/usr/bin/env node
/**
 * Migrate Brand & Relationships from Profile JSON → child table.
 *
 *   node scripts/migrate-operator-brand-to-relationships-table.mjs --apply
 *   node scripts/migrate-operator-brand-to-relationships-table.mjs --apply --master recWPKu5laVZxsvpn
 *   node scripts/migrate-operator-brand-to-relationships-table.mjs --apply --force
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import {
  BRAND_RELATIONSHIPS_TABLE,
  buildBrandRelationshipsAirtableRowsFromLegacy,
} from "../api/lib/operator-brand-relationships-map.js";
import {
  NEW_BASE_MASTER_TABLE,
  NEW_BASE_PROFILE_TABLE,
  fetchAllRecordsRest,
} from "../api/lib/operator-setup-new-base-read.js";
import { replaceOperatorBrandRelationshipsRows } from "../api/lib/operator-setup-new-base-writer.js";

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
    const rows = await fetchAllRecordsRest(BRAND_RELATIONSHIPS_TABLE);
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
  const [masters, profileRows, existingByMaster] = await Promise.all([
    fetchAllRecordsRest(NEW_BASE_MASTER_TABLE),
    fetchAllRecordsRest(NEW_BASE_PROFILE_TABLE),
    loadExistingByMaster(),
  ]);

  const masterNameById = new Map();
  for (const m of masters) {
    const f = m.fields || {};
    masterNameById.set(m.id, nz(f.company_name) || nz(f["Company Name"]) || m.id);
  }

  const profileByMaster = new Map();
  for (const row of profileRows) {
    const mid = masterIdFromLinked(row.fields || {});
    if (mid) profileByMaster.set(mid, row);
  }

  const plan = [];
  const masterFilter = nz(masterArg);

  for (const m of masters) {
    if (masterFilter && m.id !== masterFilter) continue;
    const profile = profileByMaster.get(m.id);
    if (!profile) continue;

    const existing = existingByMaster.get(m.id) || [];
    if (existing.length && !FORCE) {
      plan.push({
        masterId: m.id,
        companyName: masterNameById.get(m.id),
        action: "skip",
        reason: "child_rows_exist",
        existingCount: existing.length,
      });
      continue;
    }

    const built = buildBrandRelationshipsAirtableRowsFromLegacy(profile.fields || {});
    if (!built.rows.length) {
      plan.push({
        masterId: m.id,
        companyName: masterNameById.get(m.id),
        action: "skip",
        reason: "no_legacy_brand_json",
      });
      continue;
    }

    plan.push({
      masterId: m.id,
      companyName: masterNameById.get(m.id),
      action: APPLY ? "migrated" : "would_migrate",
      rowCount: built.rows.length,
      sources: built.sources,
      countsBySection: built.countsBySection,
    });

    if (APPLY) {
      await replaceOperatorBrandRelationshipsRows(m.id, built.rows);
    }
  }

  const reportPath = path.join(
    REPORTS,
    `migrate-operator-brand-relationships-${stamp()}.json`
  );
  fs.mkdirSync(REPORTS, { recursive: true });
  fs.writeFileSync(
    reportPath,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        mode: APPLY ? "apply" : "dry-run",
        force: FORCE,
        masterFilter: masterFilter || null,
        plan,
      },
      null,
      2
    )
  );

  console.log(
    JSON.stringify(
      {
        reportPath,
        migrated: plan.filter((p) => p.action === "migrated").length,
        skipped: plan.filter((p) => p.action === "skip").length,
        wouldMigrate: plan.filter((p) => p.action === "would_migrate").length,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
