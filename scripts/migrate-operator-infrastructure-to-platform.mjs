#!/usr/bin/env node
/**
 * Migrate Infrastructure & Data from Governance legacy fields → Operator Setup - Infrastructure Platform.
 *
 * Default: dry-run report (JSON + CSV) for every operator with migratable Governance data.
 *
 *   node scripts/migrate-operator-infrastructure-to-platform.mjs
 *   node scripts/migrate-operator-infrastructure-to-platform.mjs --master recWPKu5laVZxsvpn
 *   node scripts/migrate-operator-infrastructure-to-platform.mjs --apply
 *   node scripts/migrate-operator-infrastructure-to-platform.mjs --apply --force
 *
 * Prerequisites:
 *   node scripts/ensure-operator-infrastructure-platform-table.mjs --apply
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { randomUUID } from "crypto";
import "../load-env.js";
import {
  INFRASTRUCTURE_PLATFORM_TABLE,
  buildInfrastructurePlatformAirtableRowsFromLegacyGovernance,
} from "../api/lib/operator-infrastructure-platform-map.js";
import {
  NEW_BASE_GOVERNANCE_TABLE,
  NEW_BASE_MASTER_TABLE,
  fetchAllRecordsRest,
} from "../api/lib/operator-setup-new-base-read.js";
import { replaceOperatorInfrastructurePlatformRows } from "../api/lib/operator-setup-new-base-writer.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORTS = path.join(ROOT, "reports");

const APPLY = process.argv.includes("--apply");
const FORCE = process.argv.includes("--force");
const masterArg = process.argv.find((a, i) => process.argv[i - 1] === "--master");

const DEFAULT_HE_CALA = "recWPKu5laVZxsvpn";

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

async function loadExistingInfraPlatformByMaster() {
  let rows = [];
  try {
    rows = await fetchAllRecordsRest(INFRASTRUCTURE_PLATFORM_TABLE);
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    if (/not found|could not find|404|INVALID|UNKNOWN/i.test(msg)) {
      return new Map();
    }
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
  const [masters, governanceRows, existingByMaster] = await Promise.all([
    fetchAllRecordsRest(NEW_BASE_MASTER_TABLE),
    fetchAllRecordsRest(NEW_BASE_GOVERNANCE_TABLE),
    loadExistingInfraPlatformByMaster(),
  ]);

  const masterNameById = new Map();
  for (const m of masters) {
    const f = m.fields || {};
    masterNameById.set(
      m.id,
      nz(f.company_name) || nz(f["Company Name"]) || nz(f.companyName) || m.id
    );
  }

  let targets = governanceRows.filter((r) => masterIdFromLinked(r.fields));
  if (masterArg) {
    targets = targets.filter((r) => masterIdFromLinked(r.fields) === masterArg);
    if (!targets.length) {
      throw new Error(`No Governance row linked to Master ${masterArg}`);
    }
  }

  const plan = [];
  let wouldCreate = 0;
  let wouldSkip = 0;

  for (const govRow of targets) {
    const fields = govRow.fields || {};
    const masterId = masterIdFromLinked(fields);
    const companyName = masterNameById.get(masterId) || masterId;
    const existing = existingByMaster.get(masterId) || [];
    const built = buildInfrastructurePlatformAirtableRowsFromLegacyGovernance(fields);

    let action = "migrate";
    let reason = "";

    if (!built.rows.length) {
      action = "skip_no_data";
      reason = "No migratable infra fields on Governance row";
      wouldSkip++;
    } else if (existing.length && !FORCE) {
      action = "skip_existing";
      reason = `${existing.length} Infrastructure Platform row(s) already exist (use --force)`;
      wouldSkip++;
    } else if (existing.length && FORCE) {
      action = "migrate_replace";
      reason = `--force replaces ${existing.length} existing row(s)`;
      wouldCreate++;
    } else {
      action = "would_migrate";
      wouldCreate++;
    }

    const entry = {
      masterId,
      companyName,
      governanceRecordId: govRow.id,
      existingInfraPlatformRowCount: existing.length,
      proposedRowCount: built.rows.length,
      countsBySection: built.countsBySection,
      legacySources: built.sources,
      action,
      reason,
      sampleRows: built.rows.slice(0, 5).map((r) => ({
        section: r.section,
        row_key: r.row_key,
        title: r.title,
        extra: r.extra ? String(r.extra).slice(0, 80) : "",
      })),
    };

    if (APPLY && (action === "would_migrate" || action === "migrate_replace")) {
      try {
        const res = await replaceOperatorInfrastructurePlatformRows(
          masterId,
          built.rows,
          randomUUID()
        );
        entry.applyResult = res;
        entry.action = "applied";
      } catch (e) {
        entry.action = "apply_failed";
        entry.error = e && e.message ? e.message : String(e);
      }
    }

    plan.push(entry);
  }

  const heCala =
    plan.find((p) => p.masterId === DEFAULT_HE_CALA) ||
    plan.find((p) => /hotel equities.*cala/i.test(p.companyName || ""));

  const summary = {
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "apply" : "dry-run",
    table: INFRASTRUCTURE_PLATFORM_TABLE,
    masterFilter: masterArg || null,
    operatorsWithGovernance: targets.length,
    wouldMigrateOrApplied: wouldCreate,
    skipped: wouldSkip,
    heCala: heCala || null,
    byAction: plan.reduce((acc, p) => {
      acc[p.action] = (acc[p.action] || 0) + 1;
      return acc;
    }, {}),
    plan,
  };

  if (!fs.existsSync(REPORTS)) fs.mkdirSync(REPORTS, { recursive: true });
  const jsonPath = path.join(REPORTS, `operator-infrastructure-platform-migration-${stamp()}.json`);
  fs.writeFileSync(jsonPath, JSON.stringify(summary, null, 2));

  const csvHeader =
    "masterId,companyName,governanceRecordId,existingRows,proposedRows,action,reason,legacySources";
  const csvLines = plan.map((p) =>
    [
      csvEscape(p.masterId),
      csvEscape(p.companyName),
      csvEscape(p.governanceRecordId),
      p.existingInfraPlatformRowCount,
      p.proposedRowCount,
      csvEscape(p.action),
      csvEscape(p.reason),
      csvEscape((p.legacySources || []).join("; ")),
    ].join(",")
  );
  const csvPath = path.join(
    REPORTS,
    `operator-infrastructure-platform-migration-${stamp()}.csv`
  );
  fs.writeFileSync(csvPath, [csvHeader, ...csvLines].join("\n"));

  console.log(JSON.stringify(summary.byAction, null, 2));
  console.log("\nOperators:", targets.length, "| migrate:", wouldCreate, "| skipped:", wouldSkip);
  if (heCala) {
    console.log("\nHE CALA", DEFAULT_HE_CALA);
    console.log(
      "  action:",
      heCala.action,
      "| proposed rows:",
      heCala.proposedRowCount,
      "| sections:",
      JSON.stringify(heCala.countsBySection)
    );
    console.log("  sources:", (heCala.legacySources || []).join(", "));
  } else if (!masterArg) {
    console.log("\nHE CALA not in plan (no Governance row or filter excluded).");
  }
  console.log("\nWrote", jsonPath);
  console.log("Wrote", csvPath);

  if (!APPLY) {
    console.log("\nDry run. Re-run with --apply after ensure script --apply.");
  }

  const failed = plan.filter((p) => p.action === "apply_failed");
  if (failed.length) process.exit(1);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
