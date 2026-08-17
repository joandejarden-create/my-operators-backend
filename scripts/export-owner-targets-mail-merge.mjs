/**
 * Export approved Pilot Target List rows to mail-merge CSV (read-only).
 *
 *   node scripts/export-owner-targets-mail-merge.mjs
 *   node scripts/export-owner-targets-mail-merge.mjs --batch "Pilot Wave 1"
 *   node scripts/export-owner-targets-mail-merge.mjs --status Approved --channel Email
 *   node scripts/export-owner-targets-mail-merge.mjs --output reports/owner-targets-mail-merge.csv
 *   node scripts/export-owner-targets-mail-merge.mjs --dry-run
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  GTM_PILOT_TARGET_LIST_TABLE,
  MAP_PILOT_TARGET_LIST,
} from "../lib/gtm-owner-target/pilot-target-list-field-map.js";
import {
  recordToMailMergeRow,
  rowsToCsv,
} from "../lib/gtm-owner-target/pilot-target-list-outreach.js";
import {
  assertGtmBaseConfigured,
  assertNotProductBase,
  getGtmAirtableBase,
} from "../lib/gtm-owner-target/platform-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function argValue(flag, fallback = "") {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return fallback;
  return process.argv[idx + 1] || fallback;
}

const DRY_RUN = process.argv.includes("--dry-run");
const BATCH = argValue("--batch", "");
const STATUS = argValue("--status", "Approved");
const CHANNEL = argValue("--channel", "Email");
const OUTPUT = argValue("--output", "reports/owner-targets-mail-merge.csv");

const GTM_COMPANIES_TABLE = process.env.AIRTABLE_GTM_COMPANIES_TABLE || "Companies";
const GTM_COMPANY_NAME_FIELD = process.env.AIRTABLE_GTM_COMPANY_NAME_FIELD || "Company";

async function fetchAllRecords(base, tableName, fields) {
  const rows = [];
  await base(tableName)
    .select({ fields })
    .eachPage((page, next) => {
      rows.push(...page);
      next();
    });
  return rows;
}

async function buildCompanyNameMap(base, linkedCompanyIds) {
  const ids = [...new Set(linkedCompanyIds.filter(Boolean))];
  const map = new Map();
  if (!ids.length) return map;

  const chunkSize = 50;
  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize);
    const formula = `OR(${chunk.map((id) => `RECORD_ID()='${id}'`).join(",")})`;
    const records = await base(GTM_COMPANIES_TABLE)
      .select({ filterByFormula: formula, fields: [GTM_COMPANY_NAME_FIELD] })
      .all();
    for (const rec of records) {
      map.set(rec.id, String(rec.get(GTM_COMPANY_NAME_FIELD) || "").trim());
    }
  }
  return map;
}

async function main() {
  const { baseId } = assertGtmBaseConfigured();
  assertNotProductBase(baseId);
  const base = getGtmAirtableBase();

  const fields = [
    MAP_PILOT_TARGET_LIST.name,
    MAP_PILOT_TARGET_LIST.company,
    MAP_PILOT_TARGET_LIST.role,
    MAP_PILOT_TARGET_LIST.pilotRegion,
    MAP_PILOT_TARGET_LIST.email,
    MAP_PILOT_TARGET_LIST.linkedInUrl,
    MAP_PILOT_TARGET_LIST.emailSubject,
    MAP_PILOT_TARGET_LIST.finalApprovedEmail,
    MAP_PILOT_TARGET_LIST.outreachStatus,
    MAP_PILOT_TARGET_LIST.readyForMailMerge,
    MAP_PILOT_TARGET_LIST.doNotContact,
    MAP_PILOT_TARGET_LIST.sendChannel,
    MAP_PILOT_TARGET_LIST.mailMergeBatch,
  ];

  const records = await fetchAllRecords(base, GTM_PILOT_TARGET_LIST_TABLE, fields);
  const linkedCompanyIds = records.flatMap((r) => {
    const company = r.get(MAP_PILOT_TARGET_LIST.company);
    return Array.isArray(company) ? company : [];
  });
  const companyNameById = await buildCompanyNameMap(base, linkedCompanyIds);

  const exportRows = [];
  const skipped = [];
  const warnings = [];

  for (const rec of records) {
    const result = recordToMailMergeRow(rec.id, rec.fields, companyNameById, {
      batch: BATCH,
      status: STATUS,
      channel: CHANNEL,
    });
    if (result.skip) {
      skipped.push({
        recordId: rec.id,
        name: rec.get(MAP_PILOT_TARGET_LIST.name) || "",
        reason: result.reason,
        warnings: result.warnings,
      });
      if (result.reason === "incomplete" && result.warnings?.length) {
        warnings.push({
          recordId: rec.id,
          name: result.name || rec.get(MAP_PILOT_TARGET_LIST.name) || "",
          warnings: result.warnings,
        });
      }
      continue;
    }
    exportRows.push(result.row);
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "dry-run" : "export",
    baseId,
    tableName: GTM_PILOT_TARGET_LIST_TABLE,
    filters: { status: STATUS, channel: CHANNEL, batch: BATCH || null },
    totalRecords: records.length,
    exportedCount: exportRows.length,
    skippedCount: skipped.length,
    warnings,
    skippedSample: skipped.slice(0, 25),
    outputPath: DRY_RUN ? null : path.resolve(ROOT, OUTPUT),
  };

  console.log(
    `Pilot Target List mail-merge: ${exportRows.length} exportable / ${records.length} total (status=${STATUS}, channel=${CHANNEL}${BATCH ? `, batch=${BATCH}` : ""})`
  );

  if (warnings.length) {
    console.log("\nWarnings (incomplete approved-ready rows):");
    for (const w of warnings) {
      console.log(`  ${w.name || w.recordId}: ${w.warnings.join(", ")}`);
    }
  }

  if (!DRY_RUN && exportRows.length) {
    const outPath = path.resolve(ROOT, OUTPUT);
    fs.mkdirSync(path.dirname(outPath), { recursive: true });
    fs.writeFileSync(outPath, rowsToCsv(exportRows), "utf8");
    console.log("Wrote CSV:", outPath);
  } else if (DRY_RUN) {
    console.log("Dry-run — no CSV written.");
    if (exportRows.length) {
      console.log("Sample export row:", JSON.stringify(exportRows[0], null, 2));
    }
  } else {
    console.log("No rows matched export criteria — CSV not written.");
  }

  const reportPath = path.join(ROOT, "reports", "owner-targets-mail-merge-export.json");
  fs.mkdirSync(path.dirname(reportPath), { recursive: true });
  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
  console.log("Wrote", reportPath);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
