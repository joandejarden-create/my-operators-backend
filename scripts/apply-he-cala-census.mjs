/**
 * Apply Hotel Equities (CALA) portfolio creates/updates to Hotel Census.
 *
 * Usage:
 *   node scripts/apply-he-cala-census.mjs --dry-run
 *   node scripts/apply-he-cala-census.mjs
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import {
  HE_CALA_CENSUS_PLAN,
  HOTEL_CENSUS_TABLE,
  MAP_HE_CALA_CENSUS,
  findDuplicateCandidates,
  rowToAirtableFields,
  validateHeCalaRow,
} from "../lib/hotel-census/he-cala-census-apply.js";
import { CENSUS_FIELDS } from "../lib/hotel-census/fields.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORT_DIR = join(__dirname, "..", "reports");
const LOG_CSV = join(REPORT_DIR, "he-cala-census-apply-log.csv");
const SUMMARY_JSON = join(REPORT_DIR, "he-cala-census-apply-summary.json");

const BATCH_SIZE = 10;

function parseArgs() {
  return { dryRun: process.argv.includes("--dry-run") };
}

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

async function main() {
  const { dryRun } = parseArgs();
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId) {
    throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");
  }

  console.log(`=== HE CALA → Hotel Census (${dryRun ? "DRY RUN" : "LIVE"}) ===\n`);

  const validationFailures = [];
  for (const row of HE_CALA_CENSUS_PLAN) {
    const v = validateHeCalaRow(row);
    if (!v.pass) validationFailures.push({ portfolioKey: row.portfolioKey, errors: v.errors });
  }
  if (validationFailures.length) {
    console.error("Validation failed:", validationFailures);
    process.exit(1);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const selectFields = [
    CENSUS_FIELDS.name,
    CENSUS_FIELDS.country,
    CENSUS_FIELDS.managementCompany,
    CENSUS_FIELDS.status,
  ];

  console.log("Loading Hotel Census for duplicate check...");
  const censusRecords = await base(HOTEL_CENSUS_TABLE)
    .select({ fields: selectFields, pageSize: 100 })
    .all();

  const logRows = [];
  const creates = [];
  const updates = [];
  const skipped = [];

  for (const row of HE_CALA_CENSUS_PLAN) {
    const fields = rowToAirtableFields(row);
    const preview = JSON.stringify(fields);

    if (row.action === "update") {
      updates.push({ id: row.recordId, fields, row });
      logRows.push({
        action: "update",
        portfolioKey: row.portfolioKey,
        recordId: row.recordId,
        name: row.name,
        status: "queued",
        detail: preview,
      });
      continue;
    }

    const dupes = findDuplicateCandidates(censusRecords, row);
    if (dupes.length) {
      skipped.push({ row, dupes: dupes.map((d) => ({ id: d.id, name: d.fields?.name })) });
      logRows.push({
        action: "skip_duplicate",
        portfolioKey: row.portfolioKey,
        recordId: dupes[0].id,
        name: row.name,
        status: "skipped",
        detail: `existing: ${dupes.map((d) => d.fields?.name).join(" | ")}`,
      });
      continue;
    }

    creates.push({ fields, row });
    logRows.push({
      action: "create",
      portfolioKey: row.portfolioKey,
      recordId: "",
      name: row.name,
      status: "queued",
      detail: preview,
    });
  }

  console.log(`Plan: ${updates.length} update(s), ${creates.length} create(s), ${skipped.length} skipped\n`);
  if (skipped.length) {
    for (const s of skipped) {
      console.warn(`  SKIP ${s.row.portfolioKey}: duplicate ${s.dupes.map((d) => d.name).join(", ")}`);
    }
    console.log("");
  }

  let created = 0;
  let updated = 0;
  let errors = 0;

  if (!dryRun) {
    for (const u of updates) {
      try {
        await base(HOTEL_CENSUS_TABLE).update(
          [{ id: u.id, fields: u.fields }],
          { typecast: true }
        );
        updated++;
        const lr = logRows.find(
          (r) => r.portfolioKey === u.row.portfolioKey && r.action === "update"
        );
        if (lr) {
          lr.status = "ok";
          lr.recordId = u.id;
        }
        console.log(`  UPDATED ${u.row.name}`);
      } catch (err) {
        errors++;
        console.error(`  UPDATE FAILED ${u.row.name}:`, err.message);
        const lr = logRows.find(
          (r) => r.portfolioKey === u.row.portfolioKey && r.action === "update"
        );
        if (lr) lr.status = `error: ${err.message}`;
      }
    }

    for (let i = 0; i < creates.length; i += BATCH_SIZE) {
      const chunk = creates.slice(i, i + BATCH_SIZE);
      try {
        const records = await base(HOTEL_CENSUS_TABLE).create(
          chunk.map((c) => ({ fields: c.fields })),
          { typecast: true }
        );
        created += records.length;
        for (let j = 0; j < records.length; j++) {
          const rec = records[j];
          const row = chunk[j].row;
          console.log(`  CREATED ${row.name} → ${rec.id}`);
          const lr = logRows.find(
            (r) => r.portfolioKey === row.portfolioKey && r.action === "create"
          );
          if (lr) {
            lr.status = "ok";
            lr.recordId = rec.id;
          }
          censusRecords.push(rec);
        }
      } catch (err) {
        errors += chunk.length;
        console.error(`  CREATE BATCH FAILED:`, err.message);
        for (const c of chunk) {
          const lr = logRows.find(
            (r) => r.portfolioKey === c.row.portfolioKey && r.action === "create"
          );
          if (lr) lr.status = `error: ${err.message}`;
        }
      }
    }
  } else {
    for (const u of updates) console.log(`  [dry-run] UPDATE ${u.row.name}`, u.fields);
    for (const c of creates) console.log(`  [dry-run] CREATE ${c.row.name}`, c.fields);
    created = creates.length;
    updated = updates.length;
  }

  mkdirSync(REPORT_DIR, { recursive: true });
  const headers = ["action", "portfolioKey", "recordId", "name", "status", "detail"];
  const csv = [
    headers.join(","),
    ...logRows.map((r) => headers.map((h) => csvEscape(r[h])).join(",")),
  ].join("\n");
  writeFileSync(LOG_CSV, `${csv}\n`, "utf8");

  const summary = {
    dryRun,
    table: HOTEL_CENSUS_TABLE,
    mapping: MAP_HE_CALA_CENSUS,
    created,
    updated,
    skipped: skipped.length,
    errors,
    skippedDetail: skipped,
    at: new Date().toISOString(),
  };
  writeFileSync(SUMMARY_JSON, `${JSON.stringify(summary, null, 2)}\n`, "utf8");

  console.log("\nDone.");
  console.log(`  Created: ${created}${dryRun ? " (dry-run)" : ""}`);
  console.log(`  Updated: ${updated}${dryRun ? " (dry-run)" : ""}`);
  console.log(`  Skipped: ${skipped.length}`);
  console.log(`  Errors: ${errors}`);
  console.log(`  Log: ${LOG_CSV}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
