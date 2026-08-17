/**
 * Assign Property Type for all Hotel Equities (CALA) census rows.
 *
 * Usage:
 *   node scripts/apply-he-cala-census-property-type.mjs --dry-run
 *   node scripts/apply-he-cala-census-property-type.mjs
 *   node scripts/apply-he-cala-census-property-type.mjs --force
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/he-cala-census-apply.js";
import {
  HE_CALA_PROPERTY_TYPE_PLAN,
  HE_MGMT,
  MAP_HE_CALA_PROPERTY_TYPE,
  propertyTypeToAirtableFields,
  validatePropertyTypeRow,
} from "../lib/hotel-census/he-cala-census-property-type.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORT = join(__dirname, "..", "reports", "he-cala-census-property-type-log.csv");

function parseArgs() {
  return {
    dryRun: process.argv.includes("--dry-run"),
    force: process.argv.includes("--force"),
  };
}

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

async function main() {
  const { dryRun, force } = parseArgs();
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");

  console.log(`=== HE CALA Property Type (${dryRun ? "DRY RUN" : "LIVE"}) ===\n`);

  const validationFailures = [];
  for (const row of HE_CALA_PROPERTY_TYPE_PLAN) {
    const v = validatePropertyTypeRow(row);
    if (!v.pass) validationFailures.push({ portfolioKey: row.portfolioKey, errors: v.errors });
  }
  if (validationFailures.length) {
    console.error("Validation failed:", validationFailures);
    process.exit(1);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const F = MAP_HE_CALA_PROPERTY_TYPE;
  const logRows = [];
  let updated = 0;
  let skipped = 0;
  let errors = 0;

  for (const row of HE_CALA_PROPERTY_TYPE_PLAN) {
    let rec;
    try {
      rec = await base(HOTEL_CENSUS_TABLE).find(row.recordId);
    } catch (err) {
      errors++;
      console.error(`  ERROR ${row.name}: ${err.message}`);
      logRows.push({
        portfolioKey: row.portfolioKey,
        recordId: row.recordId,
        name: row.name,
        status: "error",
        previousPropertyType: "",
        newPropertyType: row.propertyType,
        detail: err.message,
      });
      continue;
    }

    const f = rec.fields || {};
    const mgmt = f[F.mgmt];
    const current = String(f[F.propertyType] || "").trim();
    const recName = f[F.name] || row.name;

    if (mgmt !== HE_MGMT) {
      skipped++;
      console.warn(`  SKIP ${recName}: mgmt="${mgmt ?? ""}"`);
      logRows.push({
        portfolioKey: row.portfolioKey,
        recordId: row.recordId,
        name: recName,
        status: "skipped_wrong_mgmt",
        previousPropertyType: current,
        newPropertyType: row.propertyType,
        detail: row.rationale,
      });
      continue;
    }

    if (current && current !== row.propertyType && !force) {
      skipped++;
      console.warn(`  SKIP ${recName}: already "${current}" (use --force to overwrite)`);
      logRows.push({
        portfolioKey: row.portfolioKey,
        recordId: row.recordId,
        name: recName,
        status: "skipped_has_value",
        previousPropertyType: current,
        newPropertyType: row.propertyType,
        detail: row.rationale,
      });
      continue;
    }

    if (current === row.propertyType) {
      skipped++;
      console.log(`  SKIP ${recName}: already ${row.propertyType}`);
      logRows.push({
        portfolioKey: row.portfolioKey,
        recordId: row.recordId,
        name: recName,
        status: "skipped_unchanged",
        previousPropertyType: current,
        newPropertyType: row.propertyType,
        detail: row.rationale,
      });
      continue;
    }

    const fields = propertyTypeToAirtableFields(row);

    if (dryRun) {
      console.log(
        `  [dry-run] ${recName}: ${current || "(blank)"} → ${row.propertyType}`
      );
      updated++;
      logRows.push({
        portfolioKey: row.portfolioKey,
        recordId: row.recordId,
        name: recName,
        status: "dry_run",
        previousPropertyType: current,
        newPropertyType: row.propertyType,
        detail: row.rationale,
      });
      continue;
    }

    try {
      await base(HOTEL_CENSUS_TABLE).update([{ id: row.recordId, fields }], {
        typecast: true,
      });
      updated++;
      console.log(`  UPDATED ${recName}: ${current || "(blank)"} → ${row.propertyType}`);
      logRows.push({
        portfolioKey: row.portfolioKey,
        recordId: row.recordId,
        name: recName,
        status: "ok",
        previousPropertyType: current,
        newPropertyType: row.propertyType,
        detail: row.rationale,
      });
    } catch (err) {
      errors++;
      console.error(`  FAILED ${recName}:`, err.message);
      logRows.push({
        portfolioKey: row.portfolioKey,
        recordId: row.recordId,
        name: recName,
        status: `error: ${err.message}`,
        previousPropertyType: current,
        newPropertyType: row.propertyType,
        detail: row.rationale,
      });
    }
  }

  mkdirSync(dirname(REPORT), { recursive: true });
  const headers = [
    "portfolioKey",
    "recordId",
    "name",
    "status",
    "previousPropertyType",
    "newPropertyType",
    "detail",
  ];
  writeFileSync(
    REPORT,
    `${headers.join(",")}\n${logRows.map((r) => headers.map((h) => csvEscape(r[h])).join(",")).join("\n")}\n`,
    "utf8"
  );

  console.log("\nDone.");
  console.log(`  Updated: ${updated}${dryRun ? " (dry-run)" : ""}`);
  console.log(`  Skipped: ${skipped}`);
  console.log(`  Errors: ${errors}`);
  console.log(`  Plan rows: ${HE_CALA_PROPERTY_TYPE_PLAN.length}`);
  console.log(`  Log: ${REPORT}`);

  if (errors) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
