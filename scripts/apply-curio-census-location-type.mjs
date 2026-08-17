/**
 * Populate Hotel Census Location for Curio Collection by Hilton rows missing the field.
 *
 *   node scripts/apply-curio-census-location-type.mjs --dry-run
 *   node scripts/apply-curio-census-location-type.mjs
 *   node scripts/apply-curio-census-location-type.mjs --force
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import {
  CURIO_LOCATION_TYPE_PLAN,
  MAP_CURIO_CENSUS_LOCATION,
  locationTypeToAirtableFields,
  validateLocationTypeRow,
} from "../lib/hotel-census/curio-census-location-type.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORT = join(__dirname, "..", "reports", "curio-census-location-type-log.csv");

const CURIO_MATCH = /curio collection/i;

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
  const baseId = process.env.AIRTABLE_BASE_ID_ALT || process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");

  console.log(`=== Curio Census Location (${dryRun ? "DRY RUN" : "LIVE"}) ===\n`);

  for (const row of CURIO_LOCATION_TYPE_PLAN) {
    const v = validateLocationTypeRow(row);
    if (!v.pass) {
      console.error("Validation failed:", row.name, v.errors);
      process.exit(1);
    }
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const F = MAP_CURIO_CENSUS_LOCATION;
  const logRows = [];
  let updated = 0;
  let skipped = 0;
  let errors = 0;

  for (const row of CURIO_LOCATION_TYPE_PLAN) {
    let rec;
    try {
      rec = await base(HOTEL_CENSUS_TABLE).find(row.recordId);
    } catch (err) {
      errors++;
      console.error(`  ERROR ${row.name}: ${err.message}`);
      logRows.push({
        recordId: row.recordId,
        name: row.name,
        status: "error",
        previousLocation: "",
        newLocation: row.location,
        detail: err.message,
      });
      continue;
    }

    const f = rec.fields || {};
    const affiliation = String(f[F.affiliation] || "");
    const current = String(f[F.location] || "").trim();
    const recName = f[F.name] || row.name;

    if (!CURIO_MATCH.test(affiliation)) {
      skipped++;
      console.warn(`  SKIP ${recName}: affiliation="${affiliation}"`);
      logRows.push({
        recordId: row.recordId,
        name: recName,
        status: "skipped_wrong_affiliation",
        previousLocation: current,
        newLocation: row.location,
        detail: row.rationale,
      });
      continue;
    }

    if (current && current !== row.location && !force) {
      skipped++;
      console.warn(`  SKIP ${recName}: already "${current}" (use --force to overwrite)`);
      logRows.push({
        recordId: row.recordId,
        name: recName,
        status: "skipped_has_value",
        previousLocation: current,
        newLocation: row.location,
        detail: row.rationale,
      });
      continue;
    }

    if (current === row.location) {
      skipped++;
      console.log(`  SKIP ${recName}: already ${row.location}`);
      logRows.push({
        recordId: row.recordId,
        name: recName,
        status: "skipped_unchanged",
        previousLocation: current,
        newLocation: row.location,
        detail: row.rationale,
      });
      continue;
    }

    const fields = locationTypeToAirtableFields(row);

    if (dryRun) {
      console.log(`  [dry-run] ${recName}: ${current || "(blank)"} → ${row.location}`);
      updated++;
      logRows.push({
        recordId: row.recordId,
        name: recName,
        status: "dry_run",
        previousLocation: current,
        newLocation: row.location,
        detail: row.rationale,
      });
      continue;
    }

    try {
      await base(HOTEL_CENSUS_TABLE).update([{ id: row.recordId, fields }], { typecast: true });
      updated++;
      console.log(`  UPDATED ${recName}: ${current || "(blank)"} → ${row.location}`);
      logRows.push({
        recordId: row.recordId,
        name: recName,
        status: "ok",
        previousLocation: current,
        newLocation: row.location,
        detail: row.rationale,
      });
    } catch (err) {
      errors++;
      console.error(`  FAILED ${recName}:`, err.message);
      logRows.push({
        recordId: row.recordId,
        name: recName,
        status: `error: ${err.message}`,
        previousLocation: current,
        newLocation: row.location,
        detail: row.rationale,
      });
    }
  }

  mkdirSync(dirname(REPORT), { recursive: true });
  const headers = ["recordId", "name", "status", "previousLocation", "newLocation", "detail"];
  writeFileSync(
    REPORT,
    `${headers.join(",")}\n${logRows.map((r) => headers.map((h) => csvEscape(r[h])).join(",")).join("\n")}\n`,
    "utf8"
  );

  console.log("\nDone.");
  console.log(`  Updated: ${updated}${dryRun ? " (dry-run)" : ""}`);
  console.log(`  Skipped: ${skipped}`);
  console.log(`  Errors: ${errors}`);
  console.log(`  Plan rows: ${CURIO_LOCATION_TYPE_PLAN.length}`);
  console.log(`  Log: ${REPORT}`);

  if (errors) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
