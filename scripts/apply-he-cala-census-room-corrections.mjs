/**
 * Apply HE CALA room count corrections to Hotel Census.
 *
 * Usage:
 *   node scripts/apply-he-cala-census-room-corrections.mjs --dry-run
 *   node scripts/apply-he-cala-census-room-corrections.mjs
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/he-cala-census-apply.js";
import {
  HE_CALA_ROOM_CORRECTIONS,
  HE_MGMT,
  MAP_HE_CALA_ROOM_CORRECTIONS,
  correctionToAirtableFields,
  validateRoomCorrection,
} from "../lib/hotel-census/he-cala-census-room-corrections.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORT_DIR = join(__dirname, "..", "reports");
const LOG_CSV = join(REPORT_DIR, "he-cala-census-room-corrections-log.csv");

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

  console.log(`=== HE CALA room corrections (${dryRun ? "DRY RUN" : "LIVE"}) ===\n`);

  const validationFailures = [];
  for (const row of HE_CALA_ROOM_CORRECTIONS) {
    const v = validateRoomCorrection(row);
    if (!v.pass) validationFailures.push({ portfolioKey: row.portfolioKey, errors: v.errors });
  }
  if (validationFailures.length) {
    console.error("Validation failed:", validationFailures);
    process.exit(1);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const F = MAP_HE_CALA_ROOM_CORRECTIONS;
  const logRows = [];
  let updated = 0;
  let skipped = 0;
  let errors = 0;

  for (const row of HE_CALA_ROOM_CORRECTIONS) {
    let rec;
    try {
      rec = await base(HOTEL_CENSUS_TABLE).find(row.recordId);
    } catch (err) {
      errors++;
      console.error(`  ERROR ${row.name}: record not found — ${err.message}`);
      logRows.push({
        portfolioKey: row.portfolioKey,
        recordId: row.recordId,
        name: row.name,
        status: "error",
        previousRooms: "",
        newRooms: row.rooms,
        detail: err.message,
      });
      continue;
    }

    const f = rec.fields || {};
    const mgmt = f[F.mgmt];
    const currentRooms = f[F.rooms];
    const recName = f[F.name] || "";

    if (mgmt !== HE_MGMT) {
      skipped++;
      console.warn(
        `  SKIP ${row.name}: Management Company is "${mgmt ?? ""}", expected "${HE_MGMT}"`
      );
      logRows.push({
        portfolioKey: row.portfolioKey,
        recordId: row.recordId,
        name: recName || row.name,
        status: "skipped_wrong_mgmt",
        previousRooms: currentRooms ?? "",
        newRooms: row.rooms,
        detail: `mgmt=${mgmt ?? ""}`,
      });
      continue;
    }

    if (
      row.expectedPreviousRooms != null &&
      currentRooms != null &&
      Number(currentRooms) !== row.expectedPreviousRooms
    ) {
      console.warn(
        `  WARN ${row.name}: census rooms=${currentRooms}, expected ${row.expectedPreviousRooms} before correction`
      );
    }

    if (Number(currentRooms) === row.rooms) {
      skipped++;
      console.log(`  SKIP ${row.name}: already ${row.rooms} rooms`);
      logRows.push({
        portfolioKey: row.portfolioKey,
        recordId: row.recordId,
        name: recName || row.name,
        status: "skipped_unchanged",
        previousRooms: currentRooms ?? "",
        newRooms: row.rooms,
        detail: row.rationale,
      });
      continue;
    }

    const fields = correctionToAirtableFields(row);
    const preview = JSON.stringify(fields);

    if (dryRun) {
      console.log(
        `  [dry-run] UPDATE ${recName || row.name}: ${currentRooms ?? "(blank)"} → ${row.rooms}`,
        preview
      );
      updated++;
      logRows.push({
        portfolioKey: row.portfolioKey,
        recordId: row.recordId,
        name: recName || row.name,
        status: "dry_run",
        previousRooms: currentRooms ?? "",
        newRooms: row.rooms,
        detail: `${row.rationale} | ${row.sourceUrl}`,
      });
      continue;
    }

    try {
      await base(HOTEL_CENSUS_TABLE).update([{ id: row.recordId, fields }], { typecast: true });
      updated++;
      console.log(
        `  UPDATED ${recName || row.name}: ${currentRooms ?? "(blank)"} → ${row.rooms}`
      );
      logRows.push({
        portfolioKey: row.portfolioKey,
        recordId: row.recordId,
        name: recName || row.name,
        status: "ok",
        previousRooms: currentRooms ?? "",
        newRooms: row.rooms,
        detail: `${row.rationale} | ${row.sourceUrl}`,
      });
    } catch (err) {
      errors++;
      console.error(`  UPDATE FAILED ${row.name}:`, err.message);
      logRows.push({
        portfolioKey: row.portfolioKey,
        recordId: row.recordId,
        name: recName || row.name,
        status: `error: ${err.message}`,
        previousRooms: currentRooms ?? "",
        newRooms: row.rooms,
        detail: row.rationale,
      });
    }
  }

  mkdirSync(REPORT_DIR, { recursive: true });
  const headers = [
    "portfolioKey",
    "recordId",
    "name",
    "status",
    "previousRooms",
    "newRooms",
    "detail",
  ];
  const csv = [
    headers.join(","),
    ...logRows.map((r) => headers.map((h) => csvEscape(r[h])).join(",")),
  ].join("\n");
  writeFileSync(LOG_CSV, `${csv}\n`, "utf8");

  console.log("\nDone.");
  console.log(`  Updated: ${updated}${dryRun ? " (dry-run)" : ""}`);
  console.log(`  Skipped: ${skipped}`);
  console.log(`  Errors: ${errors}`);
  console.log(`  Log: ${LOG_CSV}`);
  console.log(`  Mapping: ${JSON.stringify(MAP_HE_CALA_ROOM_CORRECTIONS)}`);

  if (errors) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
