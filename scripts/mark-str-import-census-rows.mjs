/**
 * Set Development Cost = 1 on Hotel Census rows updated by STR import apply.
 * Uses reports/str-census-import-apply-log.csv (recordId column).
 *
 * Usage:
 *   node scripts/mark-str-import-census-rows.mjs --dry-run
 *   node scripts/mark-str-import-census-rows.mjs
 */
import "../load-env.js";
import { readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";

const DEVELOPMENT_COST_FIELD = "Development Cost";
const APPLY_LOG = join(
  dirname(fileURLToPath(import.meta.url)),
  "..",
  "reports",
  "str-census-import-apply-log.csv"
);
const BATCH_SIZE = 10;

function parseArgs() {
  return { dryRun: process.argv.includes("--dry-run") };
}

function loadRecordIdsFromApplyLog() {
  if (!existsSync(APPLY_LOG)) {
    throw new Error(`Missing ${APPLY_LOG}. Run apply-str-census-import.mjs first.`);
  }
  const text = readFileSync(APPLY_LOG, "utf8");
  const lines = text.trim().split(/\r?\n/);
  const header = lines[0].split(",");
  const idIdx = header.indexOf("recordId");
  if (idIdx < 0) throw new Error("apply log missing recordId column");

  const ids = new Set();
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line) continue;
    const cols = line.split(",");
    const action = cols[0];
    if (action !== "updated" && action !== "would_update") continue;
    const recordId = cols[idIdx];
    if (recordId?.startsWith("rec")) ids.add(recordId);
  }
  return [...ids];
}

async function main() {
  const { dryRun } = parseArgs();
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");

  const recordIds = loadRecordIdsFromApplyLog();
  console.log(`=== Mark STR-import updated rows (${dryRun ? "DRY RUN" : "LIVE"}) ===\n`);
  console.log(`Table: ${HOTEL_CENSUS_TABLE}`);
  console.log(`Field: ${DEVELOPMENT_COST_FIELD} = 1`);
  console.log(`Records from apply log: ${recordIds.length}\n`);

  if (!recordIds.length) {
    console.log("Nothing to update.");
    return;
  }

  const base = new Airtable({ apiKey }).base(baseId);
  let updated = 0;
  let errors = 0;

  for (let i = 0; i < recordIds.length; i += BATCH_SIZE) {
    const chunk = recordIds.slice(i, i + BATCH_SIZE);
    if (dryRun) {
      updated += chunk.length;
      continue;
    }
    try {
      await base(HOTEL_CENSUS_TABLE).update(
        chunk.map((id) => ({
          id,
          fields: { [DEVELOPMENT_COST_FIELD]: 1 },
        })),
        { typecast: true }
      );
      updated += chunk.length;
      if (updated % 100 === 0) console.log(`  …${updated} marked`);
    } catch (err) {
      console.error("Batch failed:", err.message);
      errors += chunk.length;
    }
  }

  console.log("\nDone.");
  console.log(`  Marked: ${updated}${dryRun ? " (dry-run)" : ""}`);
  console.log(`  Errors: ${errors}`);
  console.log(
    "\nRows without Development Cost = 1 were not in the STR import apply log (not Matched by STR ID)."
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
