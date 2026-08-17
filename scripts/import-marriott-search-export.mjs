#!/usr/bin/env node
/**
 * Import a browser-exported Marriott hotel search JSON and apply enrichment to census.
 *
 * Export steps (Chrome DevTools on findHotels search results):
 * 1. Open Network tab, filter Fetch/XHR
 * 2. Reload search — save the GraphQL/search response JSON
 * 3. node scripts/import-marriott-search-export.mjs --file path/to/export.json
 * 4. node scripts/import-marriott-search-export.mjs --file path/to/export.json --apply
 */
import "../load-env.js";
import { readFileSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { normalizeMarriottSearchExport } from "../lib/marriott-brand-directory-extract.js";
import { planMarriottCensusEnrichment } from "../lib/hotel-census/plan-marriott-census-enrichment.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const BATCH = 10;

function parseArgs() {
  const args = process.argv.slice(2);
  const get = (flag) => {
    const i = args.indexOf(flag);
    return i >= 0 ? args[i + 1] : null;
  };
  return {
    file: get("--file"),
    apply: args.includes("--apply"),
    dryRun: args.includes("--dry-run"),
  };
}

async function main() {
  const opts = parseArgs();
  if (!opts.file) {
    console.error("Usage: --file path/to/marriott-search-export.json [--apply]");
    process.exit(1);
  }

  const payload = JSON.parse(readFileSync(opts.file, "utf8"));
  const directoryRows = normalizeMarriottSearchExport(payload);
  console.log("Imported directory rows:", directoryRows.length);
  if (!directoryRows.length) {
    console.error("No hotels parsed from export file.");
    process.exit(1);
  }

  const plan = await planMarriottCensusEnrichment({ directoryRows });
  mkdirSync(REPORTS, { recursive: true });
  const jsonPath = join(REPORTS, "marriott-census-import-plan.json");
  writeFileSync(jsonPath, JSON.stringify({ generatedAt: new Date().toISOString(), ...plan }, null, 2));

  console.log("Ready to apply:", plan.readyToApply);
  console.log("Report:", jsonPath);

  if (!opts.apply) {
    console.log("\nRun with --apply to write to Hotel Census.");
    return;
  }

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  let updated = 0;
  let batch = [];
  for (const row of plan.planRows) {
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    if (batch.length >= BATCH) {
      if (!opts.dryRun) await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
      updated += batch.length;
      batch = [];
    }
  }
  if (batch.length) {
    if (!opts.dryRun) await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
    updated += batch.length;
  }
  console.log(`${opts.dryRun ? "Would update" : "Updated"}: ${updated}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
