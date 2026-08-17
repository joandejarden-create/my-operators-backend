/**
 * Backfill Latitude / Longitude (and Address 1 when blank) for CALA Design Hotels census rows.
 *
 *   node scripts/apply-design-hotels-census-geocode.mjs --dry-run
 *   node scripts/apply-design-hotels-census-geocode.mjs
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import { isCalaCountry, DESIGN_HOTELS_AFFILIATION } from "../lib/design-hotels-census-enrichment.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import {
  buildGeocodePatch,
  hasCoords,
  DESIGN_HOTELS_GEO_BY_RECORD_ID,
  F_LAT,
  F_LNG,
  F_ADDR,
} from "../lib/hotel-census/design-hotels-census-geocode.js";

const REPORT = join("reports", "design-hotels-census-geocode-log.json");
const APPLY = !process.argv.includes("--dry-run");

async function main() {
  mkdirSync("reports", { recursive: true });
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");

  console.log(`=== Design Hotels census geocode (${APPLY ? "LIVE" : "DRY RUN"}) ===\n`);

  const base = new Airtable({ apiKey }).base(baseId);
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        CENSUS_FIELDS.name,
        CENSUS_FIELDS.country,
        CENSUS_FIELDS.affiliation,
        CENSUS_FIELDS.city,
        "Website",
        F_LAT,
        F_LNG,
        F_ADDR,
      ],
    })
    .all();

  const dhCala = records.filter(
    (r) =>
      isCalaCountry(r.fields[CENSUS_FIELDS.country]) &&
      r.fields[CENSUS_FIELDS.affiliation] === DESIGN_HOTELS_AFFILIATION
  );

  const missing = dhCala.filter((r) => !hasCoords(r.fields));
  console.log(`Design Hotels CALA: ${dhCala.length} rows, ${missing.length} missing coordinates\n`);

  const log = {
    dryRun: !APPLY,
    appliedAt: new Date().toISOString(),
    totalDesignHotelsCala: dhCala.length,
    missingBefore: missing.length,
    rows: [],
  };

  let updated = 0;
  let skipped = 0;

  for (const rec of missing) {
    const patch = buildGeocodePatch(rec.id, rec.fields);
    if (!patch) {
      skipped++;
      log.rows.push({
        recordId: rec.id,
        name: rec.fields.name,
        action: "skip_no_geo_map",
      });
      console.log(`SKIP (no geo map) ${rec.fields.name} → ${rec.id}`);
      continue;
    }

    if (!APPLY) {
      log.rows.push({
        recordId: rec.id,
        name: rec.fields.name,
        action: "update_dry_run",
        fields: patch,
        source: DESIGN_HOTELS_GEO_BY_RECORD_ID[rec.id]?.source,
      });
      console.log(`[dry-run] UPDATE ${rec.fields.name}`, patch);
      continue;
    }

    await base(HOTEL_CENSUS_TABLE).update(rec.id, patch, { typecast: true });
    updated++;
    log.rows.push({
      recordId: rec.id,
      name: rec.fields.name,
      action: "update",
      fields: patch,
      source: DESIGN_HOTELS_GEO_BY_RECORD_ID[rec.id]?.source,
    });
    console.log(`UPDATED ${rec.fields.name} → ${rec.id}`);
  }

  writeFileSync(REPORT, JSON.stringify(log, null, 2), "utf8");
  console.log(`\nDone: ${updated} updated, ${skipped} skipped (no map)`);
  console.log("Report:", REPORT);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
