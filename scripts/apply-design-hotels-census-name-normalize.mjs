#!/usr/bin/env node
/**
 * Normalize Hotel Census `name` for Design Hotels rows — remove legacy "a Member of Design Hotels" suffixes.
 *
 *   node scripts/apply-design-hotels-census-name-normalize.mjs --dry-run
 *   node scripts/apply-design-hotels-census-name-normalize.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import {
  DESIGN_HOTELS_AFFILIATION,
  isCalaCountry,
} from "../lib/design-hotels-census-enrichment.js";
import {
  normalizeDesignHotelsCensusName,
  hasLegacyMemberOfDesignHotelsSuffix,
} from "../lib/design-hotels-census-name-normalize.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;
const PLAN_JSON = join("reports", "design-hotels-census-name-normalize-plan.json");

async function main() {
  mkdirSync("reports", { recursive: true });

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );

  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: ["name", CENSUS_FIELDS.affiliation, CENSUS_FIELDS.country],
    })
    .all();

  const candidates = records.filter((rec) => {
    const affiliation = String(rec.fields[CENSUS_FIELDS.affiliation] || "").trim();
    const country = rec.fields[CENSUS_FIELDS.country];
    const name = String(rec.fields.name || "");
    const isDesignHotels = affiliation === DESIGN_HOTELS_AFFILIATION;
    const hasSuffix = hasLegacyMemberOfDesignHotelsSuffix(name);
    if (!hasSuffix) return false;
    if (isDesignHotels) return true;
    return isCalaCountry(country) && /design hotels/i.test(name);
  });

  const plan = candidates.map((rec) => {
    const { canonical, previous, changed } = normalizeDesignHotelsCensusName(rec.fields.name);
    return {
      recordId: rec.id,
      previousName: previous,
      canonicalName: canonical,
      country: rec.fields[CENSUS_FIELDS.country] || "",
      affiliation: rec.fields[CENSUS_FIELDS.affiliation] || "",
      changed,
    };
  });

  writeFileSync(PLAN_JSON, JSON.stringify({ generatedAt: new Date().toISOString(), plan }, null, 2));

  console.log(`Rows with legacy member-of suffix: ${plan.length}`);
  for (const row of plan) {
    console.log(`  ${row.recordId}: "${row.previousName}" → "${row.canonicalName}"`);
  }

  if (DRY_RUN) {
    console.log(`Dry run — plan written to ${PLAN_JSON}. Pass --apply to update Airtable.`);
    return;
  }

  let updated = 0;
  for (const row of plan) {
    if (!row.changed) continue;
    await base(HOTEL_CENSUS_TABLE).update(row.recordId, { name: row.canonicalName }, { typecast: true });
    updated += 1;
  }

  console.log(`Applied ${updated} name normalization(s).`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
