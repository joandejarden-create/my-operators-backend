#!/usr/bin/env node
/**
 * Apply steward-verified Choice property URLs to Hotel Census (fill-blank).
 * Usage: node scripts/apply-choice-steward-urls-batch.mjs [--dry-run]
 */
import "../load-env.js";
import { appendFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { fetchChoiceHotelAmenities } from "../lib/choice-hotel-content-fetch.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";

const DRY_RUN = process.argv.includes("--dry-run");
const LOG_PATH = join("reports", "choice-steward-verified-applies.csv");

/** Normalize steward URL to census Website (no locale prefix). */
function canonicalChoiceUrl(url) {
  return String(url || "")
    .trim()
    .replace(/^(https:\/\/www\.choicehotels\.com)\/en-[a-z]{2}\//i, "$1/");
}

/** @type {{ id: string; code: string; name: string; url: string }[]} */
const STEWARD = [
  {
    id: "recl3N6lOAzypuVzl",
    code: "MX163",
    name: "El Cid Granada Hotel",
    url: "https://www.choicehotels.com/en-uk/sinaloa/mazatlan/ascend-hotels/mx163",
  },
  {
    id: "recljXyUE3I8sCoxc",
    code: "MX165",
    name: "El Cid El Moro Beach Hotel",
    url: "https://www.choicehotels.com/en-uk/sinaloa/mazatlan/ascend-hotels/mx165",
  },
  {
    id: "recjTNbfPSlClglNl",
    code: "MX176",
    name: "Sleep Inn Leon Antares",
    url: "https://www.choicehotels.com/en-uk/guanajuato/leon/sleep-inn-hotels/mx176",
  },
];

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);

function appendLog(row, url, amenityCount) {
  if (!existsSync(LOG_PATH)) {
    appendFileSync(
      LOG_PATH,
      "appliedAt,censusRecordId,censusName,propertyId,propertyUrl,source,amenityCount\n"
    );
  }
  appendFileSync(
    LOG_PATH,
    `${new Date().toISOString()},${row.id},"${row.name}",${row.code},${url},choice_steward_verified_url,${amenityCount}\n`
  );
}

for (const entry of STEWARD) {
  const website = canonicalChoiceUrl(entry.url);
  const rec = await base(HOTEL_CENSUS_TABLE).find(entry.id);
  const f = rec.fields || {};
  const fields = {};

  if (isBlankCensusValue(f.Website)) fields.Website = website;
  if (isBlankCensusValue(f["Property ID"])) fields["Property ID"] = entry.code;

  if (isBlankCensusValue(f.Amenities)) {
    const fetched = await fetchChoiceHotelAmenities(website);
    if (fetched.amenitiesText) fields.Amenities = fetched.amenitiesText;
    console.log(`  amenity fetch: ${fetched.status} (${fetched.amenities?.length || 0})`);
  }

  console.log(`\n${f.name} (${entry.code})`);
  console.log("Before:", {
    Website: f.Website,
    PID: f["Property ID"],
    Amenities: (f.Amenities || "").slice(0, 80),
  });
  console.log("Apply:", fields);

  if (!Object.keys(fields).length) {
    console.log("Skip: nothing to apply");
    continue;
  }

  if (DRY_RUN) {
    console.log("Dry-run: no write");
    continue;
  }

  await base(HOTEL_CENSUS_TABLE).update(rec.id, fields, { typecast: true });
  const after = await base(HOTEL_CENSUS_TABLE).find(rec.id);
  console.log("After:", {
    Website: after.fields.Website,
    PID: after.fields["Property ID"],
    Amenities: (after.fields.Amenities || "").slice(0, 100),
  });
  const count = fields.Amenities ? fields.Amenities.split(";").length : 0;
  appendLog({ id: rec.id, name: f.name, code: entry.code }, website, count);
}
