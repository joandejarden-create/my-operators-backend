#!/usr/bin/env node
/**
 * Force-update Hotel Description + Amenities on a census row (corrections / verified overview export).
 *   node scripts/apply-marriott-census-record-force.mjs --record-id=rec54jkFM0zveGu7P --file fixtures/marriott-overview-pujac-sample.html
 */
import "../load-env.js";
import { readFileSync } from "node:fs";
import Airtable from "airtable";
import { parseMarriottOverviewHtml } from "../lib/marriott-hotel-content-fetch.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { CENSUS_DESCRIPTION_FIELD } from "../lib/hotel-census/hilton-description-enrichment-contract.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";

function parseArgs() {
  const args = process.argv.slice(2);
  const get = (flag) => {
    const eq = args.find((a) => a.startsWith(`${flag}=`));
    if (eq) return eq.slice(flag.length + 1);
    const i = args.indexOf(flag);
    return i >= 0 ? args[i + 1] : null;
  };
  return {
    recordId: get("--record-id"),
    file: get("--file"),
    description: get("--description"),
    amenities: get("--amenities"),
    dryRun: args.includes("--dry-run"),
  };
}

async function main() {
  const opts = parseArgs();
  if (!opts.recordId) {
    console.error("Usage: --record-id=recXXX [--file overview.html | --description=... --amenities=...]");
    process.exit(1);
  }

  let description = opts.description || "";
  let amenitiesText = opts.amenities || "";

  if (opts.file) {
    const html = readFileSync(opts.file, "utf8");
    const parsed = parseMarriottOverviewHtml(html);
    if (parsed.description) description = parsed.description;
    if (parsed.amenitiesText) amenitiesText = parsed.amenitiesText;
  }

  if (!description && !amenitiesText) {
    console.error("No description or amenities to apply.");
    process.exit(1);
  }

  /** @type {Record<string, string>} */
  const fields = {};
  if (description) fields[CENSUS_DESCRIPTION_FIELD] = description;
  if (amenitiesText) fields[CENSUS_AMENITIES_TEXT_FIELD] = amenitiesText;

  console.log("Record:", opts.recordId);
  console.log("Fields:", Object.keys(fields).join(", "));
  if (description) console.log("Description:", description.slice(0, 120) + (description.length > 120 ? "…" : ""));
  if (amenitiesText) console.log("Amenities:", amenitiesText);

  if (opts.dryRun) return;

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  await base(HOTEL_CENSUS_TABLE).update([{ id: opts.recordId, fields }], { typecast: true });
  console.log("\nUpdated.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
