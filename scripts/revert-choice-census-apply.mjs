#!/usr/bin/env node
/**
 * Revert mistaken Choice regional apply on a census row (clear Website + Property ID).
 * Usage: node scripts/revert-choice-census-apply.mjs <recordId> [--dry-run]
 */
import "../load-env.js";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { appendFileSync, existsSync } from "node:fs";
import { join } from "node:path";

const recordId = process.argv[2];
const DRY_RUN = process.argv.includes("--dry-run");
const LOG = join("reports", "choice-regional-reverts.csv");

if (!recordId) {
  console.error("Usage: node scripts/revert-choice-census-apply.mjs <recordId> [--dry-run]");
  process.exit(1);
}

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);

const rec = await base(HOTEL_CENSUS_TABLE).find(recordId);
const f = rec.fields || {};
console.log("Before:", {
  name: f.name,
  Website: f.Website,
  PropertyID: f["Property ID"],
});

const fields = { Website: "", "Property ID": "" };
if (DRY_RUN) {
  console.log("Dry-run — would clear Website + Property ID");
  process.exit(0);
}

await base(HOTEL_CENSUS_TABLE).update(recordId, fields, { typecast: true });
const after = await base(HOTEL_CENSUS_TABLE).find(recordId);
console.log("After:", {
  name: after.fields.name,
  Website: after.fields.Website,
  PropertyID: after.fields["Property ID"],
});

if (!existsSync(LOG)) {
  appendFileSync(LOG, "revertedAt,censusRecordId,censusName,reason\n");
}
appendFileSync(
  LOG,
  `${new Date().toISOString()},${recordId},"${f.name}",wrong_brand_duplicate_property_id\n`
);
