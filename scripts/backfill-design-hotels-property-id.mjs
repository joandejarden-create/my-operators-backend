#!/usr/bin/env node
/**
 * Fill-blank Property ID for Design Hotels CALA rows from designhotels.com Website slug.
 *
 *   node scripts/backfill-design-hotels-property-id.mjs
 *   node scripts/backfill-design-hotels-property-id.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import Airtable from "airtable";
import {
  slugFromPropertyUrl,
  isCalaCountry,
} from "../lib/design-hotels-census-enrichment.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";

const APPLY = process.argv.includes("--apply");
const AFFILIATION = "Design Hotels";

async function main() {
  mkdirSync("reports", { recursive: true });
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: ["name", "Website", CENSUS_PROPERTY_ID_FIELD, CENSUS_FIELDS.country],
      filterByFormula: `{${CENSUS_FIELDS.affiliation}}="${AFFILIATION}"`,
      pageSize: 100,
    })
    .all();

  const planRows = [];
  const skipped = [];
  for (const rec of records) {
    if (!isCalaCountry(rec.fields[CENSUS_FIELDS.country])) continue;
    if (!isBlankCensusValue(rec.fields[CENSUS_PROPERTY_ID_FIELD])) {
      skipped.push({ id: rec.id, reason: "pid_present" });
      continue;
    }
    const website = String(rec.fields.Website || "").trim();
    if (!website || !/designhotels\.com/i.test(website)) {
      skipped.push({ id: rec.id, name: rec.fields.name, reason: "no_designhotels_website", website });
      continue;
    }
    let slug = "";
    try {
      slug = slugFromPropertyUrl(website);
    } catch {
      skipped.push({ id: rec.id, name: rec.fields.name, reason: "invalid_website_url", website });
      continue;
    }
    if (!slug) {
      skipped.push({ id: rec.id, name: rec.fields.name, reason: "no_designhotels_slug", website });
      continue;
    }
    planRows.push({
      censusRecordId: rec.id,
      censusName: rec.fields.name,
      slug,
      applyFields: { [CENSUS_PROPERTY_ID_FIELD]: slug },
    });
  }

  writeFileSync(
    "reports/design-hotels-property-id-backfill-plan.json",
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        mode: APPLY ? "apply" : "dry-run",
        note: "Property ID = official designhotels.com hotel path slug from Website.",
        readyToApply: planRows.length,
        planRows,
        skipped,
      },
      null,
      2
    )
  );
  console.log("Ready:", planRows.length);
  for (const r of planRows) console.log(" ", r.censusName, "→", r.slug);

  if (!APPLY) {
    console.log("DRY-RUN");
    return;
  }
  for (const row of planRows) {
    await base(HOTEL_CENSUS_TABLE).update([{ id: row.censusRecordId, fields: row.applyFields }], {
      typecast: true,
    });
  }
  writeFileSync(
    "reports/design-hotels-property-id-backfill-apply-log.json",
    JSON.stringify({ generatedAt: new Date().toISOString(), updated: planRows.length, planRows }, null, 2)
  );
  console.log("Updated:", planRows.length);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
