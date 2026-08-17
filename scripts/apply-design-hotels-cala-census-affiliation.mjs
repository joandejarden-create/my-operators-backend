#!/usr/bin/env node
/**
 * Set Affiliation = Design Hotels for CALA census rows matched to designhotels.com.
 *
 *   node scripts/apply-design-hotels-cala-census-affiliation.mjs
 *   node scripts/apply-design-hotels-cala-census-affiliation.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, appendFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import {
  fetchDesignHotelsCalaProperties,
  planDesignHotelsAffiliationUpdates,
  buildDesignHotelsCensusPatch,
  DESIGN_HOTELS_AFFILIATION,
  isCalaCountry,
} from "../lib/design-hotels-census-enrichment.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { writeCsv } from "../lib/str-census-import/report-utils.mjs";

const APPLY = process.argv.includes("--apply");
const PLAN_JSON = join("reports", "design-hotels-cala-affiliation-apply-plan.json");
const LOG_CSV = join("reports", "design-hotels-cala-affiliation-applies.csv");

async function loadCensusCalaRows() {
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        "name",
        "Website",
        CENSUS_FIELDS.affiliation,
        CENSUS_FIELDS.parentCompany,
        CENSUS_FIELDS.country,
        CENSUS_FIELDS.city,
      ],
    })
    .all();
  return records.filter((r) => isCalaCountry(r.fields[CENSUS_FIELDS.country]));
}

async function main() {
  mkdirSync("reports", { recursive: true });

  console.log("Fetching designhotels.com CALA properties…");
  const properties = await fetchDesignHotelsCalaProperties();
  console.log(`Sitemap CALA hotel pages: ${properties.length}`);

  console.log("Loading CALA census…");
  const censusRows = await loadCensusCalaRows();
  console.log(`CALA census rows: ${censusRows.length}`);

  const planned = planDesignHotelsAffiliationUpdates(censusRows, properties, { minScore: 85 });
  const ready = [];
  const skipped = [];

  for (const row of planned) {
    const fields = buildDesignHotelsCensusPatch(row);
    if (!Object.keys(fields).length) {
      skipped.push({ ...row, skipReason: "already_design_hotels" });
      continue;
    }
    ready.push({ ...row, fields });
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apply: APPLY,
    affiliationTarget: DESIGN_HOTELS_AFFILIATION,
    sitemapProperties: properties.length,
    plannedRows: planned.length,
    readyToApply: ready.length,
    skippedAlreadyCorrect: skipped.length,
    ready,
    skipped,
    unmatchedSitemapSlugs: properties
      .filter((p) => !planned.some((pl) => pl.slug === p.slug || pl.propertyUrl === p.propertyUrl))
      .map((p) => ({ slug: p.slug, country: p.censusCountry, url: p.propertyUrl })),
  };

  writeFileSync(PLAN_JSON, JSON.stringify(report, null, 2));
  writeCsv(
    join("reports", "design-hotels-cala-affiliation-apply-ready.csv"),
    ready.map((r) => ({
      censusRecordId: r.censusRecordId,
      censusName: r.censusName,
      censusCountry: r.censusCountry,
      currentAffiliation: r.currentAffiliation,
      propertyUrl: r.propertyUrl,
      matchReason: r.matchReason,
      fields: JSON.stringify(r.fields),
    })),
    [
      "censusRecordId",
      "censusName",
      "censusCountry",
      "currentAffiliation",
      "propertyUrl",
      "matchReason",
      "fields",
    ]
  );

  console.log(`\nPlanned: ${planned.length} | Ready to apply: ${ready.length} | Skipped: ${skipped.length}`);
  console.log(`Unmatched sitemap hotels: ${report.unmatchedSitemapSlugs.length}`);
  console.log(`Wrote ${PLAN_JSON}`);

  if (!ready.length) {
    console.log("\nNothing to apply.");
    return;
  }

  console.log("\nSample applies:");
  for (const r of ready.slice(0, 12)) {
    console.log(
      `  ${r.censusName} (${r.censusCountry}) ${r.currentAffiliation || "∅"} → ${JSON.stringify(r.fields)}`
    );
  }

  if (!APPLY) {
    console.log("\nDry-run only — pass --apply to write Airtable.");
    return;
  }

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );

  if (!existsSync(LOG_CSV)) {
    appendFileSync(
      LOG_CSV,
      "appliedAt,censusRecordId,censusName,censusCountry,propertyUrl,matchReason,fieldsJson\n"
    );
  }

  let applied = 0;
  let failed = 0;

  for (const row of ready) {
    try {
      await base(HOTEL_CENSUS_TABLE).update(row.censusRecordId, row.fields, { typecast: true });
      applied++;
      appendFileSync(
        LOG_CSV,
        `${new Date().toISOString()},${row.censusRecordId},"${String(row.censusName).replace(/"/g, '""')}",${row.censusCountry},${row.propertyUrl},${row.matchReason},"${JSON.stringify(row.fields).replace(/"/g, '""')}"\n`
      );
      console.log("Applied", row.censusRecordId, row.censusName);
    } catch (err) {
      failed++;
      console.error("FAIL", row.censusRecordId, row.censusName, err?.message || err);
    }
  }

  console.log(`\nApplied: ${applied} Failed: ${failed}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
