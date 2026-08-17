#!/usr/bin/env node
/**
 * Set Affiliation = Small Luxury Hotels of the World for CALA census rows
 * matched to https://slh.com/explore-hotels (SLH hotel search API).
 *
 *   node scripts/apply-slh-cala-census-affiliation.mjs
 *   node scripts/apply-slh-cala-census-affiliation.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, appendFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import {
  fetchSlhCalaProperties,
  planSlhAffiliationUpdates,
  buildSlhCensusPatch,
  SLH_AFFILIATION,
  isCalaCountry,
} from "../lib/slh-census-enrichment.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { writeCsv } from "../lib/str-census-import/report-utils.mjs";

const APPLY = process.argv.includes("--apply");
const PLAN_JSON = join("reports", "slh-cala-affiliation-apply-plan.json");
const LOG_CSV = join("reports", "slh-cala-affiliation-applies.csv");
const STEWARD_JSON = join("reports", "slh-cala-affiliation-steward-review.json");

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

  console.log("Fetching SLH CALA properties from slh.com…");
  const properties = await fetchSlhCalaProperties({
    onProgress: (msg) => console.log(" ", msg),
  });
  console.log(`SLH CALA hotel pages: ${properties.length}`);

  console.log("Loading CALA census…");
  const censusRows = await loadCensusCalaRows();
  console.log(`CALA census rows: ${censusRows.length}`);

  const planned = planSlhAffiliationUpdates(censusRows, properties, { minScore: 80 });
  const ready = [];
  const skipped = [];
  const blocked = [];

  for (const row of planned.matches) {
    const { fields, blocked: isBlocked, blockReason } = buildSlhCensusPatch(row);
    if (isBlocked) {
      blocked.push({ ...row, blockReason });
      continue;
    }
    if (!Object.keys(fields).length) {
      skipped.push({ ...row, skipReason: "already_slh" });
      continue;
    }
    ready.push({ ...row, fields });
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apply: APPLY,
    affiliationTarget: SLH_AFFILIATION,
    source: "https://slh.com/explore-hotels (hotelsearchresults API)",
    slhCalaProperties: properties.length,
    matchedRows: planned.matches.length,
    readyToApply: ready.length,
    skippedAlreadyCorrect: skipped.length,
    blockedProtectedAffiliation: blocked.length,
    stewardReviewCount: planned.stewardReview.length,
    unmatchedSourceCount: planned.unmatchedSources.length,
    ready,
    skipped,
    blocked,
    stewardReview: planned.stewardReview,
    unmatchedSources: planned.unmatchedSources.map((p) => ({
      id: p.id,
      title: p.title,
      country: p.censusCountry,
      city: p.city,
      url: p.propertyUrl,
    })),
  };

  writeFileSync(PLAN_JSON, JSON.stringify(report, null, 2));
  writeFileSync(
    STEWARD_JSON,
    JSON.stringify(
      {
        generatedAt: report.generatedAt,
        stewardReview: planned.stewardReview,
        unmatchedSources: report.unmatchedSources,
        blocked,
      },
      null,
      2
    )
  );

  writeCsv(
    join("reports", "slh-cala-affiliation-apply-ready.csv"),
    ready.map((r) => ({
      censusRecordId: r.censusRecordId,
      censusName: r.censusName,
      censusCountry: r.censusCountry,
      currentAffiliation: r.currentAffiliation,
      sourceTitle: r.sourceTitle,
      propertyUrl: r.propertyUrl,
      matchScore: r.matchScore,
      matchReason: r.matchReason,
      fields: JSON.stringify(r.fields),
    })),
    [
      "censusRecordId",
      "censusName",
      "censusCountry",
      "currentAffiliation",
      "sourceTitle",
      "propertyUrl",
      "matchScore",
      "matchReason",
      "fields",
    ]
  );

  console.log(`\nMatched: ${planned.matches.length}`);
  console.log(`Ready to apply: ${ready.length}`);
  console.log(`Already SLH: ${skipped.length}`);
  console.log(`Blocked (protected affiliation): ${blocked.length}`);
  console.log(`Steward review: ${planned.stewardReview.length}`);
  console.log(`Unmatched SLH hotels: ${planned.unmatchedSources.length}`);
  console.log(`Wrote ${PLAN_JSON}`);
  console.log(`Wrote ${STEWARD_JSON}`);

  console.log("\nSample applies:");
  for (const r of ready.slice(0, 15)) {
    console.log(
      `  ${r.censusName} ← ${r.sourceTitle} (${r.censusCountry}) [${r.matchScore}/${r.matchReason}] ${r.currentAffiliation || "∅"} → Affiliation`
    );
  }

  if (blocked.length) {
    console.log("\nBlocked (protected affiliation):");
    for (const r of blocked.slice(0, 10)) {
      console.log(`  ${r.censusName} keeps ${r.currentAffiliation} (matched ${r.sourceTitle})`);
    }
  }

  if (planned.unmatchedSources.length) {
    console.log("\nUnmatched SLH (first 20):");
    for (const p of planned.unmatchedSources.slice(0, 20)) {
      console.log(`  ${p.censusCountry} | ${p.title} | ${p.propertyUrl}`);
    }
  }

  if (!APPLY) {
    console.log("\nDry-run only — pass --apply to write Airtable.");
    return;
  }

  if (!ready.length) {
    console.log("\nNothing to apply.");
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
