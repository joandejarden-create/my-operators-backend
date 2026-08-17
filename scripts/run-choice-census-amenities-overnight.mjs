#!/usr/bin/env node
/**
 * Choice census: sitemap URL match → optional amenities fetch → apply (fill-blank).
 *
 *   node scripts/run-choice-census-amenities-overnight.mjs
 *   node scripts/run-choice-census-amenities-overnight.mjs --apply
 *   node scripts/run-choice-census-amenities-overnight.mjs --apply --fetch-amenities
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { planChoiceCensusSitemapMatch } from "../lib/hotel-census/plan-choice-census-sitemap-match.js";
import { fetchChoiceHotelAmenities } from "../lib/choice-hotel-content-fetch.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const BATCH = 10;

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    apply: args.includes("--apply"),
    fetchAmenities: args.includes("--fetch-amenities"),
    delayMs: Number(args.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 300),
  };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  const opts = parseArgs();
  mkdirSync(REPORTS, { recursive: true });

  console.log("=== Choice census sitemap match plan ===\n");
  const matchPlan = await planChoiceCensusSitemapMatch({ calaOnly: true, minScore: 50 });
  writeFileSync(
    join(REPORTS, "choice-census-sitemap-match-plan.json"),
    JSON.stringify({ generatedAt: new Date().toISOString(), ...matchPlan }, null, 2)
  );
  console.log("Directory CALA URLs:", matchPlan.directoryRowsFiltered);
  console.log("Census scanned:", matchPlan.censusRowsScanned);
  console.log("Website match ready:", matchPlan.readyToApply);
  console.log("Skipped:", matchPlan.skipped.length);

  /** @type {object[]} */
  const amenityPlans = [];
  if (opts.fetchAmenities) {
    console.log("\n=== Fetch amenities from choicehotels.com (verified HTML only) ===\n");
    const base = getPlatformBase();
    const records = await base(HOTEL_CENSUS_TABLE)
      .select({
        fields: ["name", "Website", "Amenities"],
        filterByFormula: `FIND("Choice", {${CENSUS_FIELDS.parentCompany}})`,
        pageSize: 100,
      })
      .all();

    for (let i = 0; i < records.length; i++) {
      const rec = records[i];
      const url = String(rec.fields?.Website || "").trim();
      if (!url || !/choicehotels\.com/i.test(url)) continue;
      if (!isBlankCensusValue(rec.fields?.Amenities)) continue;

      console.log(` [${i + 1}/${records.length}] ${rec.fields?.name}`);
      const fetched = await fetchChoiceHotelAmenities(url);
      await sleep(opts.delayMs);

      if (!fetched.amenitiesText) {
        amenityPlans.push({
          censusRecordId: rec.id,
          censusName: rec.fields?.name,
          propertyUrl: url,
          status: "fetch_empty",
          fetchStatus: fetched.status,
          parseErrors: fetched.parseErrors,
        });
        continue;
      }

      amenityPlans.push({
        censusRecordId: rec.id,
        censusName: rec.fields?.name,
        propertyUrl: url,
        amenitiesText: fetched.amenitiesText,
        amenityCount: fetched.amenities.length,
        fetchStatus: fetched.status,
        source: fetched.source,
        applyFields: { Amenities: fetched.amenitiesText },
        status: "ready",
      });
    }

    writeFileSync(
      join(REPORTS, "choice-census-amenities-fetch-plan.json"),
      JSON.stringify({ generatedAt: new Date().toISOString(), amenityPlans }, null, 2)
    );
    console.log(
      "Amenity fetch ready:",
      amenityPlans.filter((p) => p.status === "ready").length,
      "empty/blocked:",
      amenityPlans.filter((p) => p.status !== "ready").length
    );
  }

  if (!opts.apply) {
    console.log("\nDry-run complete. Use --apply [--fetch-amenities] to write.");
    return;
  }

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );

  let updated = 0;
  let batch = [];
  const allApply = [
    ...matchPlan.planRows.filter((r) => Object.keys(r.applyFields).length),
    ...amenityPlans.filter((p) => p.status === "ready"),
  ];

  for (const row of allApply) {
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    if (batch.length >= BATCH) {
      await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
      updated += batch.length;
      batch = [];
    }
  }
  if (batch.length) {
    await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
    updated += batch.length;
  }

  console.log("\nApplied rows:", updated);
  writeFileSync(
    join(REPORTS, "choice-census-overnight-apply-log.json"),
    JSON.stringify(
      { generatedAt: new Date().toISOString(), updated, allApplyCount: allApply.length },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
