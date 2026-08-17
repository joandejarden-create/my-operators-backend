#!/usr/bin/env node
/**
 * Accor census: sitemap+metadata extract → match → optional amenities fetch → apply (fill-blank).
 *
 *   node scripts/run-accor-census-enrichment.mjs
 *   node scripts/run-accor-census-enrichment.mjs --apply --fetch-amenities
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { extractAccorPropertyUrls } from "../lib/accor-brand-directory-extract.js";
import { planAccorCensusSitemapMatch } from "../lib/hotel-census/plan-accor-census-sitemap-match.js";
import { fetchAccorHotelAmenities } from "../lib/accor-hotel-content-fetch.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const EXTRACT_JSON = join(REPORTS, "accor-property-directory-extract.json");
const BATCH = 10;

function parseArgs() {
  const args = process.argv.slice(2);
  const maxArg = args.find((a) => a.startsWith("--max-fetch="));
  return {
    apply: args.includes("--apply"),
    fetchAmenities: args.includes("--fetch-amenities"),
    skipExtract: args.includes("--skip-extract"),
    maxFetch: maxArg ? Number(maxArg.split("=")[1]) : null,
    delayMs: Number(args.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 150),
  };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  const opts = parseArgs();
  mkdirSync(REPORTS, { recursive: true });

  if (!opts.skipExtract && !existsSync(EXTRACT_JSON)) {
    console.log("=== Accor sitemap + metadata extract (CALA) ===\n");
    console.log("(Scans sitemap; fetches each hotel page for country/name — may take ~15 min)\n");
    const extracted = await extractAccorPropertyUrls({
      calaOnly: true,
      fetchMetadata: true,
      maxFetch: opts.maxFetch,
      delayMs: opts.delayMs,
    });
    if (!extracted.ok) throw new Error(extracted.error || "Accor extract failed");
    writeFileSync(EXTRACT_JSON, JSON.stringify({ generatedAt: new Date().toISOString(), ...extracted }, null, 2));
    console.log("Metadata fetched:", extracted.metadataFetched);
    console.log("CALA directory rows:", extracted.propertyRows.length);
  } else if (existsSync(EXTRACT_JSON)) {
    console.log("Using existing extract:", EXTRACT_JSON);
  } else {
    throw new Error("No Accor extract found. Run: node scripts/extract-accor-property-sitemap.mjs");
  }

  console.log("\n=== Accor census sitemap match ===\n");
  const matchPlan = await planAccorCensusSitemapMatch();
  writeFileSync(
    join(REPORTS, "accor-census-sitemap-match-plan.json"),
    JSON.stringify({ generatedAt: new Date().toISOString(), ...matchPlan }, null, 2)
  );
  console.log("Directory rows:", matchPlan.directoryRowsLoaded);
  console.log("Census scanned:", matchPlan.censusRowsScanned);
  console.log("Match ready:", matchPlan.readyToApply);
  console.log("Skipped:", matchPlan.skipped.length);

  /** @type {object[]} */
  const amenityPlans = [];
  if (opts.fetchAmenities) {
    console.log("\n=== Fetch Accor amenities (JSON-LD amenityFeature) ===\n");
    const base = getPlatformBase();
    const urlByRecord = new Map(
      matchPlan.planRows.map((r) => [r.censusRecordId, r.propertyUrl])
    );

    const records = await base(HOTEL_CENSUS_TABLE)
      .select({
        fields: ["name", "Website", "Amenities"],
        filterByFormula: `FIND("Accor", {${CENSUS_FIELDS.parentCompany}})`,
        pageSize: 100,
      })
      .all();

    let n = 0;
    for (const rec of records) {
      if (!isBlankCensusValue(rec.fields?.Amenities)) continue;
      const url =
        String(rec.fields?.Website || "").trim() ||
        urlByRecord.get(rec.id) ||
        "";
      if (!url || !/accor\.com/i.test(url)) continue;

      n++;
      console.log(` [${n}] ${rec.fields?.name}`);
      const fetched = await fetchAccorHotelAmenities(url);
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
      join(REPORTS, "accor-census-amenities-fetch-plan.json"),
      JSON.stringify({ generatedAt: new Date().toISOString(), amenityPlans }, null, 2)
    );
    console.log(
      "Amenity ready:",
      amenityPlans.filter((p) => p.status === "ready").length,
      "empty:",
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
    join(REPORTS, "accor-census-enrichment-apply-log.json"),
    JSON.stringify({ generatedAt: new Date().toISOString(), updated, allApplyCount: allApply.length }, null, 2)
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
