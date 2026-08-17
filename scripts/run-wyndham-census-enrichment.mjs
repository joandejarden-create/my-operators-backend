#!/usr/bin/env node
/**
 * Wyndham census: sitemap extract → match → optional amenities fetch → apply (fill-blank).
 *
 *   node scripts/run-wyndham-census-enrichment.mjs
 *   node scripts/run-wyndham-census-enrichment.mjs --apply
 *   node scripts/run-wyndham-census-enrichment.mjs --apply --fetch-amenities
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { extractWyndhamPropertyUrls } from "../lib/wyndham-brand-directory-extract.js";
import {
  planWyndhamCensusSitemapMatch,
  WYNDHAM_WAVE1_AFFILIATIONS,
} from "../lib/hotel-census/plan-wyndham-census-sitemap-match.js";
import { fetchWyndhamHotelAmenities } from "../lib/wyndham-hotel-content-fetch.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const EXTRACT_JSON = join(REPORTS, "wyndham-property-directory-extract.json");
const BATCH = 10;

function parseArgs() {
  const args = process.argv.slice(2);
  const affArg = args.find((a) => a.startsWith("--affiliations="))?.split("=")[1];
  /** @type {string[]|null} */
  let affiliations = null;
  if (args.includes("--wave1")) affiliations = [...WYNDHAM_WAVE1_AFFILIATIONS];
  else if (affArg) affiliations = affArg.split("|").map((s) => s.trim()).filter(Boolean);
  return {
    apply: args.includes("--apply"),
    fetchAmenities: args.includes("--fetch-amenities"),
    skipExtract: args.includes("--skip-extract"),
    affiliations,
    delayMs: Number(args.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 250),
  };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  const opts = parseArgs();
  mkdirSync(REPORTS, { recursive: true });

  if (!opts.skipExtract && !existsSync(EXTRACT_JSON)) {
    console.log("=== Wyndham sitemap extract ===\n");
    const extracted = await extractWyndhamPropertyUrls({ calaOnly: true, delayMs: 80 });
    if (!extracted.ok) throw new Error(extracted.error || "Wyndham extract failed");
    writeFileSync(EXTRACT_JSON, JSON.stringify({ generatedAt: new Date().toISOString(), ...extracted }, null, 2));
    console.log("Extracted CALA URLs:", extracted.propertyRows.length);
  } else if (existsSync(EXTRACT_JSON)) {
    console.log("Using existing extract:", EXTRACT_JSON);
  }

  console.log("\n=== Wyndham census sitemap match ===\n");
  if (opts.affiliations?.length) {
    console.log("Affiliation filter:", opts.affiliations.join(" | "));
  }
  const matchPlan = await planWyndhamCensusSitemapMatch({
    affiliations: opts.affiliations || undefined,
  });
  const planName = opts.affiliations?.length
    ? "wyndham-wave1-census-enrichment-plan.json"
    : "wyndham-census-sitemap-match-plan.json";
  writeFileSync(
    join(REPORTS, planName),
    JSON.stringify({ generatedAt: new Date().toISOString(), ...matchPlan }, null, 2)
  );
  writeFileSync(
    join(REPORTS, "wyndham-wave1-steward-review.csv"),
    [
      "censusRecordId,censusName,affiliation,reason,matchScore,propertyUrl",
      ...(matchPlan.stewardReview || []).map((r) =>
        [r.censusRecordId, r.censusName, r.affiliation, r.reason, r.matchScore, r.propertyUrl]
          .map((v) => {
            const s = String(v ?? "");
            return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
          })
          .join(",")
      ),
      ...(matchPlan.skipped || [])
        .filter((s) => s.reason === "no_directory_match")
        .map((r) =>
          [r.censusRecordId, r.censusName, r.affiliation, r.reason, "", ""]
            .map((v) => {
              const s = String(v ?? "");
              return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
            })
            .join(",")
        ),
    ].join("\n")
  );
  console.log("Directory rows:", matchPlan.directoryRowsLoaded);
  console.log("Census scanned:", matchPlan.censusRowsScanned);
  console.log("Match ready:", matchPlan.readyToApply);
  console.log("Steward:", (matchPlan.stewardReview || []).length);
  console.log("Skipped:", matchPlan.skipped.length);

  /** @type {object[]} */
  const amenityPlans = [];
  if (opts.fetchAmenities) {
    console.log("\n=== Fetch Wyndham amenities (verified HTML only) ===\n");
    const base = getPlatformBase();
    const urlByRecord = new Map(
      matchPlan.planRows.map((r) => [r.censusRecordId, r.propertyUrl])
    );

    const amenityFormula = opts.affiliations?.length
      ? `OR(${opts.affiliations.map((a) => `{${CENSUS_FIELDS.affiliation}}="${a}"`).join(",")})`
      : `FIND("Wyndham", {${CENSUS_FIELDS.parentCompany}})`;

    const records = await base(HOTEL_CENSUS_TABLE)
      .select({
        fields: ["name", "Website", "Amenities", CENSUS_FIELDS.affiliation],
        filterByFormula: amenityFormula,
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
      if (!url || !/wyndhamhotels\.com/i.test(url)) continue;

      n++;
      console.log(` [${n}] ${rec.fields?.name}`);
      const fetched = await fetchWyndhamHotelAmenities(url);
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
      join(REPORTS, "wyndham-census-amenities-fetch-plan.json"),
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
    join(REPORTS, "wyndham-census-enrichment-apply-log.json"),
    JSON.stringify({ generatedAt: new Date().toISOString(), updated, allApplyCount: allApply.length }, null, 2)
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
