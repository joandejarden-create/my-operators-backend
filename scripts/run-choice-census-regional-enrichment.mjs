#!/usr/bin/env node
/**
 * Choice census enrichment via regional browse pages (JSON-LD official names).
 *
 * Regional listings are not Akamai-blocked; property pages often are.
 * Replaces one-by-one steward URL pastes with country-level discovery + name match.
 *
 *   node scripts/run-choice-census-regional-enrichment.mjs
 *   node scripts/run-choice-census-regional-enrichment.mjs --country Mexico
 *   node scripts/run-choice-census-regional-enrichment.mjs --apply
 *   node scripts/run-choice-census-regional-enrichment.mjs --min-score 60 --min-confidence low
 */
import "../load-env.js";
import { readFileSync, writeFileSync, mkdirSync, appendFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import {
  buildChoiceCalaRegionalPages,
  buildChoiceRegionalPageForCountry,
  CHOICE_CENSUS_COUNTRY_PROPERTY_PREFIX,
  CHOICE_SITEMAP_ONLY_COUNTRIES,
  canonicalChoicePropertyUrl,
  fetchChoiceRegionalHotels,
} from "../lib/choice-regional-directory-extract.js";
import { loadChoiceSitemapDirectoryForCountry } from "../lib/choice-sitemap-only-directory.js";
import {
  buildChoiceRegionalApplyPlan,
  choiceRegionalStewardReview,
  matchChoiceRegionalToCensus,
} from "../lib/choice-census-regional-match.js";
import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { mapCensusRowForDirectoryMatch } from "../lib/hotel-census/match-brand-directory-to-census.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { writeCsv } from "../lib/str-census-import/report-utils.mjs";

const REPORTS = "reports";
const PLAN_JSON = join(REPORTS, "choice-census-regional-enrichment-plan.json");
const STEWARD_CSV = join(REPORTS, "choice-census-regional-steward-review.csv");
const UNMATCHED_CSV = join(REPORTS, "choice-census-regional-unmatched-steward.csv");
const APPLY_LOG = join(REPORTS, "choice-regional-verified-applies.csv");
const SITEMAP_EXTRACT =
  process.env.CHOICE_SITEMAP_EXTRACT ||
  "reports/independent-census-choice-property-url-extract-cala-2026-05-20.json";

function parseArgs() {
  const args = process.argv.slice(2);
  let country = "";
  const countryEq = args.find((a) => a.startsWith("--country="));
  if (countryEq) country = countryEq.split("=")[1] || "";
  else {
    const idx = args.indexOf("--country");
    if (idx >= 0 && args[idx + 1] && !args[idx + 1].startsWith("--")) {
      country = args[idx + 1];
    }
  }
  return {
    apply: args.includes("--apply"),
    dryRun: args.includes("--dry-run") || !args.includes("--apply"),
    country,
    minScore: Number(args.find((a) => a.startsWith("--min-score="))?.split("=")[1] || 65),
    minNameSim: Number(args.find((a) => a.startsWith("--min-name-sim="))?.split("=")[1] || 0.55),
    minConfidence:
      args.find((a) => a.startsWith("--min-confidence="))?.split("=")[1] || "medium",
    includeHasWebsite: args.includes("--include-has-website"),
  };
}

function propertyPrefixForCountry(countryLabel) {
  return CHOICE_CENSUS_COUNTRY_PROPERTY_PREFIX[countryLabel] || "";
}

/**
 * Merge sitemap canonical URLs for property IDs missing from regional JSON-LD.
 * @param {object[]} regionalHotels
 * @param {string} countryLabel
 */
function mergeSitemapUrls(regionalHotels, countryLabel) {
  let extract;
  try {
    extract = JSON.parse(readFileSync(SITEMAP_EXTRACT, "utf8"));
  } catch {
    return regionalHotels;
  }

  const rows = Array.isArray(extract.propertyRows) ? extract.propertyRows : [];
  const prefix = propertyPrefixForCountry(countryLabel).toUpperCase();
  const countryNorm = countryLabel.toLowerCase();
  const byId = new Map(regionalHotels.map((h) => [h.propertyId, h]));

  for (const row of rows) {
    const id = String(row.propertyId || "").toUpperCase();
    if (!id) continue;
    if (prefix && !id.startsWith(prefix)) continue;

    if (!prefix) {
      const rowCountry =
        String(row.inferredCountry || row.countryOrRegionSegment || "").toLowerCase();
      if (
        rowCountry &&
        rowCountry !== countryNorm &&
        !rowCountry.includes(countryNorm.slice(0, 4))
      ) {
        continue;
      }
    }

    const propertyUrl = canonicalChoicePropertyUrl(row.propertyUrl || "");
    if (!propertyUrl) continue;

    if (!byId.has(id)) {
      byId.set(id, {
        propertyId: id,
        name: row.inferredHotelName || row.matchedBrandSetupBrand || id,
        propertyUrl,
        citySlug: row.citySlug || "",
        source: "choice_sitemap_fallback",
        regionalCountry: countryLabel,
      });
    } else {
      const existing = byId.get(id);
      if (!existing.propertyUrl && propertyUrl) existing.propertyUrl = propertyUrl;
      if (
        existing.source === "choice_sitemap_fallback" &&
        row.inferredHotelName &&
        existing.name === id
      ) {
        existing.name = row.inferredHotelName;
      }
    }
  }

  return [...byId.values()];
}

function resolveRegionalPages(opts) {
  if (opts.country) {
    if (CHOICE_SITEMAP_ONLY_COUNTRIES.includes(opts.country)) return [];
    const page =
      buildChoiceRegionalPageForCountry(opts.country) ||
      buildChoiceCalaRegionalPages().find(
        (p) => p.country.toLowerCase() === opts.country.toLowerCase()
      );
    if (!page) {
      console.error("No regional page slug for country:", opts.country);
      process.exit(1);
    }
    return [page];
  }
  return buildChoiceCalaRegionalPages();
}

function appendApplyLog(row) {
  if (!existsSync(APPLY_LOG)) {
    appendFileSync(
      APPLY_LOG,
      "appliedAt,censusRecordId,censusName,propertyId,propertyUrl,matchScore,nameSim,confidence,source\n"
    );
  }
  appendFileSync(
    APPLY_LOG,
    `${new Date().toISOString()},${row.censusRecordId},"${row.censusName}",${row.propertyId},${row.propertyUrl},${row.matchScore},${row.nameSim},${row.matchConfidence},choice_regional_enrichment\n`
  );
}

async function main() {
  const opts = parseArgs();
  mkdirSync(REPORTS, { recursive: true });

  let pages = resolveRegionalPages(opts);

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );

  /** @type {object[]} */
  const allRegionalHotels = [];
  /** @type {object[]} */
  const fetchLog = [];
  /** @type {object[]} */
  const allAssigned = [];
  /** @type {object[]} */
  const allCensusRows = [];

  for (const page of pages) {
    console.log(`\n=== Regional fetch: ${page.label} ===`);
    const result = await fetchChoiceRegionalHotels(page.url);
    fetchLog.push({ ...page, ...result, hotelCount: result.hotels?.length || 0 });
    console.log(
      `  HTTP ${result.status} | ${result.htmlLength || 0} bytes | hotels: ${result.hotels?.length || 0}`
    );
    if (!result.ok) {
      console.warn("  Skip:", result.error);
      continue;
    }

    const merged = mergeSitemapUrls(
      result.hotels.map((h) => ({ ...h, regionalLabel: page.label, regionalCountry: page.country })),
      page.country
    );
    console.log(`  After sitemap merge: ${merged.length} properties`);
    allRegionalHotels.push(...merged);

    const countryFilter = `{${CENSUS_FIELDS.country}}="${page.country}"`;
    const censusRecords = await base(HOTEL_CENSUS_TABLE)
      .select({
        fields: [
          "name",
          "Website",
          "Property ID",
          "Amenities",
          CENSUS_FIELDS.city,
          CENSUS_FIELDS.country,
          CENSUS_FIELDS.parentCompany,
          CENSUS_FIELDS.status,
        ],
        filterByFormula: `AND(FIND("Choice", {${CENSUS_FIELDS.parentCompany}}), ${countryFilter})`,
      })
      .all();

    const censusRows = censusRecords.map(mapCensusRowForDirectoryMatch);
    allCensusRows.push(...censusRows);

    const blankWeb = censusRows.filter((r) => isBlankCensusValue(r.fields?.Website)).length;
    console.log(`  Census rows: ${censusRows.length} (${blankWeb} missing Website)`);

    const assigned = matchChoiceRegionalToCensus(merged, censusRows, {
      minScore: opts.minScore,
      minNameSim: opts.minNameSim,
      minConfidence: opts.minConfidence,
      onlyBlankWebsite: !opts.includeHasWebsite,
      regionalCountry: page.country,
    });
    console.log(`  Matches: ${assigned.length}`);
    allAssigned.push(...assigned);
  }

  const sitemapOnlyCountries = opts.country
    ? CHOICE_SITEMAP_ONLY_COUNTRIES.filter(
        (c) => c.toLowerCase() === opts.country.toLowerCase()
      )
    : CHOICE_SITEMAP_ONLY_COUNTRIES;

  for (const country of sitemapOnlyCountries) {
    console.log(`\n=== Sitemap-only: ${country} ===`);
    const merged = loadChoiceSitemapDirectoryForCountry(country, SITEMAP_EXTRACT);
    console.log(`  Sitemap properties: ${merged.length}`);
    allRegionalHotels.push(...merged);

    const countryFilter = `{${CENSUS_FIELDS.country}}="${country}"`;
    const censusRecords = await base(HOTEL_CENSUS_TABLE)
      .select({
        fields: [
          "name",
          "Website",
          "Property ID",
          "Amenities",
          CENSUS_FIELDS.city,
          CENSUS_FIELDS.country,
          CENSUS_FIELDS.parentCompany,
          CENSUS_FIELDS.status,
        ],
        filterByFormula: `AND(FIND("Choice", {${CENSUS_FIELDS.parentCompany}}), ${countryFilter})`,
      })
      .all();

    const censusRows = censusRecords.map(mapCensusRowForDirectoryMatch);
    allCensusRows.push(...censusRows);
    const blankWeb = censusRows.filter((r) => isBlankCensusValue(r.fields?.Website)).length;
    console.log(`  Census rows: ${censusRows.length} (${blankWeb} missing Website)`);

    const assigned = matchChoiceRegionalToCensus(merged, censusRows, {
      minScore: 60,
      minNameSim: 0.4,
      minConfidence: "none",
      onlyBlankWebsite: !opts.includeHasWebsite,
      regionalCountry: country,
    });
    console.log(`  Matches: ${assigned.length}`);
    allAssigned.push(...assigned);
  }

  const countries = [
    ...new Set([
      ...pages.map((p) => p.country),
      ...sitemapOnlyCountries,
    ]),
  ];
  const censusRows = allCensusRows;
  const blankWebsite = censusRows.filter((r) => isBlankCensusValue(r.fields?.Website));
  console.log(`\nChoice census (${countries.join(", ")}): ${censusRows.length}`);
  console.log(`  Missing Website: ${blankWebsite.length}`);

  const assigned = allAssigned;

  const applyPlan = buildChoiceRegionalApplyPlan(assigned);
  const steward = choiceRegionalStewardReview(assigned);

  console.log(`\n=== Match results (minScore=${opts.minScore}, confidence>=${opts.minConfidence}) ===`);
  console.log(`  Assigned: ${assigned.length}`);
  console.log(`  Apply plan (Website/PID): ${applyPlan.length}`);
  console.log(`  Steward review: ${steward.length}`);

  for (const row of applyPlan.slice(0, 25)) {
    console.log(
      `  ${row.matchScore} ${row.matchConfidence} | ${row.censusName} (${row.censusCity}) -> ${row.propertyId}`
    );
    console.log(`    regional: ${row.regionalName}`);
  }
  if (applyPlan.length > 25) console.log(`  ... +${applyPlan.length - 25} more`);

  const unmatched = blankWebsite.filter(
    (r) => !applyPlan.some((p) => p.censusRecordId === r.recordId)
  );
  console.log(`\n  Still missing Website after plan: ${unmatched.length}`);

  writeFileSync(
    PLAN_JSON,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        options: opts,
        fetchLog,
        regionalHotelCount: allRegionalHotels.length,
        censusRows: censusRows.length,
        blankWebsite: blankWebsite.length,
        assignedCount: assigned.length,
        applyPlan,
        stewardReview: steward,
        unmatched: unmatched.map((r) => ({
          censusRecordId: r.recordId,
          censusName: r.name,
          censusCity: r.city,
          censusCountry: r.country,
        })),
      },
      null,
      2
    )
  );
  console.log("\nWrote:", PLAN_JSON);

  if (steward.length) {
    writeCsv(STEWARD_CSV, steward, [
      "censusRecordId",
      "censusName",
      "regionalName",
      "propertyId",
      "propertyUrl",
      "score",
      "nameSim",
      "confidence",
    ]);
    console.log("Wrote:", STEWARD_CSV);
  }

  if (unmatched.length) {
    writeCsv(
      UNMATCHED_CSV,
      unmatched.map((r) => ({
        censusRecordId: r.recordId,
        censusName: r.name,
        censusCity: r.city,
        censusCountry: r.country,
      })),
      ["censusRecordId", "censusName", "censusCity", "censusCountry"]
    );
    console.log("Wrote:", UNMATCHED_CSV);
  }

  if (!opts.apply) {
    console.log("\nDry-run only. Pass --apply to write Website + Property ID (fill-blank).");
    return;
  }

  console.log("\n=== Applying fill-blank Website + Property ID ===\n");
  let applied = 0;
  for (const row of applyPlan) {
    if (!Object.keys(row.applyFields).length) continue;

    const rec = await base(HOTEL_CENSUS_TABLE).find(row.censusRecordId);
    const f = rec.fields || {};
    const fields = {};
    if (isBlankCensusValue(f.Website) && row.applyFields.Website) {
      fields.Website = row.applyFields.Website;
    }
    if (isBlankCensusValue(f["Property ID"]) && row.applyFields["Property ID"]) {
      fields["Property ID"] = row.applyFields["Property ID"];
    }
    if (!Object.keys(fields).length) {
      console.log(`Skip ${row.censusName}: already filled`);
      continue;
    }

    console.log(`${row.censusName} -> ${row.propertyId}`);
    console.log("  Apply:", fields);
    await base(HOTEL_CENSUS_TABLE).update(rec.id, fields, { typecast: true });
    appendApplyLog(row);
    applied++;
  }
  console.log(`\nApplied ${applied} census rows. Log: ${APPLY_LOG}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
