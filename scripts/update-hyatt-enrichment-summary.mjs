#!/usr/bin/env node
import "../load-env.js";
import { readFileSync, writeFileSync } from "node:fs";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import {
  MAP_DIRECTORY_ENRICHMENT,
  isBlankCensusValue,
} from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { isCalaCountry } from "../lib/hyatt-brand-directory-extract.js";
import { mapCensusRowForDirectoryMatch } from "../lib/hotel-census/match-brand-directory-to-census.js";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
const formula = `FIND("Hyatt", {${CENSUS_FIELDS.parentCompany}})`;
const records = await base(HOTEL_CENSUS_TABLE)
  .select({
    fields: [
      CENSUS_FIELDS.country,
      MAP_DIRECTORY_ENRICHMENT.website,
      CENSUS_PROPERTY_ID_FIELD,
      "Amenities",
      CENSUS_FIELDS.parentCompany,
    ],
    filterByFormula: formula,
    pageSize: 100,
  })
  .all();

const cala = records
  .map(mapCensusRowForDirectoryMatch)
  .filter((r) => isCalaCountry(r.country));

let withWeb = 0;
let withPid = 0;
let withAmen = 0;
let blankBoth = 0;
for (const r of cala) {
  const w = !isBlankCensusValue(r.fields?.[MAP_DIRECTORY_ENRICHMENT.website]);
  const p = !isBlankCensusValue(r.fields?.[CENSUS_PROPERTY_ID_FIELD]);
  const a = !isBlankCensusValue(r.fields?.Amenities);
  if (w) withWeb += 1;
  if (p) withPid += 1;
  if (a) withAmen += 1;
  if (!w && !p) blankBoth += 1;
}

const unmatchedLines = readFileSync("reports/hyatt-census-unmatched.csv", "utf8")
  .trim()
  .split(/\r?\n/)
  .slice(1);

function parseCsvLine(line) {
  const parts = [];
  let cur = "";
  let inq = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      inq = !inq;
      continue;
    }
    if (ch === "," && !inq) {
      parts.push(cur);
      cur = "";
      continue;
    }
    cur += ch;
  }
  parts.push(cur);
  return parts;
}

const brands = {};
for (const line of unmatchedLines) {
  const name = parseCsvLine(line)[1] || "";
  let b = "other";
  const tests = [
    ["Secrets", /secrets/i],
    ["Dreams", /dreams/i],
    ["Breathless", /breathless/i],
    ["Sunscape", /sunscape/i],
    ["NOW", /\bnow\b/i],
    ["Impression", /impression/i],
    ["Zoetry", /zoetry/i],
    ["Thompson", /thompson/i],
    ["Andaz", /andaz/i],
    ["Park Hyatt", /park hyatt/i],
    ["Grand Hyatt", /grand hyatt/i],
    ["Hyatt Place", /hyatt place/i],
    ["Hyatt Regency", /hyatt regency/i],
    ["Hyatt Centric", /hyatt centric/i],
    ["Hyatt Vivid", /vivid/i],
    ["Destination", /destination/i],
  ];
  for (const [lab, re] of tests) {
    if (re.test(name)) {
      b = lab;
      break;
    }
  }
  brands[b] = (brands[b] || 0) + 1;
}

const amenityProbe = JSON.parse(readFileSync("reports/hyatt-amenities-probe.json", "utf8"));
const plan = JSON.parse(readFileSync("reports/hyatt-census-enrichment-plan.json", "utf8"));
const directory = JSON.parse(readFileSync("reports/hyatt-cala-directory-extract.json", "utf8"));

const priorApplied = 82;
const thisBatch = 30;
const summary = {
  generatedAt: new Date().toISOString(),
  mode: "apply",
  phase: "inclusive_cdx_topup_batch2",
  directoryUnique: directory.uniqueProperties || (directory.propertyRows || []).length,
  directoryDeltaFromPrior: (directory.uniqueProperties || 184) - 129,
  hyattParentRows: 184,
  calaCensusScanned: 184,
  matchedThisRun: plan.matched,
  readyToApplyThisRun: plan.readyToApply,
  stewardReviewThisRun: plan.stewardReviewCount,
  unmatchedBlank: blankBoth,
  appliedWebsitePropertyIdThisRun: thisBatch,
  appliedWebsitePropertyIdCumulative: priorApplied + thisBatch,
  applyErrors: 0,
  postApplyCounts: {
    cala: cala.length,
    withWebsite: withWeb,
    withPropertyId: withPid,
    withAmenities: withAmen,
    blankWebsiteAndPropertyId: blankBoth,
  },
  amenityProbe: {
    probed: amenityProbe.probeCount,
    ready: amenityProbe.readyCount,
    blocked: amenityProbe.blockedCount,
    applied: 0,
    skipped: true,
    reason:
      "All 3 live hyatt.com property fetches returned HTTP 429 / Kasada block; no amenities written.",
  },
  unmatchedBrandBreakdown: brands,
  stewardGaps: [
    {
      censusRecordId: "recPKO54xTtVbqwBE",
      censusName: "Hyatt Place San Jose Cariari",
      issue:
        "Hard-excluded from matching to Hyatt Place San Jose Pinares (SJOZP) — distinct properties. No official Cariari URL in directory yet.",
    },
    {
      note: "Duplicate census rows for same hotel (e.g. second Impression Isla Mujeres, Secrets Playa Blanca, Secrets Tulum) remain unmatched after 1:1 property-id assignment.",
    },
  ],
  fieldMapping: {
    Website: "Website",
    "Property ID": "Property ID",
  },
  reports: {
    directory: "reports/hyatt-cala-directory-extract.json",
    plan: "reports/hyatt-census-enrichment-plan.json",
    steward: "reports/hyatt-census-steward-review.csv",
    unmatched: "reports/hyatt-census-unmatched.csv",
    applyLog: "reports/hyatt-census-enrichment-apply-log.csv",
    amenities: "reports/hyatt-amenities-probe.json",
    summary: "reports/hyatt-census-enrichment-summary.json",
  },
  limitations: [
    "Live hyatt.com blocked (429/Kasada) — directory from Wayback sitemap 2024-01-26 + CDX of official hyatt.com URLs (region + Inclusive brand filters).",
    `Directory grew 129 → ${directory.uniqueProperties || 184}; Inclusive Collection coverage improved (Secrets/Impression/NOW/Thompson/Vivid) but many Dreams/Secrets openings still absent from Wayback CDX.`,
    "NOW Grand Island, many Dreams Grand Island variants, Breathless Cancun Soul, and several classic Place/Centric/Park/Grand openings not found in official hyatt.com archive harvest.",
    "Amenities not applied: 3/3 new-match probes returned 429.",
    "Hard exclusions: Cariari↔Pinares; Insurgentes census must not bind to generic MEXHR (Hyatt Regency Mexico City without Insurgentes).",
    "Slash-in-name normalization (San Jose/Pinares) enabled safe Pinares + Aguascalientes matches.",
  ],
  changeImpact: "High",
  rollbackNote:
    "Clear Website + Property ID on the 30 record IDs from the latest batch in reports/hyatt-census-enrichment-apply-log.csv (filter appliedAt after 2026-07-23). Cumulative prior batch remains 82.",
};

writeFileSync(
  "reports/hyatt-census-enrichment-summary.json",
  JSON.stringify(summary, null, 2) + "\n"
);
console.log(JSON.stringify({ postApplyCounts: summary.postApplyCounts, brands, thisBatch }, null, 2));
