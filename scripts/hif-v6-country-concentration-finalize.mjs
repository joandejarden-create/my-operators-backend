#!/usr/bin/env node
/**
 * Finalize country concentration inventory with strict hotel-identity gates.
 * Priority markets are reporting/ambition dimensions — not allowlists.
 * Quarantines brand-only / boilerplate names and likely country misattribution.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  COUNTRY_PROSPECT_AMBITIONS_V6,
  PRIORITY_COUNTRIES_V6,
  computeYieldPer1000,
  emptyCountryScorecardRow,
  progressTowardAmbition,
} from "../lib/hotel-intelligence/sec-edgar/hif-v6-country-concentration-v6.js";
import { progressTowardOpportunityMilestone } from "../lib/hotel-intelligence/sec-edgar/hif-v6-opportunities-first-priority-v6.js";
import {
  ensureCountryScorecardRow,
  geographyEligibilityDisposition,
  normalizeHotelCountryCode,
} from "../lib/hotel-intelligence/sec-edgar/hif-v6-worldwide-eligibility-v6.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-country-concentration");
const B_OUT = "C:/Dev/dealality/ws-hif-v6-track-b/reports/sec-edgar-hif-v6-country-concentration";

const GENERIC_BRAND_ONLY =
  /^(the\s+)?(holiday inn(?: express)?|hampton inn(?: & suites)?|comfort (?:inn|suites)|fairfield inn(?: & suites)?|residence inn|courtyard|homewood suites|springhill suites|embassy suites|hilton garden inn|marriott|hilton|hyatt|wyndham|ramada|days inn|super 8|motel 6|best western|la quinta(?: inn)?|quality inn|crowne plaza|doubletree|sheraton|westin|radisson|ihg|premier inn)\s*(hotel|inn|suites|resort)?$/i;

function hotelKey(name, country) {
  return `${String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()}|${String(country || "").toUpperCase()}`;
}

function quarantineReason(row) {
  const n = String(row.hotelName || "").trim();
  if (n.length < 12) return "NAME_TOO_SHORT";
  if (GENERIC_BRAND_ONLY.test(n)) return "BRAND_ONLY_NO_PROPERTY_IDENTITY";
  if (/^(owned hotel|pro forma|awarded|world'?s leading|particular|per |interim )/i.test(n)) {
    return "BOILERPLATE_OR_AWARD_LANGUAGE";
  }
  if (/\b(agreement|amendment|exhibit|pro forma|owned hotel)\b/i.test(n)) {
    return "NON_HOTEL_PHRASE";
  }
  // Likely US property mislabeled as MX/BR/GB/CA from query contamination
  const usGeo =
    /\b(florida|orlando|miami|texas|california|new york|bonnet creek|las vegas|chicago|atlanta|dallas|houston|phoenix|seattle|boston|denver)\b/i;
  if (row.hotelCountry !== "US" && usGeo.test(n)) return "LIKELY_US_PROPERTY_MISATTRIBUTED";
  // Require distinctive multi-token identity beyond Brand+Hotel
  const tokens = n.split(/\s+/).filter(Boolean);
  if (tokens.length < 3 && !/\b(resort|palace|plaza|tower|bay|beach|park|royal|grand)\b/i.test(n)) {
    return "INSUFFICIENT_PROPERTY_SPECIFICITY";
  }
  return null;
}

function load(fp) {
  if (!fs.existsSync(fp)) return null;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
}

const trackA = load(path.join(OUT, "TRACK_A_UNDATED_PROSPECTS.json")) || { hotels: [] };
const trackADated = load(path.join(OUT, "TRACK_A_DATED_ACTIVE.json")) || { hotels: [] };
const trackAHist = load(path.join(OUT, "TRACK_A_HISTORICAL.json")) || { hotels: [] };
const trackB = load(path.join(B_OUT, "TRACK_B_PROSPECTS.json")) || { hotels: [] };
const trackBCand = load(path.join(B_OUT, "TRACK_B_CANDIDATES.json")) || { hotels: [] };
const priorMetrics = load(
  path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-wave34/WAVE34_COMBINED_METRICS.json"),
);
const priorToward1000 = Number(priorMetrics?.progressToward1000?.current || 194);

const all = [
  ...(trackADated.hotels || []).map((h) => ({ ...h, track: "TRACK_A", bucket: "DATED_ACTIVE" })),
  ...(trackAHist.hotels || []).map((h) => ({ ...h, track: "TRACK_A", bucket: "HISTORICAL" })),
  ...(trackA.hotels || []).map((h) => ({ ...h, track: "TRACK_A", bucket: "UNDATED" })),
  ...(trackB.hotels || []).map((h) => ({ ...h, track: "TRACK_B", bucket: "UNDATED" })),
];

const kept = [];
const quarantined = [];
const seen = new Set();
let crossTrackDupes = 0;

for (const row of all) {
  const reason = quarantineReason(row);
  if (reason) {
    quarantined.push({ ...row, quarantineReason: reason, qualificationLevel: "INVALID_OR_QUARANTINED" });
    continue;
  }
  if (row.bucket === "HISTORICAL") {
    // keep historical separately; do not count toward active country ambitions
    kept.push(row);
    continue;
  }
  const k = hotelKey(row.hotelName, row.hotelCountry);
  if (seen.has(k)) {
    crossTrackDupes += 1;
    continue;
  }
  seen.add(k);
  kept.push(row);
}

// Seed priority markets for concentration reporting; any verified country is credited dynamically.
const byCountry = Object.fromEntries(PRIORITY_COUNTRIES_V6.map((c) => [c, emptyCountryScorecardRow(c)]));
const activeProspects = kept.filter((r) => r.bucket !== "HISTORICAL");
const historical = kept.filter((r) => r.bucket === "HISTORICAL");
const geoHold = [];

for (const r of activeProspects) {
  const cc = normalizeHotelCountryCode(r.hotelCountry);
  const disp = geographyEligibilityDisposition(cc);
  if (disp.disposition === "HOLD_GEOGRAPHY_UNVERIFIED") {
    geoHold.push({ ...r, holdReason: disp.reason });
    continue;
  }
  const row = ensureCountryScorecardRow(byCountry, cc, emptyCountryScorecardRow);
  row.distinctEvidenceBackedProspects += 1;
  row.distinctHotelsResearched += 1;
  if (r.bucket === "DATED_ACTIVE") {
    row.verifiedContractualResearchOpportunities += 1;
    if (r.agreementType === "FRANCHISE") row.newlyVerifiedFranchiseExpirations += 1;
    if (r.agreementType === "MANAGEMENT") row.newlyVerifiedManagementExpirations += 1;
  } else {
    row.evidenceBackedCommercialProspectsUndated += 1;
  }
  if (r.track === "TRACK_A") row.trackAContribution += 1;
  else row.trackBContribution += 1;
  const fam = r.sourceFamily || "OTHER";
  row.sourceFamilies[fam] = (row.sourceFamilies[fam] || 0) + 1;
}
for (const r of historical) {
  const cc = normalizeHotelCountryCode(r.hotelCountry);
  if (geographyEligibilityDisposition(cc).disposition === "HOLD_GEOGRAPHY_UNVERIFIED") continue;
  ensureCountryScorecardRow(byCountry, cc, emptyCountryScorecardRow).historicalClosedSuperseded += 1;
}
for (const r of quarantined) {
  const cc = normalizeHotelCountryCode(r.hotelCountry);
  if (geographyEligibilityDisposition(cc).disposition === "HOLD_GEOGRAPHY_UNVERIFIED") continue;
  ensureCountryScorecardRow(byCountry, cc, emptyCountryScorecardRow).invalidOrQuarantined += 1;
}

for (const cc of Object.keys(byCountry)) {
  byCountry[cc].ambitionProgress = progressTowardAmbition(
    byCountry[cc].distinctEvidenceBackedProspects,
    cc,
  );
  byCountry[cc].progressTowardAmbitionMinPct = byCountry[cc].ambitionProgress?.pctOfMin ?? null;
  byCountry[cc].yieldPer1000HotelsResearched = computeYieldPer1000(
    byCountry[cc].distinctEvidenceBackedProspects,
    byCountry[cc].distinctHotelsResearched,
  );
  byCountry[cc].researchCostUsd = 0;
  byCountry[cc].isPriorityMarket = PRIORITY_COUNTRIES_V6.includes(cc);
}

const netNewClean = activeProspects.length;
const progress = progressTowardOpportunityMilestone({
  distinctEvidenceBackedOpportunities: priorToward1000 + netNewClean,
});

const examples = {};
for (const cc of PRIORITY_COUNTRIES_V6) {
  examples[cc] = activeProspects
    .filter((r) => r.hotelCountry === cc)
    .slice(0, 5)
    .map((r) => ({
      hotelName: r.hotelName,
      agreementType: r.agreementType,
      expirationDate: r.expirationDate,
      qualificationLevel: r.qualificationLevel || r.bucket,
      sourceUrl: r.sourceUrl,
      track: r.track,
      thesis: r.commercialThesis || null,
    }));
}

const summary = {
  asOf: new Date().toISOString().slice(0, 10),
  publicationState: "CUSTOMER_BLOCKED",
  researchCostUsd: 0,
  integrationBaseline: {
    merged: ["#503", "#506", "#507", "#508"],
    openAwaitingIc: ["#510", "#511", "#512", "#513", "#514", "#515"],
    originMainShort: "beb22be64",
  },
  priorToward1000,
  netNewCleanProspectsThisWave: netNewClean,
  historicalThisWave: historical.length,
  quarantinedThisWave: quarantined.length,
  geographyUnverifiedHold: geoHold.length,
  crossTrackDuplicatesRemoved: crossTrackDupes,
  progressToward1000: progress,
  worldwideEligibility: "PRIORITY_MARKETS_ARE_CONCENTRATION_NOT_ALLOWLIST",
  byCountry,
  globalCountryCodes: Object.keys(byCountry).sort(),
  trackContribution: {
    trackA: activeProspects.filter((r) => r.track === "TRACK_A").length,
    trackB: activeProspects.filter((r) => r.track === "TRACK_B").length,
  },
  ambitions: COUNTRY_PROSPECT_AMBITIONS_V6,
  recommendedNextExpansion: [
    "US: expand extractable CMBS annex formats beyond currently exhausted deal families",
    "MX: BMV issuer PDF filings with named hotels + agreement clauses (not SEC query bleed)",
    "BR: CVM document retrieval with condo-hotel vs HMA classification",
    "GB: Companies House filing documents + hotel transaction RNS once CH key available",
    "CA: SEDAR+ issuer MD&A/EXHIBIT download paths for AHIP/other REITs",
    "Keep ownership enrichment deferred until country inventories near ambition floors",
  ],
};

const md = `# HIF V6 — Five-Country Concentration Report

**As of:** ${summary.asOf}  
**Publication:** CUSTOMER_BLOCKED  
**Spend:** $0  
**Priority:** Opportunities first · ownership deferred  
**Integration:** #503/#506/#507/#508 MERGED · #510–#515 OPEN (IC sequential merge required)

## Progress toward 1,000

| Measure | Count |
|--------|------:|
| Prior inventory (Waves 1–4 est.) | ${priorToward1000} |
| Net-new **clean** prospects this wave | ${netNewClean} |
| Historical (not active) | ${historical.length} |
| Quarantined (brand-only / noise / misattribution) | ${quarantined.length} |
| **Combined toward 1,000** | **${progress.current}** |
| Remaining | ${progress.remaining} |

## Country scorecard (clean prospects only)

| Country | Ambition min | Clean prospects | Dated expirations | Undated evidence prospects | Track A | Track B | % of min |
|---------|-------------:|----------------:|------------------:|---------------------------:|--------:|--------:|---------:|
${PRIORITY_COUNTRIES_V6.map((c) => {
  const r = byCountry[c];
  return `| ${r.countryLabel} | ${r.planningAmbitionMin} | ${r.distinctEvidenceBackedProspects} | ${r.verifiedContractualResearchOpportunities} | ${r.evidenceBackedCommercialProspectsUndated} | ${r.trackAContribution} | ${r.trackBContribution} | ${r.progressTowardAmbitionMinPct ?? 0}% |`;
}).join("\n")}

## Evidence examples (clean)

${PRIORITY_COUNTRIES_V6.map((c) => {
  const ex = examples[c] || [];
  if (!ex.length) return `### ${COUNTRY_PROSPECT_AMBITIONS_V6[c].label}\n_No clean net-new property-specific prospects this wave after quarantine._\n`;
  return `### ${COUNTRY_PROSPECT_AMBITIONS_V6[c].label}\n${ex
    .map(
      (e) =>
        `- **${e.hotelName}** · ${e.agreementType}${e.expirationDate ? ` · exp ${e.expirationDate}` : " · undated"} · ${e.track} · [source](${e.sourceUrl})`,
    )
    .join("\n")}\n`;
}).join("\n")}

## Research gaps

- **US dated CMBS incremental yield** near zero this wave (prior inventory exhaustion on extractable schedules).
- **MX/BR/GB/CA** mostly undated disclosure prospects; verified franchise/management **dates** still scarce outside US CMBS.
- Portal reachability ≠ contractual coverage (CVM/BMV/SEDAR+/CH UI).
- Country attribution from SEC full-text queries can mislabel US assets — quarantine applied.

## Governance

CUSTOMER_BLOCKED · no HPC/Meta · Agent manual-only · Jev shadow · Lease deferred · no paid titles · ownership not required for discovery.
`;

fs.writeFileSync(path.join(OUT, "FINAL_CLEAN_PROSPECTS.json"), JSON.stringify({ count: activeProspects.length, hotels: activeProspects }, null, 2));
fs.writeFileSync(path.join(OUT, "FINAL_QUARANTINED.json"), JSON.stringify({ count: quarantined.length, hotels: quarantined.slice(0, 200) }, null, 2));
fs.writeFileSync(path.join(OUT, "FINAL_COUNTRY_SCORECARD.json"), JSON.stringify({ byCountry, examples, summary }, null, 2));
fs.writeFileSync(path.join(OUT, "FOUNDER_COUNTRY_CONCENTRATION_REPORT.md"), md);
fs.writeFileSync(path.join(OUT, "COMBINED_METRICS.json"), JSON.stringify(summary, null, 2));

// Mirror founder report to Track B report dir
fs.mkdirSync(B_OUT, { recursive: true });
fs.writeFileSync(path.join(B_OUT, "FOUNDER_COUNTRY_CONCENTRATION_REPORT.md"), md);
fs.writeFileSync(path.join(B_OUT, "COMBINED_METRICS.json"), JSON.stringify(summary, null, 2));

console.log(JSON.stringify(summary, null, 2));
