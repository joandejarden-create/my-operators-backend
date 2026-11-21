#!/usr/bin/env node
/**
 * Geography sanity for country-concentration prospects.
 * Downgrade SEC-keyword country tags unless physical-location evidence supports them.
 */
import fs from "node:fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  PRIORITY_COUNTRIES_V6,
  COUNTRY_PROSPECT_AMBITIONS_V6,
  emptyCountryScorecardRow,
  progressTowardAmbition,
  computeYieldPer1000,
} from "../lib/hotel-intelligence/sec-edgar/hif-v6-country-concentration-v6.js";
import { progressTowardOpportunityMilestone } from "../lib/hotel-intelligence/sec-edgar/hif-v6-opportunities-first-priority-v6.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-country-concentration");
const B_OUT = "C:/Dev/dealality/ws-hif-v6-track-b/reports/sec-edgar-hif-v6-country-concentration";

const known = {
  US: [/coconut point|bonnet creek|scottsdale|minneapolis|orlando|miami|las vegas|mandalay|mgm grand|vdara|citycenter|harmon hotel|texas|california|florida|arizona|nevada/i],
  MX: [/cancun|cancún|cabo|guadalajara|mexico city|ciudad de m[eé]xico|los cabos|riviera maya|puerto vallarta|monterrey/i],
  BR: [/s[aã]o paulo|rio de janeiro|brasilia|brasília|salvador|recife|belo horizonte|brazil|brasil/i],
  GB: [/london|manchester|edinburgh|birmingham|glasgow|united kingdom|england|scotland|wales/i],
  CA: [/toronto|vancouver|montreal|calgary|ottawa|ontario|quebec|british columbia|canada/i],
};

function countrySupport(row) {
  const blob = `${row.hotelName || ""} ${row.commercialThesis || ""} ${row.sourceUrl || ""}`;
  const tagged = row.hotelCountry;
  const scores = {};
  for (const cc of PRIORITY_COUNTRIES_V6) {
    scores[cc] = (known[cc] || []).some((re) => re.test(blob)) ? 1 : 0;
  }
  // If tagged country unsupported and another country supported → misattributed
  if (scores[tagged] === 0) {
    const alt = PRIORITY_COUNTRIES_V6.find((c) => c !== tagged && scores[c] === 1);
    if (alt) return { ok: false, reason: `MISATTRIBUTED_LIKELY_${alt}`, suggested: alt };
    // no positive geo cue for tagged country
    return { ok: false, reason: "COUNTRY_GEOGRAPHY_UNVERIFIED", suggested: null };
  }
  return { ok: true, reason: null, suggested: tagged };
}

const clean = JSON.parse(fs.readFileSync(path.join(OUT, "FINAL_CLEAN_PROSPECTS.json"), "utf8"));
const prior = JSON.parse(fs.readFileSync(path.join(OUT, "COMBINED_METRICS.json"), "utf8"));
const geoVerified = [];
const geoUnverified = [];
for (const row of clean.hotels || []) {
  const g = countrySupport(row);
  if (g.ok) geoVerified.push({ ...row, geographyVerification: "SUPPORTED" });
  else {
    geoUnverified.push({
      ...row,
      geographyVerification: g.reason,
      suggestedCountry: g.suggested,
      qualificationLevel: "HOLD_RESEARCH_CANDIDATE_GEOGRAPHY",
      countsAsCountryAmbitionProspect: false,
    });
  }
}

const byCountry = Object.fromEntries(PRIORITY_COUNTRIES_V6.map((c) => [c, emptyCountryScorecardRow(c)]));
for (const r of geoVerified) {
  const cc = r.hotelCountry;
  if (!byCountry[cc]) continue;
  byCountry[cc].distinctEvidenceBackedProspects += 1;
  byCountry[cc].distinctHotelsResearched += 1;
  byCountry[cc].evidenceBackedCommercialProspectsUndated += r.expirationDate ? 0 : 1;
  if (r.expirationDate) byCountry[cc].verifiedContractualResearchOpportunities += 1;
  if (r.track === "TRACK_A") byCountry[cc].trackAContribution += 1;
  else byCountry[cc].trackBContribution += 1;
}
for (const cc of PRIORITY_COUNTRIES_V6) {
  byCountry[cc].ambitionProgress = progressTowardAmbition(
    byCountry[cc].distinctEvidenceBackedProspects,
    cc,
  );
  byCountry[cc].progressTowardAmbitionMinPct = byCountry[cc].ambitionProgress?.pctOfMin ?? 0;
  byCountry[cc].yieldPer1000HotelsResearched = computeYieldPer1000(
    byCountry[cc].distinctEvidenceBackedProspects,
    byCountry[cc].distinctHotelsResearched,
  );
  byCountry[cc].holdResearchCandidates = geoUnverified.filter((r) => r.hotelCountry === cc).length;
}

// Global inventory: keep prior + only geography-supported net-new for ambition integrity;
// unverified remain research candidates (not country-ambition credited).
const progress = progressTowardOpportunityMilestone({
  distinctEvidenceBackedOpportunities: prior.priorToward1000 + geoVerified.length,
});

const summary = {
  ...prior,
  geographyGate: "PHYSICAL_LOCATION_EVIDENCE_REQUIRED_FOR_COUNTRY_AMBITION_CREDIT",
  netNewCleanProspectsThisWave: geoVerified.length,
  geographyUnverifiedHold: geoUnverified.length,
  progressToward1000: progress,
  byCountry,
  trackContribution: {
    trackA: geoVerified.filter((r) => r.track === "TRACK_A").length,
    trackB: geoVerified.filter((r) => r.track === "TRACK_B").length,
  },
  evidenceQualityNote:
    "SEC keyword harvest often returns hotels whose physical country differs from the query country. Only geography-supported rows credit country ambitions.",
};

const examples = {};
for (const cc of PRIORITY_COUNTRIES_V6) {
  examples[cc] = geoVerified
    .filter((r) => r.hotelCountry === cc)
    .slice(0, 5)
    .map((r) => ({
      hotelName: r.hotelName,
      agreementType: r.agreementType,
      expirationDate: r.expirationDate,
      sourceUrl: r.sourceUrl,
      track: r.track,
    }));
}

const md = `# HIF V6 — Five-Country Concentration Report (Geography-Gated)

**As of:** ${summary.asOf}  
**Publication:** CUSTOMER_BLOCKED · **Spend:** $0  
**Integration:** #503/#506/#507/#508 MERGED · #510–#515 OPEN · this wave PRs follow

## Phase 0 baseline
- \`origin/main\`: \`${summary.integrationBaseline.originMainShort}\`
- Registered workspaces: \`ws-hif-v6-track-a\`, \`ws-hif-v6-track-b\`
- Dirty HE/ADP/GDI/BE: preserved (not touched)

## Progress toward 1,000
| Measure | Count |
|--------|------:|
| Prior inventory | ${prior.priorToward1000} |
| Net-new **geography-supported** prospects | ${geoVerified.length} |
| Geography-unverified HOLD (not country-credited) | ${geoUnverified.length} |
| Quarantined earlier (brand-only/noise) | ${prior.quarantinedThisWave} |
| **Toward 1,000** | **${progress.current}** |
| Remaining | ${progress.remaining} |

## Country ambitions (geography-supported only)

| Country | Ambition min | Geo-supported prospects | % of min |
|---------|-------------:|------------------------:|---------:|
${PRIORITY_COUNTRIES_V6.map((c) => {
  const r = byCountry[c];
  return `| ${COUNTRY_PROSPECT_AMBITIONS_V6[c].label} | ${r.planningAmbitionMin} | ${r.distinctEvidenceBackedProspects} | ${r.progressTowardAmbitionMinPct}% |`;
}).join("\n")}

## Key findings
- **US dated CMBS**: no net-new extractable franchise/management expirations this wave (prior inventory exhaustion on current extractor formats).
- **MX/BR/GB/CA**: native regulator document chains still thin; SEC keyword harvest produced many geography-unverified rows (held, not counted toward country ambitions).
- **Track B**: HMLR ULPD authenticated (supporting); CH API key absent; CVM/BMV/SEDAR+ portals retrieved; no invented verified contract dates.
- Ownership enrichment remains deferred.

## Next wave recommendations
1. US — broaden CMBS HTML schedule extractor; harvest new annex formats.
2. MX — BMV/Emisnet issuer PDFs with named Mexican hotels + agreement clauses.
3. BR — CVM filing document download + condo-hotel vs HMA classification.
4. GB — Companies House document API once key present; UK transaction RNS.
5. CA — SEDAR+ exhibit/MD&A retrieval for hospitality REITs.
`;

fs.writeFileSync(path.join(OUT, "GEO_VERIFIED_PROSPECTS.json"), JSON.stringify({ count: geoVerified.length, hotels: geoVerified }, null, 2));
fs.writeFileSync(path.join(OUT, "GEO_UNVERIFIED_HOLD.json"), JSON.stringify({ count: geoUnverified.length, hotels: geoUnverified }, null, 2));
fs.writeFileSync(path.join(OUT, "COMBINED_METRICS.json"), JSON.stringify(summary, null, 2));
fs.writeFileSync(path.join(OUT, "FINAL_COUNTRY_SCORECARD.json"), JSON.stringify({ byCountry, examples, summary }, null, 2));
fs.writeFileSync(path.join(OUT, "FOUNDER_COUNTRY_CONCENTRATION_REPORT.md"), md);
fs.mkdirSync(B_OUT, { recursive: true });
fs.writeFileSync(path.join(B_OUT, "COMBINED_METRICS.json"), JSON.stringify(summary, null, 2));
fs.writeFileSync(path.join(B_OUT, "FOUNDER_COUNTRY_CONCENTRATION_REPORT.md"), md);
console.log(JSON.stringify({ progress, geoVerified: geoVerified.length, geoUnverified: geoUnverified.length, byCountry: Object.fromEntries(PRIORITY_COUNTRIES_V6.map((c) => [c, byCountry[c].distinctEvidenceBackedProspects])) }, null, 2));
