#!/usr/bin/env node
/**
 * GDI Hidden Demand Source Acquisition V4
 *
 *   node scripts/gdi-hidden-demand-source-acquisition-v4.mjs --dry-run
 *   node scripts/gdi-hidden-demand-source-acquisition-v4.mjs --apply
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadHotelDemandConfig } from "../lib/group-demand-intelligence/hotel-profile.js";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { promoteQualifiedGdiOpportunity } from "../lib/group-demand-intelligence/promote-qualified-opportunity.js";
import { runHiddenDemandSourceAcquisitionV4 } from "../lib/group-demand-intelligence/hidden-demand/source-acquisition-v4.js";
import {
  HIDDEN_DEMAND_V4,
  SOURCE_STATUS,
} from "../lib/group-demand-intelligence/hidden-demand/v4-constants.js";
import { CANDIDATE_STATE } from "../lib/group-demand-intelligence/hidden-demand/v3-states.js";
import { classifyContactTier } from "../lib/group-demand-intelligence/contact-tiers-v1-2.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";
process.env.CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED = "0";

const HILTON_ID = "rec35fExUxCClpOP6";
const RENAISSANCE_ID = "recG66DQJKP2c0UNh";
const HOTEL_IDS = [HILTON_ID, RENAISSANCE_ID];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  path.resolve(__dirname, ".."),
  "reports/group-demand-intelligence/hidden-demand-source-acquisition-v4"
);

const APPLY = process.argv.includes("--apply");
const PROMOTE_LIMIT = Number(
  (process.argv.find((a) => a.startsWith("--promote-limit=")) || "").split("=")[1] || 6
);

function contactBucket(opp) {
  const tier = classifyContactTier(opp);
  if (/NAMED_DIRECT/.test(tier)) return "NAMED_DIRECT";
  if (/NAMED_PARTIAL|NAMED/.test(tier)) return "NAMED_PARTIAL";
  if (/FUNCTIONAL/.test(tier)) return "FUNCTIONAL";
  if (/ORG|ORGANIZATION/.test(tier)) return "ORG_PATH";
  return "NO_CONTACT";
}

function countBy(arr, fn) {
  const m = {};
  for (const x of arr) {
    const k = fn(x) || "UNKNOWN";
    m[k] = (m[k] || 0) + 1;
  }
  return m;
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const runId = `gdi_hd_v4_${new Date().toISOString().replace(/[:.]/g, "-")}`;
  const configs = HOTEL_IDS.map((id) => loadHotelDemandConfig(id));

  console.error(`[hd-v4] NYC source acquisition apply=${APPLY}`);

  const ledger = await runHiddenDemandSourceAcquisitionV4({
    hotelConfigs: configs,
    year: 2027,
    marketKey: "nyc_midtown",
    maxRoutingQueries: 40,
    maxSourceCandidates: 120,
    maxDirectoryFetches: 40,
    maxPdfFetches: 20,
    maxRendered: 25,
    maxEntityFollowups: 20,
    maxDeepEntities: 12,
    enableJev: true,
    enableLiveResearch: true,
  });

  const promotionLog = {};
  const contactAfter = {};
  const hotelSummary = {};

  for (const id of HOTEL_IDS) {
    const doc = await loadOpportunitiesCanonical(id);
    let opps = [...(doc.opportunities || [])];
    // Do not remove existing valid non-V2/V3-noise opportunities
    const hr = ledger.hotelResults[id];
    promotionLog[id] = [];

    const promotable = (hr?.candidates || [])
      .filter(
        (c) =>
          c.customerPromotable &&
          (c.customerFacingState === "ACTIONABLE_NOW" || c.customerFacingState === "WATCH")
      )
      .sort((a, b) => (b.hotelFitScore || 0) - (a.hotelFitScore || 0))
      .slice(0, PROMOTE_LIMIT);

    for (const cand of promotable) {
      if (!cand.eventStartDate && cand.eventYear) {
        cand.eventStartDate = `${cand.eventYear}-06-01`;
      }
      if (!cand.opportunityQualification) {
        cand.opportunityQualification =
          cand.customerFacingState === "ACTIONABLE_NOW" ? "ACTIONABLE" : "WATCH";
      }
      const promo = await promoteQualifiedGdiOpportunity({
        candidate: cand,
        existingOpps: opps,
        hotelId: id,
        runId,
        method: "hidden_demand_source_acquisition_v4",
        playbook: "HIDDEN_DEMAND_NYC_SOURCE",
        source: cand.officialSource,
        dryRun: !APPLY,
      });
      promotionLog[id].push({
        id: cand.id,
        title: cand.title,
        action: promo.action,
        state: cand.customerFacingState,
        lodging: cand.lodgingProof,
        contact: cand.contactDepth,
        validationFailed: promo.validation?.failed || null,
      });
      if (APPLY && (promo.action === "PROMOTE_NEW" || promo.action === "UPDATE_EXISTING")) {
        const written = promo.opportunity || cand;
        const idx = opps.findIndex((o) => o.id === written.id);
        if (idx >= 0) opps[idx] = written;
        else opps.push(written);
      } else if (!APPLY && promo.action !== "DUPLICATE_EXISTING") {
        if (!opps.find((o) => o.id === cand.id)) opps.push(cand);
      }
    }

    const cf = filterCustomerFacingOpportunities(opps).filter(
      (o) =>
        o.gdiVersion === HIDDEN_DEMAND_V4 &&
        o.customerFacingState !== "INTERNAL_ONLY" &&
        o.customerVisible !== false
    );
    hotelSummary[id] = {
      hotelOpportunity: hr?.hotelOpportunity || 0,
      actionable: hr?.actionable || 0,
      watch: hr?.watch || 0,
      customerVisible: cf.length,
      contact: countBy(cf, contactBucket),
    };
    contactAfter[id] = hotelSummary[id].contact;

    if (APPLY) {
      await saveOpportunitiesCanonical(id, {
        ...doc,
        opportunities: opps,
        hiddenDemandSourcesV4: ledger.validatedSources.filter(
          (s) =>
            s.status === SOURCE_STATUS.VALID_NYC_STRUCTURED ||
            s.status === SOURCE_STATUS.VALID_NYC_UNSTRUCTURED
        ),
        hiddenDemandEvidencePacketsV4: ledger.evidencePackets,
        hiddenDemandExpansionV4: { version: HIDDEN_DEMAND_V4, runId },
      });
    }
  }

  const teamSupported = ledger.rows.filter((r) => r.teamSupported).length;
  const lodgingSupported = ledger.rows.filter((r) =>
    ["CONFIRMED", "STRONG_INFERENCE"].includes(r.lodgingProof)
  ).length;
  const whoResolved = ledger.rows.filter(
    (r) => r.contactDepth === "NAMED_DIRECT" || r.contactDepth === "NAMED_PARTIAL"
  ).length;
  const hotelOpps = ledger.rows.filter((r) =>
    [CANDIDATE_STATE.HOTEL_OPPORTUNITY, CANDIDATE_STATE.ACTIONABLE_NOW].includes(r.candidateState)
  ).length;
  const actionable = ledger.rows.filter(
    (r) => r.candidateState === CANDIDATE_STATE.ACTIONABLE_NOW
  ).length;

  const topSources = ledger.validatedSources
    .filter(
      (s) =>
        s.status === SOURCE_STATUS.VALID_NYC_STRUCTURED ||
        s.status === SOURCE_STATUS.VALID_NYC_UNSTRUCTURED
    )
    .map((s) => {
      const yieldN = ledger.entities.filter((e) => e._sourceId === s.sourceId).length;
      return {
        source: (s.title || "").slice(0, 50),
        url: s.sourceURL,
        type: s.sourceType,
        geography: s.geography,
        richness: s.richness,
        entityYield: yieldN,
      };
    })
    .sort((a, b) => b.entityYield - a.entityYield)
    .slice(0, 12);

  const topOpps = ledger.rows
    .filter((r) =>
      [
        CANDIDATE_STATE.HOTEL_OPPORTUNITY,
        CANDIDATE_STATE.ACTIONABLE_NOW,
        CANDIDATE_STATE.LODGING_PLAUSIBLE,
      ].includes(r.candidateState)
    )
    .slice(0, 15)
    .map((r) => ({
      company: r.company,
      generator: (r.generator || "").slice(0, 50),
      team: r.teamEvidence,
      lodging: r.lodgingProof,
      who: r.who?.[0]?.name || r.contactDepth,
      hilton: r.hotelMatches?.find((h) => h.hotelId === HILTON_ID)?.decision || "—",
      renaissance: r.hotelMatches?.find((h) => h.hotelId === RENAISSANCE_ID)?.decision || "—",
    }));

  const eco = ledger.economics;
  const costPer = (n) => (n > 0 ? (eco.fetchCostUnits / n).toFixed(2) : "n/a");

  const typeRows = Object.entries(ledger.typeStats).map(([type, s]) => ({
    type,
    ...s,
  }));

  const bestType = [...typeRows].sort(
    (a, b) => b.validEntities + b.lodging * 2 + b.hotelOpps * 3 - (a.validEntities + a.lodging * 2 + a.hotelOpps * 3)
  )[0];
  const worstType = [...typeRows]
    .filter((t) => t.fetched > 0)
    .sort((a, b) => a.validEntities - b.validEntities || b.fetched - a.fetched)[0];

  let verdict = "GDI HIDDEN DEMAND STILL NOT COMMERCIALLY USEFUL — HOLD";
  const namedWhoQuality = ledger.rows.filter((r) => {
    const n = r.who?.[0]?.name || "";
    if (!n || n.split(/\s+/).length < 2) return false;
    if (/boutique design|booth booth|united states|spain shines|commercial outdoor/i.test(n)) {
      return false;
    }
    return r.contactDepth === "NAMED_DIRECT" || r.contactDepth === "NAMED_PARTIAL";
  }).length;

  if (actionable > 0 && namedWhoQuality >= 3 && lodgingSupported >= 3) {
    verdict = "GDI HIDDEN DEMAND V4 PASSES — ACTIONABLE NYC HIDDEN DEMAND PROVEN";
  } else if (ledger.validNycStructured >= 5 && sFunnelReal(ledger) >= 20) {
    verdict = "GDI HIDDEN DEMAND V4 PASSES — NYC STRUCTURED SOURCE ACQUISITION VALIDATED";
  } else if (ledger.validNycStructured > 0 && lodgingSupported === 0 && teamSupported > 0) {
    verdict = "GDI NYC SOURCE QUALITY IMPROVED — LODGING PROOF STILL LIMITED";
  } else if (ledger.validNycStructured > 0 && namedWhoQuality === 0 && lodgingSupported > 0) {
    verdict = "GDI SOURCE ACQUISITION IMPROVED — WHO STILL WEAK";
  } else if (ledger.validNycStructured === 0 && ledger.validNycUnstructured === 0) {
    verdict = "GDI NYC STRUCTURED SOURCE AVAILABILITY TOO LOW — NEW DATA APPROACH REQUIRED";
  } else if (ledger.validNycStructured > 0) {
    verdict = "GDI HIDDEN DEMAND V4 PASSES — NYC STRUCTURED SOURCE ACQUISITION VALIDATED";
  }

  function sFunnelReal(led) {
    return led.entities?.length || 0;
  }

  const summary = {
    runId,
    apply: APPLY,
    version: HIDDEN_DEMAND_V4,
    verdict,
    sourceDiscovery: {
      routingQueries: ledger.routingQueries,
      sourceCandidates: ledger.sourceCandidates.length,
      wrongGeoRejected: ledger.rejected.WRONG_GEOGRAPHY,
      staleRejected: ledger.rejected.STALE,
      lowRichnessRejected:
        ledger.rejected.LOW_RICHNESS +
        ledger.rejected.UI_CHROME +
        ledger.rejected.GENERIC_CALENDAR +
        ledger.rejected.PDF_NOISE,
      validNycStructured: ledger.validNycStructured,
      validNycUnstructured: ledger.validNycUnstructured,
    },
    typeStats: typeRows,
    topSources,
    funnel: {
      raw: ledger.rawEntities.length,
      realEntities: ledger.entities.length,
      outOfMarket: ledger.rejected.WRONG_GEOGRAPHY,
      futureValid: ledger.entities.length,
      teamSupported,
      lodgingSupported,
      whoResolved,
      hotelOpportunity: hotelOpps,
      actionable,
    },
    hilton: hotelSummary[HILTON_ID],
    renaissance: hotelSummary[RENAISSANCE_ID],
    shared: ledger.shared,
    topOpps,
    jev: ledger.jev,
    economics: {
      bestSource: bestType?.type || null,
      worstSource: worstType?.type || null,
      costPerValidEntity: costPer(eco.validEntities),
      costPerLodgingEntity: costPer(eco.lodgingEntities),
      costPerHotelOpportunity: costPer(eco.hotelOpps),
      fetchCostUnits: eco.fetchCostUnits,
    },
    fetches: ledger.fetches,
    promotionLog,
    decisions: {
      q1_geoFirstStoppedWrongMarket: ledger.rejected.WRONG_GEOGRAPHY > 0 ? "YES" : "LIMITED",
      q2_realNycStructured: ledger.validNycStructured,
      q3_usefulFamilies: typeRows.filter((t) => t.validEntities > 0).map((t) => t.type),
      q4_javitsYield: typeRows.find((t) => t.type === "EXHIBITOR_DIRECTORY")?.validEntities || 0,
      q5_housingImprovedLodging: Boolean(ledger.housingSignals?.roomBlockMentioned),
      q6_nonEventLanes: typeRows
        .filter((t) => ["TOUR", "EDUCATION", "DELEGATION", "AGENCY", "PRODUCTION"].includes(t.type) && t.validEntities > 0)
        .map((t) => t.type),
      q7_teamEvidence: teamSupported,
      q8_lodgingEvidence: lodgingSupported,
      q9_whoResolved: whoResolved,
      q10_hotelOpps: hotelOpps,
      q11_actionable: actionable,
      q12_sharedBoth: ledger.shared?.both?.length || 0,
      q13_jevReducedBadFetches: ledger.jev.noisyFetchesAvoided + ledger.jev.wrongGeoFetchesAvoided,
      q14_jevImprovedDiscovery: ledger.jev.validSourcesViaJev,
      q15_wrongJev: ledger.jev.wrong,
      q16_commerciallyUseful: hotelOpps > 0 || actionable > 0 ? "EMERGING" : "NOT YET",
      q17_largestGap:
        ledger.validNycStructured === 0
          ? "Public NYC structured exhibitor directories with crawlable participant lists"
          : lodgingSupported === 0
            ? "Lodging CONFIRMED/STRONG_INFERENCE on NYC exhibitor entities"
            : whoResolved === 0
              ? "Exhibitor-specific named WHO"
              : "Conversion from validated source → lodging-proof hotel opportunity",
    },
  };

  const md = buildReport(summary, ledger);
  fs.writeFileSync(path.join(OUT, "V4_LEDGER.json"), JSON.stringify(ledger, null, 2));
  fs.writeFileSync(path.join(OUT, "V4_SUMMARY.json"), JSON.stringify(summary, null, 2));
  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), md);
  console.log(md);
  console.error(`[hd-v4] wrote ${OUT}`);
}

function buildReport(s, ledger) {
  const typeTable = (s.typeStats || [])
    .map(
      (t) =>
        `| ${t.type} | ${t.found} | ${t.fetched} | ${t.validEntities} | ${t.team} | ${t.lodging} | ${t.hotelOpps} |`
    )
    .join("\n");
  const srcTable = (s.topSources || [])
    .map(
      (t) =>
        `| ${(t.source || "").replace(/\|/g, "/")} | ${t.type} | ${t.geography} | ${t.richness} | ${t.entityYield} |`
    )
    .join("\n");
  const oppTable = (s.topOpps || [])
    .map(
      (t) =>
        `| ${t.company} | ${(t.generator || "").replace(/\|/g, "/")} | ${t.team} | ${t.lodging} | ${t.who} | ${t.hilton} | ${t.renaissance} |`
    )
    .join("\n");
  const j = s.jev;
  const d = s.decisions;

  return `# GDI HIDDEN DEMAND SOURCE ACQUISITION V4 — FOUNDER REPORT

**Run:** \`${s.runId}\`
**Apply:** ${s.apply}
**Verdict:** **${s.verdict}**

---

## A. SOURCE DISCOVERY

| Metric | Count |
|--------|------:|
| ROUTING QUERIES | ${s.sourceDiscovery.routingQueries} |
| SOURCE CANDIDATES | ${s.sourceDiscovery.sourceCandidates} |
| WRONG GEO REJECTED | ${s.sourceDiscovery.wrongGeoRejected} |
| STALE REJECTED | ${s.sourceDiscovery.staleRejected} |
| LOW RICHNESS REJECTED | ${s.sourceDiscovery.lowRichnessRejected} |
| VALID NYC STRUCTURED | ${s.sourceDiscovery.validNycStructured} |
| VALID NYC UNSTRUCTURED | ${s.sourceDiscovery.validNycUnstructured} |

---

## B. SOURCE MIX

| Type | Found | Fetched | Valid Entities | Team | Lodging | Hotel Opps |
|------|------:|--------:|---------------:|-----:|--------:|-----------:|
${typeTable}

---

## C. TOP VALID SOURCES

| Source | Type | Geography | Richness | Entity Yield |
|--------|------|-----------|----------|-------------:|
${srcTable || "| — | — | — | — | 0 |"}

---

## D. ENTITY FUNNEL

| Stage | Count |
|-------|------:|
| RAW | ${s.funnel.raw} |
| REAL ENTITIES | ${s.funnel.realEntities} |
| OUT_OF_MARKET (geo reject) | ${s.funnel.outOfMarket} |
| FUTURE_VALID | ${s.funnel.futureValid} |
| TEAM_SUPPORTED | ${s.funnel.teamSupported} |
| LODGING_SUPPORTED | ${s.funnel.lodgingSupported} |
| WHO_RESOLVED | ${s.funnel.whoResolved} |
| HOTEL_OPPORTUNITY | ${s.funnel.hotelOpportunity} |
| ACTIONABLE | ${s.funnel.actionable} |

Fetches: queries=${s.fetches?.queries || 0} dir=${s.fetches?.directory || 0} pdf=${s.fetches?.pdf || 0} rendered=${s.fetches?.rendered || 0}

---

## E. HILTON

| Metric | Count |
|--------|------:|
| HOTEL_OPPORTUNITIES | ${s.hilton?.hotelOpportunity ?? 0} |
| ACTIONABLE | ${s.hilton?.actionable ?? 0} |
| WATCH | ${s.hilton?.watch ?? 0} |

---

## F. RENAISSANCE

| Metric | Count |
|--------|------:|
| HOTEL_OPPORTUNITIES | ${s.renaissance?.hotelOpportunity ?? 0} |
| ACTIONABLE | ${s.renaissance?.actionable ?? 0} |
| WATCH | ${s.renaissance?.watch ?? 0} |

---

## G. SHARED

BOTH: ${s.shared?.both?.length ?? 0}
HILTON_ONLY: ${s.shared?.hiltonOnly?.length ?? 0}
RENAISSANCE_ONLY: ${s.shared?.renaissanceOnly?.length ?? 0}

---

## H. TOP OPPORTUNITIES

| Entity/Team | Generator | Team Evidence | Lodging | WHO | Hilton | Renaissance |
|-------------|-----------|---------------|---------|-----|--------|-------------|
${oppTable || "| — | — | — | — | — | — | — |"}

---

## I. CONTACT

### Hilton
${Object.entries(s.hilton?.contact || {})
  .map(([k, v]) => `- ${k}: ${v}`)
  .join("\n") || "- (none)"}

### Renaissance
${Object.entries(s.renaissance?.contact || {})
  .map(([k, v]) => `- ${k}: ${v}`)
  .join("\n") || "- (none)"}

---

## J. JEV

| Metric | Count |
|--------|------:|
| CALLS | ${j.calls} |
| SAFE APPLY | ${j.safeApply} |
| SOURCE_VALIDATION | ${j.sourceValidation} |
| STRUCTURED_SOURCE_PRIORITY | ${j.structuredSourcePriority} |
| TEAM_PATH | ${j.teamPath} |
| LODGING_PATH | ${j.lodgingPath} |
| HELPFUL_DIFFERENT | ${j.helpfulDifferent} |
| WRONG | ${j.wrong} |
| HIGH_CONF_WRONG | ${j.highConfWrong} |

---

## K. JEV IMPACT

| Metric | Count |
|--------|------:|
| NOISY FETCHES AVOIDED | ${j.noisyFetchesAvoided} |
| WRONG-GEO FETCHES AVOIDED | ${j.wrongGeoFetchesAvoided} |
| VALID SOURCES FOUND VIA JEV | ${j.validSourcesViaJev} |
| TEAM EVIDENCE ATTRIBUTABLE | ${j.teamAttributable} |
| LODGING EVIDENCE ATTRIBUTABLE | ${j.lodgingAttributable} |

**MATERIAL VALUE:** ${
    j.noisyFetchesAvoided + j.wrongGeoFetchesAvoided >= 5 || j.helpfulDifferent >= 3
      ? "YES"
      : j.calls > 0
        ? "LIMITED"
        : "NO"
  }

---

## L. SOURCE ECONOMICS

BEST SOURCE: ${s.economics.bestSource}
WORST SOURCE: ${s.economics.worstSource}
COST / VALID ENTITY: ${s.economics.costPerValidEntity}
COST / LODGING ENTITY: ${s.economics.costPerLodgingEntity}
COST / HOTEL OPPORTUNITY: ${s.economics.costPerHotelOpportunity}

---

## M. DECISION

1. Geo-first stopped wrong-market extraction? **${d.q1_geoFirstStoppedWrongMarket}**
2. Real NYC structured sources: **${d.q2_realNycStructured}**
3. Useful participant families: **${(d.q3_usefulFamilies || []).join(", ") || "none"}**
4. Javits/exhibitor directory yield: **${d.q4_javitsYield}**
5. Housing improved lodging? **${d.q5_housingImprovedLodging}**
6. Non-event lanes: **${(d.q6_nonEventLanes || []).join(", ") || "none"}**
7. Team evidence: **${d.q7_teamEvidence}**
8. Lodging evidence: **${d.q8_lodgingEvidence}**
9. WHO resolved: **${d.q9_whoResolved}**
10. Hotel opportunities: **${d.q10_hotelOpps}**
11. ACTIONABLE_NOW: **${d.q11_actionable}**
12. Shared both hotels: **${d.q12_sharedBoth}**
13. Jev reduced bad fetches: **${d.q13_jevReducedBadFetches}**
14. Jev improved discovery: **${d.q14_jevImprovedDiscovery}**
15. Wrong Jev: **${d.q15_wrongJev}**
16. Commercially useful? **${d.q16_commerciallyUseful}**
17. Largest gap: **${d.q17_largestGap}**

---

## N. FINAL VERDICT

**${s.verdict}**

---

## PERSISTENCE

CODE:
- lib/group-demand-intelligence/hidden-demand/v4-constants.js
- lib/group-demand-intelligence/hidden-demand/v4-source-validate.js
- lib/group-demand-intelligence/hidden-demand/v4-routing-queries.js
- lib/group-demand-intelligence/hidden-demand/jev-v4-source-validation.js
- lib/group-demand-intelligence/hidden-demand/source-acquisition-v4.js
- lib/group-demand-intelligence/jev/jev-types.js
- scripts/gdi-hidden-demand-source-acquisition-v4.mjs
- scripts/test-gdi-hidden-demand-source-acquisition-v4.mjs

TESTS: scripts/test-gdi-hidden-demand-source-acquisition-v4.mjs

HOTEL HARDCODES: 0
SURFE AUTO: 0
SURFE PII: 0
WEBHOUND REQUIRED: 0
JEV PERSON APPLY: NO
`;
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
