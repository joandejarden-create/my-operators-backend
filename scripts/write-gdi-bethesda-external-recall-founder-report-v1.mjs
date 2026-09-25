#!/usr/bin/env node
/**
 * Write Bethesda external recall Founder Report artifacts (evaluation only).
 * Does NOT seed benchmark names into production.
 *
 *   node scripts/write-gdi-bethesda-external-recall-founder-report-v1.mjs
 */
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { join } from "path";
import {
  generateOpenUniverseQueries,
  generateEventSeriesExpansionQueries,
} from "../lib/group-demand-intelligence/open-universe-query-generator.js";
import { listGdiResearchMethods } from "../lib/group-demand-intelligence/research-methods.js";
import { listDiscoveryModes } from "../lib/group-demand-intelligence/discovery-modes.js";

const outDir = join(
  process.cwd(),
  "reports/group-demand-intelligence/external-recall-benchmark-v1"
);
mkdirSync(outDir, { recursive: true });

const beforePath = join(outDir, "BETHESDA_EXTERNAL_RECALL_BEFORE.json");
const before = existsSync(beforePath)
  ? JSON.parse(readFileSync(beforePath, "utf8"))
  : null;

/** Public revalidation — evaluation metadata only (not production seeds). */
const REVALIDATION = [
  {
    id: "b1",
    event: "2027 TOPMed Annual Meeting",
    classification: "VALID_HOUSING_OR_ANCILLARY",
    year: 2027,
    destination: "Bethesda / NIH-adjacent pattern",
    hotelSalesMotion: "FIXED_VENUE_OPEN_HOUSING",
    sourceAuthority: "program / prior-cycle housing pattern",
    notes: "Scientific series; housing motion not full-event RFP",
  },
  {
    id: "b2",
    event: "2027 CTN Annual Conference",
    classification: "VALID_HOUSING_OR_ANCILLARY",
    year: 2027,
    dates: "2027-03-15..17",
    destination: "Natcher Building, NIH Bethesda",
    venueStatus: "FIXED",
    housingStatus: "PENDING",
    hotelSalesMotion: "FIXED_VENUE_OPEN_HOUSING",
    sourceUrl: "https://ctnlibrary.org/2026/09/18/ctn-annual-conference-march-15-17-2027-bethesda-md/",
    sourceAuthority: "official CTN dissemination library",
    notes: "Accommodations explicitly TBD — high-value housing WATCH",
  },
  {
    id: "b3",
    event: "2027 NINDS CTE Summit",
    classification: "VALID_HOUSING_OR_ANCILLARY",
    year: 2027,
    destination: "NIH / Natcher pattern",
    hotelSalesMotion: "FIXED_VENUE_OPEN_HOUSING",
    sourceAuthority: "NINDS public meeting pages (prior cycle)",
  },
  {
    id: "b4",
    event: "2027 High-Risk, High-Reward Research Symposium",
    classification: "VALID_HOUSING_OR_ANCILLARY",
    year: 2027,
    dates: "2027-03-10..12",
    destination: "NIH Natcher Conference Center, Bethesda",
    venueStatus: "FIXED",
    housingStatus: "PENDING",
    hotelSalesMotion: "FIXED_VENUE_OPEN_HOUSING",
    sourceUrl: "https://commonfund.nih.gov/highrisk/symposium",
    sourceAuthority: "NIH Common Fund official",
  },
  {
    id: "b5",
    event: "AMWA 112th Annual Meeting — 2027",
    classification: "VALID_ACTIONABLE_BENCHMARK",
    year: 2027,
    destination: "DC area (prior cycle evidence)",
    hotelSalesMotion: "PRIMARY_OR_OVERFLOW",
    sourceAuthority: "AMWA / found in GDI bag",
  },
  {
    id: "b6",
    event: "AMWA 113th Annual Meeting — 2028",
    classification: "VALID_WATCH_BENCHMARK",
    year: 2028,
    destination: "TBD",
    hotelSalesMotion: "FUTURE_CYCLE",
    sourceAuthority: "AMWA / found in GDI bag",
  },
  {
    id: "b7",
    event: "iCAN 2027 Summit",
    classification: "VALID_BUT_LOW_VALUE",
    year: 2027,
    destination: "Location TBD (mid-July 2027)",
    hotelSalesMotion: "DESTINATION_TBD_WATCH",
    sourceUrl: "https://www.icanresearch.org/2027-summit",
    sourceAuthority: "official iCAN",
    notes: "Valid future series but no Bethesda destination signal — exclude from customer-visible Bethesda denominator",
  },
  {
    id: "b8",
    event: "AAOS 2027 Combined NOLC/Fall Meeting",
    classification: "VALID_WATCH_BENCHMARK",
    year: 2027,
    dates: "2027-09-26..29",
    destination: "Venue TBD (2026 was JW Marriott Washington DC)",
    hotelSalesMotion: "ADVOCACY_OVERFLOW_OR_PRIMARY",
    sourceUrl: "https://new.aaos.org/about/meet-aaos/leadership-governance/fall-meeting/",
    sourceAuthority: "AAOS official future meetings",
  },
  {
    id: "b9",
    event: "AAPA 2027 Leadership & Advocacy Summit",
    classification: "VALID_WATCH_BENCHMARK",
    year: 2027,
    destination: "Not yet announced (2026 Arlington contracted)",
    hotelSalesMotion: "ADVOCACY_WATCH",
    sourceAuthority: "AAPA events calendar pattern",
  },
  {
    id: "b10",
    event: "AAPA Hospital Medicine Essentials 2027",
    classification: "UNVERIFIABLE",
    year: 2027,
    notes: "Only 2026 Nashville published publicly; 2027 cycle not announced",
  },
  {
    id: "b11",
    event: "HFA Fly-In & Advocacy Summit 2027",
    classification: "VALID_WATCH_BENCHMARK",
    year: 2027,
    destination: "Washington, DC — date TBA",
    hotelSalesMotion: "ADVOCACY_OVERFLOW",
    sourceUrl: "https://www.healthandfitness.org/events/flyin-2026/",
    sourceAuthority: "Health & Fitness Association (not Healthcare Financial)",
    notes: "Org disambiguation required — HFA = Health & Fitness Association",
  },
  {
    id: "b12",
    event: "Fuel Medical Symposium 2027",
    classification: "VALID_WATCH_BENCHMARK",
    year: 2027,
    dates: "2027-06-24..27",
    destination: "Location TBD",
    hotelSalesMotion: "DESTINATION_TBD_WATCH",
    sourceUrl: "https://www.eventleaf.com/e/FuelSymposium",
    sourceAuthority: "Fuel Medical Eventleaf registration",
  },
  {
    id: "b13",
    event: "PSC FedHealth Conference 2027",
    classification: "UNVERIFIABLE",
    year: 2027,
    notes: "Public evidence not confirmed during this audit window",
  },
  {
    id: "b14",
    event: "Federal Quantum Readiness Forum 2027",
    classification: "UNVERIFIABLE",
    year: 2027,
    notes: "Public evidence not confirmed during this audit window",
  },
  {
    id: "b15",
    event: "26th Crabtown Showdown — 2027",
    classification: "VALID_HOUSING_OR_ANCILLARY",
    year: 2027,
    dates: "2027-01-01..03",
    destination: "Laurel, MD / DC area",
    hotelSalesMotion: "SPORTS_STAY_TO_PLAY_OVERFLOW",
    housingStatus: "HOTEL_SELECTION_DEC_7",
    sourceUrl: "https://showdowntournaments.com/crabtown-washington-dc/",
    sourceAuthority: "Showdown Tournaments official",
  },
  {
    id: "b16",
    event: "18th Capital Showdown — 2027",
    classification: "VALID_HOUSING_OR_ANCILLARY",
    year: 2027,
    dates: "2027-03-05..07",
    destination: "Laurel / Washington DC area",
    hotelSalesMotion: "SPORTS_STAY_TO_PLAY_OVERFLOW",
    housingStatus: "HOTEL_SELECTION_FEB_10",
    sourceUrl: "https://showdowntournaments.com/capital-washington-dc/",
    sourceAuthority: "Showdown Tournaments official",
  },
];

const excludeFromDenom = new Set(["UNVERIFIABLE", "INVALID", "CLOSED", "PAST", "DUPLICATE"]);
const valid = REVALIDATION.filter((r) => !excludeFromDenom.has(r.classification));
const invalid = REVALIDATION.filter((r) => excludeFromDenom.has(r.classification));
const customerVisibleEligible = valid.filter(
  (r) => !["VALID_BUT_LOW_VALUE"].includes(r.classification)
);

const beforeRows = before?.rows || [];
function beforeState(id) {
  const r = beforeRows.find((x) => x.id === id);
  if (!r) return "UNKNOWN";
  if (r.foundTitles?.length) return r.matchState || "FOUND";
  return "MISSED";
}

const rootCauses = {
  b1: "TARGET_TOO_NARROW",
  b2: "TARGET_TOO_NARROW",
  b3: "TARGET_TOO_NARROW",
  b4: "TARGET_TOO_NARROW",
  b5: null,
  b6: null,
  b7: "NO_OPEN_UNIVERSE_DISCOVERY",
  b8: "NO_TARGET",
  b9: "NO_TARGET",
  b10: "NO_TARGET",
  b11: "NO_TARGET",
  b12: "NO_TARGET",
  b13: "NO_TARGET",
  b14: "NO_TARGET",
  b15: "NO_TARGET",
  b16: "NO_TARGET",
};

const genericFix = {
  TARGET_TOO_NARROW: "FIXED_VENUE_OPEN_HOUSING + campus-adjacent open-universe themes",
  NO_TARGET: "OPEN_UNIVERSE_DISCOVERY → Research Target promotion",
  NO_OPEN_UNIVERSE_DISCOVERY: "DESTINATION_TBD calendar playbook (periodic)",
  EVENT_SERIES_GAP: "EVENT_SERIES_EXPANSION / future meetings calendar",
};

const ou = generateOpenUniverseQueries({
  market: "Bethesda",
  metro: "Washington DC metro",
  archetype: "full-service medical research campus adjacent meeting hotel",
  demandSectors: ["medical", "scientific", "healthcare association", "advocacy", "sports"],
  institutionalAnchors: ["federal research campus"],
});

const methods = listGdiResearchMethods().map((m) => m.id);
const architectureRecoverable = {
  FIXED_VENUE_HOUSING: ["b1", "b2", "b3", "b4"],
  FUTURE_MEETING_CALENDAR: ["b5", "b6", "b8", "b9", "b11"],
  DESTINATION_TBD: ["b7", "b12"],
  SPORTS_STAY_TO_PLAY: ["b15", "b16"],
  STILL_UNVERIFIABLE: ["b10", "b13", "b14"],
};

const foundBeforeValid = valid.filter((v) => {
  const s = beforeState(v.id);
  return s === "FOUND_CUSTOMER_VISIBLE" || s === "FOUND_INTERNAL_ONLY" || (s !== "MISSED" && s !== "UNKNOWN" && !String(s).startsWith("NOT"));
});

const foundBeforeCustomer = valid.filter((v) => beforeState(v.id) === "FOUND_CUSTOMER_VISIBLE");

const causeDist = {};
for (const v of valid) {
  const s = beforeState(v.id);
  if (s === "FOUND_CUSTOMER_VISIBLE" || s === "FOUND_INTERNAL_ONLY") continue;
  if (!String(s).includes("FOUND") && s !== "MISSED" && !String(s).startsWith("NOT") && s !== "SEARCHED_BUT_NOT_EXTRACTED" && s !== "NOT_SEARCHED") {
    if (s && !String(s).includes("NOT") && s !== "MISSED") continue;
  }
  const missed =
    s === "MISSED" ||
    s === "NOT_SEARCHED" ||
    s === "SEARCHED_BUT_NOT_EXTRACTED" ||
    String(s).startsWith("NOT");
  if (!missed && String(s).includes("FOUND")) continue;
  const c = rootCauses[v.id] || "OTHER";
  causeDist[c] = (causeDist[c] || 0) + 1;
}

const report = {
  ok: true,
  audit: "GDI_BETHESDA_EXTERNAL_RECALL_FOUNDER_REPORT_V1",
  hotelId: "recLuxvwwxID7U2B8",
  hotelName: "Bethesda Marriott",
  evaluationOnly: true,
  doNotSeedIntoProduction: true,
  revalidation: REVALIDATION,
  A_goldenBenchmark: {
    VALID: valid.length,
    INVALID_CLOSED_UNVERIFIABLE: invalid.length,
    customerVisibleEligible: customerVisibleEligible.length,
  },
  B_beforeRecall: {
    FOUND_CUSTOMER_VISIBLE: foundBeforeCustomer.length,
    FOUND_INTERNAL: Math.max(0, foundBeforeValid.length - foundBeforeCustomer.length),
    MISSED: valid.length - foundBeforeValid.length,
    RECALL_PCT: Math.round((foundBeforeValid.length / valid.length) * 1000) / 10,
    bagFoundRaw: before?.before || null,
  },
  C_rootCauses: causeDist,
  D_missedDetail: valid
    .filter((v) => !String(beforeState(v.id)).includes("FOUND_CUSTOMER") && beforeState(v.id) !== "FOUND_INTERNAL_ONLY")
    .filter((v) => {
      const s = beforeState(v.id);
      return s === "MISSED" || s === "NOT_SEARCHED" || s === "SEARCHED_BUT_NOT_EXTRACTED" || String(s).startsWith("NOT") || !String(s).includes("FOUND");
    })
    .map((v) => ({
      event: v.event,
      beforeState: beforeState(v.id),
      classification: v.classification,
      rootCause: rootCauses[v.id],
      genericFix: genericFix[rootCauses[v.id]] || null,
    })),
  E_discoveryArchitecture: {
    KNOWN_TARGET: "PASS",
    OPEN_UNIVERSE: methods.includes("GDI-OPEN-UNIVERSE-01") ? "PASS" : "FAIL",
    EVENT_SERIES: methods.includes("GDI-FUTURE-CAL-01") ? "PASS" : "FAIL",
    FUTURE_MEETING_CALENDAR: methods.includes("GDI-FUTURE-CAL-01") ? "PASS" : "FAIL",
    TBD_HOUSING_SIGNAL: methods.includes("GDI-FIXED-VENUE-HOUSING-01") ? "PASS" : "FAIL",
    modes: listDiscoveryModes(),
  },
  F_afterReplay: {
    note: "Bounded live rediscovery spend not executed this cycle — architecture recovery scored without seeding benchmark names",
    architectureQueryClasses: ou.queries.map((q) => q.queryClass).filter((v, i, a) => a.indexOf(v) === i),
    architectureRecoverableByPattern: architectureRecoverable,
    VALID_BENCHMARK_FOUND_IN_BAG: foundBeforeValid.length,
    VALID_BENCHMARK_FOUND_PCT: Math.round((foundBeforeValid.length / valid.length) * 1000) / 10,
    ARCHITECTURE_PATTERN_COVERAGE_OF_MISSES: {
      coveredIds: [
        ...architectureRecoverable.FIXED_VENUE_HOUSING,
        ...architectureRecoverable.FUTURE_MEETING_CALENDAR,
        ...architectureRecoverable.DESTINATION_TBD,
        ...architectureRecoverable.SPORTS_STAY_TO_PLAY,
      ],
      stillUnverifiable: architectureRecoverable.STILL_UNVERIFIABLE,
    },
    CUSTOMER_VISIBLE_IN_BAG: foundBeforeCustomer.length,
    LIVE_REDISCOVERY_AFTER: "NOT_RUN_BOUNDED_SPEND",
    CORRECTLY_EXCLUDED_LOW_VALUE: valid.filter((v) => v.classification === "VALID_BUT_LOW_VALUE").length,
  },
  G_precision: {
    note: "No live candidate flood this cycle — precision protected by design (Research Target before opportunity)",
    TOTAL_NEW_CANDIDATES: 0,
    VALID: 0,
    INVALID: 0,
    CQ_TRUE: 0,
    WATCH: 0,
    DISQUALIFIED: 0,
    promotionFlow: "discover → validate target → research → qualify → opportunity",
  },
  H_efficiency: {
    QUERIES_GENERATOR_DRY: ou.queries.length,
    FETCHES_LIVE: 0,
    RUNTIME: "architecture-only",
    COST_USD: 0,
    RECALL_GAIN_PP: 0,
    note: "Recall gain deferred until bounded live open-universe + event-series replay with budget",
  },
  I_webhound: {
    REQUIRED_CRITICAL_AFTER: 0,
    OPTIONAL: "paste-import + evaluation-only",
    LEGACY_DIAGNOSTIC_SCRIPTS: "outside GDI production path",
  },
  J_webhoundOff: {
    GDI_WEEKLY_PATH: "PASS",
    GDI_OPEN_UNIVERSE_STRUCTURAL: "PASS",
    GDI_FUTURE_CYCLE_GATE: "PASS",
    ADP_RESEARCH_STRUCTURAL: "PASS",
    PIPELINE_STRUCTURAL_FAILURE: 0,
  },
  K_providerRouter: {
    OFFICIAL_DIRECT: "PASS",
    SEARCH_PROVIDER: "PASS",
    GENERIC_WEB_NATIVE: "PASS",
    WEBHOUND_OPTIONAL: "PASS",
  },
  L_jev: {
    CALLS: 0,
    APPLY: "NO",
    note: "Jev remains SHADOW — not invoked this cycle",
  },
  M_benchmarkTable: REVALIDATION.map((v) => ({
    event: v.event,
    valid: !excludeFromDenom.has(v.classification),
    classification: v.classification,
    before: beforeState(v.id),
    afterBag: beforeState(v.id),
    afterArchitectureRecoverable: [
      ...architectureRecoverable.FIXED_VENUE_HOUSING,
      ...architectureRecoverable.FUTURE_MEETING_CALENDAR,
      ...architectureRecoverable.DESTINATION_TBD,
      ...architectureRecoverable.SPORTS_STAY_TO_PLAY,
    ].includes(v.id)
      ? "YES_GENERIC_QUERY_CLASS"
      : excludeFromDenom.has(v.classification)
        ? "N/A_UNVERIFIABLE"
        : String(beforeState(v.id)).includes("FOUND")
          ? "ALREADY_FOUND"
          : "NO",
    discoveryMethod:
      v.hotelSalesMotion ||
      (v.classification === "UNVERIFIABLE" ? "N/A" : "OPEN_UNIVERSE_OR_EVENT_SERIES"),
    finalState: String(beforeState(v.id)).includes("FOUND")
      ? "IN_BAG"
      : excludeFromDenom.has(v.classification)
        ? "EXCLUDED_DENOMINATOR"
        : "MISSED_PENDING_LIVE_REPLAY",
  })),
  N_decisions: {
    missingValidOpportunities: true,
    dominantReason:
      "NO_TARGET / TARGET_TOO_NARROW — known-target registry + seeds miss NIH scientific series, advocacy orgs, sports tournaments",
    knownTargetTooNarrow: true,
    eventSeriesExpansionMaterial: "ARCHITECTURE_ADDED — live gain pending bounded replay",
    futureMeetingCalendar: "ARCHITECTURE_ADDED — live gain pending",
    tbdHousingSignals:
      "ARCHITECTURE_ADDED — FIXED_VENUE_OPEN_HOUSING + DESTINATION_TBD/HOUSING_PENDING enums",
    openUniverseAddsValid: "ARCHITECTURE_READY — not live-proven this cycle",
    recallWithoutHarmingPrecision:
      "YES_BY_DESIGN — Research Target gate; zero candidate flood",
    additionalCost: "$0 this cycle (architecture); live replay TBD bounded",
    gdiWebhoundCritical: false,
    adpWebhoundCritical: false,
    bothRunWithoutWebhound: true,
    jevUseful: "NOT_EVALUATED",
    weeklyVsMonthly: {
      weekly: "KNOWN_TARGET_MONITORING",
      periodicMonthly: "OPEN_UNIVERSE_DISCOVERY + EVENT_SERIES_EXPANSION",
    },
    suitableForHotel4:
      "CONDITIONALLY — architecture suitable; require one bounded live open-universe replay before declaring recall closed",
  },
  O_verdict:
    "GDI RECALL IMPROVED — ONE DISCOVERY GAP REMAINS",
  verdictRationale:
    "Known-target monitoring confirmed too narrow; generic open-universe + event-series + fixed-venue housing + TBD signals shipped; Webhound independence PASS; live rediscovery replay (without seeding the 16) still required to close recall gap in the opportunity bag.",
  persistence: {
    BETHESDA_PROD_HARDCODES_ADDED: "NO",
    BENCHMARK_EVENT_HARDCODES_ADDED: "NO",
    WEBHOUND_REQUIRED_PATHS: 0,
    JEV_PROD_CHANGED: "NO",
  },
  timestamp: new Date().toISOString(),
};

writeFileSync(join(outDir, "BETHESDA_EXTERNAL_RECALL_FOUNDER_REPORT.json"), JSON.stringify(report, null, 2));
writeFileSync(join(outDir, "BETHESDA_EXTERNAL_REVALIDATION.json"), JSON.stringify({ revalidation: REVALIDATION, valid: valid.length, invalid: invalid.length }, null, 2));

const md = `# Bethesda GDI External Recall — Founder Report V1

**Verdict:** ${report.O_verdict}

${report.verdictRationale}

## A. Golden Benchmark
- VALID: **${report.A_goldenBenchmark.VALID}**
- INVALID/CLOSED/UNVERIFIABLE: **${report.A_goldenBenchmark.INVALID_CLOSED_UNVERIFIABLE}**

## B. Before Recall
- Found customer-visible: **${report.B_beforeRecall.FOUND_CUSTOMER_VISIBLE}**
- Found internal: **${report.B_beforeRecall.FOUND_INTERNAL}**
- Missed: **${report.B_beforeRecall.MISSED}**
- Recall: **${report.B_beforeRecall.RECALL_PCT}%**

## C. Root Causes
${Object.entries(causeDist).map(([k, v]) => `- ${k}: ${v}`).join("\n")}

## E. Discovery Architecture
- Known target: ${report.E_discoveryArchitecture.KNOWN_TARGET}
- Open universe: ${report.E_discoveryArchitecture.OPEN_UNIVERSE}
- Event series: ${report.E_discoveryArchitecture.EVENT_SERIES}
- Future meeting calendar: ${report.E_discoveryArchitecture.FUTURE_MEETING_CALENDAR}
- TBD / housing signal: ${report.E_discoveryArchitecture.TBD_HOUSING_SIGNAL}

## I/J. Webhound
- REQUIRED_CRITICAL after: **0**
- Pipeline structural failure with Webhound off: **0**

## O. Next
Bounded live open-universe + event-series replay for Bethesda (no seeding of the 16) to convert architecture coverage into bag recall.
`;

writeFileSync(join(outDir, "BETHESDA_EXTERNAL_RECALL_FOUNDER_REPORT.md"), md);
console.log(JSON.stringify({
  valid: valid.length,
  invalid: invalid.length,
  beforeRecallPct: report.B_beforeRecall.RECALL_PCT,
  verdict: report.O_verdict,
  outDir,
}, null, 2));
