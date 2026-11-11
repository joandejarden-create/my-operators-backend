/**
 * Live discovery benchmark authorization package (V4).
 * Isolated, nontransferable hotel allocations. No execution until separate GO.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  COMMERCIAL_DEMAND_DISCOVERY_V4,
  THREE_HOTEL_BENCHMARK,
  generateHotelDemandDiscoveryStrategies,
  loadHotelGdiConfig,
} from "./commercial-demand-discovery-v4.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");
const OUT_DIR = path.join(ROOT, "reports/gdi-commercial-demand-discovery-v4");

/** Provider price card — explicit; do not invent rates at execution time. */
export const V4_DISCOVERY_PROVIDER_PRICING = Object.freeze({
  asOf: "2026-10-26",
  currency: "USD",
  direct_public: { unitUsd: 0, note: "Preferred first path" },
  firecrawl_scrape: { unitUsd: 0.005, note: "Thin-page scrape only when VoI justifies" },
  exa_contents: { unitUsd: 0.01, note: "Independent / program search; require authorize" },
  parallel_pro_task: { unitUsd: 0.05, note: "DISABLED unless separately authorized; default off" },
  gpt_4o_mini_planner: { ceilingUsd: 0.01, note: "Dynamic planner ceiling if used" },
});

/**
 * Proposed isolated ceilings — discovery objective, not 22-candidate revisit.
 * Conservative first live discovery wave; founder may raise on separate GO.
 */
export const V4_PROPOSED_HOTEL_CEILINGS_USD = Object.freeze({
  recC2kbCaS48jFjFF: 1.25, // Nobu
  recLuxvwwxID7U2B8: 1.25, // Bethesda
  recnPB3Q02NO8Ma1o: 1.00, // Wyndham
  aggregateHardCeilingUsd: 3.5,
  transferable: false,
  priorLedgersNotDrawn: true,
  parallelDefaultDisabled: true,
});

export function buildLiveDiscoveryBenchmarkPlanV4() {
  const hotels = THREE_HOTEL_BENCHMARK.map((h) => {
    const config = loadHotelGdiConfig(h.hotelId);
    const strategies = generateHotelDemandDiscoveryStrategies(config || h);
    return {
      hotelId: h.hotelId,
      hotelName: h.displayName,
      isolatedCeilingUsd: V4_PROPOSED_HOTEL_CEILINGS_USD[h.hotelId],
      ledgerPath: `reports/gdi-commercial-demand-discovery-v4/ledgers/${h.hotelId}-discovery-budget-ledger.json`,
      objective: "NEW opportunity discovery — not review of existing 22 frozen candidates",
      excludeFromPrimaryQueue: [
        // Known V3 cohort orgs — may still appear via related-entity; do not count as "new discovery" KPIs
        "London DMC",
        "Spectra DMC",
        "2B UK DMC",
        "Miss Jones Group",
        "HeadBox",
        "Hire Space",
        "Principal. London DMC",
        "CMAA San Diego Chapter",
        "BOMA San Diego",
        "National Association of Development Organizations (NADO) / DDAA",
      ],
      strategyPreview: {
        count: strategies.strategyCount,
        allElevenSegments: strategies.allElevenSegmentsCovered,
        sampleQuestions: strategies.strategies.slice(0, 4).map((s) => s.researchQuestion),
      },
      reportingRequired: [
        "newly_discovered_unique_organizations",
        "newly_discovered_programs_or_demand_signals",
        "candidates_investigated_to_defensible_outcomes",
        "account_development_vs_program_specific",
        "credible_revenue_potential",
        "usable_contact_routes",
        "independent_publisher_coverage",
        "commercially_justified_next_sales_actions",
        "false_positives",
        "cost_per_useful_new_prospect",
      ],
      stopRule: "Stop when commercial value of information no longer justifies spend — not at a predetermined useful-prospect count.",
    };
  });

  return {
    version: COMMERCIAL_DEMAND_DISCOVERY_V4,
    status: "PREPARED_AWAITING_SEPARATE_LIVE_GO",
    doNotExecuteUntilSeparateGo: true,
    noPaidCallsInThisPass: true,
    objective: "Autonomous hotel-specific demand discovery of previously unknown business a sales professional can defensibly pursue before commitment.",
    soleOrchestrator: "opportunity-research-supervisor-v1 (ORS) via matureOpportunity",
    v3QualificationPreserved: true,
    publicationGateUnchanged: true,
    providerPricing: V4_DISCOVERY_PROVIDER_PRICING,
    ceilings: V4_PROPOSED_HOTEL_CEILINGS_USD,
    hotels,
    restrictions: [
      "No cross-use of V3 targeted live ledger or three-hotel benchmark ledgers",
      "Parallel disabled unless separately authorized",
      "No Airtable writes, deploy, client publication, or weekly production activation",
      "Do not modify production publication gate",
      "Do not stop after a predetermined useful-prospect count",
    ],
    blockersBeforeLive: [
      "Founder must explicitly authorize live discovery spend and approve per-hotel ceilings",
      "Provider flags (Exa/Firecrawl) must be enabled for independent-source expansion when public org paths exhaust",
      "Confirm Parallel remains off unless a separate Parallel GO is issued",
    ],
    goNoGoRecommendation: {
      offlineImplementation: "GO",
      liveDiscovery: "NO-GO until separate spend authorization citing this package",
      rationale: "Offline certification and V3 regression must pass; live discovery spends new budget against isolated ledgers only after explicit GO.",
    },
  };
}

export function writeLiveDiscoveryBenchmarkPlanV4() {
  const plan = buildLiveDiscoveryBenchmarkPlanV4();
  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.mkdirSync(path.join(OUT_DIR, "ledgers"), { recursive: true });
  const jsonPath = path.join(OUT_DIR, "LIVE_DISCOVERY_BENCHMARK_PLAN.json");
  fs.writeFileSync(jsonPath, JSON.stringify(plan, null, 2));
  const md = [
    "# V4 Live Discovery Benchmark Plan",
    "",
    `**Status:** ${plan.status}`,
    `**Aggregate ceiling (proposed):** $${plan.ceilings.aggregateHardCeilingUsd} (nontransferable per-hotel allocations)`,
    "",
    "## Hotels",
    ...plan.hotels.map((h) =>
      `- **${h.hotelName}** (\`${h.hotelId}\`) — $${h.isolatedCeilingUsd} · ${h.strategyPreview.count} strategies · 11-seg ${h.strategyPreview.allElevenSegments ? "Y" : "N"}`),
    "",
    "## Provider pricing",
    ...Object.entries(plan.providerPricing).filter(([k]) => k !== "asOf" && k !== "currency").map(([k, v]) =>
      `- ${k}: $${v.unitUsd ?? v.ceilingUsd} — ${v.note}`),
    "",
    "## GO/NO-GO",
    `- Offline implementation: **${plan.goNoGoRecommendation.offlineImplementation}**`,
    `- Live discovery: **${plan.goNoGoRecommendation.liveDiscovery}**`,
    "",
    plan.goNoGoRecommendation.rationale,
    "",
    "## Blockers",
    ...plan.blockersBeforeLive.map((b) => `- ${b}`),
    "",
  ];
  const mdPath = path.join(OUT_DIR, "LIVE_DISCOVERY_BENCHMARK_PLAN.md");
  fs.writeFileSync(mdPath, md.join("\n"));
  return { plan, jsonPath, mdPath };
}
