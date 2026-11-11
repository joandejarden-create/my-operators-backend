/**
 * Three-hotel research benchmark authorization preparation (V2).
 * Nontransferable per-hotel ceilings. No live paid research until separately approved.
 */

import { quotePlannerUsd, PLANNER_CEILING_USD } from "./dynamic-research-planner-v1.js";
import { quoteBenchmarkCall } from "./benchmark-provider-pricing-v1.js";
import { SEGMENT_E2E_HOTELS } from "./segment-e2e-budget-v1.js";

export const THREE_HOTEL_BENCHMARK_VERSION = "gdi-three-hotel-benchmark-authorization-v2";
export const THREE_HOTEL_BENCHMARK_REPORTS_DIR = "reports/gdi-three-hotel-benchmark-authorization-v2";

export const THREE_HOTEL_BENCHMARK_HOTELS = Object.freeze([
  {
    hotelId: "recC2kbCaS48jFjFF",
    hotelName: "Nobu Hotel London Portman Square",
    segmentId: "professional_planners_dmc",
    objective: "Demonstrate repeated account-led opportunity development and meaningful segment discovery breadth.",
    ceilingUsd: 1.5,
    ledgerRelative: "reports/gdi-three-hotel-benchmark-authorization-v2/ledgers/nobu-professional-planners-dmc-budget-ledger.json",
  },
  {
    hotelId: "recLuxvwwxID7U2B8",
    hotelName: "Bethesda Marriott",
    segmentId: "associations_government",
    objective: "Identify future meetings, establish real hotel fit, distinguish competitive commitments, and investigate future-cycle opportunities.",
    ceilingUsd: 1.5,
    ledgerRelative: "reports/gdi-three-hotel-benchmark-authorization-v2/ledgers/bethesda-associations-government-budget-ledger.json",
  },
  {
    hotelId: "recnPB3Q02NO8Ma1o",
    hotelName: "Wyndham San Diego Bayside",
    segmentId: "corporate_meetings_training",
    objective: "Discover corporate group opportunities and exclude already-committed demand unless there is a credible alternative or future-cycle mechanism.",
    ceilingUsd: 1.5,
    ledgerRelative: "reports/gdi-three-hotel-benchmark-authorization-v2/ledgers/wyndham-corporate-meetings-training-budget-ledger.json",
  },
]);

function providerCard() {
  const planner = quotePlannerUsd();
  let firecrawl = { bounded: false, maxUsd: null };
  let exa = { bounded: false, maxUsd: null };
  try {
    firecrawl = quoteBenchmarkCall({
      provider: "firecrawl",
      operation: "scrape",
      params: { url: "https://www.example.com/", formats: ["markdown"] },
    });
  } catch {
    firecrawl = { bounded: false, maxUsd: null };
  }
  try {
    exa = quoteBenchmarkCall({
      provider: "exa",
      operation: "search",
      params: { query: "hotel group demand", numResults: 3 },
    });
  } catch {
    exa = { bounded: false, maxUsd: null };
  }
  return { planner, firecrawl, exa };
}

/**
 * Isolated authorization manifest — AWAITING_AUTHORIZATION; budgets nontransferable.
 */
export function buildThreeHotelBenchmarkAuthorizationManifestV2() {
  const pricing = providerCard();
  const hotels = THREE_HOTEL_BENCHMARK_HOTELS.map((h) => ({
    ...h,
    censusAlignedWithSegmentE2e: Boolean(SEGMENT_E2E_HOTELS[h.hotelId]),
    budgetTransferable: false,
    liveExecutionAuthorized: false,
    maxPlannerCalls: 20,
    maxSubstantiveRetrievals: 30,
    maxCandidatesInvestigated: 20,
    perCallCeilingUsd: PLANNER_CEILING_USD,
    firecrawlMaxUsdEach: pricing.firecrawl.bounded ? Number(pricing.firecrawl.maxUsd) : null,
    exaMaxUsdEach: pricing.exa.bounded ? Number(pricing.exa.maxUsd) : null,
    parallelRefusedUnlessSeparatelyAuthorized: true,
  }));

  return {
    version: THREE_HOTEL_BENCHMARK_VERSION,
    status: "AWAITING_AUTHORIZATION",
    liveExecutionAuthorized: false,
    aggregateCeilingUsd: hotels.reduce((sum, h) => sum + h.ceilingUsd, 0),
    budgetsNontransferableAcrossHotels: true,
    usesPriorResearchLedgers: false,
    refusedLedgers: [
      "reports/gdi-segment-pilot-professional-planners-dmc-nobu-v1/budget-ledger.json",
      "reports/gdi-autonomous-longform-research-certification-v1/budget-ledger.json",
      "reports/gdi-cross-organization-live-recovery-certification-v1/budget-ledger.json",
      "reports/gdi-segment-e2e-certification-v1/live/budget-ledger.json",
      "reports/gdi-eight-hotel-research-benchmark-v1/cohort-a-live/budget-ledger.json",
      "data/group-demand-intelligence/nobu-london-portman-budget/authorization-ledger-v1.json",
    ],
    providerPricing: {
      gpt4oMiniPlanner: {
        model: pricing.planner.model,
        estimatedUsdEach: pricing.planner.estimatedUsd,
        hardCeilingUsdEach: PLANNER_CEILING_USD,
      },
      directPublic: { estimatedUsdEach: 0, preferred: true },
      firecrawlScrape: {
        bounded: pricing.firecrawl.bounded === true,
        maxUsdEach: pricing.firecrawl.bounded ? Number(pricing.firecrawl.maxUsd) : null,
        allowedOnlyIfPriced: true,
      },
      exa: {
        bounded: pricing.exa.bounded === true,
        maxUsdEach: pricing.exa.bounded ? Number(pricing.exa.maxUsd) : null,
        note: "Only if priced and VoI justifies after existing evidence exhausted",
      },
      parallel: { refusedUnlessSeparatelyAuthorized: true },
    },
    hotels,
    stoppingLimitsShared: {
      doNotStopSolelyBecauseTwoUsefulProspectsFound: true,
      requireDiscoveryCoverageReporting: true,
      publicationEligibilityUnchangedUnlessSeparatelyAuthorized: true,
    },
    publicationEligibilityChanged: false,
    airtableWrites: 0,
    deploy: false,
    customerFacingClassificationsUnchanged: true,
    doNotConductNewPaidResearchUntilApproved: true,
  };
}

export function prepareThreeHotelBenchmarkAuthorizationV2() {
  const manifest = buildThreeHotelBenchmarkAuthorizationManifestV2();
  return {
    prepared: true,
    executed: false,
    liveCalls: 0,
    spent: false,
    status: manifest.status,
    version: THREE_HOTEL_BENCHMARK_VERSION,
    reportsDir: THREE_HOTEL_BENCHMARK_REPORTS_DIR,
    authorizationManifest: manifest,
    stopBeforeLiveExecution: true,
  };
}
