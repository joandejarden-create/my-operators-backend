/**
 * Waterstone Apify + Exa benchmark plan (proposal only — zero paid until founder GO).
 */

import { listEnabledApifyActors, getApifyActorById } from "./apify-actor-registry-v1.js";
import { buildFourteenWorkflowApifyCoverageMatrix } from "./apify-fourteen-workflow-coverage-v1.js";

export const WATERSTONE_APIFY_BENCHMARK_HOTEL = Object.freeze({
  name: "Waterstone Resort & Marina Boca Raton",
  hotelId: "recgMYovrrZDJMqzX",
  city: "Boca Raton",
  market: "Boca Raton / South Florida",
  needMonths: ["2027-08", "2027-09", "2027-10"],
  needNote: "Founder-defined TEST_SCENARIO Hotel Commercial Priorities — do not overwrite",
});

function estimateActorUsd(actorId, maxItems) {
  const a = getApifyActorById(actorId);
  const id = actorId || "";
  if (id.includes("aitorsm/eventbrite")) return Number((0.00005 + maxItems * 0.015).toFixed(2));
  if (id.includes("solidcode/eventbrite")) return Number((0.00005 + maxItems * 0.0012).toFixed(2));
  if (id.includes("parseforge/eventbrite")) return Number((0.006 + maxItems * 0.0045).toFixed(2));
  if (id.includes("crawler-google-places")) return Number((0.001 + maxItems * 0.004).toFixed(2));
  if (id.includes("facebook-events")) return Number((0.001 + maxItems * 0.01).toFixed(2));
  if (id.includes("the-knot") || id.includes("wedding")) return Number((0.001 + maxItems * 0.006).toFixed(2));
  if (id.includes("meetup")) return Number((0.001 + maxItems * 0.001).toFixed(2));
  if (id.includes("espn")) return Number((0.001 + maxItems * 0.001).toFixed(2));
  if (id.includes("website-content")) return Number((0.08).toFixed(2));
  if (id.includes("google-search")) return Number((0.01 + maxItems * 0.0025).toFixed(2));
  return Number((0.01 + maxItems * 0.005).toFixed(2));
}

/**
 * Controlled Track A (Exa-only) vs Track B (Apify discovery + Exa/ORS qualify).
 */
export function buildWaterstoneApifyBenchmarkPlanV1() {
  const coverage = buildFourteenWorkflowApifyCoverageMatrix();
  const primaryWorkflows = coverage.workflows.filter(
    (w) => w.classification === "PRIMARY_APIFY_DISCOVERY",
  );

  const proposedRuns = [
    {
      family: "WEDDINGS_SOCIAL",
      actorId: "dionysus_way/the-knot-marketplace-scraper---wedding-vendor-leads",
      maxItems: 40,
      inputHint: {
        startUrls: [
          "https://www.theknot.com/marketplace/wedding-planners-boca-raton-fl",
          "https://www.theknot.com/marketplace/wedding-venues-boca-raton-fl",
        ],
        maxPages: 3,
      },
      track: "RELATIONSHIP",
      note: "Vendor listings → Research Continuation; NOT dated room blocks",
    },
    {
      family: "WEDDINGS_SOCIAL",
      actorId: "hanamira/wedding-vendor-scraper",
      maxItems: 30,
      inputHint: {
        mode: "search",
        location: "Boca Raton, FL",
        categories: ["wedding-planners", "venues"],
        maxResultsPerCategory: 15,
      },
      track: "RELATIONSHIP",
      note: "Cross-directory corroboration; dedupe vs The Knot + Maps",
    },
    {
      family: "LOCAL_DEMAND_PARTNERSHIPS",
      actorId: "compass/crawler-google-places",
      maxItems: 50,
      inputHint: {
        searchStringsArray: [
          "wedding planner",
          "wedding venue",
          "event venue",
          "conference center",
          "corporate office park",
        ],
        locationQuery: "Boca Raton, FL",
        maxCrawledPlacesPerSearch: 10,
      },
      track: "RELATIONSHIP",
      note: "Local generators & venues without rooms",
    },
    {
      family: "PUBLISHED_EVENTS",
      actorId: "solidcode/eventbrite-scraper",
      maxItems: 80,
      inputHint: {
        country: "united-states",
        city: "Boca Raton",
        startDate: "2027-08-01",
        endDate: "2027-10-31",
        eventFormat: "in-person",
        maxResults: 80,
      },
      track: "DATED_EVENT",
      note: "Absolute date range for need months — preferred low unit cost",
    },
    {
      family: "PUBLISHED_EVENTS",
      actorId: "aitorsm/eventbrite",
      maxItems: 40,
      inputHint: {
        country: "united-states",
        city: "boca-raton",
        startDate: "2027-08-01",
        endDate: "2027-10-31",
        maxResults: 40,
        enrichOrganizers: true,
      },
      track: "DATED_EVENT",
      note: "Organizer enrichment for ORS buyer handoff; higher $/result",
    },
    {
      family: "PUBLISHED_EVENTS",
      actorId: "filip_cicvarek/meetup-scraper",
      maxItems: 40,
      inputHint: {
        city: "Boca Raton",
        state: "FL",
        country: "US",
        startDateRange: "2027-08-01",
        endDateRange: "2027-10-31",
        maxResults: 40,
      },
      track: "DATED_EVENT",
      note: "Validate Meetup date-range behavior for far-future windows",
    },
    {
      family: "PUBLISHED_EVENTS",
      actorId: "apify/facebook-events-scraper",
      maxItems: 30,
      inputHint: {
        searchQueries: ["Boca Raton conference", "Boca Raton gala", "Delray Beach wedding expo"],
        maxEvents: 30,
      },
      track: "DATED_EVENT",
      note: "Public events only; demote competing-hotel hosts",
    },
    {
      family: "SPORTS_ENTERTAINMENT",
      actorId: "hgservices/apify-actor-espn",
      maxItems: 60,
      inputHint: {
        leagues: ["mlb", "nfl", "nba", "mls", "ncaaf"],
        dateMode: "range",
        startDate: "2027-08-01",
        endDate: "2027-10-31",
        teams: ["MIA", "FLA", "MIA Marlins", "Inter Miami", "Miami Dolphins", "Miami Heat"],
        timezone: "America/New_York",
        gameStatus: ["pre"],
      },
      track: "DATED_EVENT",
      note: "South Florida team filter mandatory; lodging thesis via ORS only",
    },
    {
      family: "SEARCH_SUPPLEMENT",
      actorId: "apify/google-search-scraper",
      maxItems: 20,
      inputHint: {
        queries: "Boca Raton association conference 2027\nSouth Florida incentive travel 2027",
        maxPagesPerQuery: 1,
        countryCode: "us",
      },
      track: "DISCOVERY_SEED",
      note: "Supplemental SERP seeds → Exa Deep Research; do not replace Exa Agent",
    },
  ].map((r) => ({
    ...r,
    estimateUsd: estimateActorUsd(r.actorId, r.maxItems),
    enabled: Boolean(getApifyActorById(r.actorId)?.enabled),
  }));

  const apifyDiscoveryUsd = Number(
    proposedRuns.reduce((s, r) => s + (r.enabled ? r.estimateUsd : 0), 0).toFixed(2),
  );

  const exaQualifyUsd = {
    trackA_exaOnly: 28,
    trackB_deepResearchOnApifyCandidates: 22,
    trackB_buyerFollowups: 10,
    note: "Planning estimates aligned with V6 COMMERCIAL_DEPTH posture — NOT a live quote; Exa pricing varies by research depth",
  };

  const scenarios = [
    {
      id: "APIFY_PILOT_LEAN",
      label: "Lean Apify pilot + ORS qualify",
      apifyUsd: Number((apifyDiscoveryUsd * 0.55).toFixed(2)),
      exaUsd: 18,
      aggregateUsd: null,
      runs: proposedRuns.filter((r) =>
        [
          "solidcode/eventbrite-scraper",
          "compass/crawler-google-places",
          "dionysus_way/the-knot-marketplace-scraper---wedding-vendor-leads",
          "filip_cicvarek/meetup-scraper",
        ].includes(r.actorId),
      ),
    },
    {
      id: "APIFY_PILOT_FULL",
      label: "Full five-family Apify discovery + Exa/ORS",
      apifyUsd: apifyDiscoveryUsd,
      exaUsd: exaQualifyUsd.trackB_deepResearchOnApifyCandidates + exaQualifyUsd.trackB_buyerFollowups,
      aggregateUsd: null,
      runs: proposedRuns,
    },
    {
      id: "EXA_ONLY_CONTROL",
      label: "Track A control — Exa-only (no Apify)",
      apifyUsd: 0,
      exaUsd: exaQualifyUsd.trackA_exaOnly,
      aggregateUsd: null,
      runs: [],
    },
  ].map((s) => ({
    ...s,
    aggregateUsd: Number((s.apifyUsd + s.exaUsd).toFixed(2)),
  }));

  return {
    version: "gdi-apify-waterstone-benchmark-plan-v1",
    hotel: WATERSTONE_APIFY_BENCHMARK_HOTEL,
    paidExecution: "NOT_AUTHORIZED",
    tracks: {
      A: {
        name: "Exa-only",
        description: "Current GDI architecture without Apify discovery",
        measures: [
          "uniqueCandidates",
          "netNewBeyondGdi",
          "augSepOctEvidence",
          "namedBuyerCoverage",
          "commerciallyAccessible",
          "independentlyQualified",
          "directVsSpeculativeOverflow",
          "completeV2Cibs",
          "providerSpend",
        ],
      },
      B: {
        name: "Apify Discovery + Exa Deep Research / ORS",
        description:
          "Structured Apify discovery → normalize → cross-provider dedup → commercial screen → same ORS qualification as Track A",
        proposedRuns,
        measures: "identical to Track A",
      },
    },
    primaryApifyWorkflows: primaryWorkflows.map((w) => ({
      workflowNumber: w.workflowNumber,
      strategyId: w.strategyId,
      enabledActors: w.enabledActors,
    })),
    costScenarios: scenarios,
    recommendedScenarioId: "APIFY_PILOT_FULL",
    comparisonRules: [
      "Identical commercial accessibility classes and qualification standards",
      "Do not declare Apify superior on row count alone",
      "One candidate with Exa+Apify sources counts once",
      "Competing-hotel hosted events are not overflow proof",
      "Wedding venue/planner listings stay relationship track until dated demand evidenced",
      "Mock/zero-paid certification ≠ live benchmark",
    ],
    founderApprovalsRequired: [
      "Authorize live Apify spend (set APIFY_TOKEN + GDI_APIFY_LIVE=1 + authorizeApifyLive)",
      "Authorize Exa Deep Research / ORS paid follow-up budget for Track A and Track B",
      "Confirm aggregate ceiling (recommended FULL ≈ Apify + Exa combined)",
      "Confirm ToS/legal acceptance for Eventbrite / The Knot / WeddingWire / Maps / Facebook / ESPN scraping at pilot volume",
      "Explicitly withhold --authorize-publish / staging to customer drawer until post-benchmark review",
    ],
    enabledActorCount: listEnabledApifyActors().length,
  };
}
