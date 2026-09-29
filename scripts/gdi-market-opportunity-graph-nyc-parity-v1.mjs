#!/usr/bin/env node
/**
 * GDI Market Opportunity Graph + NYC Cross-Hotel Parity V1
 *
 * Shadow evaluation only — no customer promotion, no Bethesda, no cron, no Webhound.
 *
 *   node scripts/gdi-market-opportunity-graph-nyc-parity-v1.mjs
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { filterSalespersonView } from "../lib/group-demand-intelligence/opportunity-factory.js";
import { applyLiveCommercialQuality } from "../lib/group-demand-intelligence/live-commercial-quality-v1.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import {
  buildNycControlGeographyProfiles,
  NYC_CONTROL_HOTELS,
  extractMarketOpportunityPacket,
  buildHotelOpportunityLayer,
  evaluateHotelGeographicApplicability,
  evaluateCrossHotelFit,
  decideSupportingDataNextAction,
  buildMarketOpportunityGraph,
  fanoutDecisionForGeography,
  wouldBlindFanoutToMidtown,
  classifyOpportunityGeography,
  GEO_APPLICABILITY,
  HOTEL_OPP_FINAL_STATE,
  applicabilityIsCandidate,
} from "../lib/group-demand-intelligence/market-opportunity-graph/index.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/group-demand-intelligence/market-opportunity-graph-nyc-parity-v1"
);
const NOW = new Date().toISOString().slice(0, 10);

function gitHead() {
  try {
    return execSync("git rev-parse HEAD", { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return "unknown";
  }
}

function writeJson(name, data) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(data, null, 2), "utf8");
}

function oppId(o) {
  return o?.id || o?.opportunityId || null;
}

function listVisible(opportunities) {
  return filterCustomerFacingOpportunities(
    filterSalespersonView(
      (opportunities || []).map((o) =>
        applyLiveCommercialQuality(o, { nowDate: NOW })
      )
    ),
    { nowDate: NOW }
  );
}

function evaluatePair(marketPacket, hotelProfile, seedOpp) {
  const geoApp = evaluateHotelGeographicApplicability(seedOpp || marketPacket, hotelProfile);
  const fit = evaluateCrossHotelFit({
    marketPacket,
    hotelProfile,
    geographicApplicability: geoApp,
    seedOpp,
  });
  const jev = decideSupportingDataNextAction({
    marketPacket,
    geographicApplicability: geoApp,
    hotelFit: fit,
  });
  const layer = buildHotelOpportunityLayer({
    marketPacket,
    hotelProfile,
    geographicApplicability: geoApp,
    hotelFit: fit,
    finalState: fit.finalState,
    missingData: fit.missingData,
    jevNextAction: jev,
    seedHotelOpportunityId: marketPacket.sourceHotelOpportunityId,
  });
  return { geoApp, fit, jev, layer };
}

function rootCauseBucket(hiltonEval) {
  if (hiltonEval.fit.finalState === HOTEL_OPP_FINAL_STATE.CLOSED) return "CLOSED";
  if (hiltonEval.fit.finalState === HOTEL_OPP_FINAL_STATE.NOT_APPLICABLE) {
    return "GEOGRAPHICALLY_NOT_APPLICABLE";
  }
  if (hiltonEval.fit.finalState === HOTEL_OPP_FINAL_STATE.NOT_FIT) return "NOT_FIT";
  if (hiltonEval.fit.finalState === HOTEL_OPP_FINAL_STATE.HOTEL_MATCHED_NEEDS_MORE_DATA) {
    return "DATA_INCOMPLETE";
  }
  if (hiltonEval.fit.finalState === HOTEL_OPP_FINAL_STATE.CUSTOMER_READY) {
    return "EVALUATED_AND_FIT";
  }
  return "OTHER";
}

async function main() {
  const head = gitHead();
  console.log(`[preflight] HEAD=${head}`);

  const geoProfiles = buildNycControlGeographyProfiles();
  writeJson("NYC_HOTEL_GEOGRAPHY.json", geoProfiles);

  const renDoc = await loadOpportunitiesCanonical(NYC_CONTROL_HOTELS.RENAISSANCE);
  const hilDoc = await loadOpportunitiesCanonical(NYC_CONTROL_HOTELS.HILTON);
  const nowDoc = await loadOpportunitiesCanonical(NYC_CONTROL_HOTELS.NOW_NOW);

  const renVisible = listVisible(renDoc.opportunities || []);
  const hilVisible = listVisible(hilDoc.opportunities || []);
  const nowVisible = listVisible(nowDoc.opportunities || []);
  const renReady = renVisible.filter((o) =>
    isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok
  );
  const hilReady = hilVisible.filter((o) =>
    isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok
  );
  const nowReady = nowVisible.filter((o) =>
    isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok
  );

  console.log(
    `[counts] Ren ready=${renReady.length} visible=${renVisible.length} | Hilton ready=${hilReady.length} total=${(hilDoc.opportunities||[]).length} | NOW ready=${nowReady.length} visible=${nowVisible.length}`
  );

  // Seed corpus: Ren 11
  const marketPackets = renReady.map((o) =>
    extractMarketOpportunityPacket(o, NYC_CONTROL_HOTELS.RENAISSANCE)
  );
  writeJson("RENAISSANCE_11_MARKET_PACKETS.json", marketPackets);

  const hiltonRows = [];
  const nowRows = [];
  const hotelLinks = [];
  const rootCause = {
    NOT_EVALUATED_FOR_HILTON: 0,
    EVALUATED_AND_FIT: 0,
    EVALUATED_AND_NOT_FIT: 0,
    GEOGRAPHICALLY_NOT_APPLICABLE: 0,
    COMMERCIAL_STATUS_CLOSED: 0,
    DATA_INCOMPLETE: 0,
    OTHER: 0,
  };

  // Discovery bias: Hilton never had these IDs
  const hiltonIds = new Set((hilDoc.opportunities || []).map(oppId));
  const hiltonTitles = new Set(
    (hilDoc.opportunities || []).map((o) =>
      String(o.title || "")
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, " ")
        .trim()
    )
  );

  let jevActions = 0;
  let pairs = 0;
  let blockersResolved = 0;
  let stateAdvances = 0;
  let shadowReadyHilton = 0;
  let shadowReadyNow = 0;

  for (const seed of renReady) {
    const packet = extractMarketOpportunityPacket(
      seed,
      NYC_CONTROL_HOTELS.RENAISSANCE
    );
    const seedTitleNorm = String(seed.title || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim();

    // Was Hilton previously evaluated? (same title or series)
    const priorHilton = (hilDoc.opportunities || []).find(
      (h) =>
        oppId(h) === oppId(seed) ||
        (seed.eventSeriesId && h.eventSeriesId === seed.eventSeriesId) ||
        String(h.title || "")
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, " ")
          .trim() === seedTitleNorm
    );
    const neverEvaluated = !priorHilton;

    const hilEval = evaluatePair(packet, geoProfiles.HILTON, seed);
    pairs += 1;
    if (hilEval.jev.action !== "STOP_NO_FURTHER_EVIDENCE") jevActions += 1;
    hotelLinks.push(hilEval.layer);

    let cause = rootCauseBucket(hilEval);
    if (neverEvaluated && cause === "EVALUATED_AND_FIT") {
      // Still count as fit recovered via cross-eval; also track never evaluated
    }
    if (neverEvaluated) rootCause.NOT_EVALUATED_FOR_HILTON += 1;
    if (cause === "EVALUATED_AND_FIT") rootCause.EVALUATED_AND_FIT += 1;
    else if (cause === "NOT_FIT") rootCause.EVALUATED_AND_NOT_FIT += 1;
    else if (cause === "GEOGRAPHICALLY_NOT_APPLICABLE") {
      rootCause.GEOGRAPHICALLY_NOT_APPLICABLE += 1;
    } else if (cause === "CLOSED") rootCause.COMMERCIAL_STATUS_CLOSED += 1;
    else if (cause === "DATA_INCOMPLETE") rootCause.DATA_INCOMPLETE += 1;
    else rootCause.OTHER += 1;

    if (hilEval.fit.finalState === HOTEL_OPP_FINAL_STATE.CUSTOMER_READY) {
      shadowReadyHilton += 1;
      stateAdvances += 1;
    } else if (
      hilEval.fit.finalState === HOTEL_OPP_FINAL_STATE.HOTEL_MATCHED_NEEDS_MORE_DATA
    ) {
      stateAdvances += 1;
    }
    if ((hilEval.fit.missingData || []).length === 0 && neverEvaluated) {
      blockersResolved += 1;
    }

    hiltonRows.push({
      opportunityId: oppId(seed),
      title: seed.title,
      marketOpportunityId: packet.marketOpportunityId,
      geography: packet.geography,
      renaissanceFit: seed.hotelFitScore ?? null,
      hiltonApplicable: hilEval.geoApp.applicability,
      hiltonApplicableReasons: hilEval.geoApp.reasons,
      hiltonFit: hilEval.fit.hotelFitScore,
      hiltonMissing: hilEval.fit.missingData,
      hiltonProductConstraint: hilEval.fit.productConstraint,
      jevAction: hilEval.jev.action,
      jevRationale: hilEval.jev.rationale,
      final: hilEval.fit.finalState,
      neverEvaluatedForHilton: neverEvaluated,
      priorHiltonId: priorHilton ? oppId(priorHilton) : null,
      priorHiltonPriority: priorHilton?.priority || null,
      rootCause: cause,
    });

    const nowEval = evaluatePair(packet, geoProfiles.NOW_NOW, seed);
    pairs += 1;
    if (nowEval.jev.action !== "STOP_NO_FURTHER_EVIDENCE") jevActions += 1;
    hotelLinks.push(nowEval.layer);
    if (nowEval.fit.finalState === HOTEL_OPP_FINAL_STATE.CUSTOMER_READY) {
      shadowReadyNow += 1;
      stateAdvances += 1;
    }

    let nowClass = "NOT_APPLICABLE";
    if (applicabilityIsCandidate(nowEval.geoApp.applicability)) {
      if (
        nowEval.fit.finalState === HOTEL_OPP_FINAL_STATE.CUSTOMER_READY ||
        nowEval.fit.finalState === HOTEL_OPP_FINAL_STATE.HOTEL_MATCHED_NEEDS_MORE_DATA
      ) {
        nowClass = "APPLICABLE";
      } else if (nowEval.fit.finalState === HOTEL_OPP_FINAL_STATE.NOT_FIT) {
        nowClass = "POSSIBLY_APPLICABLE";
      } else {
        nowClass = "POSSIBLY_APPLICABLE";
      }
    } else if (nowEval.geoApp.applicability === GEO_APPLICABILITY.WEAK) {
      nowClass = "POSSIBLY_APPLICABLE";
    }

    nowRows.push({
      opportunityId: oppId(seed),
      title: seed.title,
      marketOpportunityId: packet.marketOpportunityId,
      geography: packet.geography,
      nowApplicable: nowEval.geoApp.applicability,
      nowClass,
      nowFit: nowEval.fit.hotelFitScore,
      nowMissing: nowEval.fit.missingData,
      jevAction: nowEval.jev.action,
      final: nowEval.fit.finalState,
      reasons: [...(nowEval.geoApp.reasons || []), ...(nowEval.fit.reasons || [])].slice(
        0,
        6
      ),
    });
  }

  writeJson("RENAISSANCE_TO_HILTON.json", hiltonRows);
  writeJson("RENAISSANCE_TO_NOW_NOW.json", nowRows);
  writeJson("ROOT_CAUSE_11_VS_0.json", rootCause);

  // Hilton reverse audit — sample non-ready rows
  const hiltonReverse = [];
  for (const h of (hilDoc.opportunities || []).slice(0, 32)) {
    const packet = extractMarketOpportunityPacket(h, NYC_CONTROL_HOTELS.HILTON);
    const renApp = evaluateHotelGeographicApplicability(h, geoProfiles.RENAISSANCE);
    const nowApp = evaluateHotelGeographicApplicability(h, geoProfiles.NOW_NOW);
    hiltonReverse.push({
      hiltonId: oppId(h),
      title: String(h.title || "").slice(0, 100),
      priority: h.priority,
      marketOpportunityId: packet.marketOpportunityId,
      geography: packet.geography,
      renaissanceRelevant: applicabilityIsCandidate(renApp.applicability),
      renaissanceApplicability: renApp.applicability,
      nowRelevant: applicabilityIsCandidate(nowApp.applicability),
      nowApplicability: nowApp.applicability,
      currentState: h.priority || h.customerFacingState,
      surfaceFailLikely: h.priority === "DISQUALIFIED",
    });
  }
  writeJson("HILTON_REVERSE_AUDIT.json", hiltonReverse);

  // Discovery bias
  const hotelIsolated = hiltonRows.filter((r) => r.neverEvaluatedForHilton).length;
  const bias = {
    renaissanceReadySeed: renReady.length,
    hotelIsolatedOpportunities: hotelIsolated,
    marketSharedOpportunities: marketPackets.length,
    crossHotelEvaluatedThisPass: pairs,
    neverCrossHotelEvaluatedBefore: hotelIsolated,
    note: "Renaissance ready rows had no matching Hilton title/series in Hilton bag — hotel-isolated discovery",
  };
  writeJson("DISCOVERY_BIAS_AUDIT.json", bias);

  // Graph
  const graph = buildMarketOpportunityGraph({
    marketPackets,
    hotelLinks,
    hotels: [
      geoProfiles.RENAISSANCE,
      geoProfiles.HILTON,
      geoProfiles.NOW_NOW,
    ],
  });
  writeJson("MARKET_OPPORTUNITY_GRAPH.json", graph);

  // Boundary + Brooklyn safeguard tests (shadow)
  const boundary = {
    timesSquare: classifyOpportunityGeography({
      title: "Broadway League Midtown Meeting",
      destinationStatus: "Times Square",
    }),
    javits: classifyOpportunityGeography({
      title: "Trade Show at Javits Center",
      destinationStatus: "Javits Center",
    }),
    noho: classifyOpportunityGeography({
      title: "NYU conference Washington Square",
      destinationStatus: "NoHo",
    }),
    brooklyn: classifyOpportunityGeography({
      title: "Brooklyn Barclays sports housing",
      destinationStatus: "Downtown Brooklyn",
    }),
    metroOnly: classifyOpportunityGeography({
      title: "Association meeting New York City",
      destinationStatus: "New York City",
    }),
  };
  const brooklynSafeguard = {
    brooklynOppGeo: boundary.brooklyn,
    wouldAutomaticallyReachMidtown: wouldBlindFanoutToMidtown(boundary.brooklyn),
    fanout: fanoutDecisionForGeography(boundary.brooklyn),
    expected: "NO",
  };
  writeJson("MARKET_BOUNDARY_TESTS.json", { boundary, brooklynSafeguard });

  const summary = {
    head,
    generatedAt: new Date().toISOString(),
    counts: {
      renaissanceReady: renReady.length,
      hiltonReadyBefore: hilReady.length,
      hiltonReadyAfterShadow: shadowReadyHilton,
      hiltonHotelMatchedNeedsData: hiltonRows.filter(
        (r) => r.final === HOTEL_OPP_FINAL_STATE.HOTEL_MATCHED_NEEDS_MORE_DATA
      ).length,
      hiltonNotFit: hiltonRows.filter(
        (r) =>
          r.final === HOTEL_OPP_FINAL_STATE.NOT_FIT ||
          r.final === HOTEL_OPP_FINAL_STATE.NOT_APPLICABLE
      ).length,
      nowReadyBefore: nowReady.length,
      nowReadyAfterShadow: shadowReadyNow,
      hiltonCanonicalTotal: (hilDoc.opportunities || []).length,
      nowVisible: nowVisible.length,
    },
    rootCause,
    bias,
    jev: {
      pairEvaluations: pairs,
      jevActions,
      blockersResolved,
      stateAdvances,
      newReadyShadow: shadowReadyHilton + shadowReadyNow,
      fetches: 0,
      note: "Router decisions only — network fetches deferred",
    },
    graph: {
      marketOpportunities: graph.marketOpportunities,
      hotelOpportunityLinks: graph.hotelOpportunityLinks,
      duplicateMarketOppsMerged: graph.duplicateMarketOppsMerged,
    },
    customerMutation: false,
    cron: "HELD",
    deploy: "NOT_RUN",
  };
  writeJson("RUN_SUMMARY.json", summary);

  console.log("[done]", JSON.stringify(summary.counts));
  console.log("[rootCause]", rootCause);
  console.log(`[out] ${OUT}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
