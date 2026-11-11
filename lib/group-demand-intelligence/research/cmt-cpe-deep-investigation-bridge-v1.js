/**
 * Phase 3B-R — bounded deep investigation + ORS maturation bridge for CMT/CPE.
 * Reuses runDeepResearchOnExistingGdiOpportunity + createFixtureResearcher.
 * Mocked / offline only — failed providers never imply commercial disqualification.
 */

import {
  createFixtureResearcher,
  runDeepResearchOnExistingGdiOpportunity,
} from "./deep-research-gdi-integration-v1.js";
import { runScalingIntelligentInvestigationLoop } from "./scaling-intelligent-investigation-loop-v1.js";

export const CMT_CPE_DEEP_INVESTIGATION_BRIDGE_V1 = "gdi-cmt-cpe-deep-investigation-bridge-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Run offline ORS maturation on a discovered candidate.
 * Preserves investigationId and evidence history; explicit stopReason.
 */
export async function runCmtCpeBoundedDeepInvestigation({
  opportunity = {},
  hotel = {},
  corpus = null,
  researcher = null,
  paidSearch = null,
  budgetCeilingUsd = 0,
  maxIterations = 3,
  runInvestigationLoop = false,
} = {}) {
  const investigationId = text(opportunity.investigationId || opportunity.id);
  if (!investigationId) {
    return {
      version: CMT_CPE_DEEP_INVESTIGATION_BRIDGE_V1,
      ok: false,
      stopReason: "missing_investigation_id",
      commercialDisqualificationImplied: false,
    };
  }
  if (!opportunity.organizationName) {
    return {
      version: CMT_CPE_DEEP_INVESTIGATION_BRIDGE_V1,
      ok: false,
      investigationId,
      stopReason: "missing_organization",
      commercialDisqualificationImplied: false,
    };
  }

  const seeded = {
    ...opportunity,
    id: opportunity.id || investigationId,
    investigationId,
    hotelId: opportunity.hotelId || hotel.hotelId || null,
    hotelName: hotel.displayName || hotel.name || opportunity.hotelName || null,
    hotelDisplayName: hotel.displayName || hotel.name || null,
    evidenceHistory: Array.isArray(opportunity.evidenceHistory)
      ? [...opportunity.evidenceHistory]
      : [],
    attemptedStrategies: Array.isArray(opportunity.attemptedStrategies)
      ? [...opportunity.attemptedStrategies]
      : [],
  };

  const pageCorpus =
    corpus ||
    Object.fromEntries(
      (opportunity.sources || [])
        .map((s) => (typeof s === "string" ? s : s?.url))
        .filter(Boolean)
        .slice(0, 6)
        .map((url) => [
          url,
          [
            opportunity.organizationName,
            opportunity.activityHeadline || opportunity.eventName,
            opportunity.evidenceQuote,
            opportunity.hotelDemandThesis,
            opportunity.whatIsHappening,
            opportunity.contactName
              ? `${opportunity.contactName}, ${opportunity.contactRole || "contact"}`
              : null,
          ]
            .filter(Boolean)
            .join(". "),
        ]),
    );

  let deepResult = null;
  let stopReason = null;
  try {
    deepResult = await runDeepResearchOnExistingGdiOpportunity({
      opportunity: seeded,
      researcher: researcher || createFixtureResearcher(pageCorpus),
      corpus: pageCorpus,
      offlineLongform: true,
      options: {
        maxIterations,
        paidBlocked: true,
        resetResearchAttempts: false,
      },
    });
  } catch (err) {
    stopReason = `deep_research_error:${String(err?.message || err).slice(0, 160)}`;
    return {
      version: CMT_CPE_DEEP_INVESTIGATION_BRIDGE_V1,
      ok: false,
      investigationId,
      stopReason,
      commercialDisqualificationImplied: false,
      note: "Provider/research failure does not commercially disqualify the opportunity.",
      evidenceHistory: seeded.evidenceHistory,
    };
  }

  let matured = deepResult?.opportunity || deepResult?.workingOpportunity || seeded;
  const explicitStop =
    deepResult?.stopReason ||
    deepResult?.stoppingTrigger ||
    deepResult?.disposition ||
    "deep_investigation_completed_offline";

  let loopResult = null;
  if (runInvestigationLoop === true) {
    try {
      const offlineFetchPage = async (url) => ({
        ok: true,
        url,
        text: pageCorpus[url] || `Offline fixture page for ${url}`,
      });
      loopResult = await runScalingIntelligentInvestigationLoop({
        opportunity: matured,
        hotel,
        allowFreeFetch: true,
        maxIterations: Math.min(2, maxIterations),
        maxPageFetches: 4,
        fetchPage: offlineFetchPage,
        paidSearch:
          typeof paidSearch === "function"
            ? paidSearch
            : async () => ({ status: "mocked_skip", hits: [], costUsd: 0 }),
      });
      if (loopResult?.stopReason) stopReason = loopResult.stopReason;
      if (loopResult?.opportunity) {
        matured = loopResult.opportunity;
      }
    } catch (err) {
      stopReason = `investigation_loop_error:${String(err?.message || err).slice(0, 120)}`;
      // Failed loop ≠ commercial disqualification
    }
  }

  const evidenceHistory = [
    ...(matured.evidenceHistory || seeded.evidenceHistory || []),
    {
      at: new Date().toISOString(),
      stage: "cmt_cpe_bounded_deep_investigation",
      stopReason: stopReason || explicitStop,
      offline: true,
    },
  ];

  return {
    version: CMT_CPE_DEEP_INVESTIGATION_BRIDGE_V1,
    ok: true,
    investigationId,
    opportunity: { ...matured, evidenceHistory, investigationId },
    deepResearch: {
      disposition: deepResult?.disposition || null,
      stopReason: explicitStop,
      iterations: deepResult?.iterations || deepResult?.iterationCount || null,
    },
    investigationLoop: loopResult
      ? {
          stopReason: loopResult.stopReason || null,
          attemptedStrategies: loopResult.attemptedStrategies || null,
        }
      : null,
    stopReason: stopReason || explicitStop,
    commercialDisqualificationImplied: false,
    budgetCeilingUsd: Number(budgetCeilingUsd) || 0,
    offlineOnly: true,
  };
}
