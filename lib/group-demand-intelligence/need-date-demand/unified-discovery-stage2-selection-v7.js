/**
 * Stage 2 selection logic (V7.1) — commercial quality first.
 * Preserves V6 waves A–G, 14 workflows, wedding research, Apify+Exa, shared ORS,
 * canonical dedup, commercial accessibility, ten-section drawer, auth, ledgers,
 * publication restrictions — while blocking PARTY BUS-class failures.
 */

import {
  PREQUAL_DECISION,
  rankCandidatesForDeepResearch,
  buildAdaptiveDeepResearchPlan,
  classifyDiscoveryCandidate,
} from "./unified-discovery-commercial-prequalify-v7.js";

export const STAGE2_SELECTION_CONTRACT_V7_1 = Object.freeze({
  version: "gdi-stage2-selection-v7.1",
  preserves: [
    "V6_WAVES_A_G",
    "FOURTEEN_WORKFLOWS",
    "MANDATORY_WEDDING_RESEARCH",
    "APIFY_PLUS_EXA",
    "SHARED_ORS",
    "CANONICAL_DEDUPLICATION",
    "COMMERCIAL_ACCESSIBILITY",
    "TEN_SECTION_V2_DRAWER",
    "PROVIDER_AUTHORIZATION",
    "BUDGET_LEDGERS",
    "PUBLICATION_RESTRICTIONS",
  ],
  hardRejects: [
    "UNRELATED_ACTIVITY",
    "GENERIC_EVENT_LISTING_LOW_YIELD",
    "NIGHTLIFE_TRANSPORT_ENTERTAINMENT",
    "OBJECT_OBJECT_ORG_WITHOUT_TYPED_EXTRACTION",
  ],
  rankingAxes: [
    "commercial_demand_thesis",
    "realistic_hotel_revenue_potential",
    "commercial_accessibility",
    "hotel_specific_suitability",
    "buyer_discoverability",
    "timing_relevance_secondary",
    "evidence_quality",
    "incremental_vs_inventory",
  ],
});

/**
 * Rank and gate Stage 2 Deep Research queue from a shared pool.
 * Never promotes verified dates alone over commercial thesis.
 */
export function selectStage2DeepResearchQueue(pool = [], opts = {}) {
  const maxDeep = Number(opts.maxDeepResearch || 5);
  const maxRelationship = Number(opts.maxRelationshipResearch || 8);

  const inputs = pool.map((c) => ({
    title: c.sourceTitle || c.title,
    organizationName:
      c.organizationName === "[object Object]" ? null : c.organizationName,
    summary: c.summary,
    category: c.category,
    venueName: c.location || c.venueName,
    sourceUrl: c.sourceUrl,
    city: c.city,
    region: c.region,
    startDate: c.startDate,
    endDate: c.endDate,
    recordKind:
      c.researchTrack === "ACCOUNT_RELATIONSHIP_DEMAND" ||
      /knot|wedding/i.test(String(c.actorOrRequestId || ""))
        ? "VENDOR_OR_VENUE_LISTING"
        : c.demandMechanism || c.recordKind,
    relationshipTrackOnly:
      c.researchTrack === "ACCOUNT_RELATIONSHIP_DEMAND" ||
      c.commercialScreen === "RELATIONSHIP_CONTINUATION_CANDIDATE",
    headline: c.sourceTitle || c.title,
  }));

  const ranked = rankCandidatesForDeepResearch(inputs);
  const deepQueue = [];
  const relationshipQueue = [];
  const rejected = [];

  for (const row of ranked) {
    if (row.prequal.decision === PREQUAL_DECISION.ADVANCE_TO_DEEP_RESEARCH) {
      if (deepQueue.length < maxDeep) {
        deepQueue.push({
          ...row,
          deepResearchPlan: buildAdaptiveDeepResearchPlan(row),
        });
      } else {
        rejected.push({
          reason: "DEEP_QUEUE_CAP",
          title: row.candidate.title,
          qualityClass: row.classification.qualityClass,
        });
      }
    } else if (row.prequal.decision === PREQUAL_DECISION.RELATIONSHIP_RESEARCH) {
      if (relationshipQueue.length < maxRelationship) {
        relationshipQueue.push({
          ...row,
          deepResearchPlan: buildAdaptiveDeepResearchPlan(row),
        });
      } else {
        rejected.push({
          reason: "RELATIONSHIP_QUEUE_CAP",
          title: row.candidate.title,
          qualityClass: row.classification.qualityClass,
        });
      }
    } else {
      rejected.push({
        reason: row.prequal.rejectionReason || row.prequal.decision,
        title: row.candidate.title,
        qualityClass: row.classification.qualityClass,
        rankReasons: row.rankReasons,
      });
    }
  }

  // Mandatory wedding research: ensure at least one planner/relationship enters queue when present
  const hasWedding = [...deepQueue, ...relationshipQueue].some((r) =>
    /wedding|planner|bridal|knot/i.test(
      `${r.candidate.title} ${r.candidate.sourceUrl} ${r.classification.qualityClass}`,
    ),
  );
  if (!hasWedding) {
    const weddingFallback = ranked.find(
      (r) =>
        r.classification.qualityClass === "PLANNER_OR_RELATIONSHIP_LEAD" &&
        !relationshipQueue.includes(r),
    );
    if (weddingFallback && relationshipQueue.length < maxRelationship) {
      relationshipQueue.push({
        ...weddingFallback,
        deepResearchPlan: buildAdaptiveDeepResearchPlan(weddingFallback),
        forcedWeddingResearch: true,
      });
    }
  }

  return {
    contract: STAGE2_SELECTION_CONTRACT_V7_1,
    deepQueue,
    relationshipQueue,
    rejected,
    rankedSummary: ranked.map((r) => ({
      title: r.candidate.title,
      decision: r.prequal.decision,
      qualityClass: r.classification.qualityClass,
      rankReasons: r.rankReasons,
    })),
    partyBusGuard: {
      wouldSelectPartyBus: ranked.some(
        (r) =>
          /party bus/i.test(r.candidate.title || "") &&
          (r.prequal.decision === PREQUAL_DECISION.ADVANCE_TO_DEEP_RESEARCH ||
            r.prequal.decision === PREQUAL_DECISION.RELATIONSHIP_RESEARCH),
      ),
      note: "Must be false — nightlife transport must REJECT_EARLY",
    },
  };
}

export function auditCandidateCommercialQuality(c) {
  const classification = classifyDiscoveryCandidate(c);
  return { classification, ...rankCandidatesForDeepResearch([c])[0] };
}
