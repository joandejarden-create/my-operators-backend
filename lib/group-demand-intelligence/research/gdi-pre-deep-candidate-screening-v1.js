/**
 * Pre-deep screening of ALL discovery candidates before maxCandidates / paid deep.
 * Deterministic / zero-cost. Extends commercial-pursuit lowCostCommercialScreen
 * + published inventory dedupe + global out-of-market retention outcomes.
 */

import { lowCostCommercialScreen, SCREEN_DECISION } from "./commercial-pursuit-strategy-v1.js";
import {
  classifyDiscoveryForGlobalRetention,
  HOTEL_MATCH_REASON,
  GLOBAL_INTELLIGENCE_STATE,
} from "../global-intelligence/gdi-global-discovered-intelligence-contract-v1.js";
import {
  loadPublishedPackageInventory,
  evaluateAgainstPublishedInventory,
} from "./gdi-published-inventory-dedupe-v1.js";
import {
  evaluatePrePaidDeepDedupe,
  buildInventoryDedupeContextForHotels,
} from "./gdi-paid-deep-two-stage-dedupe-v1.js";
import { dedupeCandidatesAcrossSegments } from "./gdi-cross-segment-opportunity-dedupe-v2.js";

export const GDI_PRE_DEEP_CANDIDATE_SCREENING_V1 =
  "gdi-pre-deep-candidate-screening-v1";

export const PRE_DEEP_SCREEN_OUTCOME = Object.freeze({
  ELIGIBLE_FOR_DEEP_RESEARCH: "ELIGIBLE_FOR_DEEP_RESEARCH",
  ELIGIBLE_FOR_TARGETED_RESEARCH: "ELIGIBLE_FOR_TARGETED_RESEARCH",
  SCREEN_ONLY: "SCREEN_ONLY",
  DEFER_BUDGET: "DEFER_BUDGET",
  GLOBAL_INTELLIGENCE_ONLY: "GLOBAL_INTELLIGENCE_ONLY",
  EXISTING_OPPORTUNITY_ENRICHMENT: "EXISTING_OPPORTUNITY_ENRICHMENT",
  DUPLICATE_NO_NEW_EVIDENCE: "DUPLICATE_NO_NEW_EVIDENCE",
  REJECT_UNSUPPORTED: "REJECT_UNSUPPORTED",
});

export const PRE_DEEP_SCREENING_VERSION = "gdi-pre-deep-candidate-screening-v1.2";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function mapPursuitToOutcome(screen, globalClass, publishedEval) {
  if (publishedEval?.decision === "DUPLICATE_NO_NEW_EVIDENCE") {
    return {
      outcome: PRE_DEEP_SCREEN_OUTCOME.DUPLICATE_NO_NEW_EVIDENCE,
      eligibleForDeep: false,
    };
  }
  if (publishedEval?.decision === "EXISTING_OPPORTUNITY_ENRICHMENT") {
    return {
      outcome: PRE_DEEP_SCREEN_OUTCOME.EXISTING_OPPORTUNITY_ENRICHMENT,
      eligibleForDeep: publishedEval.allowDeepResearch !== false,
    };
  }

  if (
    globalClass?.hotelMatchReason === HOTEL_MATCH_REASON.OUT_OF_MARKET_FOR_CURRENT_HOTEL
  ) {
    return {
      outcome: PRE_DEEP_SCREEN_OUTCOME.GLOBAL_INTELLIGENCE_ONLY,
      eligibleForDeep: false,
    };
  }

  if (
    globalClass?.globalState === GLOBAL_INTELLIGENCE_STATE.REJECTED_UNSUPPORTED ||
    screen?.decision === SCREEN_DECISION.REJECT_IRRELEVANT
  ) {
    // Pursuit may reject out-of-market as REJECT_IRRELEVANT — already handled above.
    if (/catchment_mismatch|out_of_market/i.test((screen?.reasons || []).join(" "))) {
      return {
        outcome: PRE_DEEP_SCREEN_OUTCOME.GLOBAL_INTELLIGENCE_ONLY,
        eligibleForDeep: false,
      };
    }
    return {
      outcome: PRE_DEEP_SCREEN_OUTCOME.REJECT_UNSUPPORTED,
      eligibleForDeep: false,
    };
  }

  if (screen?.decision === SCREEN_DECISION.SCREEN_ONLY) {
    return {
      outcome: PRE_DEEP_SCREEN_OUTCOME.SCREEN_ONLY,
      eligibleForDeep: false,
    };
  }

  if (screen?.decision === SCREEN_DECISION.RESEARCH_CONTINUATION) {
    return {
      outcome: PRE_DEEP_SCREEN_OUTCOME.ELIGIBLE_FOR_TARGETED_RESEARCH,
      eligibleForDeep: true,
      preferTier3: true,
    };
  }

  if (screen?.decision === SCREEN_DECISION.ADVANCE_TO_DEEP_RESEARCH) {
    return {
      outcome: PRE_DEEP_SCREEN_OUTCOME.ELIGIBLE_FOR_DEEP_RESEARCH,
      eligibleForDeep: true,
    };
  }

  return {
    outcome: PRE_DEEP_SCREEN_OUTCOME.SCREEN_ONLY,
    eligibleForDeep: false,
  };
}

/**
 * Screen one candidate (zero-cost).
 */
export function screenCandidatePreDeep(candidate = {}, context = {}) {
  const hotelId = context.hotelId || candidate.hotelId || null;
  const pursuit = lowCostCommercialScreen(candidate, {
    segmentId: candidate.researchSegment || candidate.segmentId,
  });
  const globalClass = classifyDiscoveryForGlobalRetention(candidate, {
    hotelProfile: context.hotelProfile || {},
  });
  const publishedEval = evaluateAgainstPublishedInventory(
    candidate,
    context.publishedInventory || null,
  );
  const localDedupe = evaluatePrePaidDeepDedupe(candidate, {
    inventoryOrgKeys: context.inventoryOrgKeys || null,
    inventoryOpportunityIds: context.inventoryOpportunityIds || null,
    completedDeepResearchIds: context.completedDeepResearchIds || null,
    paidDeepOrgKeys: context.paidDeepOrgKeys || null,
  });

  let mapped = mapPursuitToOutcome(pursuit, globalClass, publishedEval);

  // Local hotel inventory org match without distinct edition → duplicate
  if (
    localDedupe.skipPaidDeep &&
    localDedupe.reasonCode === "EXISTING_HOTEL_INVENTORY_ORG" &&
    mapped.outcome !== PRE_DEEP_SCREEN_OUTCOME.EXISTING_OPPORTUNITY_ENRICHMENT
  ) {
    mapped = {
      outcome: PRE_DEEP_SCREEN_OUTCOME.DUPLICATE_NO_NEW_EVIDENCE,
      eligibleForDeep: false,
    };
  }

  if (
    localDedupe.skipPaidDeep &&
    /ALREADY_SETTLED|CROSS_SEGMENT_ORG/.test(String(localDedupe.reasonCode || ""))
  ) {
    mapped = {
      outcome: PRE_DEEP_SCREEN_OUTCOME.DUPLICATE_NO_NEW_EVIDENCE,
      eligibleForDeep: false,
    };
  }

  const decisionReason = [
    ...(pursuit.reasons || []),
    publishedEval.reasonCode,
    localDedupe.reasonCode,
    globalClass.hotelMatchReason,
    mapped.preferTier3 ? "prefer_tier3_targeted" : null,
  ]
    .filter(Boolean)
    .join("|");

  return {
    version: PRE_DEEP_SCREENING_VERSION,
    contract: GDI_PRE_DEEP_CANDIDATE_SCREENING_V1,
    candidateId: candidate.id || candidate.opportunityId || null,
    hotelId,
    segmentId: candidate.researchSegment || candidate.segmentId || null,
    organizationName: text(candidate.organizationName) || null,
    outcome: mapped.outcome,
    eligibleForDeep: mapped.eligibleForDeep === true,
    preferTier3: mapped.preferTier3 === true,
    decisionReason,
    screeningVersion: PRE_DEEP_SCREENING_VERSION,
    runId: context.runId || null,
    pursuitDecision: pursuit.decision,
    pursuitStrategy: pursuit.strategy,
    publishedDecision: publishedEval.decision,
    publishedMatchedIds: publishedEval.matchedOpportunityIds || [],
    globalState: globalClass.globalState,
    hotelMatchReason: globalClass.hotelMatchReason,
    localDedupeReasonCode: localDedupe.reasonCode,
    outOfMarketForCurrentHotel: globalClass.outOfMarketForCurrentHotel === true,
  };
}

/**
 * Screen every candidate before deep-research selection / cap.
 */
export function screenAllCandidatesPreDeep(candidates = [], context = {}) {
  const hotelIds = [
    ...new Set(
      [
        context.hotelId,
        ...candidates.map((c) => c.hotelId),
      ].filter(Boolean),
    ),
  ];
  const primaryHotelId = context.hotelId || hotelIds[0] || null;
  const publishedInventory =
    context.publishedInventory ||
    (primaryHotelId ? loadPublishedPackageInventory(primaryHotelId) : null);
  const localInventory =
    context.localInventory ||
    (hotelIds.length
      ? buildInventoryDedupeContextForHotels(hotelIds)
      : { inventoryOrgKeys: new Set(), inventoryOpportunityIds: new Set() });

  const screenContext = {
    ...context,
    hotelId: primaryHotelId,
    publishedInventory,
    inventoryOrgKeys: localInventory.inventoryOrgKeys,
    inventoryOpportunityIds: localInventory.inventoryOpportunityIds,
  };

  const decisions = [];
  const eligible = [];
  const counts = Object.fromEntries(
    Object.values(PRE_DEEP_SCREEN_OUTCOME).map((k) => [k, 0]),
  );

  for (const cand of candidates) {
    const decision = screenCandidatePreDeep(cand, screenContext);
    decisions.push(decision);
    counts[decision.outcome] = (counts[decision.outcome] || 0) + 1;
    if (decision.eligibleForDeep) {
      eligible.push({
        ...cand,
        preDeepScreen: decision,
      });
    } else {
      // Annotate non-eligible for audit trail on the object if mutable
      if (cand && typeof cand === "object") {
        cand.preDeepScreen = decision;
      }
    }
  }

  // Cross-segment program dedupe BEFORE deep selection / maxCandidates.
  // Fairness floors must not re-select the same CARPHA / Advantage path twice.
  let crossSegmentDedupe = null;
  let eligibleAfterDedupe = eligible;
  if (context.crossSegmentDedupe !== false && eligible.length > 1) {
    crossSegmentDedupe = dedupeCandidatesAcrossSegments(eligible, {
      hotelId: primaryHotelId,
    });
    eligibleAfterDedupe = crossSegmentDedupe.canonicalCandidates || eligible;
    const removed = Number(crossSegmentDedupe.suppressedCount || 0);
    if (removed > 0) {
      counts[PRE_DEEP_SCREEN_OUTCOME.DUPLICATE_NO_NEW_EVIDENCE] =
        (counts[PRE_DEEP_SCREEN_OUTCOME.DUPLICATE_NO_NEW_EVIDENCE] || 0) + removed;
      for (const peer of crossSegmentDedupe.suppressedCandidates || []) {
        decisions.push({
          version: PRE_DEEP_SCREENING_VERSION,
          contract: GDI_PRE_DEEP_CANDIDATE_SCREENING_V1,
          candidateId: peer.id || peer.opportunityId || null,
          hotelId: primaryHotelId,
          segmentId: peer.researchSegment || peer.segmentId || null,
          organizationName: text(peer.organizationName) || null,
          outcome: PRE_DEEP_SCREEN_OUTCOME.DUPLICATE_NO_NEW_EVIDENCE,
          eligibleForDeep: false,
          decisionReason:
            peer.preDeepScreen?.decisionReason ||
            "cross_segment_dedupe_v2",
          crossSegmentDedupeV2: peer.crossSegmentDedupeV2 || null,
        });
      }
    }
  }

  return {
    version: PRE_DEEP_SCREENING_VERSION,
    contract: GDI_PRE_DEEP_CANDIDATE_SCREENING_V1,
    runId: context.runId || null,
    hotelId: primaryHotelId,
    candidateCount: candidates.length,
    screenedCount: decisions.length,
    eligibleForDeepCount: eligibleAfterDedupe.length,
    eligibleForDeepCountBeforeCrossSegmentDedupe: eligible.length,
    outcomeCounts: counts,
    publishedPackage: publishedInventory?.packageMeta || null,
    crossSegmentDedupe,
    decisions,
    eligibleCandidates: eligibleAfterDedupe,
  };
}

export function summarizePreDeepScreenLedger(ledger = {}) {
  return {
    version: ledger.version,
    candidateCount: ledger.candidateCount,
    screenedCount: ledger.screenedCount,
    eligibleForDeepCount: ledger.eligibleForDeepCount,
    outcomeCounts: ledger.outcomeCounts,
    publishedPackage: ledger.publishedPackage,
    duplicatesPrevented:
      (ledger.outcomeCounts?.[PRE_DEEP_SCREEN_OUTCOME.DUPLICATE_NO_NEW_EVIDENCE] || 0),
    globalOnlyRetained:
      (ledger.outcomeCounts?.[PRE_DEEP_SCREEN_OUTCOME.GLOBAL_INTELLIGENCE_ONLY] || 0),
  };
}

export default {
  GDI_PRE_DEEP_CANDIDATE_SCREENING_V1,
  PRE_DEEP_SCREEN_OUTCOME,
  screenCandidatePreDeep,
  screenAllCandidatesPreDeep,
  summarizePreDeepScreenLedger,
};
