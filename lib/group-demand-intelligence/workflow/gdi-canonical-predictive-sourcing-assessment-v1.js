/**
 * Minimum evidence-backed predictive sourcing assessment V1.
 *
 * Builds on CAD (#602/#603/#608), competitor-commitment mapping, and deep-research
 * fields already on Research SoT. Does NOT invent a parallel predictive engine.
 * Does NOT certify "full predictive capability" from CAD classification alone.
 *
 * Unsupported dates → UNKNOWN. Estimated window ≠ confirmed open contract.
 */

import {
  CAD_CLASS,
  CAD_SOURCING_WINDOW_STATE,
  CAD_ATTAINABILITY,
} from "./gdi-competitive-account-discovery-contract-v1.js";
import {
  extractCompetitorHotelFromResearchEvidence,
  isEventVenueOnlyMention,
} from "../commercial-consistency/gdi-research-competitor-commitment-mapping-v1.js";

export const GDI_CANONICAL_PREDICTIVE_SOURCING_ASSESSMENT_V1 =
  "gdi-canonical-predictive-sourcing-assessment-v1";

export const PREDICTIVE_EVIDENCE_STATUS = Object.freeze({
  EVIDENCED: "EVIDENCED",
  INFERRED: "INFERRED",
  UNKNOWN: "UNKNOWN",
  NOT_ASSESSED: "NOT_ASSESSED",
  GAP: "GAP",
});

export const PREDICTIVE_CAPABILITY_STATUS = Object.freeze({
  MINIMUM_WORKFLOW_AVAILABLE: "MINIMUM_WORKFLOW_AVAILABLE",
  PARTIAL_THROUGH_CAD_AND_DEEP_RESEARCH: "PARTIAL_THROUGH_CAD_AND_DEEP_RESEARCH",
  FULL_PREDICTIVE_ENGINE_NOT_CLAIMED: "FULL_PREDICTIVE_ENGINE_NOT_CLAIMED",
});

function text(v) {
  return String(v == null ? "" : v).trim();
}

function statusFromWindow(state) {
  if (state === CAD_SOURCING_WINDOW_STATE.CONFIRMED) {
    return PREDICTIVE_EVIDENCE_STATUS.EVIDENCED;
  }
  if (state === CAD_SOURCING_WINDOW_STATE.INFERRED) {
    return PREDICTIVE_EVIDENCE_STATUS.INFERRED;
  }
  return PREDICTIVE_EVIDENCE_STATUS.UNKNOWN;
}

/**
 * Assess one demand account against the founder 10-point predictive checklist.
 */
export function assessPredictiveSourcingForOpportunity(opportunity = {}, opts = {}) {
  const opp = opportunity && typeof opportunity === "object" ? opportunity : {};
  const cad = opp.competitiveAccountDiscovery || opts.cad || null;
  const research = {
    competitorHotel:
      cad?.incumbent?.hotelName ||
      opp.likelyCompetitor ||
      opp.competitorHotel ||
      opp.incumbentHotel ||
      null,
    venue: opp.venue || opp.eventVenue || null,
    lodgingEvidence: opp.lodgingEvidence || "",
    hotelSourcingDecisionEvidence: opp.hotelSourcingDecisionEvidence || "",
  };
  const competitorFromEvidence =
    extractCompetitorHotelFromResearchEvidence(research) || research.competitorHotel;
  const venueOnly =
    Boolean(competitorFromEvidence) &&
    isEventVenueOnlyMention(research, competitorFromEvidence);

  const hostHotels = {
    status: competitorFromEvidence && !venueOnly
      ? cad?.incumbent?.relationship === "VERIFIED"
        ? PREDICTIVE_EVIDENCE_STATUS.EVIDENCED
        : PREDICTIVE_EVIDENCE_STATUS.INFERRED
      : venueOnly
        ? PREDICTIVE_EVIDENCE_STATUS.UNKNOWN
        : PREDICTIVE_EVIDENCE_STATUS.UNKNOWN,
    value: venueOnly ? null : competitorFromEvidence || null,
    note: venueOnly
      ? "Event venue alone is not an incumbent hotel commitment"
      : null,
  };

  const recurringCycle = {
    status: /recurring|annual|next (edition|cycle|year)|future cycle/i.test(
      `${opp.title || ""} ${opp.whyThisHotel || ""} ${opp.recommendedNextStep || ""} ${opp.whatIsHappening || ""}`,
    )
      ? PREDICTIVE_EVIDENCE_STATUS.INFERRED
      : PREDICTIVE_EVIDENCE_STATUS.UNKNOWN,
    value: null,
    note: "Verified recurring cycle requires edition evidence — not assumed from CAD class alone",
  };

  const windowState =
    cad?.sourcingWindow?.state || CAD_SOURCING_WINDOW_STATE.UNKNOWN;
  const nextSourcingWindow = {
    status: statusFromWindow(windowState),
    value: {
      state: windowState,
      start: cad?.sourcingWindow?.start || null,
      end: cad?.sourcingWindow?.end || null,
      basis: cad?.sourcingWindow?.basis || null,
    },
    note:
      windowState === CAD_SOURCING_WINDOW_STATE.CONFIRMED
        ? "Confirmed window still ≠ open contract"
        : "Unsupported dates classified UNKNOWN",
  };

  const commitmentConstraints = {
    status: cad?.incumbent?.relationship
      ? PREDICTIVE_EVIDENCE_STATUS.INFERRED
      : PREDICTIVE_EVIDENCE_STATUS.UNKNOWN,
    value: {
      relationship: cad?.incumbent?.relationship || null,
      cadClass: cad?.cadClass || null,
    },
  };

  const attainability = {
    status: cad?.attainability
      ? PREDICTIVE_EVIDENCE_STATUS.INFERRED
      : PREDICTIVE_EVIDENCE_STATUS.UNKNOWN,
    value: cad?.attainability || CAD_ATTAINABILITY.MONITOR,
  };

  const overflowPathway = {
    status: /overflow|alternate lodging|residual|housing partner/i.test(
      `${opp.whyThisHotel || ""} ${opp.lodgingEvidence || ""} ${JSON.stringify(opp.competitiveContext || {})}`,
    )
      ? PREDICTIVE_EVIDENCE_STATUS.INFERRED
      : PREDICTIVE_EVIDENCE_STATUS.UNKNOWN,
    value: null,
  };

  const namedStakeholders = {
    status:
      text(opp.primaryContact?.name) ||
      text(opp.contactName) ||
      (Array.isArray(opp.namedStakeholders) && opp.namedStakeholders.length)
        ? PREDICTIVE_EVIDENCE_STATUS.EVIDENCED
        : PREDICTIVE_EVIDENCE_STATUS.UNKNOWN,
    value: opp.primaryContact || opp.namedStakeholders || null,
  };

  const hotelPursuitRecommendation = {
    status: text(opp.recommendedNextStep || opp.recommendedAction)
      ? PREDICTIVE_EVIDENCE_STATUS.EVIDENCED
      : PREDICTIVE_EVIDENCE_STATUS.UNKNOWN,
    value: opp.recommendedNextStep || opp.recommendedAction || null,
  };

  const weeklyChangeMaterial = {
    status: PREDICTIVE_EVIDENCE_STATUS.NOT_ASSESSED,
    value: null,
    note: "Weekly Change mapping requires inventory persistence; not claimed from CAD alone",
    gap: true,
  };

  const checklist = {
    historicalOrCurrentHostHotels: hostHotels,
    verifiedRecurringDemandCycle: recurringCycle,
    nextRealisticSourcingOpportunity: nextSourcingWindow,
    evidenceSupportingSourcingWindow: nextSourcingWindow,
    existingCommitmentOrContractConstraints: commitmentConstraints,
    competitiveAttainabilityAndLimitations: attainability,
    potentialOverflowOrAlternativeLodging: overflowPathway,
    namedCommerciallyRelevantStakeholders: namedStakeholders,
    hotelSpecificPursuitRecommendation: hotelPursuitRecommendation,
    materialChangesForWeeklyChange: weeklyChangeMaterial,
  };

  const gaps = Object.entries(checklist)
    .filter(
      ([, v]) =>
        v.status === PREDICTIVE_EVIDENCE_STATUS.UNKNOWN ||
        v.status === PREDICTIVE_EVIDENCE_STATUS.NOT_ASSESSED ||
        v.status === PREDICTIVE_EVIDENCE_STATUS.GAP ||
        v.gap === true,
    )
    .map(([k]) => k);

  return {
    contract: GDI_CANONICAL_PREDICTIVE_SOURCING_ASSESSMENT_V1,
    opportunityId: opp.id || opp.opportunityId || null,
    cadClass: cad?.cadClass || null,
    fqoPass: false,
    estimatedWindowIsNotOpenContract: true,
    fullPredictiveEngineClaimed: false,
    capabilityStatus:
      PREDICTIVE_CAPABILITY_STATUS.PARTIAL_THROUGH_CAD_AND_DEEP_RESEARCH,
    checklist,
    gaps,
    remainingGaps: [
      ...gaps,
      cad?.cadClass === CAD_CLASS.FQO_CANDIDATE
        ? null
        : "CAD_CLASS_ALONE_IS_NOT_PREDICTIVE_CERTIFICATION",
    ].filter(Boolean),
  };
}

/**
 * Attach predictive sourcing assessments onto CAD-enriched rows.
 */
export function attachPredictiveSourcingAssessments(rows = []) {
  return (Array.isArray(rows) ? rows : []).map((row) => {
    const assessment = assessPredictiveSourcingForOpportunity(row);
    return {
      ...row,
      predictiveSourcingAssessment: assessment,
    };
  });
}

export function summarizePredictiveSourcingCapability(assessments = []) {
  const list = Array.isArray(assessments) ? assessments : [];
  const gapCounts = {};
  for (const a of list) {
    for (const g of a.gaps || []) {
      gapCounts[g] = (gapCounts[g] || 0) + 1;
    }
  }
  return {
    contract: GDI_CANONICAL_PREDICTIVE_SOURCING_ASSESSMENT_V1,
    assessedCount: list.length,
    capabilityStatus:
      PREDICTIVE_CAPABILITY_STATUS.MINIMUM_WORKFLOW_AVAILABLE,
    fullPredictiveEngineClaimed: false,
    note:
      "Minimum evidence-backed predictive sourcing workflow available through CAD + competitor-commitment + deep-research fields. Full predictive engine is NOT claimed.",
    gapCounts,
    preciseRemainingGaps: [
      "WEEKLY_CHANGE_MATERIALITY_REQUIRES_PERSISTENCE_JOIN",
      "VERIFIED_RECURRING_CYCLE_NEEDS_EDITION_EVIDENCE_NOT_HEURISTICS",
      "CONFIRMED_SOURCING_WINDOW_STILL_REQUIRES_OPEN_CONTRACT_EVIDENCE_FOR_FQO",
      "NO_STANDALONE_PREDICTIVE_CAPTURE_ENGINE",
    ],
  };
}
