/**
 * HIF V6 — Commercial actionability / priority (founder 2026-11-21).
 * Research & sales-planning signal — not publication approval.
 * Unresolved combined agreement type cannot auto-become BCT/MCT.
 * Near-term expiration alone ≠ Priority A.
 */

import { TIMING_HORIZON, DECISION_WINDOW_KIND } from "./hif-v6-commercial-timing-model-v6.js";
import { GEO_CLASS } from "./hif-v6-geographic-confidence-v6.js";

export const HIF_V6_COMMERCIAL_ACTIONABILITY = "hif-v6-commercial-actionability-v6";

export const COMMERCIAL_PRIORITY = Object.freeze({
  A_PURSUE_QUALIFY_NOW: "PRIORITY_A",
  B_NEAR_TERM_RESEARCH: "PRIORITY_B",
  C_STRATEGIC_WATCHLIST: "PRIORITY_C",
  D_LONG_TERM_INTELLIGENCE: "PRIORITY_D",
  HOLD: "HOLD",
});

export const TIMING_RELEVANCE = Object.freeze({
  IMMEDIATE: "IMMEDIATE",
  NEAR_12M: "NEAR_12M",
  MID_24M: "MID_24M",
  LONGER: "LONGER",
  HISTORICAL: "HISTORICAL",
  UNKNOWN: "UNKNOWN",
});

function agreementTypeConfidence(row) {
  if (row.agreementTypeResolved === false || /UNKNOWN|UNRESOLVED|COMBINED/i.test(String(row.agreementType || row.dateMeaning || row.commercialList || ""))) {
    return "UNRESOLVED";
  }
  if (/MULTIPLE/i.test(String(row.agreementType || row.dateMeaning || ""))) return "MULTIPLE";
  if (/FRANCHISE/i.test(String(row.agreementType || ""))) return "FRANCHISE";
  if (/MANAGE/i.test(String(row.agreementType || ""))) return "MANAGEMENT";
  if (/OPERAT/i.test(String(row.agreementType || ""))) return "OPERATING";
  return "RESOLVED_OTHER";
}

function timingRelevance(horizon) {
  if (horizon === TIMING_HORIZON.NOW_ACTIVE_PROCESS || horizon === TIMING_HORIZON.M0_6 || horizon === TIMING_HORIZON.M6_12) {
    return TIMING_RELEVANCE.NEAR_12M;
  }
  if (horizon === TIMING_HORIZON.M12_24) return TIMING_RELEVANCE.MID_24M;
  if (horizon === TIMING_HORIZON.HISTORICAL_SUPERSEDED) return TIMING_RELEVANCE.HISTORICAL;
  if (horizon === TIMING_HORIZON.UNKNOWN) return TIMING_RELEVANCE.UNKNOWN;
  return TIMING_RELEVANCE.LONGER;
}

function signalStrength(row, timing) {
  let score = 0;
  if (timing.decisionWindowKind === DECISION_WINDOW_KIND.CONFIRMED_DECISION_WINDOW) score += 3;
  else if (timing.decisionWindowKind === DECISION_WINDOW_KIND.EVIDENCE_INFERRED_WINDOW) score += 2;
  else if (timing.decisionWindowKind === DECISION_WINDOW_KIND.EXPIRATION_ONLY_PROXY) score += 1;
  if (row.activeCommercialTrigger || row.confirmedFranchiseNonrenewal || row.confirmedManagerDeparture) score += 2;
  if (/BRAND_CONVERSION_TARGETS|MANAGEMENT_CONTRACT_TARGETS/.test(String(row.commercialList || "")) && row.agreementTypeResolved) {
    score += 1;
  }
  if (score >= 4) return "STRONG";
  if (score >= 2) return "MODERATE";
  if (score >= 1) return "WEAK";
  return "INSUFFICIENT";
}

/**
 * Assign commercial priority. Conservative: expiration-only near-term ≠ Priority A.
 */
export function classifyCommercialActionability(row, timing, geo) {
  const agr = agreementTypeConfidence(row);
  const timingRel = timingRelevance(timing.timingHorizon);
  const signal = signalStrength(row, timing);
  const geoOk = geo.geographicClass === GEO_CLASS.VERIFIED;
  const geoReview = geo.geographicClass === GEO_CLASS.REVIEW_REQUIRED;
  const geoBad = geo.geographicClass === GEO_CLASS.UNRESOLVED;
  const statusConfidence =
    row.currentStatusVerified === true
      ? "VERIFIED_CURRENT"
      : timing.decisionWindowKind === DECISION_WINDOW_KIND.EXPIRATION_ONLY_PROXY
        ? "EXPIRATION_UNCHECKED_FOR_RENEWAL"
        : "UNKNOWN";

  const reasons = [];

  if (geoBad) {
    return {
      commercialPriority: COMMERCIAL_PRIORITY.HOLD,
      timingRelevance: timingRel,
      commercialSignalStrength: signal,
      agreementTypeConfidence: agr,
      geographicConfidence: geo.geographicClass,
      currentStatusConfidence: statusConfidence,
      reasons: ["PHYSICAL_COUNTRY_UNRESOLVED"],
    };
  }

  if (timing.timingHorizon === TIMING_HORIZON.HISTORICAL_SUPERSEDED) {
    return {
      commercialPriority: COMMERCIAL_PRIORITY.D_LONG_TERM_INTELLIGENCE,
      timingRelevance: TIMING_RELEVANCE.HISTORICAL,
      commercialSignalStrength: signal,
      agreementTypeConfidence: agr,
      geographicConfidence: geo.geographicClass,
      currentStatusConfidence: statusConfidence,
      reasons: ["HISTORICAL_OR_SUPERSEDED_PRESERVED"],
    };
  }

  // Priority A: confirmed decision window OR active process, resolved agr type (not MULTIPLE/UNRESOLVED), geo verified, near-term
  const near =
    timing.timingHorizon === TIMING_HORIZON.NOW_ACTIVE_PROCESS ||
    timing.timingHorizon === TIMING_HORIZON.M0_6 ||
    timing.timingHorizon === TIMING_HORIZON.M6_12;

  if (
    near &&
    timing.decisionWindowKind === DECISION_WINDOW_KIND.CONFIRMED_DECISION_WINDOW &&
    (agr === "FRANCHISE" || agr === "MANAGEMENT" || agr === "OPERATING") &&
    geoOk &&
    signal !== "INSUFFICIENT"
  ) {
    reasons.push("CONFIRMED_NEAR_TERM_DECISION_WINDOW");
    return {
      commercialPriority: COMMERCIAL_PRIORITY.A_PURSUE_QUALIFY_NOW,
      timingRelevance: TIMING_RELEVANCE.IMMEDIATE,
      commercialSignalStrength: signal,
      agreementTypeConfidence: agr,
      geographicConfidence: geo.geographicClass,
      currentStatusConfidence: statusConfidence,
      reasons,
    };
  }

  if (
    near &&
    timing.decisionWindowKind === DECISION_WINDOW_KIND.EVIDENCE_INFERRED_WINDOW &&
    (agr === "FRANCHISE" || agr === "MANAGEMENT") &&
    (geoOk || geoReview) &&
    signal === "STRONG"
  ) {
    reasons.push("ACTIVE_OR_INFERRED_NEAR_TERM_SIGNAL");
    return {
      commercialPriority: COMMERCIAL_PRIORITY.A_PURSUE_QUALIFY_NOW,
      timingRelevance: TIMING_RELEVANCE.IMMEDIATE,
      commercialSignalStrength: signal,
      agreementTypeConfidence: agr,
      geographicConfidence: geo.geographicClass,
      currentStatusConfidence: statusConfidence,
      reasons,
    };
  }

  // Explicit: expiration-only cannot be Priority A
  if (near && timing.decisionWindowKind === DECISION_WINDOW_KIND.EXPIRATION_ONLY_PROXY) {
    reasons.push("NEAR_TERM_EXPIRATION_ONLY_PROXY_NOT_PRIORITY_A");
  }

  // Priority B: within ~24 months, needs more qualification
  const within24 =
    near ||
    timing.timingHorizon === TIMING_HORIZON.M12_24;

  if (within24 && agr !== "UNRESOLVED" && !geoBad) {
    reasons.push("WITHIN_24M_NEEDS_FURTHER_QUALIFICATION");
    return {
      commercialPriority: COMMERCIAL_PRIORITY.B_NEAR_TERM_RESEARCH,
      timingRelevance: near ? TIMING_RELEVANCE.NEAR_12M : TIMING_RELEVANCE.MID_24M,
      commercialSignalStrength: signal,
      agreementTypeConfidence: agr,
      geographicConfidence: geo.geographicClass,
      currentStatusConfidence: statusConfidence,
      reasons,
    };
  }

  if (within24 && agr === "UNRESOLVED" && !geoBad) {
    reasons.push("WITHIN_24M_BUT_AGREEMENT_TYPE_UNRESOLVED");
    return {
      commercialPriority: COMMERCIAL_PRIORITY.B_NEAR_TERM_RESEARCH,
      timingRelevance: near ? TIMING_RELEVANCE.NEAR_12M : TIMING_RELEVANCE.MID_24M,
      commercialSignalStrength: signal,
      agreementTypeConfidence: agr,
      geographicConfidence: geo.geographicClass,
      currentStatusConfidence: statusConfidence,
      reasons,
    };
  }

  if (
    timing.timingHorizon === TIMING_HORIZON.M24_36 ||
    timing.timingHorizon === TIMING_HORIZON.Y3_5
  ) {
    reasons.push("LONGER_HORIZON_CREDIBLE_CONTRACTUAL_INTELLIGENCE");
    return {
      commercialPriority: COMMERCIAL_PRIORITY.C_STRATEGIC_WATCHLIST,
      timingRelevance: TIMING_RELEVANCE.LONGER,
      commercialSignalStrength: signal,
      agreementTypeConfidence: agr,
      geographicConfidence: geo.geographicClass,
      currentStatusConfidence: statusConfidence,
      reasons,
    };
  }

  if (timing.timingHorizon === TIMING_HORIZON.Y5_PLUS) {
    reasons.push("LONG_DATED_CONTRACTUAL_INTELLIGENCE");
    return {
      commercialPriority: COMMERCIAL_PRIORITY.D_LONG_TERM_INTELLIGENCE,
      timingRelevance: TIMING_RELEVANCE.LONGER,
      commercialSignalStrength: signal,
      agreementTypeConfidence: agr,
      geographicConfidence: geo.geographicClass,
      currentStatusConfidence: statusConfidence,
      reasons,
    };
  }

  reasons.push("TIMING_OR_EVIDENCE_INSUFFICIENT");
  return {
    commercialPriority: COMMERCIAL_PRIORITY.HOLD,
    timingRelevance: timingRel,
    commercialSignalStrength: signal,
    agreementTypeConfidence: agr,
    geographicConfidence: geo.geographicClass,
    currentStatusConfidence: statusConfidence,
    reasons,
  };
}
