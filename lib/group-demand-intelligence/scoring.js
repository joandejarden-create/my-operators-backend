/**
 * Group Demand Intelligence — Hotel Fit + Evidence Confidence + Priority.
 * Weights documented in config/group-demand-intelligence/scoring-weights.json.
 * Reviewed 2026-09-13 vs GM framework: weights already match (no change).
 */

import {
  BOOKING_WINDOW,
  DEMAND_TERRITORY_FIT,
  OPPORTUNITY_QUALIFICATION,
  OPPORTUNITY_TYPE,
  PRIORITY,
  VENUE_SOURCING_STATUS,
} from "./claim-types.js";
import { runHighPriorityQualityGate } from "./qualification-precision.js";
import {
  isCoreTerritory,
  isOutsideTerritory,
} from "./demand-territory.js";

/**
 * Hotel Fit component weights — matches GM-requested framework:
 * 25% Physical · 20% Geographic/Demand Territory · 15% Timing · 15% Commercial
 * 10% Historical · 10% Competitive Accessibility · 5% Contactability
 *
 * Field name `geographyFit` is the Demand Territory Fit component (kept for schema stability).
 */
export const DEFAULT_HOTEL_FIT_WEIGHTS = Object.freeze({
  physicalFit: 0.25,
  geographyFit: 0.2,
  timing: 0.15,
  commercialValue: 0.15,
  historicalFit: 0.1,
  competitiveAccessibility: 0.1,
  contactability: 0.05,
});

/** Customer-facing labels for Hotel Fit components */
export const HOTEL_FIT_COMPONENT_LABELS = Object.freeze({
  physicalFit: "Physical Fit",
  geographyFit: "Demand Territory Fit",
  timing: "Timing / Winnability",
  commercialValue: "Commercial Potential",
  historicalFit: "Historical Hotel / Brand Fit",
  competitiveAccessibility: "Competitive Accessibility",
  contactability: "Contactability",
});

export const EVIDENCE_CONFIDENCE_TOOLTIP =
  "How confident Dealality is in the facts underlying the opportunity based on source quality, recency, corroboration and how much is verified versus inferred.";

function clampScore(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return 0;
  return Math.max(0, Math.min(100, Math.round(x)));
}

function weightedTotal(components, weights = DEFAULT_HOTEL_FIT_WEIGHTS) {
  let sum = 0;
  let wSum = 0;
  for (const [key, weight] of Object.entries(weights)) {
    const v = Number(components[key]);
    if (!Number.isFinite(v)) continue;
    sum += v * weight;
    wSum += weight;
  }
  if (wSum <= 0) return 0;
  return clampScore(sum / wSum);
}

/**
 * @param {object} input component scores 0–100
 * @param {object} [weights]
 */
export function computeHotelFitScore(input = {}, weights = DEFAULT_HOTEL_FIT_WEIGHTS) {
  const components = {
    physicalFit: clampScore(input.physicalFit),
    geographyFit: clampScore(input.geographyFit),
    timing: clampScore(input.timing),
    commercialValue: clampScore(input.commercialValue),
    historicalFit: clampScore(input.historicalFit),
    competitiveAccessibility: clampScore(input.competitiveAccessibility),
    contactability: clampScore(input.contactability),
  };
  return {
    hotelFitScore: weightedTotal(components, weights),
    components,
    weights: { ...weights },
    componentLabels: { ...HOTEL_FIT_COMPONENT_LABELS },
    scoringVersion: "gdi-hotel-fit-v1",
  };
}

/**
 * Plain-English Hotel Fit explanation from components + optional territory.
 */
export function buildHotelFitExplanation({
  components = {},
  demandTerritoryFit = null,
  fitExplanation = null,
} = {}) {
  if (fitExplanation && String(fitExplanation).trim()) {
    return String(fitExplanation).trim();
  }
  const parts = [];
  if ((components.physicalFit || 0) >= 75) {
    parts.push("event size aligns with the hotel's meeting and room-block capacity");
  } else if ((components.physicalFit || 0) >= 55) {
    parts.push("event size is workable but not a perfect capacity match");
  }
  if (isCoreTerritory(demandTerritoryFit) || (components.geographyFit || 0) >= 75) {
    parts.push("the hotel is in a credible demand territory for this opportunity");
  } else if ((components.geographyFit || 0) >= 55) {
    parts.push("the hotel is a possible but stretch alternative for this demand");
  }
  if ((components.timing || 0) >= 75) {
    parts.push("hotel selection still appears open");
  }
  if ((components.contactability || 0) >= 70) {
    parts.push("an actionable planning contact has been identified");
  }
  if (!parts.length) {
    return "Fit reflects physical capacity, demand territory, timing, commercial potential, history, competition, and contactability.";
  }
  return `Strong fit signals because ${parts.join(", ")}.`;
}

/**
 * Evidence Confidence 0–100 — intentionally independent of Hotel Fit.
 */
export function computeEvidenceConfidence(input = {}) {
  const sourceAuthority = clampScore(input.sourceAuthority ?? 50);
  const independentSources = clampScore(
    Math.min(100, (Number(input.independentSourceCount) || 0) * 20)
  );
  const directness = clampScore(input.directness ?? 50);
  const recency = clampScore(input.recency ?? 50);
  const verifiedFieldRatio = clampScore((Number(input.verifiedFieldRatio) || 0) * 100);
  const firstParty = clampScore(input.firstPartyShare ?? 40);
  const completeness = clampScore(input.completeness ?? 40);
  const conflictPenalty = clampScore(input.conflictPenalty ?? 0);

  const raw =
    sourceAuthority * 0.2 +
    independentSources * 0.15 +
    directness * 0.15 +
    recency * 0.1 +
    verifiedFieldRatio * 0.15 +
    firstParty * 0.1 +
    completeness * 0.15 -
    conflictPenalty * 0.2;

  return {
    evidenceConfidence: clampScore(raw),
    factors: {
      sourceAuthority,
      independentSources,
      directness,
      recency,
      verifiedFieldRatio,
      firstParty,
      completeness,
      conflictPenalty,
    },
    version: "gdi-evidence-confidence-v1",
    tooltip: EVIDENCE_CONFIDENCE_TOOLTIP,
  };
}

/**
 * Plain-English Evidence Confidence explanation from evidence rows + score.
 */
export function buildEvidenceConfidenceExplanation({
  evidenceConfidence = 0,
  evidence = [],
  evidenceConfidenceExplanation = null,
} = {}) {
  if (evidenceConfidenceExplanation && String(evidenceConfidenceExplanation).trim()) {
    return String(evidenceConfidenceExplanation).trim();
  }
  const rows = Array.isArray(evidence) ? evidence : [];
  const verified = rows.filter((e) => e.claimKind === "FACT").length;
  const estimated = rows.filter((e) => e.claimKind === "ESTIMATED").length;
  const inferred = rows.filter((e) => e.claimKind === "INFERENCE").length;
  const firstParty = rows.filter((e) =>
    /official|government|association|nist|nih|first.?party/i.test(
      `${e.sourceType || ""} ${e.sourceTitle || ""} ${e.sourceDomain || ""}`
    )
  ).length;
  const score = Number(evidenceConfidence) || 0;
  const band =
    score >= 80 ? "High confidence" : score >= 55 ? "Moderate confidence" : "Lower confidence";
  return `${band}. ${verified} verified field(s), ${estimated} estimated, ${inferred} inferred. ${firstParty} source(s) look first-party or official. Confidence reflects source quality, recency, corroboration, and how much is verified versus inferred — not Hotel Fit.`;
}

function isActionableBookingWindow(bw) {
  return (
    bw === BOOKING_WINDOW.CONTACT_NOW ||
    bw === BOOKING_WINDOW.QUALIFY_NOW ||
    bw === BOOKING_WINDOW.RESEARCH_FURTHER
  );
}

/**
 * Priority classification — salesperson-facing junk stays DISQUALIFIED / hidden.
 *
 * Conceptual order (qualification precision):
 * Venue/Sourcing → Hotel Opportunity Validity → Room Demand → Geography →
 * Hotel Fit → Timing → Contactability → Priority.
 * A high Hotel Fit cannot override a failed qualification gate.
 */
export function classifyPriority({
  hotelFitScore,
  evidenceConfidence,
  bookingWindowStatus,
  disqualifyReasons = [],
  demandTerritoryFit = null,
  opportunityQualification = null,
  opportunityType = null,
  venueSourcingStatus = null,
  roomDemandStatus = null,
  eventLocationStatus = null,
  contactQuality = null,
  hotelOpportunityThesis = null,
  whyNow = null,
  highPriorityGate = null,
  opportunity = null,
} = {}) {
  if (Array.isArray(disqualifyReasons) && disqualifyReasons.length > 0) {
    return {
      priority: PRIORITY.DISQUALIFIED,
      reason: disqualifyReasons.join("; "),
    };
  }

  if (isOutsideTerritory(demandTerritoryFit)) {
    return {
      priority: PRIORITY.DISQUALIFIED,
      reason: "Outside realistic demand territory for this hotel",
    };
  }

  if (
    (venueSourcingStatus === VENUE_SOURCING_STATUS.FULLY_PLACED &&
      opportunityType !== OPPORTUNITY_TYPE.FUTURE_CYCLE &&
      opportunityType !== OPPORTUNITY_TYPE.REACTIVATION) ||
    (opportunityQualification === OPPORTUNITY_QUALIFICATION.CLOSED &&
      opportunityType !== OPPORTUNITY_TYPE.FUTURE_CYCLE &&
      opportunityType !== OPPORTUNITY_TYPE.REACTIVATION) ||
    opportunityType === OPPORTUNITY_TYPE.CLOSED_DISQUALIFIED
  ) {
    return {
      priority: PRIORITY.DISQUALIFIED,
      reason:
        "Qualification closed — venue/hotel already placed or no realistic sales path (Hotel Fit does not override)",
    };
  }

  if (
    venueSourcingStatus === VENUE_SOURCING_STATUS.FULLY_PLACED ||
    venueSourcingStatus === VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE
  ) {
    if (opportunityType === OPPORTUNITY_TYPE.FUTURE_CYCLE || opportunityType === OPPORTUNITY_TYPE.REACTIVATION) {
      return {
        priority: PRIORITY.WATCHLIST,
        reason:
          "Current/primary venue placed — retain as future cycle / reactivation watch only (not open sourcing)",
      };
    }
    return {
      priority: PRIORITY.DISQUALIFIED,
      reason: "Primary venue selected with no overflow evidence — cannot be High",
    };
  }

  const fit = Number(hotelFitScore) || 0;
  const conf = Number(evidenceConfidence) || 0;
  const bw = String(bookingWindowStatus || "");

  if (bw === BOOKING_WINDOW.LIKELY_TOO_LATE) {
    return {
      priority: PRIORITY.DISQUALIFIED,
      reason: "Booking window likely closed",
    };
  }

  if (opportunityType === OPPORTUNITY_TYPE.FUTURE_CYCLE || bw === BOOKING_WINDOW.TOO_EARLY) {
    if (fit >= 50 || conf >= 35) {
      return {
        priority: PRIORITY.WATCHLIST,
        reason: "Future-cycle / too early — monitor, do not treat as current High pursuit",
      };
    }
  }

  const gate =
    highPriorityGate ||
    runHighPriorityQualityGate(opportunity || { whyNow, hotelFitScore: fit, evidenceConfidence: conf, bookingWindowStatus: bw }, {
      venueSourcingStatus,
      opportunityType,
      roomDemandStatus,
      eventLocationStatus,
      contactQuality,
      hotelOpportunityThesis,
      opportunityQualification,
      demandTerritoryFit,
      hotelFitScore: fit,
      evidenceConfidence: conf,
      bookingWindowStatus: bw,
    });

  const qualOpen =
    opportunityQualification === OPPORTUNITY_QUALIFICATION.VERIFIED_OPEN ||
    opportunityQualification === OPPORTUNITY_QUALIFICATION.STRONG;

  if (
    qualOpen &&
    gate.pass &&
    (fit >= 75 ||
      (fit >= 72 &&
        (opportunity?.captureCapacityState === "IDEAL_CAPTURE_RANGE" ||
          opportunity?.captureCapacityState === "PLAUSIBLE_CAPTURE"))) &&
    conf >= 55 &&
    isActionableBookingWindow(bw) &&
    (opportunityType === OPPORTUNITY_TYPE.PRIMARY_PURSUIT ||
      opportunityType === OPPORTUNITY_TYPE.OVERFLOW_HOUSING)
  ) {
    return {
      priority: PRIORITY.HIGH,
      reason:
        "Qualification open + venue/housing path + room-demand support + Hotel Fit + evidence + actionable why-now",
    };
  }

  // Strong fit alone is not enough for High when qualification gate fails
  if (fit >= 75 && conf >= 55 && isActionableBookingWindow(bw) && !qualOpen) {
    return {
      priority: PRIORITY.MEDIUM,
      reason: `Hotel Fit is strong but Opportunity Qualification is ${opportunityQualification || "incomplete"} — High blocked by qualification gate`,
    };
  }

  if (
    (qualOpen || opportunityQualification === OPPORTUNITY_QUALIFICATION.MODERATE) &&
    fit >= 60 &&
    conf >= 40 &&
    bw !== BOOKING_WINDOW.TOO_EARLY
  ) {
    return {
      priority: PRIORITY.MEDIUM,
      reason: "Credible pursue/qualify path with incomplete High gate or moderate qualification",
    };
  }

  if (opportunityType === OPPORTUNITY_TYPE.REACTIVATION && fit >= 55) {
    return {
      priority: PRIORITY.MEDIUM,
      reason: "Reactivation opportunity — commercially useful, not automatic High",
    };
  }

  if (fit >= 50 || conf >= 35) {
    return {
      priority: PRIORITY.WATCHLIST,
      reason: "Promising future opportunity; not yet actionable",
    };
  }

  return {
    priority: PRIORITY.DISQUALIFIED,
    reason: "Insufficient fit and evidence for salesperson view",
  };
}

export function reconstructScoreAudit(opportunity) {
  return {
    hotelFit: {
      score: opportunity.hotelFitScore,
      components: {
        physicalFit: opportunity.physicalFitScore,
        geographyFit: opportunity.geographyFitScore,
        timing: opportunity.timingScore,
        commercialValue: opportunity.commercialValueScore,
        historicalFit: opportunity.historicalFitScore,
        competitiveAccessibility: opportunity.competitiveAccessibilityScore,
        contactability: opportunity.contactabilityScore,
      },
      componentLabels: { ...HOTEL_FIT_COMPONENT_LABELS },
      weights: DEFAULT_HOTEL_FIT_WEIGHTS,
      explanation:
        opportunity.fitExplanation ||
        buildHotelFitExplanation({
          components: {
            physicalFit: opportunity.physicalFitScore,
            geographyFit: opportunity.geographyFitScore,
            timing: opportunity.timingScore,
            commercialValue: opportunity.commercialValueScore,
            historicalFit: opportunity.historicalFitScore,
            competitiveAccessibility: opportunity.competitiveAccessibilityScore,
            contactability: opportunity.contactabilityScore,
          },
          demandTerritoryFit: opportunity.demandTerritoryFit,
          fitExplanation: opportunity.fitExplanation,
        }),
    },
    evidenceConfidence: opportunity.evidenceConfidence,
    evidenceConfidenceExplanation:
      opportunity.evidenceConfidenceExplanation ||
      buildEvidenceConfidenceExplanation({
        evidenceConfidence: opportunity.evidenceConfidence,
        evidence: opportunity.evidence,
        evidenceConfidenceExplanation: opportunity.evidenceConfidenceExplanation,
      }),
    evidenceConfidenceTooltip: EVIDENCE_CONFIDENCE_TOOLTIP,
    priority: opportunity.priority,
    bookingWindowStatus: opportunity.bookingWindowStatus,
    demandTerritoryFit: opportunity.demandTerritoryFit || null,
    sourcingStatus: opportunity.sourcingStatus || "UNKNOWN",
    opportunityType: opportunity.opportunityType || null,
    venueSourcingStatus: opportunity.venueSourcingStatus || null,
    roomDemandStatus: opportunity.roomDemandStatus || null,
    opportunityQualification: opportunity.opportunityQualification || null,
    contactQuality: opportunity.contactQuality || null,
    eventLocationStatus: opportunity.eventLocationStatus || null,
    hotelOpportunityThesis: opportunity.hotelOpportunityThesis || null,
  };
}
