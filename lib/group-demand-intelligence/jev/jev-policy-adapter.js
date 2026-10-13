/**
 * Dealality policy adapter — Jev recommends; hard gates + thresholds decide.
 * Jev never writes opportunities and never overrides hard gates.
 */

import { getThreshold } from "./jev-config.js";
import { HARD_GATES, isValidChoice } from "./jev-types.js";

/**
 * Detect deterministic hard gates from policy context.
 * @returns {string[]} gate codes
 */
export function detectHardGates(policyContext = {}) {
  const gates = [];
  const p = policyContext || {};
  if (p.pastEvent === true || p.eventForwardness === "HISTORICAL_ONLY" || p.hardPastEvent) {
    gates.push(HARD_GATES.PAST_EVENT);
  }
  if (p.fullyPlaced === true || p.sourcingStatus === "FULLY_PLACED") {
    gates.push(HARD_GATES.FULLY_PLACED);
  }
  if (p.noGeographicFit === true || p.geoOk === false) {
    gates.push(HARD_GATES.NO_GEOGRAPHIC_FIT);
  }
  if (p.exclusivePartnerFound === true) {
    gates.push(HARD_GATES.EXCLUSIVE_PARTNER_FOUND);
  }
  if (p.noValidSource === true || p.sourceOk === false) {
    gates.push(HARD_GATES.NO_VALID_SOURCE);
  }
  if (p.privacyReject === true || p.signalQuality === "PRIVACY_REJECT") {
    gates.push(HARD_GATES.PRIVACY_REJECT);
  }
  if (p.invalidDate === true) {
    gates.push(HARD_GATES.INVALID_DATE);
  }
  if (p.duplicateCanonical === true) {
    gates.push(HARD_GATES.DUPLICATE_CANONICAL_OPPORTUNITY);
  }
  if (Array.isArray(p.hardGates)) {
    for (const g of p.hardGates) {
      if (g && !gates.includes(g)) gates.push(g);
    }
  }
  return gates;
}

/**
 * Apply policy on top of a normalized Jev decision.
 * In shadow mode, finalPolicyDecision mirrors existingDecision (no behavior change).
 */
export function applyJevPolicy({
  decisionType,
  jevSelected,
  confidence,
  existingDecision = null,
  policyContext = {},
  shadow = true,
} = {}) {
  const hardGates = detectHardGates(policyContext);
  const threshold = getThreshold(decisionType);
  const conf = typeof confidence === "number" ? confidence : null;
  const lowConfidence = conf == null || conf < threshold;
  const invalidChoice = jevSelected && !isValidChoice(decisionType, jevSelected);

  let policyOutcome = "USE_EXISTING";
  let acceptedJev = null;
  let reason = "default_existing";

  if (hardGates.length) {
    policyOutcome = "HARD_GATE";
    acceptedJev = null;
    reason = `hard_gate:${hardGates.join(",")}`;
  } else if (invalidChoice) {
    policyOutcome = "INVALID_JEV_SCHEMA";
    reason = "invalid_choice";
  } else if (lowConfidence) {
    policyOutcome = "LOW_CONFIDENCE_FALLBACK";
    reason = `below_threshold_${threshold}`;
  } else if (jevSelected) {
    policyOutcome = "JEV_CANDIDATE";
    acceptedJev = jevSelected;
    reason = "jev_above_threshold";
  }

  // V1 shadow: production path always keeps existing decision
  const finalPolicyDecision = shadow
    ? existingDecision != null
      ? existingDecision
      : acceptedJev
    : hardGates.length
      ? existingDecision
      : acceptedJev != null
        ? acceptedJev
        : existingDecision;

  const matchExisting =
    existingDecision == null || jevSelected == null
      ? null
      : String(existingDecision) === String(jevSelected);

  return {
    hardGates,
    threshold,
    lowConfidence,
    policyOutcome,
    acceptedJev,
    reason,
    shadow: shadow === true,
    finalPolicyDecision,
    matchExisting,
    // Explicit: Jev must not auto-retire / auto-promote
    blockedActions: {
      autoRetire: true,
      autoPromote: true,
      writeOpportunity: true,
    },
  };
}

/**
 * Map high-risk false-pursue / false-stop patterns for evaluation reports.
 */
export function classifyHighRiskError({
  decisionType,
  jevSelected,
  expected,
  policyContext = {},
} = {}) {
  const hard = detectHardGates(policyContext);
  const pursueLike = new Set([
    "RESEARCH_NOW",
    "LIKELY_ACTIONABLE",
    "STRONG_DEMAND_SIGNAL",
    "FOLLOWUP_HIGH_VALUE",
    "CONTINUE_RESEARCH",
    "SPECIFIC_EVENT_CANDIDATE",
  ]);
  const stopLike = new Set([
    "STOP",
    "STOP_NO_USEFUL_EVIDENCE",
    "STOP_NO_USEFUL_NEW_EVIDENCE",
    "LIKELY_REJECT",
    "NO_DEMAND_SIGNAL",
    "PAUSE",
    "RETIRE_CANDIDATE",
  ]);

  const out = [];
  if (hard.includes(HARD_GATES.PAST_EVENT) && pursueLike.has(jevSelected)) {
    out.push("PAST_EVENT_FALSE_PURSUE");
  }
  if (
    (policyContext.localNoRoom === true ||
      jevSelected === "LIKELY_LOCAL_NO_ROOM" ||
      expected === "LIKELY_LOCAL_NO_ROOM") &&
    (jevSelected === "LIKELY_ACTIONABLE" || jevSelected === "STRONG_DEMAND_SIGNAL")
  ) {
    out.push("LOCAL_NO_ROOM_FALSE_PURSUE");
  }
  if (hard.includes(HARD_GATES.PRIVACY_REJECT) && pursueLike.has(jevSelected)) {
    out.push("PRIVACY_FALSE_PURSUE");
  }
  if (hard.includes(HARD_GATES.FULLY_PLACED) && pursueLike.has(jevSelected)) {
    out.push("FULLY_PLACED_FALSE_PURSUE");
  }
  if (
    (expected === "LIKELY_ACTIONABLE" ||
      expected === "TRUE_ACTIONABLE" ||
      expected === "RESEARCH_NOW" ||
      expected === "CONTINUE_RESEARCH") &&
    stopLike.has(jevSelected)
  ) {
    out.push("TRUE_OPPORTUNITY_FALSE_STOP");
  }
  return out;
}
