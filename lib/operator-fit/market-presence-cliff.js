/**
 * Market Presence cliff diagnostics — deterministic, no weight retune.
 */

import {
  establishesCurrentGeographicEligibility,
  MARKET_PRESENCE_TYPE,
  STRONG_GEOGRAPHIC_SUPPORT,
} from "../operator-intelligence/market-presence.js";

/**
 * @param {{ fromType: string, toType: string, alignmentBefore?: number, alignmentAfter?: number, eligibilityBefore?: string, eligibilityAfter?: string, readinessBefore?: string, readinessAfter?: string }} input
 */
export function diagnoseMarketPresenceCliff(input = {}) {
  const fromType = String(input.fromType || "");
  const toType = String(input.toType || "");
  const fromStrong = establishesCurrentGeographicEligibility(fromType);
  const toStrong = establishesCurrentGeographicEligibility(toType);

  let verdict = "Correct eligibility behavior";
  let detail =
    "Strong presence types establish geographic eligibility; weak types (Strategic Interest, Historical, Claimed Capability) do not.";

  if (fromStrong === toStrong) {
    if (fromType && toType && fromType !== toType) {
      verdict = "Data-model issue";
      detail =
        "Presence type changed without eligibility strength change — check if taxonomy collapses distinct evidence states.";
    } else {
      verdict = "No cliff";
      detail = "No eligibility-strength transition.";
    }
  } else if (!fromStrong && toStrong) {
    const fromWeak =
      /strategic interest|historical|claimed capability|active development/i.test(fromType);
    verdict = fromWeak ? "Correct eligibility behavior" : "Bad taxonomy behavior";
    detail = fromWeak
      ? `${fromType || "(none)"} → ${toType} correctly unlocks geographic eligibility.`
      : `Unexpected unlock from ${fromType} → ${toType}; review taxonomy equivalence.`;
  } else if (fromStrong && !toStrong) {
    verdict = "Correct eligibility behavior";
    detail = `Loss of strong presence (${fromType} → ${toType || "none"}) correctly removes eligibility.`;
  }

  const alignDelta =
    input.alignmentBefore != null && input.alignmentAfter != null
      ? Math.round((Number(input.alignmentAfter) - Number(input.alignmentBefore)) * 10) / 10
      : null;

  return {
    previousPresenceType: fromType || null,
    newPresenceType: toType || null,
    fromEstablishesEligibility: fromStrong,
    toEstablishesEligibility: toStrong,
    eligibilityBefore: input.eligibilityBefore || null,
    eligibilityAfter: input.eligibilityAfter || null,
    alignmentBefore: input.alignmentBefore ?? null,
    alignmentAfter: input.alignmentAfter ?? null,
    alignmentDelta: alignDelta,
    readinessBefore: input.readinessBefore || null,
    readinessAfter: input.readinessAfter || null,
    discontinuous: fromStrong !== toStrong,
    verdict,
    detail,
    scoringRetuneRecommended: false,
    knownStrongTypes: [...STRONG_GEOGRAPHIC_SUPPORT],
    taxonomyReference: MARKET_PRESENCE_TYPE,
  };
}
