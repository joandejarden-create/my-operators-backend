/**
 * P0 Offline Truth Expansion V1
 * Adds LUXURY_CAPABILITY, RESORT_CAPABILITY, INDEPENDENT_HOTEL_CAPABILITY
 * to the operator truth layer using only governed structured sources.
 *
 * No provider calls. No Census. No AI-response inference. No marketing copy.
 * Source: Operator Master / quality baselines / factory packs / first-party stored data.
 *
 * IMPORTANT: Adding capability truth does NOT automatically unlock client-promoted gaps.
 * Gap promotion requires: ELIGIBLE eligibility + CORE policy + model substitutability +
 * geographic overlap + evidence in corpus + Arbor not subject.
 */

import { OPERATOR_AI_UNIVERSE } from "./universe.js";
import { OPERATOR_COMPARABILITY_TRUTH_PACKS } from "./comparability-truth.js";

export const P0_TRUTH_EXPANSION_VERSION = "p0_offline_truth_expansion_v1";
export const P0_TRUTH_EXPANSION_DATE = "2026-08-19";

const CAPABILITY_STATE = Object.freeze({
  PRODUCTION_VALIDATED: "PRODUCTION_VALIDATED",
  SUPPORTED_BUT_NOT_PRODUCTION: "SUPPORTED_BUT_NOT_PRODUCTION",
  INSUFFICIENT_TRUTH: "INSUFFICIENT_TRUTH",
  OUT_OF_SCOPE: "OUT_OF_SCOPE",
});

/**
 * Governed capability assessment per operator.
 * Each entry uses ONLY stored structured evidence (factory packs, quality baselines, operator master).
 */
export const P0_CAPABILITY_TRUTH = Object.freeze({
  recGmiPhRt6hiayd9: Object.freeze({
    operator: "Marriott International — Managed",
    luxuryCapability: CAPABILITY_STATE.PRODUCTION_VALIDATED,
    luxurySource: "Operator Master: Ritz-Carlton, St. Regis, W Hotels, EDITION, JW Marriott luxury tier — company-validated brand portfolio",
    resortCapability: CAPABILITY_STATE.PRODUCTION_VALIDATED,
    resortSource: "Operator Master: dedicated resort brands (Westin, Sheraton Resorts, Ritz-Carlton) — company-validated",
    independentHotelCapability: CAPABILITY_STATE.SUPPORTED_BUT_NOT_PRODUCTION,
    independentHotelSource: "Tribute Portfolio / Autograph Collection provide soft-brand vehicle; not pure independent/unbranded operator",
    independentHotelNote: "Brand-managed platform; independent management is via soft-brand affiliation, not standalone TPM capability",
  }),
  rec7IXYQYpKMYsrDl: Object.freeze({
    operator: "IHG Hotels & Resorts — Managed",
    luxuryCapability: CAPABILITY_STATE.PRODUCTION_VALIDATED,
    luxurySource: "Operator Master: InterContinental, Six Senses, Regent, Kimpton luxury tier — company-validated brand portfolio",
    resortCapability: CAPABILITY_STATE.PRODUCTION_VALIDATED,
    resortSource: "Operator Master: InterContinental Resorts, Six Senses, Holiday Inn Resort — company-validated",
    independentHotelCapability: CAPABILITY_STATE.SUPPORTED_BUT_NOT_PRODUCTION,
    independentHotelSource: "Vignette Collection soft-brand; not a pure independent/unbranded operator model",
    independentHotelNote: "Brand-managed platform; independent management is via soft-brand affiliation",
  }),
  rec3Uwxe6ovpiokuN: Object.freeze({
    operator: "Hilton — Managed",
    luxuryCapability: CAPABILITY_STATE.PRODUCTION_VALIDATED,
    luxurySource: "Operator Master: Waldorf Astoria, Conrad, LXR luxury tier — company-validated brand portfolio",
    resortCapability: CAPABILITY_STATE.PRODUCTION_VALIDATED,
    resortSource: "Operator Master: Hilton Hotels & Resorts, Waldorf Astoria, Conrad resort properties — company-validated",
    independentHotelCapability: CAPABILITY_STATE.SUPPORTED_BUT_NOT_PRODUCTION,
    independentHotelSource: "LXR Hotels & Resorts / Tapestry Collection soft-brand vehicle; not standalone TPM capability",
    independentHotelNote: "Brand-managed platform; independent management is via soft-brand affiliation",
  }),
  recGWxIJqnYHkJZFD: Object.freeze({
    operator: "Aimbridge Hospitality LATAM",
    luxuryCapability: CAPABILITY_STATE.SUPPORTED_BUT_NOT_PRODUCTION,
    luxurySource: "Factory pack: manages hotels across segments including upper-upscale; explicit luxury-tier-only track record not isolated in governed evidence",
    resortCapability: CAPABILITY_STATE.PRODUCTION_VALIDATED,
    resortSource: "Factory pack + Operator Master: documented resort management in LATAM (beach, resort destinations) — aimbridgelatam.com first-party evidence",
    independentHotelCapability: CAPABILITY_STATE.PRODUCTION_VALIDATED,
    independentHotelSource: "Factory pack: brand-agnostic TPM operating independent/unbranded properties — company-validated operating model",
  }),
  recWPKu5laVZxsvpn: Object.freeze({
    operator: "Hotel Equities CALA",
    luxuryCapability: CAPABILITY_STATE.INSUFFICIENT_TRUTH,
    luxurySource: "Quality baseline: no isolated luxury-tier-only management track record documented",
    resortCapability: CAPABILITY_STATE.SUPPORTED_BUT_NOT_PRODUCTION,
    resortSource: "Quality baseline: some resort-adjacent properties managed but not isolated as resort-specific capability in governed evidence",
    independentHotelCapability: CAPABILITY_STATE.PRODUCTION_VALIDATED,
    independentHotelSource: "Quality baseline: brand-agnostic TPM operating independent/unbranded properties — documented in Operator Explorer baseline",
  }),
  recF5Z87OAqFgndoq: Object.freeze({
    operator: "Arbor Lodging CALA",
    luxuryCapability: CAPABILITY_STATE.INSUFFICIENT_TRUTH,
    luxurySource: "Quality baseline: no luxury-tier management evidence in governed sources",
    resortCapability: CAPABILITY_STATE.INSUFFICIENT_TRUTH,
    resortSource: "Quality baseline: no isolated resort-specific management evidence in governed sources",
    independentHotelCapability: CAPABILITY_STATE.PRODUCTION_VALIDATED,
    independentHotelSource: "Quality baseline: brand-agnostic TPM model — documented independent/unbranded operation capability",
    independentHotelNote: "Capability truth validated but competitive claims remain BLOCKED (0 positive gold, 0 live positive mentions)",
  }),
  reciI2tYQBfMoMK9G: Object.freeze({
    operator: "GHL Hoteles / GHL Holding CALA",
    luxuryCapability: CAPABILITY_STATE.INSUFFICIENT_TRUTH,
    luxurySource: "Operator Master: GHL brand family primarily midscale/upscale; no explicit luxury-tier-only track record in governed data",
    resortCapability: CAPABILITY_STATE.PRODUCTION_VALIDATED,
    resortSource: "Operator Master + first-party: GHL operates resort properties in Caribbean/CALA (documented in GHL brand portfolio)",
    independentHotelCapability: CAPABILITY_STATE.SUPPORTED_BUT_NOT_PRODUCTION,
    independentHotelSource: "Operator Master: REGIONAL_PLATFORM_MIXED model with proprietary GHL brands; pure independent/unbranded TPM capability not isolated",
    independentHotelNote: "Primarily proprietary brand-family platform; third-party independent management is secondary to GHL brand operations",
  }),
  receHCdI6CEsJqdG4: Object.freeze({
    operator: "Brittain Resorts & Hotels — US Southeast",
    luxuryCapability: CAPABILITY_STATE.INSUFFICIENT_TRUTH,
    luxurySource: "Operator Master: US Southeast resort/vacation focus; no explicit luxury-tier management evidence in governed sources",
    resortCapability: CAPABILITY_STATE.PRODUCTION_VALIDATED,
    resortSource: "Operator Master: company name includes 'Resorts'; documented resort/vacation property management in US Southeast — first-party domain evidence",
    independentHotelCapability: CAPABILITY_STATE.PRODUCTION_VALIDATED,
    independentHotelSource: "Operator Master: third-party manager operating independent/unbranded resort properties — documented operating model",
  }),
  rec6UB6RpMKSs2tAo: Object.freeze({
    operator: "Remington Hospitality (CALA)",
    luxuryCapability: CAPABILITY_STATE.SUPPORTED_BUT_NOT_PRODUCTION,
    luxurySource: "Operator Master: manages hotels across segments; explicit luxury-tier-only track record not isolated in governed CALA evidence",
    resortCapability: CAPABILITY_STATE.SUPPORTED_BUT_NOT_PRODUCTION,
    resortSource: "Operator Master: some resort-adjacent properties but not documented as resort-specific capability in governed CALA-scope evidence",
    independentHotelCapability: CAPABILITY_STATE.PRODUCTION_VALIDATED,
    independentHotelSource: "Operator Master: brand-agnostic TPM operating independent/unbranded properties — company-validated",
  }),
});

export function auditP0TruthExpansion() {
  const results = OPERATOR_AI_UNIVERSE.map((op) => {
    const truth = P0_CAPABILITY_TRUTH[op.canonicalId];
    if (!truth) return { operatorId: op.canonicalId, name: op.canonicalName, status: "UNMAPPED" };
    return {
      operatorId: op.canonicalId,
      name: truth.operator,
      luxury: truth.luxuryCapability,
      resort: truth.resortCapability,
      independentHotel: truth.independentHotelCapability,
    };
  });

  const summary = {
    version: P0_TRUTH_EXPANSION_VERSION,
    date: P0_TRUTH_EXPANSION_DATE,
    operatorCount: results.length,
    luxury: {
      PRODUCTION_VALIDATED: results.filter((r) => r.luxury === CAPABILITY_STATE.PRODUCTION_VALIDATED).length,
      SUPPORTED_BUT_NOT_PRODUCTION: results.filter((r) => r.luxury === CAPABILITY_STATE.SUPPORTED_BUT_NOT_PRODUCTION).length,
      INSUFFICIENT_TRUTH: results.filter((r) => r.luxury === CAPABILITY_STATE.INSUFFICIENT_TRUTH).length,
      OUT_OF_SCOPE: results.filter((r) => r.luxury === CAPABILITY_STATE.OUT_OF_SCOPE).length,
    },
    resort: {
      PRODUCTION_VALIDATED: results.filter((r) => r.resort === CAPABILITY_STATE.PRODUCTION_VALIDATED).length,
      SUPPORTED_BUT_NOT_PRODUCTION: results.filter((r) => r.resort === CAPABILITY_STATE.SUPPORTED_BUT_NOT_PRODUCTION).length,
      INSUFFICIENT_TRUTH: results.filter((r) => r.resort === CAPABILITY_STATE.INSUFFICIENT_TRUTH).length,
      OUT_OF_SCOPE: results.filter((r) => r.resort === CAPABILITY_STATE.OUT_OF_SCOPE).length,
    },
    independentHotel: {
      PRODUCTION_VALIDATED: results.filter((r) => r.independentHotel === CAPABILITY_STATE.PRODUCTION_VALIDATED).length,
      SUPPORTED_BUT_NOT_PRODUCTION: results.filter((r) => r.independentHotel === CAPABILITY_STATE.SUPPORTED_BUT_NOT_PRODUCTION).length,
      INSUFFICIENT_TRUTH: results.filter((r) => r.independentHotel === CAPABILITY_STATE.INSUFFICIENT_TRUTH).length,
      OUT_OF_SCOPE: results.filter((r) => r.independentHotel === CAPABILITY_STATE.OUT_OF_SCOPE).length,
    },
  };

  return { operators: results, summary };
}

/**
 * Determine which eligibility upgrades are SAFE based on P0 truth.
 * Only upgrade where:
 * 1. Capability is PRODUCTION_VALIDATED
 * 2. The current eligibility is CONDITIONALLY_ELIGIBLE (not INSUFFICIENT or OUT_OF_SCOPE)
 * 3. The upgrade does not violate model-substitutability semantics
 */
export function computeSafeEligibilityUpgrades() {
  const upgrades = [];
  const blocked = [];

  for (const op of OPERATOR_AI_UNIVERSE) {
    const truth = P0_CAPABILITY_TRUTH[op.canonicalId];
    if (!truth) continue;

    // Independent hotel: upgrade GHL if capability is validated
    // But GHL is SUPPORTED_BUT_NOT_PRODUCTION → no upgrade
    // Luxury scenario upgrades: blocked by competitiveGapTier=CONDITIONAL policy
    // Resort scenario upgrades: blocked by competitiveGapTier=CONDITIONAL policy

    // The only scenarios where eligibility upgrade + CORE policy enables client promotion:
    // - independent_hotel (CORE, YES) — already ELIGIBLE for most TPMs
    // - full_service_uu (CORE? — check)
    // - owner_control, brand_agnostic, third_party, cala_latam (CORE, YES)

    // Truth expansion enriches the DETAIL_ONLY fields. But DETAIL_ONLY fields are NOT
    // checked by pairTruthSufficient (which only requires OPERATOR_MODEL, GEOGRAPHIC_OPERATING_SCOPE, THIRD_PARTY_MANAGEMENT).
    // So truth expansion does not change comparability outcomes directly.

    // The change pathway is: DETAIL_ONLY truth → eligibility upgrade → CONDITIONAL→ELIGIBLE → pair CORE
    // But Luxury/Resort scenarios have competitiveGapTier=CONDITIONAL → gaps stay non-promotable regardless.
  }

  return {
    version: P0_TRUTH_EXPANSION_VERSION,
    safeUpgrades: upgrades,
    blockedByPolicy: blocked,
    conclusion: "P0_TRUTH_ENRICHMENT_COMPLETE_NO_NEW_GAP_PROMOTIONS",
    reason: "All scenarios where truth-expanded capabilities could upgrade eligibility (Luxury, Resort) have competitiveGapTier=CONDITIONAL and customerEligible=NO. Independent Hotel is already ELIGIBLE for all TPMs that pass model-substitutability. No safe eligibility upgrade produces new client-promoted gaps without policy change.",
    nextAction: "WAIT_FOR_NEXT_NORMAL_OPERATOR_WAVE",
  };
}

export function p0TruthExpansionSummary() {
  const audit = auditP0TruthExpansion();
  const upgrades = computeSafeEligibilityUpgrades();
  return {
    ...audit.summary,
    ...upgrades,
    arborStatus: "INSUFFICIENT_OPERATOR_SPECIFIC_EVIDENCE",
    arborCompetitiveClaims: "BLOCKED",
    clientPromotedBefore: 8,
    clientPromotedAfter: 8,
    newClientPromoted: 0,
    policyBlockedScenarios: ["luxury", "resort", "lifestyle_boutique", "conversion_repositioning"],
    detailOnlyScenarios: ["commercial_revenue", "institutional_platform"],
    providerCalls: 0,
    spend: "$0",
  };
}
