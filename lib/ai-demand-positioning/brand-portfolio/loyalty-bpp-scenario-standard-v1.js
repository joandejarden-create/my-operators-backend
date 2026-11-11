/**
 * LOYALTY_BPP_SCENARIO_STANDARD_V1
 *
 * Canonical loyalty-constrained Brand & Portfolio measurement family.
 * Separate from Core ADP (ADP_SCENARIO_DENSITY_TIER_STANDARD_V1).
 *
 * Density decision (methodological, not padding to Core 63):
 * - STANDARD target **28** scenarios × 4 providers = 112 observations
 * - Allowed band **21–32** (minimum coverage without frame redundancy)
 * - EXPANDED **42–63** only when stewarded distinct prompts exist (disclose)
 * - LEGACY_PULSE **8–12** (first/second cycle) = valid historical, not STANDARD-certified
 *
 * Rationale vs 63: seven traveler intents × four frames ≈ 28 distinct commercial
 * needs. Expanding to 63 without new distinct intents creates near-duplicate prompts
 * and false precision. Prefer depth of distinct intents over padded counts.
 */

import crypto from "crypto";
import { PROVIDERS } from "../data-model.js";
import {
  PROMPT_TEMPLATES_BY_LENS_TYPE,
  LOYALTY_PROMPT_WORDING_POLICY_V1,
} from "./brand-portfolio-first-cycle-contract-v1.js";
import { MIN_PEER_THRESHOLDS_V1 } from "./peer-expansion-hierarchy-v1.js";
import { PORTFOLIO_TYPES } from "./brand-portfolio-position-contract-v1.js";

export const LOYALTY_BPP_SCENARIO_STANDARD_V1 = "LOYALTY_BPP_SCENARIO_STANDARD_V1";
export const MEASUREMENT_FAMILY_CORE_ADP = "CORE_ADP";
export const MEASUREMENT_FAMILY_LOYALTY_BPP = "LOYALTY_BPP";

export const LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1 = Object.freeze({
  id: "LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1",
  targetScenarioCount: 28,
  allowedMin: 21,
  allowedMax: 32,
  providerCount: 4,
  providers: Object.freeze([...PROVIDERS]),
  expectedObservationsTarget: 112,
  methodologyVersion: "loyalty_bpp_scenario_methodology_v1",
  rationale:
    "Seven traveler-intent categories × four distinct frames yields ~28 commercially distinct loyalty-constrained prompts without padding. Not interchangeable with Core ADP 60–65.",
});

export const LOYALTY_BPP_SCENARIO_DENSITY_EXPANDED_V1 = Object.freeze({
  id: "LOYALTY_BPP_SCENARIO_DENSITY_EXPANDED_V1",
  allowedMin: 42,
  allowedMax: 63,
  note: "Stewarded expanded loyalty packs only when each added prompt has a distinct commercial need. Disclose vs STANDARD.",
});

export const LOYALTY_BPP_SCENARIO_DENSITY_LEGACY_PULSE_V1 = Object.freeze({
  id: "LOYALTY_BPP_SCENARIO_DENSITY_LEGACY_PULSE_V1",
  allowedMin: 8,
  allowedMax: 12,
  note: "Historical first/second-cycle loyalty pulse. Valid for its era; does not satisfy LOYALTY_BPP_SCENARIO_STANDARD_V1 without compatibility evidence.",
});

/** Traveler-intent categories for loyalty-constrained demand */
export const LOYALTY_INTENT_CATEGORIES_V1 = Object.freeze([
  Object.freeze({
    id: "business",
    label: "Loyalty-constrained business travel",
    needPhrases: Object.freeze([
      "a business trip",
      "corporate meetings and evening work sessions",
      "a short executive stay near major offices",
    ]),
  }),
  Object.freeze({
    id: "group_meeting",
    label: "Loyalty-constrained meetings and group travel",
    needPhrases: Object.freeze([
      "a mid-size meeting of about 50–150 people",
      "a small corporate offsite with meeting space",
      "a team gathering with banquet needs",
    ]),
  }),
  Object.freeze({
    id: "leisure",
    label: "Loyalty-constrained leisure",
    needPhrases: Object.freeze([
      "a weekend leisure getaway",
      "a vacation stay with easy local exploring",
      "a first-time leisure visit to the destination",
    ]),
  }),
  Object.freeze({
    id: "premium",
    label: "Loyalty-constrained premium / luxury travel",
    needPhrases: Object.freeze([
      "an upscale stay with elevated service",
      "a premium leisure trip",
      "a high-end business or leisure stay",
    ]),
  }),
  Object.freeze({
    id: "location",
    label: "Loyalty-constrained location convenience",
    needPhrases: Object.freeze([
      "walkable access to the main business district",
      "convenience to the primary demand generators",
      "easy airport or transit access",
    ]),
  }),
  Object.freeze({
    id: "value_benefits",
    label: "Loyalty-constrained value / benefits",
    needPhrases: Object.freeze([
      "maximizing loyalty points value on a stay",
      "using points and elite benefits efficiently",
      "getting strong loyalty program value for the money",
    ]),
  }),
  Object.freeze({
    id: "special_preference",
    label: "Loyalty-constrained special traveler preferences",
    needPhrases: Object.freeze([
      "a couples weekend within the loyalty program",
      "a celebration stay using loyalty hotels",
      "a wellness-oriented stay among loyalty hotels",
    ]),
  }),
]);

export const LOYALTY_FRAME_SET_V1 = Object.freeze([
  "recommend",
  "best_for",
  "where_should",
  "compare",
]);

export const LOYALTY_PEER_COHORT_RULES_V1 = Object.freeze({
  id: "LOYALTY_PEER_COHORT_RULES_V1",
  expansionHierarchy: "PEER_EXPANSION_HIERARCHY_V1",
  minPeersForRank: MIN_PEER_THRESHOLDS_V1.rankUsable,
  minPeersForBenchmarkIndex: MIN_PEER_THRESHOLDS_V1.benchmarkUsable,
  scopeLabelRule:
    "Always label as a measured peer cohort within the governed market/ecosystem — never as the full worldwide loyalty portfolio.",
  identity: Object.freeze({
    requireCanonicalEntityId: true,
    requireAliasResolution: true,
    prohibitSubjectNameInDiscoveryPrompt: true,
  }),
  independentHotels: Object.freeze({
    state: "LOYALTY_BPP_NOT_APPLICABLE",
    alternative: "INDEPENDENT_POSITIONING_BPP_V1",
    note: "Do not invent loyalty membership for independents.",
  }),
});

function sha16(value) {
  return crypto.createHash("sha256").update(String(value || "")).digest("hex").slice(0, 16);
}

/**
 * Build universal loyalty prompt templates (parameterized; no subject hotel name).
 */
export function buildLoyaltyPromptTemplatesV1({
  loyaltyConstraint,
  parentConstraint,
  geo,
  portfolioType = PORTFOLIO_TYPES.LOYALTY_ECOSYSTEM,
} = {}) {
  const lensType =
    portfolioType === PORTFOLIO_TYPES.INDEPENDENT_POSITIONING
      ? "INDEPENDENT_POSITIONING"
      : portfolioType === PORTFOLIO_TYPES.COLLECTION_PORTFOLIO
        ? "COLLECTION"
        : portfolioType === PORTFOLIO_TYPES.HARD_BRAND
          ? "HARD_BRAND"
          : "LOYALTY_ECOSYSTEM";
  const frames = PROMPT_TEMPLATES_BY_LENS_TYPE[lensType] || PROMPT_TEMPLATES_BY_LENS_TYPE.LOYALTY_ECOSYSTEM;
  const templates = [];
  let slot = 0;
  for (const cat of LOYALTY_INTENT_CATEGORIES_V1) {
    for (let fi = 0; fi < LOYALTY_FRAME_SET_V1.length; fi++) {
      const frame = LOYALTY_FRAME_SET_V1[fi];
      const needPhrase = cat.needPhrases[Math.min(fi, cat.needPhrases.length - 1)];
      const useLoyaltyName =
        cat.id === "value_benefits" || frame === "recommend" && fi === 0 && cat.id === "business"
          ? false
          : cat.id === "value_benefits";
      // Points/benefits use loyalty program name; others use parent portfolio wording policy
      const constraint =
        cat.id === "value_benefits"
          ? loyaltyConstraint || parentConstraint
          : parentConstraint || loyaltyConstraint;
      const frameKey =
        cat.id === "value_benefits" && frames.recommend_points
          ? "recommend_points"
          : frame;
      const tpl =
        frames[frameKey] ||
        frames.recommend ||
        "Which {constraint} hotels in {geo} are best for {need}?";
      const exactPrompt = tpl
        .replace(/\{constraint\}/g, constraint || "{LOYALTY_OR_PARENT}")
        .replace(/\{loyaltyConstraint\}/g, loyaltyConstraint || constraint || "{LOYALTY_PROGRAM}")
        .replace(/\{geo\}/g, geo || "{DESTINATION}")
        .replace(/\{need\}/g, needPhrase);
      slot += 1;
      templates.push({
        slotId: `loyalty_${String(slot).padStart(2, "0")}`,
        scenarioId: `loy_${cat.id}_${frame}_${slot}`,
        categoryId: cat.id,
        categoryLabel: cat.label,
        frame: frameKey,
        needPhrase,
        constraintUsed: constraint,
        exactPrompt,
        injectsSubjectHotelName: false,
        measurementFamily: MEASUREMENT_FAMILY_LOYALTY_BPP,
        templateVersion: LOYALTY_BPP_SCENARIO_STANDARD_V1,
        wordingPolicy: LOYALTY_PROMPT_WORDING_POLICY_V1,
      });
    }
  }
  return {
    count: templates.length,
    densityTier: LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.id,
    target: LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.targetScenarioCount,
    templates,
    manifestHash: sha16(JSON.stringify(templates.map((t) => t.exactPrompt))),
  };
}

export function classifyLoyaltyDensityTier(scenarioCount) {
  const n = Number(scenarioCount);
  if (!Number.isFinite(n) || n <= 0) return { tier: "ABSENT", standardCertified: false };
  if (
    n >= LOYALTY_BPP_SCENARIO_DENSITY_LEGACY_PULSE_V1.allowedMin &&
    n <= LOYALTY_BPP_SCENARIO_DENSITY_LEGACY_PULSE_V1.allowedMax
  ) {
    return {
      tier: LOYALTY_BPP_SCENARIO_DENSITY_LEGACY_PULSE_V1.id,
      standardCertified: false,
      note: "Legacy pulse — migration required for STANDARD certification",
    };
  }
  if (
    n >= LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.allowedMin &&
    n <= LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.allowedMax
  ) {
    return { tier: LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.id, standardCertified: true };
  }
  if (
    n >= LOYALTY_BPP_SCENARIO_DENSITY_EXPANDED_V1.allowedMin &&
    n <= LOYALTY_BPP_SCENARIO_DENSITY_EXPANDED_V1.allowedMax
  ) {
    return {
      tier: LOYALTY_BPP_SCENARIO_DENSITY_EXPANDED_V1.id,
      standardCertified: true,
      disclosureRequired: true,
    };
  }
  if (n > 0 && n < LOYALTY_BPP_SCENARIO_DENSITY_LEGACY_PULSE_V1.allowedMin) {
    return { tier: "BELOW_LEGACY_PULSE", standardCertified: false };
  }
  return { tier: "OUT_OF_BAND", standardCertified: false };
}

/**
 * Fail-closed loyalty scenario-universe certification.
 */
export function certifyLoyaltyBppScenarioUniverse({
  scenarios = [],
  portfolioType = null,
  peerCountExcludingSubject = null,
  notApplicable = false,
} = {}) {
  const failures = [];
  const warnings = [];

  if (notApplicable || portfolioType === PORTFOLIO_TYPES.INDEPENDENT_POSITIONING) {
    return {
      gate: "loyalty_scenario_universe_certification",
      contract: LOYALTY_BPP_SCENARIO_STANDARD_V1,
      pass: true,
      notApplicable: true,
      state: LOYALTY_PEER_COHORT_RULES_V1.independentHotels.state,
      failures: [],
      warnings: ["LOYALTY_NOT_APPLICABLE_INDEPENDENT_OR_DECLARED"],
    };
  }

  const n = (scenarios || []).length;
  const density = classifyLoyaltyDensityTier(n);
  if (!density.standardCertified) {
    failures.push(`LOYALTY_DENSITY_NOT_STANDARD:${density.tier}:n=${n}`);
  }

  const cats = new Set((scenarios || []).map((s) => s.categoryId || s.intent || s.territory));
  for (const c of LOYALTY_INTENT_CATEGORIES_V1) {
    if (![...cats].some((x) => String(x).includes(c.id) || String(x) === c.id)) {
      warnings.push(`CATEGORY_COVERAGE_GAP:${c.id}`);
    }
  }

  const subjectInject = (scenarios || []).filter((s) => s.injectsSubjectHotelName === true);
  if (subjectInject.length) failures.push("SUBJECT_HOTEL_NAME_IN_DISCOVERY_PROMPT");

  if (
    peerCountExcludingSubject != null &&
    peerCountExcludingSubject < LOYALTY_PEER_COHORT_RULES_V1.minPeersForRank
  ) {
    failures.push(`PEER_COHORT_BELOW_RANK_MIN:${peerCountExcludingSubject}`);
  }

  return {
    gate: "loyalty_scenario_universe_certification",
    contract: LOYALTY_BPP_SCENARIO_STANDARD_V1,
    pass: failures.length === 0,
    notApplicable: false,
    scenarioCount: n,
    density,
    failures,
    warnings,
    expectedObservations: n * LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.providerCount,
  };
}

export function nineVsSixtyThreeStabilityAnalysis() {
  return {
    id: "NINE_VS_SIXTY_THREE_STABILITY_ANALYSIS_V1",
    hotelReference: "adp_hotel_phillips_kansas_city",
    legacyPulseScenarios: 9,
    legacyPulseObservations: 36,
    proposedStandardScenarios: LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.targetScenarioCount,
    proposedStandardObservations:
      LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.targetScenarioCount *
      LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.providerCount,
    coreReferenceScenarios: 63,
    findings: [
      "Nine loyalty scenarios produce a usable pulse rank but high sensitivity to single-scenario flips (±1 scenario ≈ ±11 pp on unique-scenario rates).",
      "Padding loyalty prompts to 63 without new distinct traveler needs increases redundancy and does not automatically improve rank confidence.",
      "Standardizing at 28 (7 intents × 4 frames) improves category coverage and reduces single-prompt leverage while remaining commercially interpretable.",
      "Core 63 and Loyalty 28 answer different research questions; equalizing counts alone does not create metric interchangeability.",
      "Provider leave-one-out on a 9-scenario pulse can move Portfolio AI Presence by large absolute amounts; denser STANDARD loyalty reduces that volatility.",
    ],
    verdict:
      "Nine-scenario pulse is LEGACY_VALID but INSUFFICIENT for LOYALTY_BPP_SCENARIO_STANDARD_V1. Target 28, not forced 63.",
  };
}
