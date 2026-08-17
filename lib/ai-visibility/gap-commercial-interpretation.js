/**
 * P0E V1 — Commercial interpretation layer above raw P0C gaps (read-only).
 * Does NOT change Presence, Questions Missing, or P0C gap measurement.
 */

import {
  loadDecisionEligibilityConfig,
  getBrandDecisionEligibility,
  ELIGIBILITY as LEGACY_ELIGIBILITY,
} from "./brand-decision-eligibility.js";
import { loadScenarioRegistry, buildScenarioRegistryIndex } from "./scenario-registry.js";

export const GAP_INTERPRETATION_RULE_VERSION =
  "ai_visibility_gap_commercial_interpretation_v1";

/** Client-facing eligibility states (P0E V1 final readiness). */
export const SCENARIO_ELIGIBILITY = Object.freeze({
  ELIGIBLE: "ELIGIBLE",
  CONDITIONALLY_ELIGIBLE: "CONDITIONALLY_ELIGIBLE",
  OUT_OF_SCOPE: "OUT_OF_SCOPE",
  UNKNOWN: "UNKNOWN",
});

export const GAP_COMMERCIAL_MEANING = Object.freeze({
  TRUE_COMPETITIVE_GAP: "TRUE_COMPETITIVE_GAP",
  EXPECTED_POSITIONING_DIFFERENCE: "EXPECTED_POSITIONING_DIFFERENCE",
  SCENARIO_OUT_OF_SCOPE: "SCENARIO_OUT_OF_SCOPE",
  INSUFFICIENT_CONTEXT: "INSUFFICIENT_CONTEXT",
  REQUIRES_REVIEW: "REQUIRES_REVIEW",
});

export const ACTION_DISPOSITION = Object.freeze({
  ACTION_REQUIRED: "ACTION_REQUIRED",
  REVIEW_REQUIRED: "REVIEW_REQUIRED",
  NO_ACTION_EXPECTED_POSITIONING: "NO_ACTION_EXPECTED_POSITIONING",
  MONITOR_ONLY: "MONITOR_ONLY",
  INSUFFICIENT_EVIDENCE: "INSUFFICIENT_EVIDENCE",
});

export const ROOT_CAUSE_TAXONOMY = Object.freeze({
  VISIBILITY_GAP: "VISIBILITY_GAP",
  POSITIONING_GAP: "POSITIONING_GAP",
  REPRESENTATION_GAP: "REPRESENTATION_GAP",
  SOURCE_CITATION_GAP: "SOURCE_CITATION_GAP",
  EXPECTED_BRAND_POSITIONING: "EXPECTED_BRAND_POSITIONING",
  UNKNOWN_ROOT_CAUSE: "UNKNOWN_ROOT_CAUSE",
  GENUINE_COMPETITIVE_DISADVANTAGE: "GENUINE_COMPETITIVE_DISADVANTAGE",
});

export const REVIEW_ACTION_TYPES = Object.freeze({
  DEVELOPMENT_WEBSITE_REVIEW: "DEVELOPMENT_WEBSITE_REVIEW",
  OWNER_EDUCATION_REVIEW: "OWNER_EDUCATION_REVIEW",
  BRAND_POSITIONING_REVIEW: "BRAND_POSITIONING_REVIEW",
  SOURCE_CITATION_REVIEW: "SOURCE_CITATION_REVIEW",
  STRUCTURED_INFORMATION_REVIEW: "STRUCTURED_INFORMATION_REVIEW",
  COMPETITIVE_DIFFERENTIATION_REVIEW: "COMPETITIVE_DIFFERENTIATION_REVIEW",
  MARKET_SPECIFIC_CONTENT_REVIEW: "MARKET_SPECIFIC_CONTENT_REVIEW",
  AI_PERCEPTION_REVIEW: "AI_PERCEPTION_REVIEW",
  NO_ACTION: "NO_ACTION",
  MONITOR: "MONITOR",
});

/**
 * Governed scenarioId → decision territory for brand-decision-eligibility lookup.
 * intentFamily alone is insufficient (e.g. Brand Selection → New Build).
 */
export const SCENARIO_DECISION_TERRITORY = Object.freeze({
  scenario_independent_uu_conversion_v1: "Conversion",
  scenario_conversion_suitability_v1: "Conversion",
  scenario_newbuild_uu_brand_selection_v1: "New Build",
  scenario_soft_brand_collection_affiliation_v1: "Collection / Soft Brand",
  scenario_owner_flexibility_control_v1: "Soft-Brand Affiliation Flexibility",
  scenario_owner_economics_v1: "Soft-Brand Affiliation Flexibility",
  scenario_lifestyle_individuality_positioning_v1: "Lifestyle Positioning",
  scenario_chainscale_positioning_fit_v1: "Upper-Upscale Positioning",
  scenario_branded_residences_capability_v1: "Branded Residences",
  scenario_distribution_loyalty_v1: "Conversion",
  scenario_market_entry_geographic_relevance_v1: "Conversion",
  scenario_hma_vs_franchise_v1: "Conversion",
});

const CONDITIONALLY_ELIGIBLE_TERRITORIES = new Set([
  "Soft-Brand Affiliation Flexibility",
  "Owner Economics / Flexibility",
  "New Build",
]);

/**
 * @param {string|null|undefined} scenarioId
 * @param {object} [scenarioIndex]
 */
export function resolveScenarioDecisionTerritory(scenarioId, scenarioIndex = null) {
  if (!scenarioId) return null;
  if (SCENARIO_DECISION_TERRITORY[scenarioId]) {
    return SCENARIO_DECISION_TERRITORY[scenarioId];
  }
  const idx =
    scenarioIndex ||
    buildScenarioRegistryIndex(loadScenarioRegistry());
  const row = idx.scenarioById?.get?.(scenarioId);
  const intent = row?.intentFamily || null;
  if (intent === "Owner Flexibility") return "Soft-Brand Affiliation Flexibility";
  if (intent === "Brand Selection") return "New Build";
  if (intent === "Chain Scale / Positioning") return "Upper-Upscale Positioning";
  return intent;
}

/**
 * Map legacy fixture eligibility to P0E V1 states.
 * @param {string} legacyState
 * @param {string|null} decisionTerritory
 */
export function mapLegacyEligibilityToScenarioState(legacyState, decisionTerritory = null) {
  if (legacyState === LEGACY_ELIGIBILITY.ELIGIBLE) return SCENARIO_ELIGIBILITY.ELIGIBLE;
  if (legacyState === LEGACY_ELIGIBILITY.NOT_ELIGIBLE) return SCENARIO_ELIGIBILITY.OUT_OF_SCOPE;
  if (legacyState === LEGACY_ELIGIBILITY.UNKNOWN) {
    if (decisionTerritory && CONDITIONALLY_ELIGIBLE_TERRITORIES.has(decisionTerritory)) {
      return SCENARIO_ELIGIBILITY.CONDITIONALLY_ELIGIBLE;
    }
    return SCENARIO_ELIGIBILITY.UNKNOWN;
  }
  return SCENARIO_ELIGIBILITY.UNKNOWN;
}

/**
 * @param {string} brandId
 * @param {string|null|undefined} scenarioId
 * @param {object} [opts]
 */
export function resolveBrandScenarioEligibility(brandId, scenarioId, opts = {}) {
  const config = opts.config || loadDecisionEligibilityConfig();
  const scenarioIndex =
    opts.scenarioIndex || buildScenarioRegistryIndex(loadScenarioRegistry());
  const decisionTerritory = resolveScenarioDecisionTerritory(scenarioId, scenarioIndex);
  if (!decisionTerritory) {
    return {
      brandId,
      scenarioId: scenarioId || null,
      decisionTerritory: null,
      eligibilityStatus: SCENARIO_ELIGIBILITY.UNKNOWN,
      eligibilityReason: "No governed decision territory mapping for scenario.",
      governedEvidence: null,
      ruleVersion: GAP_INTERPRETATION_RULE_VERSION,
      legacyState: LEGACY_ELIGIBILITY.UNKNOWN,
    };
  }
  const legacy = getBrandDecisionEligibility(brandId, decisionTerritory, config);
  const eligibilityStatus = mapLegacyEligibilityToScenarioState(
    legacy.state,
    decisionTerritory
  );
  return {
    brandId,
    scenarioId: scenarioId || null,
    decisionTerritory,
    eligibilityStatus,
    eligibilityReason: legacy.reason,
    governedEvidence: legacy.source,
    ruleVersion: GAP_INTERPRETATION_RULE_VERSION,
    legacyState: legacy.state,
  };
}

function resolveReviewActionType(gap, disposition) {
  if (disposition === ACTION_DISPOSITION.NO_ACTION_EXPECTED_POSITIONING) {
    return REVIEW_ACTION_TYPES.NO_ACTION;
  }
  if (disposition === ACTION_DISPOSITION.MONITOR_ONLY) {
    return REVIEW_ACTION_TYPES.MONITOR;
  }
  const scenario = gap.scenarioId || "";
  if (scenario.includes("newbuild") || scenario.includes("conversion")) {
    return REVIEW_ACTION_TYPES.DEVELOPMENT_WEBSITE_REVIEW;
  }
  if (scenario.includes("soft_brand") || scenario.includes("flexibility")) {
    return REVIEW_ACTION_TYPES.BRAND_POSITIONING_REVIEW;
  }
  if (scenario.includes("residences")) {
    return REVIEW_ACTION_TYPES.COMPETITIVE_DIFFERENTIATION_REVIEW;
  }
  if (scenario.includes("lifestyle")) {
    return REVIEW_ACTION_TYPES.BRAND_POSITIONING_REVIEW;
  }
  if (disposition === ACTION_DISPOSITION.ACTION_REQUIRED) {
    return REVIEW_ACTION_TYPES.COMPETITIVE_DIFFERENTIATION_REVIEW;
  }
  return REVIEW_ACTION_TYPES.COMPETITIVE_DIFFERENTIATION_REVIEW;
}

function buildReviewCopy(gap, interpretation, brandName) {
  const subject = brandName || "This brand";
  const scenario = interpretation.decisionTerritory || "this owner decision";

  if (interpretation.actionDisposition === ACTION_DISPOSITION.NO_ACTION_EXPECTED_POSITIONING) {
    return `No action indicated — ${subject}'s absence in ${scenario} scenarios appears consistent with governed brand architecture.`;
  }
  if (interpretation.actionDisposition === ACTION_DISPOSITION.MONITOR_ONLY) {
    return "Monitor on the next comparable monitoring window — evidence is not yet persistent enough for executive action.";
  }
  if (interpretation.actionDisposition === ACTION_DISPOSITION.INSUFFICIENT_EVIDENCE) {
    return "Scenario relevance is not fully governed — treat as an internal review item only.";
  }
  if (interpretation.actionDisposition === ACTION_DISPOSITION.REVIEW_REQUIRED) {
    return `Review whether ${subject}'s visibility in ${scenario} reflects a representation gap, positioning choice, or market reality before changing materials.`;
  }
  if (interpretation.actionDisposition === ACTION_DISPOSITION.ACTION_REQUIRED) {
    return (
      TERRITORY_EXECUTIVE_REVIEW[interpretation.decisionTerritory] ||
      `Review owner-facing materials for ${scenario}.`
    );
  }
  return "Review competitive differentiation where peers appear and the subject brand does not.";
}

/**
 * Interpret one production P0C gap for commercial / executive use.
 * @param {object} gap
 * @param {object} [opts]
 */
export function interpretProductionGap(gap, opts = {}) {
  const brandNamesById = opts.brandNamesById || {};
  const brandName = brandNamesById[gap.subjectBrandId] || gap.subjectBrandId;
  const eligibility = resolveBrandScenarioEligibility(gap.subjectBrandId, gap.scenarioId, opts);

  let commercialMeaning = GAP_COMMERCIAL_MEANING.REQUIRES_REVIEW;
  let actionDisposition = ACTION_DISPOSITION.REVIEW_REQUIRED;
  let rootCause = ROOT_CAUSE_TAXONOMY.UNKNOWN_ROOT_CAUSE;
  let executiveEligible = false;
  let detailVisible = true;

  if (gap.classification === "MONITOR") {
    commercialMeaning = GAP_COMMERCIAL_MEANING.REQUIRES_REVIEW;
    actionDisposition = ACTION_DISPOSITION.MONITOR_ONLY;
    rootCause = ROOT_CAUSE_TAXONOMY.UNKNOWN_ROOT_CAUSE;
    executiveEligible = false;
    detailVisible = true;
  } else if (eligibility.eligibilityStatus === SCENARIO_ELIGIBILITY.OUT_OF_SCOPE) {
    commercialMeaning = GAP_COMMERCIAL_MEANING.EXPECTED_POSITIONING_DIFFERENCE;
    actionDisposition = ACTION_DISPOSITION.NO_ACTION_EXPECTED_POSITIONING;
    rootCause = ROOT_CAUSE_TAXONOMY.EXPECTED_BRAND_POSITIONING;
    executiveEligible = false;
    detailVisible = true;
  } else if (eligibility.eligibilityStatus === SCENARIO_ELIGIBILITY.UNKNOWN) {
    commercialMeaning = GAP_COMMERCIAL_MEANING.INSUFFICIENT_CONTEXT;
    actionDisposition =
      gap.classification === "HIGH_PRIORITY" || gap.classification === "PRIORITY"
        ? ACTION_DISPOSITION.REVIEW_REQUIRED
        : ACTION_DISPOSITION.INSUFFICIENT_EVIDENCE;
    rootCause = ROOT_CAUSE_TAXONOMY.UNKNOWN_ROOT_CAUSE;
    executiveEligible = false;
    detailVisible = actionDisposition !== ACTION_DISPOSITION.INSUFFICIENT_EVIDENCE;
  } else if (eligibility.eligibilityStatus === SCENARIO_ELIGIBILITY.CONDITIONALLY_ELIGIBLE) {
    commercialMeaning = GAP_COMMERCIAL_MEANING.REQUIRES_REVIEW;
    actionDisposition = ACTION_DISPOSITION.REVIEW_REQUIRED;
    rootCause = ROOT_CAUSE_TAXONOMY.UNKNOWN_ROOT_CAUSE;
    executiveEligible = false;
    detailVisible = true;
  } else if (eligibility.eligibilityStatus === SCENARIO_ELIGIBILITY.ELIGIBLE) {
    const persistent =
      gap.persistence === "STRONGLY_REPEATED" ||
      gap.persistence === "REPEATED_ACROSS_PERIODS" ||
      (gap.observationCount || 0) >= 5;

    if (gap.classification === "HIGH_PRIORITY" && persistent) {
      commercialMeaning = GAP_COMMERCIAL_MEANING.TRUE_COMPETITIVE_GAP;
      actionDisposition = ACTION_DISPOSITION.ACTION_REQUIRED;
      rootCause = ROOT_CAUSE_TAXONOMY.VISIBILITY_GAP;
      executiveEligible = true;
    } else if (gap.classification === "HIGH_PRIORITY" || gap.classification === "PRIORITY") {
      commercialMeaning = GAP_COMMERCIAL_MEANING.TRUE_COMPETITIVE_GAP;
      actionDisposition = ACTION_DISPOSITION.REVIEW_REQUIRED;
      rootCause = ROOT_CAUSE_TAXONOMY.VISIBILITY_GAP;
      executiveEligible = true;
    } else {
      commercialMeaning = GAP_COMMERCIAL_MEANING.REQUIRES_REVIEW;
      actionDisposition = ACTION_DISPOSITION.REVIEW_REQUIRED;
      rootCause = ROOT_CAUSE_TAXONOMY.UNKNOWN_ROOT_CAUSE;
      executiveEligible = gap.classification === "REVIEW";
    }
  }

  const reviewActionType = resolveReviewActionType(gap, actionDisposition);
  const reviewAction = buildReviewCopy(gap, { actionDisposition, decisionTerritory: eligibility.decisionTerritory }, brandName);

  return {
    gapId: gap.gapId || null,
    subjectBrandId: gap.subjectBrandId,
    subjectBrandName: brandName,
    scenarioId: gap.scenarioId || null,
    ...eligibility,
    commercialMeaning,
    actionDisposition,
    rootCause,
    executiveEligible,
    detailVisible,
    reviewActionType,
    reviewAction,
    rawClassification: gap.classification || null,
    rawPersistence: gap.persistence || null,
    P0C_RAW_PRESERVED: true,
    ruleVersion: GAP_INTERPRETATION_RULE_VERSION,
  };
}

/**
 * Batch interpret production gaps + audit counts.
 * @param {object[]} gaps
 * @param {object} [opts]
 */
export function auditGapInterpretations(gaps = [], opts = {}) {
  const production = gaps.filter(
    (g) => g.classification && g.lifecycleStatus !== "NOT_COMPARABLE"
  );
  const interpreted = production.map((g) => interpretProductionGap(g, opts));

  const counts = {
    RAW_P0C_PRODUCTION_GAPS: production.length,
    ELIGIBLE_GAPS: 0,
    OUT_OF_SCOPE_GAPS: 0,
    CONDITIONALLY_ELIGIBLE_GAPS: 0,
    UNKNOWN_GAPS: 0,
    EXECUTIVE_GAPS: 0,
    NO_ACTION_GAPS: 0,
    ACTION_REQUIRED: 0,
    REVIEW_REQUIRED: 0,
    MONITOR_ONLY: 0,
    INSUFFICIENT_EVIDENCE: 0,
  };

  for (const row of interpreted) {
    if (row.eligibilityStatus === SCENARIO_ELIGIBILITY.ELIGIBLE) counts.ELIGIBLE_GAPS += 1;
    if (row.eligibilityStatus === SCENARIO_ELIGIBILITY.OUT_OF_SCOPE) counts.OUT_OF_SCOPE_GAPS += 1;
    if (row.eligibilityStatus === SCENARIO_ELIGIBILITY.CONDITIONALLY_ELIGIBLE) {
      counts.CONDITIONALLY_ELIGIBLE_GAPS += 1;
    }
    if (row.eligibilityStatus === SCENARIO_ELIGIBILITY.UNKNOWN) counts.UNKNOWN_GAPS += 1;
    if (row.executiveEligible) counts.EXECUTIVE_GAPS += 1;
    if (row.actionDisposition === ACTION_DISPOSITION.NO_ACTION_EXPECTED_POSITIONING) {
      counts.NO_ACTION_GAPS += 1;
    }
    if (counts[row.actionDisposition] != null) counts[row.actionDisposition] += 1;
  }

  return {
    ruleVersion: GAP_INTERPRETATION_RULE_VERSION,
    interpretations: interpreted,
    counts,
    executiveEligibleGaps: interpreted.filter((r) => r.executiveEligible),
    noActionGaps: interpreted.filter(
      (r) => r.actionDisposition === ACTION_DISPOSITION.NO_ACTION_EXPECTED_POSITIONING
    ),
    detailReviewGaps: interpreted.filter(
      (r) =>
        !r.executiveEligible &&
        r.detailVisible &&
        (r.actionDisposition === ACTION_DISPOSITION.REVIEW_REQUIRED ||
          r.actionDisposition === ACTION_DISPOSITION.NO_ACTION_EXPECTED_POSITIONING)
    ),
  };
}

/**
 * Build client-safe competitive gap headline with commercial context.
 * @param {object} gap
 * @param {object} interpretation
 * @param {object} brandNamesById
 * @param {object} scenarioIndex
 */
function compactBrandDisplay(name) {
  return String(name || "")
    .replace(/\s+by Marriott$/i, "")
    .trim();
}

function compactPeerDisplay(name) {
  return String(name || "")
    .replace(/\s+by Marriott$/i, "")
    .replace(/^Autograph Collection$/i, "Autograph")
    .replace(/^Tribute Portfolio$/i, "Tribute")
    .replace(/^Curio Collection by Hilton$/i, "Curio")
    .trim();
}

const TERRITORY_HEADLINE_LABEL = {
  Conversion: "independent conversion discussions",
  "Collection / Soft Brand": "collection affiliation discussions",
  "Lifestyle Positioning": "lifestyle positioning discussions",
  "Branded Residences": "branded-residences discussions",
  "New Build": "new-build selection discussions",
  "Upper-Upscale Positioning": "upper-upscale positioning discussions",
};

const TERRITORY_EXECUTIVE_REVIEW = {
  Conversion: "Review independent-conversion development positioning.",
  "Collection / Soft Brand": "Review collection-affiliation owner messaging.",
  "Lifestyle Positioning": "Review lifestyle positioning in owner-facing materials.",
  "Branded Residences": "Review branded-residences owner messaging.",
  "New Build": "Review new-build development positioning.",
  "Upper-Upscale Positioning": "Review upper-upscale positioning materials.",
};

export function buildCommercialGapHeadline(gap, interpretation, brandNamesById, scenarioIndex) {
  const subject = compactBrandDisplay(brandNamesById[gap.subjectBrandId] || gap.subjectBrandId);
  const peers = (gap.peerBrandIds || [])
    .map((id) => compactPeerDisplay(brandNamesById[id] || id))
    .filter(Boolean)
    .slice(0, 3);
  const peerPhrase =
    peers.length >= 2
      ? `${peers.slice(0, 2).join(", ")}${peers.length > 2 ? ` and ${peers.length - 2} others` : ""}`
      : peers[0] || "eligible peers";
  const territory = interpretation.decisionTerritory || "monitored owner-decision scenario";
  const scenarioName =
    TERRITORY_HEADLINE_LABEL[territory] ||
    scenarioIndex?.scenarioById?.get?.(gap.scenarioId)?.scenarioName ||
    territory;

  if (interpretation.actionDisposition === ACTION_DISPOSITION.NO_ACTION_EXPECTED_POSITIONING) {
    return `${subject} appears less often than ${peerPhrase} in ${scenarioName}. Consistent with governed brand architecture.`;
  }

  return `${subject} is repeatedly absent from ${scenarioName} where ${peerPhrase} appear.`;
}
