/**
 * Centralized Operator Fit v2 configuration.
 * Single source of truth for weights, evidence classes, ceilings, risk caps,
 * table-stakes tokens, and structure mapping — do not scatter thresholds in UI.
 *
 * @see docs/architecture/decisions/operator-fit-phase-1-2-founder-decisions.md
 */

/** Owner-facing preserved structure labels (never rename). */
export const PRESERVED_OPERATING_STRUCTURE_VALUES = Object.freeze([
  "Third-Party Management",
  "Franchise + Operator",
  "Franchise Only",
  "Owner-Operated",
  "Lease",
  "Asset Management",
  "To Be Confirmed",
]);

/**
 * Internal canonical keys ↔ preserved labels + live SI/operator aliases.
 * Mapping only — does not rename persisted Airtable values.
 */
export const OPERATING_STRUCTURE_CANONICAL = Object.freeze({
  third_party_management: {
    preserved: "Third-Party Management",
    aliases: [
      "Third-Party Management",
      "Full third-party management",
      "Third-party managed",
      "third-party management",
      "third party management",
    ],
  },
  franchise_plus_operator: {
    preserved: "Franchise + Operator",
    aliases: [
      "Franchise + Operator",
      "Franchise with third-party operator",
      "Franchise support",
    ],
  },
  franchise_only: {
    preserved: "Franchise Only",
    aliases: ["Franchise Only", "Franchise", "franchise only"],
  },
  owner_operated: {
    preserved: "Owner-Operated",
    aliases: [
      "Owner-Operated",
      "Owner-operated",
      "Owner-operated with commercial support",
      "Commercial-only support",
    ],
  },
  lease: {
    preserved: "Lease",
    aliases: ["Lease", "Hybrid / Operating Lease Structure", "operating lease"],
  },
  asset_management: {
    preserved: "Asset Management",
    aliases: ["Asset Management", "Asset management support"],
  },
  to_be_confirmed: {
    preserved: "To Be Confirmed",
    aliases: ["To Be Confirmed", "Undecided", "Not applicable", "Hybrid / project-specific"],
  },
  brand_managed: {
    preserved: null, // not in preserved owner list; distinct candidate type
    display: "Brand Managed",
    aliases: ["Brand-managed", "Brand managed", "Management"],
  },
});

/** Evidence classes (ordinal quality). */
export const EVIDENCE_CLASSES = Object.freeze({
  VERIFIED_PROJECT: "verified_project_level",
  INDEPENDENT_REFERENCED: "independently_referenced",
  DETAILED_OPERATOR_PROVIDED: "detailed_operator_provided",
  PORTFOLIO_LEVEL: "portfolio_level_operator",
  GENERAL_CLAIM: "general_operator_claim",
  UNKNOWN: "unknown",
});

export const EVIDENCE_CLASS_RANK = Object.freeze({
  [EVIDENCE_CLASSES.VERIFIED_PROJECT]: 5,
  [EVIDENCE_CLASSES.INDEPENDENT_REFERENCED]: 4,
  [EVIDENCE_CLASSES.DETAILED_OPERATOR_PROVIDED]: 3,
  [EVIDENCE_CLASSES.PORTFOLIO_LEVEL]: 2,
  [EVIDENCE_CLASSES.GENERAL_CLAIM]: 1,
  [EVIDENCE_CLASSES.UNKNOWN]: 0,
});

/** Confidence labels + displayed score ceilings (founder 1.8). */
export const EVIDENCE_CONFIDENCE = Object.freeze({
  LIMITED: {
    label: "Limited",
    displayedScoreCeiling: 69,
  },
  MODERATE: {
    label: "Moderate",
    displayedScoreCeiling: 84,
  },
  STRONG: {
    label: "Strong",
    displayedScoreCeiling: null,
  },
});

/**
 * Strong requires at least one independently supported / referenced / verified
 * source covering material alignment factors. Operator-reported alone → not Strong.
 */
export const EVIDENCE_CONFIDENCE_RULES = Object.freeze({
  strongRequiresMinClassRank: EVIDENCE_CLASS_RANK[EVIDENCE_CLASSES.INDEPENDENT_REFERENCED],
  strongRequiresMaterialFactorCoveragePct: 40,
  moderateRequiresMinClassRank: EVIDENCE_CLASS_RANK[EVIDENCE_CLASSES.DETAILED_OPERATOR_PROVIDED],
  moderateRequiresKnownWeightPct: 45,
  limitedMaxKnownWeightPct: 45,
});

/**
 * Operator–Project Alignment factor weights.
 * Unknown factors stay in the denominator with 0 contribution (founder 1.6).
 * No table-stakes capability presence factors.
 */
export const OPERATOR_PROJECT_FACTORS = Object.freeze({
  geographyMarket: {
    key: "geographyMarket",
    label: "Geographic and market alignment",
    weight: 22,
    maxContribution: 100,
  },
  segmentPositioning: {
    key: "segmentPositioning",
    label: "Hotel segment and positioning alignment",
    weight: 14,
    maxContribution: 100,
  },
  assetDevelopmentExperience: {
    key: "assetDevelopmentExperience",
    label: "Comparable asset and development experience",
    weight: 20,
    maxContribution: 100,
  },
  projectComplexity: {
    key: "projectComplexity",
    label: "Project-complexity alignment",
    weight: 12,
    maxContribution: 100,
  },
  brandExperience: {
    key: "brandExperience",
    label: "Brand experience (operator portfolio)",
    weight: 10,
    maxContribution: 100,
  },
  ownershipGovernance: {
    key: "ownershipGovernance",
    label: "Ownership and governance alignment",
    weight: 10,
    maxContribution: 100,
  },
  regionalResources: {
    key: "regionalResources",
    label: "Regional resource alignment",
    weight: 6,
    maxContribution: 100,
  },
  commercialDifferentiator: {
    key: "commercialDifferentiator",
    label: "Project-specific commercial differentiator",
    weight: 6,
    maxContribution: 100,
  },
});

/** Layer weights for primary raw Operator Alignment (visible separately in UI). */
export const PRIMARY_LAYER_WEIGHTS = Object.freeze({
  operatorProjectAlignment: 70,
  operatingStructureAlignment: 15,
  brandOperatorCompatibility: 15,
});

/** Execution risk — capped penalty points subtracted from raw before ceiling. */
export const EXECUTION_RISK = Object.freeze({
  maxTotalPenaltyPoints: 25,
  penalties: {
    geographicMobilization: 8,
    brandApprovalUncertainty: 6,
    missingStructureSupport: 10,
    limitedComparableExperience: 8,
    unconfirmedRegionalResources: 5,
    unconfirmedPreOpeningCapacity: 5,
    materialDataGaps: 6,
    competitiveConflict: 8,
  },
});

/**
 * Tokens treated as table-stakes — presence never adds positive differentiation.
 * Absence may warn / validate / gate when specifically required.
 */
export const TABLE_STAKES_CAPABILITY_TOKENS = Object.freeze([
  "revenue management",
  "sales",
  "marketing",
  "digital distribution",
  "procurement",
  "accounting",
  "financial reporting",
  "finance",
  "human resources",
  "hr",
  "hr / training",
  "owner reporting",
  "owner relations",
  "generic pre-opening",
  "pre-opening planning",
  "pre-opening support",
  "full hotel management",
]);

/** Eligibility statuses. */
export const ELIGIBILITY_STATUS = Object.freeze({
  NOT_ELIGIBLE: "Not Currently Eligible",
  WITH_CONDITIONS: "Eligible With Conditions",
  ELIGIBLE: "Eligible",
  PREFERRED: "Preferred Eligibility",
});

/** Brand–operator compatibility categories. */
export const BRAND_OPERATOR_COMPAT = Object.freeze({
  SUPPORTED: "Supported",
  PARTIALLY_SUPPORTED: "Partially Supported",
  UNKNOWN: "Unknown",
  UNSUPPORTED: "Unsupported",
  NOT_APPLICABLE: "Not Applicable",
});

/** Candidate types. */
export const CANDIDATE_TYPE = Object.freeze({
  THIRD_PARTY_OPERATOR: "Third-Party Operator",
  BRAND_MANAGED: "Brand Managed",
});

/** Field presence states — never collapse into one falsy. */
export const FIELD_STATE = Object.freeze({
  PRESENT: "present",
  ABSENT: "absent",
  UNKNOWN: "unknown",
  NOT_APPLICABLE: "not_applicable",
  INVALID: "invalid",
  INFERRED: "inferred",
});

export const TOP5_MAX = 5;
