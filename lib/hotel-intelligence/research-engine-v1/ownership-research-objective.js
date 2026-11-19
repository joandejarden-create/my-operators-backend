/**
 * HE-INTEL-001 — Ownership research objective (founder correction).
 *
 * Primary success is actionable ultimate ownership intelligence — not merely
 * identifying a single-purpose legal titleholder LLC.
 *
 * Shared by P3A/P3B benchmarking and Hotel Explorer ownership presentation.
 * Does not invent a competing research framework.
 */

export const OWNERSHIP_RESEARCH_OBJECTIVE_ID =
  "HE_INTEL_001_OWNERSHIP_RESEARCH_OBJECTIVE_V1";
export const OWNERSHIP_RESEARCH_OBJECTIVE_VERSION = "1.0.0";

/** Drawer questions the HE ownership UI must ultimately answer. */
export const OWNERSHIP_DRAWER_QUESTIONS = Object.freeze([
  "Who owns this hotel?",
  "Who should I speak with about it?",
  "How can I reach them?",
  "How do we know?",
]);

/**
 * Distinct ownership / control / management roles.
 * Management companies and brands are never owners.
 */
export const OWNERSHIP_ROLE = Object.freeze({
  LEGAL_TITLEHOLDER: "LEGAL_TITLEHOLDER",
  ECONOMIC_OR_BENEFICIAL_OWNER: "ECONOMIC_OR_BENEFICIAL_OWNER",
  CONTROLLING_INVESTOR_OR_GROUP: "CONTROLLING_INVESTOR_OR_GROUP",
  ASSET_MANAGER_OR_OWNERSHIP_REPRESENTATIVE: "ASSET_MANAGER_OR_OWNERSHIP_REPRESENTATIVE",
  HOTEL_MANAGEMENT_OPERATOR: "HOTEL_MANAGEMENT_OPERATOR",
  BRAND_OR_FRANCHISE: "BRAND_OR_FRANCHISE",
});

export const OWNERSHIP_SUCCESS_TIER = Object.freeze({
  FULL_ACTIONABLE: "FULL_ACTIONABLE",
  PARTIAL_LEGAL_ENTITY_ONLY: "PARTIAL_LEGAL_ENTITY_ONLY",
  PARTIAL_REPRESENTATIVE_PATHWAY: "PARTIAL_REPRESENTATIVE_PATHWAY",
  UNRESOLVED: "UNRESOLVED",
});

export const PRIMARY_OWNERSHIP_GOALS = Object.freeze([
  "ULTIMATE_OWNER_INVESTOR_FAMILY_FUND_REIT_OR_CONTROLLING_ORG",
  "NAMED_OWNERSHIP_DECISION_MAKERS",
  "VERIFIED_CONTACT_AND_CREDIBLE_PATHWAYS",
  "OWNERSHIP_STRUCTURE_AND_LEGAL_ENTITY_CHAIN",
]);

export const OWNERSHIP_DISTINCTION_RULES = Object.freeze({
  legal_entities_are_intermediary_evidence: true,
  legal_entity_only_is_partial_not_full_success: true,
  do_not_mistake_management_or_brand_for_owner: true,
  never_invent_beneficial_owners_percentages_names_emails_phones: true,
  when_ultimate_ownership_not_public:
    "Mark UNRESOLVED and provide strongest verified ownership representative or institutional contact pathway",
});

/**
 * Extended claim types for ultimate ownership intelligence.
 * Compatible with prior OWNER_ENTITY / ECONOMIC_SPONSOR labels.
 */
export const OWNERSHIP_CLAIM_TYPES = Object.freeze({
  LEGAL_TITLEHOLDER: "LEGAL_TITLEHOLDER",
  ULTIMATE_OWNER: "ULTIMATE_OWNER",
  CONTROLLING_INVESTOR: "CONTROLLING_INVESTOR",
  OWNERSHIP_REPRESENTATIVE: "OWNERSHIP_REPRESENTATIVE",
  OWNERSHIP_STRUCTURE_EDGE: "OWNERSHIP_STRUCTURE_EDGE",
  OWNERSHIP_DECISION_MAKER: "OWNERSHIP_DECISION_MAKER",
  ALTERNATIVE_CONTACT_PATHWAY: "ALTERNATIVE_CONTACT_PATHWAY",
  // retained aliases
  OWNER_ENTITY: "OWNER_ENTITY",
  ECONOMIC_SPONSOR: "ECONOMIC_SPONSOR",
});

export function isBrandOrOperatorMistakenForOwner(claimType, roleHint) {
  const t = String(claimType || "");
  const r = String(roleHint || "");
  if (t === "OPERATOR_ENTITY" || t === "BRAND" || r === OWNERSHIP_ROLE.HOTEL_MANAGEMENT_OPERATOR) {
    return true;
  }
  if (r === OWNERSHIP_ROLE.BRAND_OR_FRANCHISE) return true;
  return false;
}

/**
 * Classify research outcome for a hotel claim set (accepted claim types + flags).
 * @param {object} input
 * @param {Set<string>|string[]} input.acceptedClaimTypes
 * @param {boolean} [input.hasVerifiedOwnershipDecisionMaker]
 * @param {boolean} [input.hasVerifiedDirectContact]
 * @param {boolean} [input.hasCredibleAlternativePathway]
 * @param {boolean} [input.hasOwnershipStructureChain]
 * @param {boolean} [input.ultimateOwnerUnresolved]
 */
export function classifyOwnershipResearchSuccess(input = {}) {
  const types = input.acceptedClaimTypes instanceof Set
    ? input.acceptedClaimTypes
    : new Set(input.acceptedClaimTypes || []);

  const hasUltimate =
    types.has(OWNERSHIP_CLAIM_TYPES.ULTIMATE_OWNER) ||
    types.has(OWNERSHIP_CLAIM_TYPES.CONTROLLING_INVESTOR) ||
    types.has(OWNERSHIP_CLAIM_TYPES.ECONOMIC_SPONSOR);
  const hasLegalOnly =
    (types.has(OWNERSHIP_CLAIM_TYPES.LEGAL_TITLEHOLDER) ||
      types.has(OWNERSHIP_CLAIM_TYPES.OWNER_ENTITY)) &&
    !hasUltimate;
  const hasDecisionMaker =
    input.hasVerifiedOwnershipDecisionMaker === true ||
    types.has(OWNERSHIP_CLAIM_TYPES.OWNERSHIP_DECISION_MAKER) ||
    types.has("DECISION_MAKER");
  const hasDirectContact =
    input.hasVerifiedDirectContact === true ||
    types.has("PUBLIC_EMAIL") ||
    types.has("PUBLIC_PHONE");
  const hasAltPathway =
    input.hasCredibleAlternativePathway === true ||
    types.has(OWNERSHIP_CLAIM_TYPES.ALTERNATIVE_CONTACT_PATHWAY) ||
    types.has(OWNERSHIP_CLAIM_TYPES.OWNERSHIP_REPRESENTATIVE);
  const hasStructure =
    input.hasOwnershipStructureChain === true ||
    types.has(OWNERSHIP_CLAIM_TYPES.OWNERSHIP_STRUCTURE_EDGE) ||
    types.has("ORG_RELATIONSHIP");

  const actionable =
    hasUltimate &&
    hasDecisionMaker &&
    (hasDirectContact || hasAltPathway) &&
    hasStructure;

  if (actionable) {
    return {
      tier: OWNERSHIP_SUCCESS_TIER.FULL_ACTIONABLE,
      fullResearchSuccess: true,
      partial: false,
      unresolvedUltimateOwnership: false,
      note: "Verified ultimate ownership + decision-maker + contact pathway + structure",
    };
  }

  if (input.ultimateOwnerUnresolved === true || (!hasUltimate && !hasLegalOnly)) {
    if (hasAltPathway || hasDecisionMaker) {
      return {
        tier: OWNERSHIP_SUCCESS_TIER.PARTIAL_REPRESENTATIVE_PATHWAY,
        fullResearchSuccess: false,
        partial: true,
        unresolvedUltimateOwnership: true,
        note: "Ultimate ownership UNRESOLVED; strongest verified representative/pathway retained",
      };
    }
    return {
      tier: OWNERSHIP_SUCCESS_TIER.UNRESOLVED,
      fullResearchSuccess: false,
      partial: false,
      unresolvedUltimateOwnership: true,
      note: "Ultimate ownership not publicly ascertainable",
    };
  }

  if (hasLegalOnly) {
    return {
      tier: OWNERSHIP_SUCCESS_TIER.PARTIAL_LEGAL_ENTITY_ONLY,
      fullResearchSuccess: false,
      partial: true,
      unresolvedUltimateOwnership: true,
      note: "Legal-entity-only result is partial — not full research success",
    };
  }

  return {
    tier: OWNERSHIP_SUCCESS_TIER.PARTIAL_REPRESENTATIVE_PATHWAY,
    fullResearchSuccess: false,
    partial: true,
    unresolvedUltimateOwnership: !hasUltimate,
    note: "Partial ownership intelligence; actionable bar not met",
  };
}

/**
 * Rubric guidance: legal-entity-only must not score as full ownership success.
 */
export function scoreUltimateOwnershipDimension(acceptedTypes = new Set()) {
  const types = acceptedTypes instanceof Set ? acceptedTypes : new Set(acceptedTypes);
  if (
    types.has(OWNERSHIP_CLAIM_TYPES.ULTIMATE_OWNER) ||
    types.has(OWNERSHIP_CLAIM_TYPES.CONTROLLING_INVESTOR)
  ) {
    return 3;
  }
  if (types.has(OWNERSHIP_CLAIM_TYPES.ECONOMIC_SPONSOR)) return 2;
  if (
    types.has(OWNERSHIP_CLAIM_TYPES.LEGAL_TITLEHOLDER) ||
    types.has(OWNERSHIP_CLAIM_TYPES.OWNER_ENTITY)
  ) {
    return 1; // partial
  }
  return 0;
}

export function writeOwnershipObjectiveGuarantees() {
  return {
    objective_id: OWNERSHIP_RESEARCH_OBJECTIVE_ID,
    objective_version: OWNERSHIP_RESEARCH_OBJECTIVE_VERSION,
    drawer_questions: OWNERSHIP_DRAWER_QUESTIONS,
    primary_goals: PRIMARY_OWNERSHIP_GOALS,
    legal_entity_only_is_partial: true,
    never_invent: true,
    brand_or_operator_is_not_owner: true,
  };
}
