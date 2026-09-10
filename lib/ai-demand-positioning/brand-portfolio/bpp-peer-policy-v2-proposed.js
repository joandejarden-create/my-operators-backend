/**
 * ADP_BPP_PEER_POLICY_V2 — PROPOSED (inactive until founder-approved MCR).
 *
 * Doctrine candidates:
 *   BPP_LOYALTY_SUBSTITUTION_OVERRIDES_CHAIN_SCALE_EXCLUSION
 *   SAME_ECOSYSTEM_SAME_MARKET_IS_PRIMARY_BPP_RELEVANCE
 *   CHAIN_SCALE_IS_CONTEXT_NOT_A_GATE
 *
 * Do NOT import this into production BPP payload builders until
 * ADP_METHODOLOGY_CHANGE_REQUEST_V1 for BPP peer selection is APPROVED.
 */

export const ADP_BPP_PEER_POLICY_V2 = "ADP_BPP_PEER_POLICY_V2";
export const ADP_BPP_PEER_POLICY_V2_STATUS = "PROPOSED_PENDING_FOUNDER_APPROVAL";
export const ADP_BPP_PEER_POLICY_V2_ACTIVE = false;

export const BPP_LOYALTY_SUBSTITUTION_OVERRIDES_CHAIN_SCALE_EXCLUSION =
  "BPP_LOYALTY_SUBSTITUTION_OVERRIDES_CHAIN_SCALE_EXCLUSION";
export const SAME_ECOSYSTEM_SAME_MARKET_IS_PRIMARY_BPP_RELEVANCE =
  "SAME_ECOSYSTEM_SAME_MARKET_IS_PRIMARY_BPP_RELEVANCE";
export const CHAIN_SCALE_IS_CONTEXT_NOT_A_GATE = "CHAIN_SCALE_IS_CONTEXT_NOT_A_GATE";

export const ADP_BPP_FULL_LOYALTY_ECOSYSTEM_MARKET_DISCOVERY =
  "ADP_BPP_FULL_LOYALTY_ECOSYSTEM_MARKET_DISCOVERY";
export const ADP_BPP_CHAIN_SCALE_NOT_HARD_EXCLUSION =
  "ADP_BPP_CHAIN_SCALE_NOT_HARD_EXCLUSION";
export const ADP_BPP_LOYALTY_SUBSTITUTION_PEER_DISCOVERY =
  "ADP_BPP_LOYALTY_SUBSTITUTION_PEER_DISCOVERY";
export const ADP_BPP_PEER_ROLE_CLASSIFICATION = "ADP_BPP_PEER_ROLE_CLASSIFICATION";
export const ADP_BPP_SUPPRESSION_REQUIRES_FULL_ECOSYSTEM_DISCOVERY =
  "ADP_BPP_SUPPRESSION_REQUIRES_FULL_ECOSYSTEM_DISCOVERY";

/**
 * NOTE (2026-09-10): Inventory completeness + canonical discovery are active as
 * DATA_QUALITY / DISCOVERY gates under current PEER_EXPANSION_HIERARCHY_V1.
 * See bpp-canonical-ecosystem-discovery-v1.js — these do NOT activate V2
 * peer-role/benchmark semantics. V2 remains PENDING_FOUNDER_APPROVAL.
 */

/** Peer role labels under V2 (descriptive; not exclusion). */
export const BPP_PEER_ROLE_V2 = Object.freeze({
  LIKE_FOR_LIKE_PORTFOLIO_PEER: "LIKE_FOR_LIKE_PORTFOLIO_PEER",
  BROADER_LOYALTY_ALTERNATIVE: "BROADER_LOYALTY_ALTERNATIVE",
});

/**
 * Proposed geographic discovery ladder (loyalty ecosystem required at every level).
 */
export const BPP_PEER_DISCOVERY_LADDER_V2 = Object.freeze([
  {
    level: 1,
    id: "L1_SAME_ECOSYSTEM_SAME_GOVERNED_MARKET_SUBMARKET",
    rule: "Same loyalty ecosystem + same governed market/submarket",
  },
  {
    level: 2,
    id: "L2_SAME_ECOSYSTEM_ADJACENT_APPROVED_SUBMARKET_METRO",
    rule: "Same loyalty ecosystem + adjacent approved submarket / metro",
  },
  {
    level: 3,
    id: "L3_SAME_ECOSYSTEM_BROADER_GOVERNED_METRO",
    rule: "Same loyalty ecosystem + broader governed metro market (not statewide/national/global)",
  },
]);

/**
 * Hard exclusion classes that remain valid under V2.
 * Chain scale / service level / room count / luxury-vs-upscale are NOT listed here.
 */
export const BPP_PEER_HARD_EXCLUSION_CLASSES_V2 = Object.freeze([
  "NOT_SAME_LOYALTY_ECOSYSTEM",
  "NOT_SAME_REAL_HOTEL_MARKET",
  "EXTREME_FORMAT_MISMATCH_SUBSTITUTION_IMPLAUSIBLE",
  "EXTENDED_STAY_ONLY_WHERE_IMPLAUSIBLE",
  "AIRPORT_ONLY_OR_RESORT_ONLY_MARKET_MISMATCH",
  "CLOSED_INACTIVE_OR_DUPLICATE",
  "NON_CANONICAL_IDENTITY",
  "GEOGRAPHICALLY_IRRELEVANT_PADDING",
]);

/**
 * Descriptive peer attributes (context only — never hard exclusion alone).
 */
export const BPP_PEER_CONTEXT_DESCRIPTORS_V2 = Object.freeze([
  "chainScale",
  "serviceLevel",
  "roomCount",
  "brand",
  "collection",
  "distanceOrSubmarket",
  "peerRole",
]);

/**
 * Loyalty-substitution test (product question).
 */
export const BPP_LOYALTY_SUBSTITUTION_TEST_V2 =
  "Would a points-oriented traveler searching this market plausibly consider this hotel because it participates in the same loyalty ecosystem?";

/**
 * Benchmark policy options pending founder choice.
 * Recommendation recorded in methodology change request (not activated).
 */
export const BPP_BENCHMARK_POLICY_OPTIONS_V2 = Object.freeze({
  A_ALL_ACCEPTED_PEERS_EQUAL: "ALL_ACCEPTED_SAME_ECOSYSTEM_PEERS_EQUAL_WEIGHT",
  B_LIKE_FOR_LIKE_BENCHMARK_PLUS_VISIBLE_BROADER:
    "LIKE_FOR_LIKE_BENCHMARK_INDEX__BROADER_ALTERNATIVES_VISIBLE_CONTEXT",
});

export const BPP_BENCHMARK_RECOMMENDATION_V2 = Object.freeze({
  preferred: BPP_BENCHMARK_POLICY_OPTIONS_V2.A_ALL_ACCEPTED_PEERS_EQUAL,
  fallbackIfDistortion:
    BPP_BENCHMARK_POLICY_OPTIONS_V2.B_LIKE_FOR_LIKE_BENCHMARK_PLUS_VISIBLE_BROADER,
  rationale:
    "Founder intent is the full loyalty-choice landscape. Prefer equal-weight use of all accepted same-ecosystem market peers when n≥5 and format mismatches are already hard-excluded. If thin like-for-like packs would distort index vs visible broader alternatives, use dual-layer presentation rather than exclusion.",
});

export const BPP_CUSTOMER_DISCLOSURE_COPY_V2 = Object.freeze({
  sectionIntro:
    "Brand & Portfolio Position compares this hotel with other hotels in the same loyalty ecosystem that a traveler could choose in this market. These alternatives may span different brands and service levels.",
  preferredBenchmarkLabel: "Loyalty ecosystem benchmark",
  deprecatedImpliesSameScale: ["same-scale benchmark", "same-scale portfolio benchmark"],
});

/**
 * Choice Privileges / Americas Radisson ecosystem brands relevant to BPP V2.
 * EMEA Radisson Hotel Group brands are NOT assumed identical.
 */
export const CHOICE_PRIVILEGES_ECOSYSTEM_BRANDS_AMERICAS_V2 = Object.freeze([
  "Choice Hotels (hard brands: Comfort, Quality, Sleep Inn, Clarion, Cambria, Ascend, etc.)",
  "Radisson",
  "Radisson Blu",
  "Radisson RED",
  "Radisson Collection",
  "Radisson Individuals",
  "Park Plaza (Americas / Choice-eligible)",
  "Park Inn by Radisson (Americas / Choice-eligible)",
  "Country Inn & Suites by Radisson (Americas / Choice-eligible)",
  "Faranda Collection / Faranda Grand when member of Radisson Individuals (Choice Privileges)",
]);

export function isBppPeerPolicyV2Active() {
  return ADP_BPP_PEER_POLICY_V2_ACTIVE === true;
}
