/**
 * Portfolio peer-set + benchmark methodology (DESIGN — not finalized for production math).
 */

import { PORTFOLIO_TYPES } from "./brand-portfolio-position-contract-v1.js";

export const PEER_SET_METHODOLOGY_V1 = Object.freeze({
  version: "ADP_PORTFOLIO_PEER_SET_METHODOLOGY_V1_DRAFT",
  principles: [
    "Peer universe must be analytically defensible for the traveler context",
    "Do not rank subject against every global loyalty member",
    "Geography relevance required (same market / competing catchments)",
    "Scenario relevance required (same territory filter)",
    "Active/open properties only unless historical peer snapshot says otherwise",
    "Canonical entity IDs required",
    "Peer set versioned: peerSetId + peerSetVersion + effectiveDate + members",
  ],
  byPortfolioType: Object.freeze({
    [PORTFOLIO_TYPES.HARD_BRAND_PORTFOLIO]:
      "Sister properties of the same hard brand within governed geography radius / market pack",
    [PORTFOLIO_TYPES.COLLECTION_PORTFOLIO]:
      "Same collection (e.g. Curio) within governed geography; not all Hilton Honors hotels",
    [PORTFOLIO_TYPES.LOYALTY_ECOSYSTEM]:
      "Loyalty-constrained peers relevant to the same market/context — explicit inclusion list, not global dump",
    [PORTFOLIO_TYPES.INDEPENDENT_POSITIONING]:
      "Governed independent (and optionally soft-brand) competitive set — methodology PENDING founder definition",
  }),
  versioningFields: ["peerSetId", "peerSetVersion", "canonicalMembers", "effectiveDate", "reason"],
  historicalRule: "PORTFOLIO_AFFILIATION_SNAPSHOT_INTEGRITY — never rewrite old periods with current affiliation",
});

export const PORTFOLIO_BENCHMARK_CANDIDATE_V1 = Object.freeze({
  status: "CANDIDATE_NOT_FINAL",
  portfolioBenchmarkAiPresence:
    "Mean AI Presence of governed relevant portfolio peers in the same eligible BRAND_PORTFOLIO_DEMAND scenario universe and territory scope",
  portfolioPresenceIndex: {
    formula: "subject_portfolio_presence_rate / portfolio_benchmark_presence_rate × 100",
    versionLabel: "PORTFOLIO_PRESENCE_INDEX_V1_CANDIDATE",
    interpretation: {
      100: "parity with governed portfolio peer average",
      above100: "subject appears more often than peer average",
      below100: "subject appears less often than peer average",
    },
    doNotSilentlyReuseCoreIndex: true,
    finalizeWhen: [
      "peer_set_methodology_founder_approved",
      "denominator_grain_locked_observation_or_scenario",
      "min_peer_count_rule_locked",
      "zero_and_missing_peer_rules_locked",
    ],
  },
});

/** Conceptual prompt templates — not execution strings. */
export const BRAND_PORTFOLIO_PROMPT_TEMPLATE_SHAPES_V1 = Object.freeze({
  HARD_BRAND: "{frame} {hardBrand} hotels in {geo} {travelerNeed}",
  COLLECTION: "{frame} {collection} hotels in {geo} {travelerNeed}",
  LOYALTY: "{frame} {loyaltyProgram} hotels in {geo} {travelerNeed}",
  INDEPENDENT: "{frame} independent hotels in {geo} {travelerNeed}",
  notes: [
    "Do not ship literal examples as production prompts without template versioning",
    "Must pass BRAND_PORTFOLIO_PROMPT_ELIGIBILITY_INTEGRITY",
    "Must bind profileHash + lensId + peerSetVersion at render",
  ],
});
