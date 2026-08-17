/**
 * Production recommendation taxonomy simplification — candidate mappings.
 * Internal 10-class labels remain immutable; production roles are derived only.
 */

export const INTERNAL_ROLES = Object.freeze([
  "first_recommendation",
  "ranked_recommendation",
  "explicit_recommendation",
  "associated_option",
  "comparator",
  "discussed",
  "passing_mention",
  "negative_or_qualified",
  "source_only",
  "no_mention",
]);

/** Candidate A — 6 states (brief-specified) */
export const CANDIDATE_A = Object.freeze({
  id: "A_6_STATE",
  name: "Six-state production taxonomy",
  states: [
    "LEADING_RECOMMENDATION",
    "RECOMMENDED_OR_CONSIDERED",
    "DISCUSSED",
    "COMPARATOR_OR_QUALIFIED",
    "PASSING_OR_SOURCE",
    "NOT_MENTIONED",
  ],
  mapping: {
    first_recommendation: "LEADING_RECOMMENDATION",
    ranked_recommendation: "RECOMMENDED_OR_CONSIDERED",
    explicit_recommendation: "RECOMMENDED_OR_CONSIDERED",
    associated_option: "RECOMMENDED_OR_CONSIDERED",
    discussed: "DISCUSSED",
    comparator: "COMPARATOR_OR_QUALIFIED",
    negative_or_qualified: "COMPARATOR_OR_QUALIFIED",
    passing_mention: "PASSING_OR_SOURCE",
    source_only: "PASSING_OR_SOURCE",
    no_mention: "NOT_MENTIONED",
  },
});

/** Candidate B — 5 states (brief-specified) */
export const CANDIDATE_B = Object.freeze({
  id: "B_5_STATE",
  name: "Five-state production taxonomy",
  states: [
    "LEADING",
    "IN_CONSIDERATION",
    "DISCUSSED",
    "NON_POSITIVE_REFERENCE",
    "NOT_PRESENT",
  ],
  mapping: {
    first_recommendation: "LEADING",
    ranked_recommendation: "IN_CONSIDERATION",
    explicit_recommendation: "IN_CONSIDERATION",
    associated_option: "IN_CONSIDERATION",
    discussed: "DISCUSSED",
    comparator: "NON_POSITIVE_REFERENCE",
    negative_or_qualified: "NON_POSITIVE_REFERENCE",
    passing_mention: "NON_POSITIVE_REFERENCE",
    source_only: "NON_POSITIVE_REFERENCE",
    no_mention: "NOT_PRESENT",
  },
});

/**
 * Candidate C — evidence-based from dominant DEV confusions:
 * associated↔discussed, ranked→associated, comparator↔discussed, passing noise.
 * Collapses the unstable mid-band into fewer client states while keeping lead + negative.
 */
export const CANDIDATE_C = Object.freeze({
  id: "C_4_STATE_DECISION_SIGNAL",
  name: "Four-state decision-signal taxonomy",
  states: [
    "LEADING_RECOMMENDATION",
    "IN_RECOMMENDATION_OR_CONSIDERATION_SET",
    "MENTIONED_WITHOUT_RECOMMENDATION_CLAIM",
    "NOT_MENTIONED",
  ],
  mapping: {
    first_recommendation: "LEADING_RECOMMENDATION",
    ranked_recommendation: "IN_RECOMMENDATION_OR_CONSIDERATION_SET",
    explicit_recommendation: "IN_RECOMMENDATION_OR_CONSIDERATION_SET",
    associated_option: "IN_RECOMMENDATION_OR_CONSIDERATION_SET",
    discussed: "MENTIONED_WITHOUT_RECOMMENDATION_CLAIM",
    comparator: "MENTIONED_WITHOUT_RECOMMENDATION_CLAIM",
    negative_or_qualified: "MENTIONED_WITHOUT_RECOMMENDATION_CLAIM",
    passing_mention: "MENTIONED_WITHOUT_RECOMMENDATION_CLAIM",
    source_only: "MENTIONED_WITHOUT_RECOMMENDATION_CLAIM",
    no_mention: "NOT_MENTIONED",
  },
});

/**
 * Candidate D — three production signal states.
 * First Recommendation is NOT a production role; it is a separate boolean metric.
 */
export const CANDIDATE_D = Object.freeze({
  id: "D_3_STATE_SIGNAL",
  name: "Three-state production signal taxonomy",
  states: ["RECOMMENDED", "MENTIONED", "NOT_MENTIONED"],
  mapping: {
    first_recommendation: "RECOMMENDED",
    ranked_recommendation: "RECOMMENDED",
    explicit_recommendation: "RECOMMENDED",
    associated_option: "MENTIONED",
    comparator: "MENTIONED",
    discussed: "MENTIONED",
    passing_mention: "MENTIONED",
    negative_or_qualified: "MENTIONED",
    source_only: "MENTIONED",
    no_mention: "NOT_MENTIONED",
  },
  firstRecommendationSeparate: true,
  safeCopy: {
    RECOMMENDED: "AI responses explicitly recommended the brand.",
    MENTIONED:
      "The brand appeared in the response without a governed recommendation claim.",
    NOT_MENTIONED: "The brand did not appear in the monitored response.",
    FIRST_RECOMMENDATION: "The brand was the first recommendation.",
  },
});

/**
 * Candidate E — presence + independent evidence flags (not a single mutually exclusive role).
 */
export const CANDIDATE_E = Object.freeze({
  id: "E_SIGNAL_AND_FLAGS",
  name: "Presence plus independent evidence flags",
  presenceStates: ["PRESENT", "NOT_PRESENT"],
  flags: [
    "RECOMMENDED",
    "FIRST_RECOMMENDATION",
    "NEGATIVE_OR_QUALIFIED",
    "COMPARATOR",
  ],
  adoptionStatus: "ADOPTED_AS_PRODUCTION_ARCHITECTURE",
  note:
    "Production client contract is signal/flag architecture (see lib/ai-visibility/signal-architecture/). Internal 10-class remains research-only.",
});

/** Derive Candidate E labels from an internal 10-class role. */
export function deriveCandidateEFromInternalRole(internalRole) {
  const role = internalRole == null ? "no_mention" : String(internalRole);
  const present = role !== "no_mention";
  return {
    PRESENT: present,
    NOT_PRESENT: !present,
    presence: present ? "PRESENT" : "NOT_PRESENT",
    RECOMMENDED: ["first_recommendation", "ranked_recommendation", "explicit_recommendation"].includes(
      role
    ),
    FIRST_RECOMMENDATION: role === "first_recommendation",
    NEGATIVE_OR_QUALIFIED: role === "negative_or_qualified",
    COMPARATOR: role === "comparator",
  };
}

export const CANDIDATES = Object.freeze([CANDIDATE_A, CANDIDATE_B, CANDIDATE_C]);
export const SIGNAL_CANDIDATES = Object.freeze([CANDIDATE_D, CANDIDATE_E]);

export function mapInternalToProduction(internalRole, candidate) {
  const role = internalRole == null ? "no_mention" : String(internalRole);
  const mapped = candidate.mapping[role];
  if (!mapped) {
    throw new Error(`Unmapped internal role: ${role} for candidate ${candidate.id}`);
  }
  return mapped;
}

/**
 * Which production states count toward Recommendation Share under each candidate.
 * Must not silently expand beyond current POSITIVE_RECOMMENDATION_ROLES without flagging.
 * Current internal positive set: first, ranked, explicit (NOT associated).
 */
export function recommendationShareProductionStates(candidateId) {
  if (candidateId === CANDIDATE_A.id) {
    return {
      // Compatible path: only LEADING + subset of RECOMMENDED_OR_CONSIDERED that maps from first/ranked/explicit
      // Problem: RECOMMENDED_OR_CONSIDERED also includes associated_option → COMPATIBILITY ISSUE
      statesCountingAsPositiveRec: ["LEADING_RECOMMENDATION"],
      statesCountingAsConsiderationOnly: ["RECOMMENDED_OR_CONSIDERED"],
      issue:
        "RECOMMENDED_OR_CONSIDERED mixes associated_option (currently NOT in Recommendation Share) with ranked/explicit (ARE in share). Must split or redefine share before adopt.",
      compatibleShareDefinition:
        "Share numerator = LEADING_RECOMMENDATION + RECOMMENDED_OR_CONSIDERED only when internal role ∈ {ranked, explicit}; associated stays presence-only unless product explicitly expands share.",
    };
  }
  if (candidateId === CANDIDATE_B.id) {
    return {
      statesCountingAsPositiveRec: ["LEADING"],
      statesCountingAsConsiderationOnly: ["IN_CONSIDERATION"],
      issue:
        "IN_CONSIDERATION includes associated_option + ranked + explicit — same share-expansion risk as A.",
      compatibleShareDefinition:
        "Keep share on internal first/ranked/explicit; production IN_CONSIDERATION is not automatically share-positive.",
    };
  }
  return {
    statesCountingAsPositiveRec: ["LEADING_RECOMMENDATION"],
    statesCountingAsConsiderationOnly: ["IN_RECOMMENDATION_OR_CONSIDERATION_SET"],
    issue:
      "IN_RECOMMENDATION_OR_CONSIDERATION_SET includes associated_option; Recommendation Share must remain gated by internal positive roles or an explicit product decision to include consideration-set membership.",
    compatibleShareDefinition:
      "Default: share uses internal POSITIVE_RECOMMENDATION_ROLES only; production set is a display rollup, not an automatic share expander.",
  };
}

export const PRODUCT_ASSESSMENT = Object.freeze({
  A_6_STATE: {
    CLIENT_VALUE:
      "High — separates lead, recommended/considered band, discussion, non-positive, weak mention, absent.",
    INTERPRETABILITY:
      "Good — states are explainable; RECOMMENDED_OR_CONSIDERED blends endorsement with consideration (needs careful copy).",
    OVERCLAIM_RISK:
      "Medium — collapsing associated into recommended/considered can overstate preference if UI says “recommended”.",
    METRIC_COMPATIBILITY:
      "Partial — First Rec / Questions Won stay on LEADING; Recommendation Share conflicts unless associated excluded from share.",
  },
  B_5_STATE: {
    CLIENT_VALUE:
      "High — simpler; merges weak mentions into NON_POSITIVE_REFERENCE.",
    INTERPRETABILITY: "Strong — five labels are easy to teach.",
    OVERCLAIM_RISK:
      "Medium-high — IN_CONSIDERATION naming is safer than “recommended”, but still broad.",
    METRIC_COMPATIBILITY:
      "Partial — same share issue; DISCUSSED kept separate which helps Presence vs preference.",
  },
  C_4_STATE_DECISION_SIGNAL: {
    CLIENT_VALUE:
      "Very high for reliability — answers: lead / in set / mentioned without claim / absent.",
    INTERPRETABILITY:
      "Strong if copy avoids “AI recommends” for the mid set; name is explicit about claim level.",
    OVERCLAIM_RISK:
      "Lower than A/B if UI treats IN_RECOMMENDATION_OR_CONSIDERATION_SET as “appeared in options/consideration”, not “AI picked”.",
    METRIC_COMPATIBILITY:
      "Best for Presence + First Rec; Recommendation Share still needs internal gating for associated.",
  },
});
