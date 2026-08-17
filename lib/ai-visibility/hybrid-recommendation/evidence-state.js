/**
 * Deterministic recommendation evidence-state classification for hybrid routing.
 * Returns DECISIVE | AMBIGUOUS | INSUFFICIENT — no confidence scores.
 */

import { GOVERNED_RECOMMENDATION_ROLES } from "./taxonomy.js";

export const EVIDENCE_STATES = Object.freeze(["DECISIVE", "AMBIGUOUS", "INSUFFICIENT"]);
export const ROUTES = Object.freeze(["DETERMINISTIC", "ADJUDICATOR", "ABSTAIN"]);

/**
 * Collect plausible roles from extracted evidence (mutually competing set).
 * @param {object} evidence
 * @returns {string[]}
 */
export function plausibleRolesFromEvidence(evidence) {
  const ev = evidence?.recommendationEvidence || {};
  const st = evidence?.structure || {};
  const roles = new Set();

  if (ev.directNegativeCue) roles.add("negative_or_qualified");

  const position =
    st.orderedPosition != null
      ? st.orderedPosition
      : st.tableRank != null
        ? st.tableRank
        : evidence?.rankPosition != null
          ? evidence.rankPosition
          : null;

  if (ev.leadCue) roles.add("first_recommendation");
  if ((st.confirmedRankStructure || evidence?.confirmedRankStructure) && position === 1) {
    roles.add("first_recommendation");
  }
  if ((st.confirmedRankStructure || evidence?.confirmedRankStructure) && position != null && position > 1) {
    roles.add("ranked_recommendation");
  }

  // Rank-like formatting without confirmed meaningful order → competing interpretations
  if (
    !st.confirmedRankStructure &&
    !evidence?.confirmedRankStructure &&
    (st.rawRankMarker != null || st.orderedPosition != null) &&
    !st.numberedHeading
  ) {
    roles.add("ranked_recommendation");
    roles.add("associated_option");
  }

  if (ev.directPositiveCue || ev.sectionPositiveCue) roles.add("explicit_recommendation");
  if (ev.considerationSetCue) roles.add("associated_option");
  if (
    evidence?.confirmedDecisionSet &&
    (evidence?.sectionType === "CONSIDERATION_SET_SECTION" ||
      st.headingSemanticType === "CONSIDERATION_SET_SECTION")
  ) {
    roles.add("associated_option");
  }

  if (ev.comparatorCue) roles.add("comparator");
  if (ev.sourceOnlyCue) roles.add("source_only");
  if (ev.incidentalCue && !ev.descriptiveCue) roles.add("passing_mention");

  // Descriptive / default discussion
  if (roles.size === 0) {
    roles.add("discussed");
  } else if (
    ev.descriptiveCue &&
    (ev.considerationSetCue || ev.directPositiveCue || ev.comparatorCue || ev.sectionPositiveCue)
  ) {
    // Competing boundary: decision cue vs neutral description
    roles.add("discussed");
  }
  return [...roles].filter((r) => GOVERNED_RECOMMENDATION_ROLES.includes(r));
}

/**
 * Ambiguity patterns derived from DEV error clusters (no case IDs).
 */
export function detectAmbiguityReasons(evidence, plausible) {
  const ev = evidence?.recommendationEvidence || {};
  const st = evidence?.structure || {};
  const reasons = [];
  const set = new Set(plausible);

  if (set.has("associated_option") && set.has("explicit_recommendation")) {
    reasons.push("consideration_set_plus_positive_language");
  }
  if (set.has("first_recommendation") && set.has("explicit_recommendation")) {
    reasons.push("first_explicit_boundary");
  }
  if (set.has("associated_option") && set.has("discussed")) {
    reasons.push("associated_discussed_boundary");
  }
  if (set.has("ranked_recommendation") && set.has("associated_option")) {
    reasons.push("rank_like_formatting_without_clear_meaningful_order");
  }
  if (set.has("comparator") && (set.has("explicit_recommendation") || set.has("discussed"))) {
    reasons.push("comparison_plus_recommendation_or_discussion");
  }
  if (
    (ev.sectionPositiveCue || evidence?.sectionType === "RECOMMENDATION_SET_SECTION") &&
    (ev.descriptiveCue || evidence?.sectionType === "UNKNOWN_SECTION")
  ) {
    reasons.push("section_level_recommendation_vs_neutral_entity_paragraph");
  }
  if (
    ev.considerationSetCue &&
    evidence?.propagationSource &&
    Number(evidence?.propagationDistance || 0) > 160
  ) {
    reasons.push("distant_section_propagation_association");
  }
  if (
    !st.confirmedRankStructure &&
    st.rawRankMarker != null &&
    !ev.leadCue &&
    set.has("first_recommendation")
  ) {
    reasons.push("numbering_without_lead_or_rank_semantics");
  }

  // Competing multi-role without a single exclusive decisive cue
  if (plausible.length >= 2 && reasons.length === 0) {
    reasons.push("multiple_plausible_roles");
  }

  return reasons;
}

/**
 * @param {object} evidence - extractEntityLocalEvidence output (or aggregated)
 * @param {{ deterministicRole?: string, entityPresent?: boolean }} [opts]
 */
export function classifyRecommendationEvidenceState(evidence, opts = {}) {
  const entityPresent = opts.entityPresent !== false;
  if (!entityPresent) {
    return {
      evidenceState: "DECISIVE",
      plausibleRoles: ["no_mention"],
      ambiguityReasons: [],
      route: "DETERMINISTIC",
      DETERMINISTIC_ROLE: "no_mention",
    };
  }

  if (!evidence || typeof evidence !== "object") {
    return {
      evidenceState: "INSUFFICIENT",
      plausibleRoles: [],
      ambiguityReasons: ["missing_evidence_object"],
      route: "ABSTAIN",
      DETERMINISTIC_ROLE: opts.deterministicRole || null,
    };
  }

  const plausible = plausibleRolesFromEvidence(evidence);
  const reasons = detectAmbiguityReasons(evidence, plausible);
  const detRole = opts.deterministicRole || null;

  if (plausible.length === 0) {
    return {
      evidenceState: "INSUFFICIENT",
      plausibleRoles: [],
      ambiguityReasons: ["no_plausible_roles"],
      route: "ABSTAIN",
      DETERMINISTIC_ROLE: detRole,
    };
  }

  // Exclusive strong negatives / lead / confirmed rank-1 are decisive when not competing with opposite polarity
  const ev = evidence.recommendationEvidence || {};
  const exclusiveDecisive =
    (ev.directNegativeCue && plausible.length === 1) ||
    (ev.leadCue && !ev.directNegativeCue && !reasons.includes("first_explicit_boundary")) ||
    ((evidence.structure?.confirmedRankStructure || evidence.confirmedRankStructure) &&
      !reasons.includes("rank_like_formatting_without_clear_meaningful_order") &&
      plausible.filter((r) => r === "first_recommendation" || r === "ranked_recommendation")
        .length === 1 &&
      !ev.directNegativeCue);

  if (reasons.length > 0 && !exclusiveDecisive) {
    return {
      evidenceState: "AMBIGUOUS",
      plausibleRoles: plausible,
      ambiguityReasons: reasons,
      route: "ADJUDICATOR",
      DETERMINISTIC_ROLE: detRole,
    };
  }

  if (plausible.length === 1 || exclusiveDecisive) {
    return {
      evidenceState: "DECISIVE",
      plausibleRoles: plausible,
      ambiguityReasons: [],
      route: "DETERMINISTIC",
      DETERMINISTIC_ROLE: detRole || plausible[0],
    };
  }

  // Multiple plausible without flagged ambiguity pattern — still ambiguous
  return {
    evidenceState: "AMBIGUOUS",
    plausibleRoles: plausible,
    ambiguityReasons: reasons.length ? reasons : ["multiple_plausible_roles"],
    route: "ADJUDICATOR",
    DETERMINISTIC_ROLE: detRole,
  };
}

/**
 * Build routing record for one case/mention.
 */
export function buildHybridRouteRecord({
  evidence,
  deterministicRole,
  entityPresent = true,
}) {
  const state = classifyRecommendationEvidenceState(evidence, {
    deterministicRole,
    entityPresent,
  });
  return {
    DETERMINISTIC_ROLE: deterministicRole || state.DETERMINISTIC_ROLE,
    EVIDENCE_STATE: state.evidenceState,
    AMBIGUITY_REASONS: state.ambiguityReasons,
    PLAUSIBLE_ROLES: state.plausibleRoles,
    ROUTE: state.route,
  };
}
