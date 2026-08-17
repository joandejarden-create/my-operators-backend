/**
 * AI Visibility recommendation classifier v4 (deterministic).
 *
 * Two-stage architecture (Hardening 5):
 *   Stage 1 — entity-local evidence extraction (recommendation-evidence-v4)
 *   Stage 2 — mutually exclusive role decision from evidence object only
 *
 * Does not scan unbounded raw response text for role assignment.
 * v3.3 remains available for comparison; production mention extraction uses v4.
 */

import {
  extractEntityLocalEvidence,
  aggregateEntityEvidence,
  RECOMMENDATION_EVIDENCE_VERSION,
} from "./recommendation-evidence-v4.js";
import { detectResponseSections } from "./recommendation-classifier-v3.js";

export const RECOMMENDATION_CLASSIFIER_VERSION =
  "ai_visibility_recommendation_classifier_v4";

export const RECOMMENDATION_ROLE_PRECEDENCE = Object.freeze([
  "negative_or_qualified",
  "first_recommendation",
  "ranked_recommendation",
  "explicit_recommendation",
  "associated_option",
  "comparator",
  "discussed",
  "passing_mention",
  "source_only",
  "no_mention",
]);

export { RECOMMENDATION_EVIDENCE_VERSION };

/**
 * Stage 2: decide role from aggregated (or single-mention) evidence only.
 * @param {object|null} evidence
 * @param {{ entityPresent?: boolean }} [opts]
 */
export function decideRecommendationRoleFromEvidence(evidence, opts = {}) {
  const entityPresent = opts.entityPresent !== false;
  if (!entityPresent || !evidence) {
    return {
      role: "no_mention",
      explicitRecommendation: false,
      recommendationPosition: null,
      reason: "entity_absent",
      classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
    };
  }

  const ev = evidence.recommendationEvidence || {};
  const st = evidence.structure || {};
  const position =
    st.orderedPosition != null
      ? st.orderedPosition
      : st.tableRank != null
        ? st.tableRank
        : null;

  if (ev.directNegativeCue) {
    return {
      role: "negative_or_qualified",
      explicitRecommendation: false,
      recommendationPosition: null,
      reason: "direct_negative_evidence",
      classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
      evidence,
    };
  }

  if (ev.leadCue) {
    return {
      role: "first_recommendation",
      explicitRecommendation: true,
      recommendationPosition: 1,
      reason: "lead_cue_evidence",
      classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
      evidence,
    };
  }

  if (st.confirmedRankStructure && position === 1) {
    return {
      role: "first_recommendation",
      explicitRecommendation: true,
      recommendationPosition: 1,
      reason: "confirmed_rank_position_1",
      classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
      evidence,
    };
  }

  if (st.confirmedRankStructure && position != null && position > 1) {
    return {
      role: "ranked_recommendation",
      explicitRecommendation: true,
      recommendationPosition: position,
      reason: "confirmed_rank_position_gt_1",
      classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
      evidence,
    };
  }

  // Ranked heading + list membership without numeric position → associated, not ranked
  // (unordered shortlist under ranked heading still needs order markers for ranked role)
  if (ev.directPositiveCue) {
    return {
      role: "explicit_recommendation",
      explicitRecommendation: true,
      recommendationPosition: null,
      reason: "direct_positive_evidence",
      classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
      evidence,
    };
  }

  if (ev.considerationSetCue) {
    return {
      role: "associated_option",
      explicitRecommendation: false,
      recommendationPosition: null,
      reason: "consideration_set_evidence",
      classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
      evidence,
    };
  }

  if (ev.comparatorCue) {
    return {
      role: "comparator",
      explicitRecommendation: false,
      recommendationPosition: null,
      reason: "comparator_evidence",
      classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
      evidence,
    };
  }

  if (ev.sourceOnlyCue && !ev.descriptiveCue) {
    return {
      role: "source_only",
      explicitRecommendation: false,
      recommendationPosition: null,
      reason: "source_only_evidence",
      classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
      evidence,
    };
  }

  if (ev.incidentalCue && !ev.descriptiveCue) {
    return {
      role: "passing_mention",
      explicitRecommendation: false,
      recommendationPosition: null,
      reason: "incidental_evidence",
      classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
      evidence,
    };
  }

  if (ev.descriptiveCue || true) {
    return {
      role: "discussed",
      explicitRecommendation: false,
      recommendationPosition: null,
      reason: ev.descriptiveCue ? "descriptive_evidence" : "default_discussed",
      classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
      evidence,
    };
  }
}

/**
 * Classify a single mention span via evidence → decision tree.
 */
export function classifyMentionRoleV4(mention) {
  const text = String(mention?.text || "");
  const start = Number(mention?.start ?? mention?.mentionPosition ?? 0);
  const end = Number(mention?.end ?? start + String(mention?.rawMention || "").length);
  const sections = mention?.sections || detectResponseSections(text);
  const evidence = extractEntityLocalEvidence({
    text,
    start,
    end,
    rawMention: mention?.rawMention,
    canonicalEntityId: mention?.canonicalEntityId,
    canonicalEntityName: mention?.canonicalEntityName,
    sections,
  });
  const decided = decideRecommendationRoleFromEvidence(evidence, { entityPresent: true });
  return {
    ...decided,
    sectionRole: evidence.structure?.headingSemanticType || null,
    evidence,
  };
}

/**
 * Across-mention first assignment: only from evidence-backed first / confirmed rank-1.
 * Does NOT promote bare explicit_recommendation to first.
 */
export function assignFirstRecommendationAcrossMentionsV4(classifiedMentions, _text) {
  const mentions = (classifiedMentions || []).map((m) => ({ ...m }));

  const firsts = mentions.filter((m) => m.role === "first_recommendation" && m.canonicalEntityId);
  if (firsts.length <= 1) return mentions;

  // Keep earliest evidence-backed first; demote others to ranked/explicit
  firsts.sort((a, b) => {
    const ap = a.recommendationPosition ?? 1;
    const bp = b.recommendationPosition ?? 1;
    if (ap !== bp) return ap - bp;
    return a.mentionPosition - b.mentionPosition;
  });
  const keeperId = firsts[0].canonicalEntityId;
  for (const m of mentions) {
    if (m.role !== "first_recommendation") continue;
    if (m.canonicalEntityId === keeperId) continue;
    m.role =
      m.recommendationPosition && m.recommendationPosition > 1
        ? "ranked_recommendation"
        : "explicit_recommendation";
  }
  return mentions;
}

/**
 * Classify all spans for one entity using aggregated evidence (preferred for scoring).
 */
export function classifyEntityFromMentionSpans(args) {
  const {
    text,
    spans,
    canonicalEntityId,
    canonicalEntityName,
    sections,
  } = args;
  const source = String(text || "");
  const secs = sections || detectResponseSections(source);
  const perMention = (spans || []).map((sp) =>
    extractEntityLocalEvidence({
      text: source,
      start: sp.start,
      end: sp.end,
      rawMention: sp.rawMention,
      canonicalEntityId,
      canonicalEntityName,
      sections: secs,
    })
  );
  const aggregated = aggregateEntityEvidence(perMention);
  return decideRecommendationRoleFromEvidence(aggregated, {
    entityPresent: perMention.length > 0,
  });
}

export function questionStatusFromRecommendationRole(role, entityPresent) {
  if (!entityPresent) return "MISSING";
  if (role === "first_recommendation") return "FIRST_RECOMMENDED";
  if (role === "ranked_recommendation" || role === "explicit_recommendation") return "RECOMMENDED";
  if (role === "negative_or_qualified") return "NEGATIVE_OR_NOT_RECOMMENDED";
  if (role === "associated_option" || role === "passing_mention") return "PRESENT";
  if (role === "discussed" || role === "comparator" || role === "source_only") {
    return "DISCUSSION_ONLY";
  }
  if (entityPresent) return "PRESENT";
  return "NOT_APPLICABLE";
}

// Compat aliases used by extract-mentions wiring
export function classifyMentionRole(mention) {
  return classifyMentionRoleV4(mention).role;
}

export function assignFirstRecommendationAcrossMentions(classifiedMentions, text) {
  return assignFirstRecommendationAcrossMentionsV4(classifiedMentions, text);
}
