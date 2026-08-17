/**
 * AI Visibility recommendation classifier v4.1 (deterministic).
 *
 * Hardening 6: evidence v4.1 (bounded section propagation) → decision tree.
 * v4 + evidence v4 preserved for comparison.
 */

import {
  extractEntityLocalEvidence,
  aggregateEntityEvidence,
  buildTypedSections,
  RECOMMENDATION_EVIDENCE_VERSION,
} from "./recommendation-evidence-v4_1.js";

export const RECOMMENDATION_CLASSIFIER_VERSION =
  "ai_visibility_recommendation_classifier_v4_1";

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
 * Mutually exclusive role decision from evidence object only.
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
        : evidence.rankPosition != null
          ? evidence.rankPosition
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

  if ((st.confirmedRankStructure || evidence.confirmedRankStructure) && position === 1) {
    return {
      role: "first_recommendation",
      explicitRecommendation: true,
      recommendationPosition: 1,
      reason: "confirmed_rank_position_1",
      classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
      evidence,
    };
  }

  if ((st.confirmedRankStructure || evidence.confirmedRankStructure) && position != null && position > 1) {
    return {
      role: "ranked_recommendation",
      explicitRecommendation: true,
      recommendationPosition: position,
      reason: "confirmed_rank_position_gt_1",
      classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
      evidence,
    };
  }

  // Direct positive OR bounded recommendation-set section propagation → explicit
  if (ev.directPositiveCue || ev.sectionPositiveCue) {
    return {
      role: "explicit_recommendation",
      explicitRecommendation: true,
      recommendationPosition: null,
      reason: ev.sectionPositiveCue ? "section_recommendation_set" : "direct_positive_evidence",
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
  if (
    evidence.confirmedDecisionSet &&
    (evidence.sectionType === "CONSIDERATION_SET_SECTION" ||
      evidence.structure?.headingSemanticType === "CONSIDERATION_SET_SECTION") &&
    (evidence.structure?.isUnorderedList ||
      evidence.structure?.isOrderedList ||
      evidence.structure?.tableRank != null ||
      evidence.structure?.orderedPosition != null)
  ) {
    return {
      role: "associated_option",
      explicitRecommendation: false,
      recommendationPosition: null,
      reason: "confirmed_consideration_section_membership",
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

  if (ev.sourceOnlyCue) {
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

  return {
    role: "discussed",
    explicitRecommendation: false,
    recommendationPosition: null,
    reason: ev.descriptiveCue ? "descriptive_evidence" : "default_discussed",
    classifierVersion: RECOMMENDATION_CLASSIFIER_VERSION,
    evidence,
  };
}

export function classifyMentionRoleV4(mention) {
  return classifyMentionRoleV4_1(mention);
}

export function classifyMentionRoleV4_1(mention) {
  const text = String(mention?.text || "");
  const start = Number(mention?.start ?? mention?.mentionPosition ?? 0);
  const end = Number(mention?.end ?? start + String(mention?.rawMention || "").length);
  const typedSections = mention?.typedSections || buildTypedSections(text);
  const evidence = extractEntityLocalEvidence({
    text,
    start,
    end,
    rawMention: mention?.rawMention,
    canonicalEntityId: mention?.canonicalEntityId,
    canonicalEntityName: mention?.canonicalEntityName,
    typedSections,
  });
  const decided = decideRecommendationRoleFromEvidence(evidence, { entityPresent: true });
  return {
    ...decided,
    sectionRole: evidence.sectionType || null,
    evidence,
  };
}

export function assignFirstRecommendationAcrossMentionsV4(classifiedMentions, text) {
  return assignFirstRecommendationAcrossMentionsV4_1(classifiedMentions, text);
}

export function assignFirstRecommendationAcrossMentionsV4_1(classifiedMentions, _text) {
  const mentions = (classifiedMentions || []).map((m) => ({ ...m }));
  const firsts = mentions.filter((m) => m.role === "first_recommendation" && m.canonicalEntityId);
  if (firsts.length <= 1) return mentions;
  firsts.sort(
    (a, b) =>
      (a.recommendationPosition ?? 1) - (b.recommendationPosition ?? 1) ||
      a.mentionPosition - b.mentionPosition
  );
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

export function classifyEntityFromMentionSpans(args) {
  const { text, spans, canonicalEntityId, canonicalEntityName } = args;
  const source = String(text || "");
  const typedSections = args?.typedSections || buildTypedSections(source);
  const perMention = (spans || []).map((sp) =>
    extractEntityLocalEvidence({
      text: source,
      start: sp.start,
      end: sp.end,
      rawMention: sp.rawMention,
      canonicalEntityId,
      canonicalEntityName,
      typedSections,
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

export function classifyMentionRole(mention) {
  return classifyMentionRoleV4_1(mention).role;
}

export function assignFirstRecommendationAcrossMentions(classifiedMentions, text) {
  return assignFirstRecommendationAcrossMentionsV4_1(classifiedMentions, text);
}
