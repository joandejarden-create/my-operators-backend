/**
 * Deterministic node decisions for hierarchical recommendation tree.
 */

import { NODE_OUTPUTS } from "./tree.js";

function cues(evidence) {
  const ev = evidence?.recommendationEvidence || {};
  const st = evidence?.structure || {};
  const position =
    st.orderedPosition != null
      ? st.orderedPosition
      : st.tableRank != null
        ? st.tableRank
        : evidence?.rankPosition != null
          ? evidence.rankPosition
          : null;
  return {
    ev,
    st,
    position,
    confirmedRank: Boolean(st.confirmedRankStructure || evidence?.confirmedRankStructure),
  };
}

/**
 * @returns {{ result: string|null, needsAdjudicator: boolean, reason: string, ambiguityReasons: string[] }}
 */
export function decideNodeDeterministic(nodeId, evidence, opts = {}) {
  const entityPresent = opts.entityPresent !== false;
  const { ev, st, position, confirmedRank } = cues(evidence);

  if (nodeId === "Q1") {
    if (!entityPresent) {
      return { result: "ABSENT", needsAdjudicator: false, reason: "entity_absent", ambiguityReasons: [] };
    }
    if (ev.sourceOnlyCue && !ev.directPositiveCue && !ev.leadCue && !ev.considerationSetCue) {
      return { result: "SOURCE_ONLY", needsAdjudicator: false, reason: "source_only_cue", ambiguityReasons: [] };
    }
    if (
      ev.incidentalCue &&
      !ev.descriptiveCue &&
      !ev.directPositiveCue &&
      !ev.leadCue &&
      !ev.considerationSetCue &&
      !ev.comparatorCue
    ) {
      return { result: "INCIDENTAL", needsAdjudicator: false, reason: "incidental_cue", ambiguityReasons: [] };
    }
    // Default substantive when present with any discussion/decision signal or descriptive
    if (
      ev.descriptiveCue ||
      ev.directPositiveCue ||
      ev.leadCue ||
      ev.considerationSetCue ||
      ev.comparatorCue ||
      ev.directNegativeCue ||
      ev.sectionPositiveCue ||
      evidence?.confirmedDecisionSet
    ) {
      return { result: "SUBSTANTIVE", needsAdjudicator: false, reason: "substantive_cues", ambiguityReasons: [] };
    }
    // Present but weak cues — still treat as substantive discussion default (not absent)
    return { result: "SUBSTANTIVE", needsAdjudicator: false, reason: "default_substantive_present", ambiguityReasons: [] };
  }

  if (nodeId === "Q2") {
    if (ev.directNegativeCue && !ev.directPositiveCue && !ev.leadCue) {
      return { result: "YES_NEGATIVE", needsAdjudicator: false, reason: "direct_negative", ambiguityReasons: [] };
    }
    if (ev.directNegativeCue && (ev.directPositiveCue || ev.leadCue)) {
      return {
        result: null,
        needsAdjudicator: true,
        reason: "negative_vs_positive_conflict",
        ambiguityReasons: ["negative_vs_positive_conflict"],
      };
    }
    return { result: "NO_NEGATIVE", needsAdjudicator: false, reason: "no_negative_cue", ambiguityReasons: [] };
  }

  if (nodeId === "Q3") {
    const decisionish =
      ev.considerationSetCue ||
      ev.directPositiveCue ||
      ev.leadCue ||
      ev.sectionPositiveCue ||
      confirmedRank ||
      evidence?.sectionType === "RECOMMENDATION_SET_SECTION" ||
      evidence?.sectionType === "CONSIDERATION_SET_SECTION" ||
      evidence?.sectionType === "LEAD_RECOMMENDATION_SECTION" ||
      evidence?.sectionType === "RANKED_RECOMMENDATION_SECTION";

    if (ev.comparatorCue && !decisionish) {
      return { result: "COMPARATOR", needsAdjudicator: false, reason: "comparator_cue", ambiguityReasons: [] };
    }
    if (ev.comparatorCue && decisionish) {
      return {
        result: null,
        needsAdjudicator: true,
        reason: "comparator_vs_decision_conflict",
        ambiguityReasons: ["comparator_vs_decision_conflict"],
      };
    }
    if (decisionish) {
      return { result: "DECISION_OPTION", needsAdjudicator: false, reason: "decision_cues", ambiguityReasons: [] };
    }
    if (ev.descriptiveCue) {
      return {
        result: "NEUTRAL_DISCUSSION",
        needsAdjudicator: false,
        reason: "descriptive_only",
        ambiguityReasons: [],
      };
    }
    // Weak substantive — may need semantic help
    return {
      result: null,
      needsAdjudicator: true,
      reason: "q3_unclear_substance_framing",
      ambiguityReasons: ["q3_unclear_substance_framing"],
    };
  }

  if (nodeId === "Q4") {
    const endorsement = Boolean(ev.directPositiveCue || ev.leadCue || ev.sectionPositiveCue);
    const consideration = Boolean(
      ev.considerationSetCue ||
        (evidence?.confirmedDecisionSet &&
          (evidence?.sectionType === "CONSIDERATION_SET_SECTION" ||
            st.headingSemanticType === "CONSIDERATION_SET_SECTION"))
    );
    if (endorsement && !consideration) {
      return {
        result: "DIRECT_ENDORSEMENT",
        needsAdjudicator: false,
        reason: "direct_positive_or_lead",
        ambiguityReasons: [],
      };
    }
    if (consideration && !endorsement) {
      return {
        result: "CONSIDERATION_SET",
        needsAdjudicator: false,
        reason: "consideration_without_endorsement",
        ambiguityReasons: [],
      };
    }
    if (endorsement && consideration) {
      return {
        result: null,
        needsAdjudicator: true,
        reason: "consideration_plus_endorsement",
        ambiguityReasons: ["consideration_plus_endorsement"],
      };
    }
    return {
      result: null,
      needsAdjudicator: true,
      reason: "q4_unclear",
      ambiguityReasons: ["q4_unclear"],
    };
  }

  if (nodeId === "Q5") {
    if (ev.leadCue || confirmedRank) {
      return {
        result: "MEANINGFUL_ORDER",
        needsAdjudicator: false,
        reason: "lead_or_confirmed_rank",
        ambiguityReasons: [],
      };
    }
    // Numbered formatting without confirmed rank semantics
    if (st.rawRankMarker != null && !st.numberedHeading && !confirmedRank) {
      return {
        result: null,
        needsAdjudicator: true,
        reason: "numbering_without_confirmed_order",
        ambiguityReasons: ["numbering_without_confirmed_order"],
      };
    }
    return {
      result: "NO_MEANINGFUL_ORDER",
      needsAdjudicator: false,
      reason: "no_order_evidence",
      ambiguityReasons: [],
    };
  }

  if (nodeId === "Q6") {
    if (ev.leadCue || (confirmedRank && position === 1)) {
      return { result: "LEAD", needsAdjudicator: false, reason: "lead_or_rank1", ambiguityReasons: [] };
    }
    if (confirmedRank && position != null && position > 1) {
      return { result: "NON_LEAD", needsAdjudicator: false, reason: "rank_gt_1", ambiguityReasons: [] };
    }
    if (confirmedRank || ev.rankCue) {
      return {
        result: null,
        needsAdjudicator: true,
        reason: "order_without_clear_lead_slot",
        ambiguityReasons: ["order_without_clear_lead_slot"],
      };
    }
    return {
      result: null,
      needsAdjudicator: true,
      reason: "q6_unclear_lead",
      ambiguityReasons: ["q6_unclear_lead"],
    };
  }

  return {
    result: null,
    needsAdjudicator: true,
    reason: "unknown_node",
    ambiguityReasons: ["unknown_node"],
  };
}

export function assertNodeOutput(nodeId, value) {
  return NODE_OUTPUTS[nodeId]?.includes(value) === true;
}
