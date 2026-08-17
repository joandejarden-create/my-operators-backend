/**
 * Hierarchical recommendation taxonomy contracts (Q1–Q6).
 * Narrow enums — not a 10-way class choice.
 */

export const HIERARCHICAL_CLASSIFIER_VERSION =
  "ai_visibility_recommendation_classifier_v5_hierarchical";

export const NODE_IDS = Object.freeze(["Q1", "Q2", "Q3", "Q4", "Q5", "Q6"]);

export const NODE_OUTPUTS = Object.freeze({
  Q1: ["ABSENT", "SOURCE_ONLY", "INCIDENTAL", "SUBSTANTIVE"],
  Q2: ["YES_NEGATIVE", "NO_NEGATIVE"],
  Q3: ["COMPARATOR", "DECISION_OPTION", "NEUTRAL_DISCUSSION"],
  Q4: ["CONSIDERATION_SET", "DIRECT_ENDORSEMENT"],
  Q5: ["MEANINGFUL_ORDER", "NO_MEANINGFUL_ORDER"],
  Q6: ["LEAD", "NON_LEAD"],
});

export const NODE_DEFINITIONS = Object.freeze({
  Q1: {
    title: "Entity role polarity / substance",
    question: "Is the entity absent, source-only, incidental, or substantive?",
  },
  Q2: {
    title: "Negative / discouraged",
    question:
      "Does the substantive treatment materially discourage, exclude, or negatively qualify the entity?",
  },
  Q3: {
    title: "Comparator vs decision option vs neutral discussion",
    question:
      "Is the entity used principally as comparison/reference, a decision option/recommendation candidate, or neutral substantive discussion?",
  },
  Q4: {
    title: "Consideration set vs direct endorsement",
    question:
      "Is the entity included as a viable option without direct positive endorsement, or directly positively recommended/endorsed?",
  },
  Q5: {
    title: "Meaningful order",
    question:
      "Is there explicit meaningful preference/ranking evidence (not bare numbering)?",
  },
  Q6: {
    title: "Lead position",
    question:
      "Is the entity clearly #1 / first / top / primary recommendation?",
  },
});

/**
 * Map final governed role ← hierarchical path.
 */
export function composeRoleFromNodePath(path) {
  if (!path || path.Q1 === "ABSENT") return "no_mention";
  if (path.Q1 === "SOURCE_ONLY") return "source_only";
  if (path.Q1 === "INCIDENTAL") return "passing_mention";
  // SUBSTANTIVE
  if (path.Q2 === "YES_NEGATIVE") return "negative_or_qualified";
  if (path.Q3 === "COMPARATOR") return "comparator";
  if (path.Q3 === "NEUTRAL_DISCUSSION") return "discussed";
  // DECISION_OPTION
  if (path.Q4 === "CONSIDERATION_SET") return "associated_option";
  // DIRECT_ENDORSEMENT
  if (path.Q5 === "NO_MEANINGFUL_ORDER") return "explicit_recommendation";
  // MEANINGFUL_ORDER
  if (path.Q6 === "LEAD") return "first_recommendation";
  if (path.Q6 === "NON_LEAD") return "ranked_recommendation";
  return null;
}

/**
 * Derive expected node labels from final human role where uniquely determined.
 * @returns {Record<string, string|null>} null => NODE_GROUND_TRUTH_NOT_DERIVABLE
 */
export function deriveNodeLabelsFromHumanRole(humanRole) {
  const r = String(humanRole || "");
  const out = { Q1: null, Q2: null, Q3: null, Q4: null, Q5: null, Q6: null };

  if (r === "no_mention") {
    out.Q1 = "ABSENT";
    return out;
  }
  if (r === "source_only") {
    out.Q1 = "SOURCE_ONLY";
    return out;
  }
  if (r === "passing_mention") {
    out.Q1 = "INCIDENTAL";
    return out;
  }

  out.Q1 = "SUBSTANTIVE";

  if (r === "negative_or_qualified") {
    out.Q2 = "YES_NEGATIVE";
    return out;
  }
  out.Q2 = "NO_NEGATIVE";

  if (r === "comparator") {
    out.Q3 = "COMPARATOR";
    return out;
  }
  if (r === "discussed") {
    out.Q3 = "NEUTRAL_DISCUSSION";
    return out;
  }

  // decision-option roles
  if (
    [
      "associated_option",
      "explicit_recommendation",
      "ranked_recommendation",
      "first_recommendation",
    ].includes(r)
  ) {
    out.Q3 = "DECISION_OPTION";
  } else {
    return out;
  }

  if (r === "associated_option") {
    out.Q4 = "CONSIDERATION_SET";
    return out;
  }

  out.Q4 = "DIRECT_ENDORSEMENT";

  if (r === "explicit_recommendation") {
    out.Q5 = "NO_MEANINGFUL_ORDER";
    return out;
  }

  out.Q5 = "MEANINGFUL_ORDER";

  if (r === "first_recommendation") {
    out.Q6 = "LEAD";
    return out;
  }
  if (r === "ranked_recommendation") {
    out.Q6 = "NON_LEAD";
    return out;
  }

  return out;
}

export const NODE_GROUND_TRUTH_NOT_DERIVABLE = "NODE_GROUND_TRUTH_NOT_DERIVABLE";
