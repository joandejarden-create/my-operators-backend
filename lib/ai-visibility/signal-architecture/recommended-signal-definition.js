/**
 * AI_SIGNAL_RECOMMENDED — locked production semantic definition (study + contract).
 *
 * Classification rules are NOT changed here. This module documents the product
 * question and study helpers for binary re-adjudication.
 */

export const RECOMMENDED_DEFINITION_LOCK_VERSION =
  "ai_signal_recommended_definition_lock_v1";

export const RECOMMENDED_PRODUCT_QUESTION =
  "Does the response affirmatively place this specific canonical entity into the decision set for the user's stated hotel decision?";

/** Locked TRUE / FALSE contract (product). */
export const RECOMMENDED_DEFINITION_LOCK = Object.freeze({
  version: RECOMMENDED_DEFINITION_LOCK_VERSION,
  question: RECOMMENDED_PRODUCT_QUESTION,
  presenceRequired: true,
  countsAsRecommended: Object.freeze({
    explicitRecommendation: true,
    shortlist: true,
    optionToConsider: true,
    relevantChoiceList: true,
    rankedRecommendationList: true,
    recommendationTableOrList: true,
    qualifiedAffirmative: true,
  }),
  doesNotCount: Object.freeze({
    descriptiveMention: true,
    marketContextOnly: true,
    comparatorOnly: true,
    parentSiblingContextOnly: true,
    historicalOrExampleOnly: true,
    negativeExclusion: true,
    sourceCitationOnly: true,
    passingMentionUnrelated: true,
  }),
  notes: Object.freeze([
    "RECOMMENDED is about inclusion in the actionable consideration/decision set — not limited to the verb 'recommend'.",
    "PROMPT_INTENT + RESPONSE_STRUCTURE + ENTITY_ROLE must be considered together.",
    "FIRST_RECOMMENDATION remains a separate blocked signal.",
    "NEGATIVE_OR_QUALIFIED remains a separate future signal; qualified affirmative can still be Recommended TRUE.",
    "associated_option must be split: AFFIRMATIVE_CONSIDERATION_OPTION vs CONTEXTUAL_ASSOCIATED_ENTITY.",
  ]),
  multiEntityHandling: Object.freeze({
    implementNow: false,
    rule:
      "Every named entity in a coordinated recommendation/consideration list independently receives RECOMMENDED=TRUE when the recommendation scope applies to the whole list (comma lists, bullets, tables, slash lists, parenthetical lists, coordinated conjunctions). Do not require repeated verbs.",
  }),
  tableListInheritance: Object.freeze({
    implementNow: false,
    rule:
      "Recommendation framing at section/column/heading level (Recommended Brands, brands to consider, shortlist, best options, etc.) may inherit to each row/item even without a per-row verb.",
  }),
  futureRecommendationShare: Object.freeze({
    enabled: false,
    formula:
      "comparable governed responses where AI_SIGNAL_RECOMMENDED=TRUE / comparable governed responses",
    blockedUntil: "Recommended certification",
  }),
});

/** Prompt families that ask for brand/operator decision options. */
export const BRAND_DECISION_PROMPT_FAMILIES = Object.freeze([
  "Brand Selection",
  "Conversion",
  "Soft Brand / Collection",
  "Soft Brand",
  "Lifestyle",
  "Upper-Upscale",
  "Owner Flexibility",
  "Branded Residences",
  "Development Strategy",
  "Affiliation Flexibility",
]);

const AFFIRMATIVE_DECISION_RE =
  /\b(recommend(?:ed|ing)?|recomiend|shortlist|lista\s+corta|brands?\s+to\s+consider|options?\s+to\s+consider|options?\s+include|relevant\s+options?|best\s+options?|strong\s+(?:fit|candidate|option|options|choice)|should\s+consider|may\s+(?:also\s+)?consider|worth\s+considering|commonly\s+considered|brands?\s+commonly\s+considered|consider(?:ation)?\s+set|top\s+pick|primary\s+(?:option|candidate)|suitable\s+brands?|marcas?\s+a\s+considerar|opciones\s+(?:recomendadas|incluyen)|issue\s+an\s+rfp\s+to|solicit\s+proposals?)\b/i;

const CONTEXTUAL_ASSOCIATED_RE =
  /\b(commonly\s+associated(?:\s+with)?|often\s+associated(?:\s+with)?|associated\s+with\s+(?:branded|the)|brands?\s+commonly\s+associated)\b/i;

const NEGATIVE_EXCLUSION_RE =
  /\b(not\s+recommend(?:ed)?|would\s+not\s+(?:be\s+)?suitable|less\s+suitable|weaker\s+fit|avoid|poor\s+fit|not\s+ideal|too\s+restrictive|would\s+not\s+fit|no\s+recomend|poco\s+adecuado|evitar)\b/i;

const DESCRIPTIVE_CONTEXT_RE =
  /\b(operates?|operated|launched|has\s+a\s+presence|offers?|is\s+part\s+of|footprint|portfolio|collection\s+of|alliance|history|expanded|market\s+participants?|background|brand\s+overview|including\s+brands?\s+such\s+as)\b/i;

const COMPARATOR_RE =
  /\b(competes?\s+with|compared\s+(?:to|with)|versus|vs\.?|alternative\s+to|unlike|similar\s+to)\b/i;

const PARENT_SIBLING_RE =
  /\b((?:marriott|hilton|ihg|hyatt|wyndham|accor)\s+(?:operates?|brands?|portfolio)|brands?\s+including|family\s+of\s+brands?)\b/i;

const LIST_STRUCTURE_RE =
  /(^\s*[-*•]\s+|\b\d+[\).]\s+|,\s+and\s+|\s+\/\s+|\|.+\|)/im;

const QUALIFIED_AFFIRMATIVE_RE =
  /\b(could\s+be\s+a\s+strong\s+option|good\s+option\s+if|could\s+work\s+(?:well\s+)?(?:if|where)|consider\s+\w+\s+if|strong\s+option\s+if|if\s+.{0,40}priority)\b/i;

/**
 * @param {string|null|undefined} family
 */
export function isBrandDecisionPromptFamily(family) {
  const f = String(family || "").trim();
  if (!f) return null;
  return BRAND_DECISION_PROMPT_FAMILIES.some(
    (x) => f.toLowerCase() === x.toLowerCase() || f.toLowerCase().includes(x.toLowerCase())
  );
}

/**
 * Split rule for old associated_option population.
 * On brand-decision prompts, associated_option defaults to affirmative consideration
 * unless clear contextual-only cues dominate.
 * @returns {'AFFIRMATIVE_CONSIDERATION_OPTION'|'CONTEXTUAL_ASSOCIATED_ENTITY'|'AMBIGUOUS'}
 */
export function classifyAssociatedOptionPopulation({
  snippet = "",
  promptFamily = null,
  text = "",
} = {}) {
  const local = `${snippet}\n${String(text || "").slice(0, 400)}`;
  const decisionPrompt = isBrandDecisionPromptFamily(promptFamily);

  const contextualOnly =
    CONTEXTUAL_ASSOCIATED_RE.test(local) ||
    (COMPARATOR_RE.test(local) && !AFFIRMATIVE_DECISION_RE.test(local)) ||
    (PARENT_SIBLING_RE.test(local) && !AFFIRMATIVE_DECISION_RE.test(local)) ||
    (NEGATIVE_EXCLUSION_RE.test(local) && !QUALIFIED_AFFIRMATIVE_RE.test(local)) ||
    (DESCRIPTIVE_CONTEXT_RE.test(local) &&
      !AFFIRMATIVE_DECISION_RE.test(local) &&
      decisionPrompt === false);

  if (contextualOnly) return "CONTEXTUAL_ASSOCIATED_ENTITY";

  const affirmativeCue =
    AFFIRMATIVE_DECISION_RE.test(local) || QUALIFIED_AFFIRMATIVE_RE.test(local);
  const structuredListCue =
    /(^\s*[-*•]\s+\*?[A-ZÀ-Ö]|\b\d+[\).]\s+\*?[A-ZÀ-Ö]|recommended\s+brands?|brands?\s+to\s+consider|shortlist:)/im.test(
      `${snippet}\n${local}`
    );

  if (affirmativeCue || structuredListCue) {
    return "AFFIRMATIVE_CONSIDERATION_OPTION";
  }

  // Locked product semantics: on brand-decision questions, associated_option
  // usually means affirmative consideration-set membership.
  if (decisionPrompt === true) {
    return "AFFIRMATIVE_CONSIDERATION_OPTION";
  }

  if (decisionPrompt === false) {
    return "CONTEXTUAL_ASSOCIATED_ENTITY";
  }

  return "AMBIGUOUS";
}

/**
 * Independent study label for discussed / descriptive vs decision-set.
 * Conservative: human `discussed` defaults descriptive unless strong decision-set cues.
 * @returns {'AFFIRMATIVE_DECISION_SET'|'DESCRIPTIVE_DISCUSSION'|'AMBIGUOUS'}
 */
export function classifyDiscussedPopulation({
  snippet = "",
  promptFamily = null,
  humanRole = null,
} = {}) {
  const local = String(snippet || "");
  if (NEGATIVE_EXCLUSION_RE.test(local)) return "DESCRIPTIVE_DISCUSSION";
  if (COMPARATOR_RE.test(local) && !AFFIRMATIVE_DECISION_RE.test(local)) {
    return "DESCRIPTIVE_DISCUSSION";
  }

  const strongAffirm =
    AFFIRMATIVE_DECISION_RE.test(local) || QUALIFIED_AFFIRMATIVE_RE.test(local);

  // Human already labelled a positive recommendation role (FN audit path)
  if (
    ["first_recommendation", "ranked_recommendation", "explicit_recommendation"].includes(
      humanRole
    )
  ) {
    if (strongAffirm) return "AFFIRMATIVE_DECISION_SET";
    if (DESCRIPTIVE_CONTEXT_RE.test(local) && !strongAffirm) return "AMBIGUOUS";
    // Positive human role remains strong prior for this audit slice
    return "AFFIRMATIVE_DECISION_SET";
  }

  // Human role = discussed: require strong affirmative language (not bare commas/lists)
  if (humanRole === "discussed") {
    if (strongAffirm && isBrandDecisionPromptFamily(promptFamily) !== false) {
      return "AMBIGUOUS"; // possible missed recommendation — queue for human review
    }
    return "DESCRIPTIVE_DISCUSSION";
  }

  if (strongAffirm) return "AFFIRMATIVE_DECISION_SET";
  if (DESCRIPTIVE_CONTEXT_RE.test(local)) return "DESCRIPTIVE_DISCUSSION";
  return "AMBIGUOUS";
}

/**
 * Propose locked binary Recommended label for a DEV case (study only).
 * @returns {{ proposed: boolean|null, ambiguous: boolean, reason: string, associatedSplit?: string, discussedClass?: string }}
 */
export function proposeLockedRecommendedBinary({
  humanRole,
  promptFamily = null,
  snippet = "",
  text = "",
  entityPresent = true,
} = {}) {
  if (entityPresent === false || humanRole === "no_mention") {
    return {
      proposed: false,
      ambiguous: false,
      reason: "presence_false_or_no_mention",
    };
  }

  const role = String(humanRole || "");

  if (role === "negative_or_qualified") {
    // Negative exclusion → Recommended FALSE; qualified affirmative may be mis-bucketed in 10-class
    if (QUALIFIED_AFFIRMATIVE_RE.test(snippet) && !NEGATIVE_EXCLUSION_RE.test(snippet)) {
      return {
        proposed: true,
        ambiguous: true,
        reason: "negative_or_qualified_role_but_qualified_affirmative_language",
      };
    }
    return {
      proposed: false,
      ambiguous: false,
      reason: "negative_exclusion_or_qualified_negative",
    };
  }

  if (role === "comparator") {
    if (AFFIRMATIVE_DECISION_RE.test(snippet)) {
      return {
        proposed: null,
        ambiguous: true,
        reason: "comparator_role_with_affirmative_decision_language",
      };
    }
    return { proposed: false, ambiguous: false, reason: "comparator_only" };
  }

  if (role === "source_only" || role === "passing_mention") {
    if (
      AFFIRMATIVE_DECISION_RE.test(snippet) ||
      LIST_STRUCTURE_RE.test(snippet)
    ) {
      return {
        proposed: null,
        ambiguous: true,
        reason: `${role}_with_possible_decision_set_structure`,
      };
    }
    return { proposed: false, ambiguous: false, reason: role };
  }

  if (
    role === "first_recommendation" ||
    role === "ranked_recommendation" ||
    role === "explicit_recommendation"
  ) {
    if (NEGATIVE_EXCLUSION_RE.test(snippet) && !AFFIRMATIVE_DECISION_RE.test(snippet)) {
      return {
        proposed: null,
        ambiguous: true,
        reason: "positive_human_role_but_negative_exclusion_language",
      };
    }
    return {
      proposed: true,
      ambiguous: false,
      reason: `human_positive_role_${role}`,
    };
  }

  if (role === "associated_option") {
    const split = classifyAssociatedOptionPopulation({
      snippet,
      promptFamily,
      text,
    });
    if (split === "AFFIRMATIVE_CONSIDERATION_OPTION") {
      return {
        proposed: true,
        ambiguous: false,
        reason: "associated_option_affirmative_consideration",
        associatedSplit: split,
      };
    }
    if (split === "CONTEXTUAL_ASSOCIATED_ENTITY") {
      return {
        proposed: false,
        ambiguous: false,
        reason: "associated_option_contextual_only",
        associatedSplit: split,
      };
    }
    return {
      proposed: null,
      ambiguous: true,
      reason: "associated_option_ambiguous_split",
      associatedSplit: split,
    };
  }

  if (role === "discussed") {
    const cls = classifyDiscussedPopulation({
      snippet,
      promptFamily,
      humanRole: role,
    });
    if (cls === "AFFIRMATIVE_DECISION_SET") {
      return {
        proposed: true,
        ambiguous: false,
        reason: "discussed_affirmative_decision_set",
        discussedClass: cls,
      };
    }
    if (cls === "DESCRIPTIVE_DISCUSSION") {
      return {
        proposed: false,
        ambiguous: false,
        reason: "discussed_descriptive",
        discussedClass: cls,
      };
    }
    return {
      proposed: null,
      ambiguous: true,
      reason: "discussed_possible_missed_recommendation",
      discussedClass: cls,
    };
  }

  return {
    proposed: null,
    ambiguous: true,
    reason: `unhandled_role_${role || "null"}`,
  };
}
