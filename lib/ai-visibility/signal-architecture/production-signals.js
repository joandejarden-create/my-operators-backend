/**
 * Production AI Visibility signal/flag architecture (Candidate E — adopted).
 * Independent signals; no composite score; no forced mutually exclusive role.
 * Internal 10-class recommendationStatus remains for audit/research only.
 */

import { POSITIVE_RECOMMENDATION_ROLES } from "../metrics.js";

export const SIGNAL_ARCHITECTURE_VERSION = "ai_intelligence_signal_architecture_v1";
export const SIGNAL_ARCHITECTURE_ADOPTION = "ADOPTED";

/** Production signal identifiers (client contract). */
export const PRODUCTION_SIGNALS = Object.freeze({
  AI_SIGNAL_PRESENCE: "AI_SIGNAL_PRESENCE",
  AI_SIGNAL_RECOMMENDED: "AI_SIGNAL_RECOMMENDED",
  AI_SIGNAL_FIRST_RECOMMENDATION: "AI_SIGNAL_FIRST_RECOMMENDATION",
  AI_SIGNAL_NEGATIVE_OR_QUALIFIED: "AI_SIGNAL_NEGATIVE_OR_QUALIFIED",
  AI_SIGNAL_COMPARATOR: "AI_SIGNAL_COMPARATOR",
});

export const PRODUCTION_SIGNAL_IDS = Object.freeze(Object.values(PRODUCTION_SIGNALS));

/** Short keys used in gates / scorecards. */
export const SIGNAL_KEYS = Object.freeze({
  PRESENCE: "PRESENCE",
  RECOMMENDED: "RECOMMENDED",
  FIRST_RECOMMENDATION: "FIRST_RECOMMENDATION",
  NEGATIVE_OR_QUALIFIED: "NEGATIVE_OR_QUALIFIED",
  COMPARATOR: "COMPARATOR",
});

export const SIGNAL_KEY_TO_ID = Object.freeze({
  PRESENCE: PRODUCTION_SIGNALS.AI_SIGNAL_PRESENCE,
  RECOMMENDED: PRODUCTION_SIGNALS.AI_SIGNAL_RECOMMENDED,
  FIRST_RECOMMENDATION: PRODUCTION_SIGNALS.AI_SIGNAL_FIRST_RECOMMENDATION,
  NEGATIVE_OR_QUALIFIED: PRODUCTION_SIGNALS.AI_SIGNAL_NEGATIVE_OR_QUALIFIED,
  COMPARATOR: PRODUCTION_SIGNALS.AI_SIGNAL_COMPARATOR,
});

export const SIGNAL_GATE_IDS = Object.freeze({
  PRESENCE_GATE: "PRESENCE_GATE",
  RECOMMENDED_GATE: "RECOMMENDED_GATE",
  FIRST_REC_GATE: "FIRST_REC_GATE",
  NEGATIVE_GATE: "NEGATIVE_GATE",
  COMPARATOR_GATE: "COMPARATOR_GATE",
});

/** Locked product definitions. */
export const SIGNAL_DEFINITIONS = Object.freeze({
  PRESENCE: Object.freeze({
    id: PRODUCTION_SIGNALS.AI_SIGNAL_PRESENCE,
    key: SIGNAL_KEYS.PRESENCE,
    gate: SIGNAL_GATE_IDS.PRESENCE_GATE,
    trueWhen:
      "the canonical entity appears in the eligible AI response",
    falseWhen: "the canonical entity does not appear",
    notes: "Derived from entity resolution / presence, not from recommendation role.",
  }),
  RECOMMENDED: Object.freeze({
    id: PRODUCTION_SIGNALS.AI_SIGNAL_RECOMMENDED,
    key: SIGNAL_KEYS.RECOMMENDED,
    gate: SIGNAL_GATE_IDS.RECOMMENDED_GATE,
    trueWhen:
      "the response affirmatively places the canonical entity into the actionable decision/consideration set for the user's stated hotel decision (explicit recommendation, shortlist, option-to-consider, ranked/table/list membership, or qualified affirmative)",
    falseWhen:
      "entity is merely present as description, market context, comparator, parent/sibling context, historical example, negative exclusion, source-only, or unrelated passing mention",
    presenceRequired: true,
    notes: Object.freeze([
      "Locked definition: ai_signal_recommended_definition_lock_v1",
      "Not limited to the verb 'recommend'.",
      "Old associated_option must be split: AFFIRMATIVE_CONSIDERATION_OPTION → TRUE; CONTEXTUAL_ASSOCIATED_ENTITY → FALSE.",
      "Internal 10-class remains research-only; binary production signal is independent.",
      "Classifier remediation may lag this contract — do not publish until gate PASS.",
    ]),
  }),
  FIRST_RECOMMENDATION: Object.freeze({
    id: PRODUCTION_SIGNALS.AI_SIGNAL_FIRST_RECOMMENDATION,
    key: SIGNAL_KEYS.FIRST_RECOMMENDATION,
    gate: SIGNAL_GATE_IDS.FIRST_REC_GATE,
    trueWhen: "strict first/lead recommendation evidence exists",
    doNotInferFrom: Object.freeze([
      "first textual mention",
      "ordinary numbered list",
      "consideration-set position",
      "document order alone",
    ]),
  }),
  NEGATIVE_OR_QUALIFIED: Object.freeze({
    id: PRODUCTION_SIGNALS.AI_SIGNAL_NEGATIVE_OR_QUALIFIED,
    key: SIGNAL_KEYS.NEGATIVE_OR_QUALIFIED,
    gate: SIGNAL_GATE_IDS.NEGATIVE_GATE,
    trueWhen:
      "entity is materially discouraged, excluded, or negatively qualified",
  }),
  COMPARATOR: Object.freeze({
    id: PRODUCTION_SIGNALS.AI_SIGNAL_COMPARATOR,
    key: SIGNAL_KEYS.COMPARATOR,
    gate: SIGNAL_GATE_IDS.COMPARATOR_GATE,
    trueWhen: "entity is principally used as a comparison/reference",
  }),
});

/**
 * Required fields on every production signal observation payload.
 * No composite score. Signals may coexist where logically valid.
 */
export const SIGNAL_PAYLOAD_FIELDS = Object.freeze([
  "value",
  "evidenceRefs",
  "validationStatus",
  "classifierVersion",
  "sourceResponseId",
  "provider",
  "language",
  "geography",
]);

const POSITIVE_SET = new Set(POSITIVE_RECOMMENDATION_ROLES);

/**
 * Build an empty signal payload shell (governance contract).
 * @param {object} partial
 */
export function buildSignalPayload(partial = {}) {
  const out = {
    value: partial.value ?? null,
    evidenceRefs: Array.isArray(partial.evidenceRefs) ? [...partial.evidenceRefs] : [],
    validationStatus: partial.validationStatus ?? null,
    classifierVersion: partial.classifierVersion ?? null,
    sourceResponseId: partial.sourceResponseId ?? null,
    provider: partial.provider ?? null,
    language: partial.language ?? null,
    geography: partial.geography ?? null,
  };
  for (const k of SIGNAL_PAYLOAD_FIELDS) {
    if (!(k in out)) out[k] = null;
  }
  return out;
}

/**
 * Derive production signal booleans from an internal 10-class role.
 * Does not replace evidence-based classifiers — mapping contract for audit/tests.
 * @param {string|null|undefined} internalRole
 */
export function deriveProductionSignalsFromInternalRole(internalRole) {
  const role = internalRole == null ? "no_mention" : String(internalRole);
  const presence = role !== "no_mention";
  const recommended = POSITIVE_SET.has(role);
  const first = role === "first_recommendation";
  return {
    [SIGNAL_KEYS.PRESENCE]: presence,
    [SIGNAL_KEYS.RECOMMENDED]: recommended,
    [SIGNAL_KEYS.FIRST_RECOMMENDATION]: first,
    [SIGNAL_KEYS.NEGATIVE_OR_QUALIFIED]: role === "negative_or_qualified",
    [SIGNAL_KEYS.COMPARATOR]: role === "comparator",
    coexistenceValid: {
      firstImpliesRecommended: !first || recommended,
      recommendedDoesNotRequireFirst: true,
      associatedNotRecommended: role !== "associated_option" || !recommended,
    },
  };
}

/**
 * Validate that FIRST ⇒ RECOMMENDED and associated never counts as recommended.
 * @param {{ PRESENCE?: boolean, RECOMMENDED?: boolean, FIRST_RECOMMENDATION?: boolean }} signals
 * @param {string} [internalRole]
 */
export function assertSignalConsistency(signals = {}, internalRole) {
  const errors = [];
  if (signals.FIRST_RECOMMENDATION === true && signals.RECOMMENDED !== true) {
    errors.push("FIRST_IS_RECOMMENDED violated");
  }
  if (internalRole === "associated_option" && signals.RECOMMENDED === true) {
    errors.push("ASSOCIATED_NOT_RECOMMENDED violated");
  }
  if (internalRole === "discussed" && signals.RECOMMENDED === true) {
    errors.push("DISCUSSION_NOT_RECOMMENDED violated");
  }
  if (
    ["first_recommendation", "ranked_recommendation", "explicit_recommendation"].includes(
      internalRole
    ) &&
    signals.RECOMMENDED !== true
  ) {
    errors.push(`${String(internalRole).toUpperCase()}_IS_RECOMMENDED violated`);
  }
  return { ok: errors.length === 0, errors };
}
