/**
 * Blocked recommendation / preference / win-loss signals — moat guardrail registry.
 * Do not rename these to sneak blocked metrics into customer payloads.
 */

export const BLOCKED_SIGNAL_REGISTRY_VERSION = "dealality_blocked_signals_v1";

/** Signals blocked from client promotion until separately certified. */
export const BLOCKED_CLIENT_SIGNALS = Object.freeze([
  "RECOMMENDATION_RATE",
  "RECOMMENDATION_SHARE",
  "FIRST_RECOMMENDATION",
  "FIRST_CHOICE",
  "QUESTIONS_WON",
  "WIN_RATE",
  "WIN_LOSS",
  "HEAD_TO_HEAD_WINNER",
  "DISPLACEMENT_WIN",
  "RECOMMENDATION_POSITION",
  "AVERAGE_RECOMMENDATION_POSITION",
  "PREFERENCE_SCORE",
  "AI_PREFERENCE_INDEX",
  "OPERATOR_DECISION_SHARE_OF_VOICE",
  "OWNER_DECISION_SHARE_OF_VOICE",
]);

/** Canonical fields that must never appear in production observation records. */
export const BLOCKED_CANONICAL_FIELDS = Object.freeze([
  "Recommended",
  "NotRecommended",
  "Winner",
  "Loser",
  "Preferred",
  "Displaced",
  "FirstChoice",
  "RecommendationRank",
  "winCount",
  "lossCount",
  "recommendationFrequency",
  "displacementFrequency",
]);

/** Payload field patterns that leak methodology if exposed to customers. */
export const INTERNAL_ONLY_FIELD_PATTERNS = Object.freeze([
  /^rawScore$/i,
  /^benchmarkMembers$/i,
  /^allCompetitorScores$/i,
  /^promptTextFullCorpus$/i,
  /^mutationRule$/i,
  /^classifierThreshold$/i,
  /^normalizationRule$/i,
  /^researchRecommendation$/i,
  /^fullObservationLedger$/i,
  /^promptGenerationRules$/i,
  /^cohortSelectionRules$/i,
  /^methodologyWeights$/i,
]);

export const INDEX_STATUS = Object.freeze({
  AI_PRESENCE_INDEX: "ONLY_V1_INDEX_CANDIDATE",
  AI_CONSIDERATION_INDEX: "BLOCKED_PENDING_VALIDATED_CONSIDERATION_MEASUREMENT",
  AI_PREFERENCE_INDEX: "BLOCKED_PENDING_VALIDATED_PREFERENCE_SIGNAL",
  AI_REPRESENTATION_INDEX: "RESEARCH_DESIGN_ONLY",
});

export const PROPRIETARY_RAW_SCORE_STATUS = "NOT_REQUIRED_YET";

/**
 * @param {object} payload
 * @returns {{ ok: boolean, violations: string[] }}
 */
export function auditCustomerPayloadForBlockedSignals(payload = {}) {
  const violations = [];
  const stack = [{ path: "", value: payload }];
  while (stack.length) {
    const { path: p, value } = stack.pop();
    if (value == null || typeof value !== "object") continue;
    if (Array.isArray(value)) {
      value.forEach((v, i) => stack.push({ path: `${p}[${i}]`, value: v }));
      continue;
    }
    for (const [key, val] of Object.entries(value)) {
      const fullPath = p ? `${p}.${key}` : key;
      const upper = key.toUpperCase();
      if (BLOCKED_CLIENT_SIGNALS.includes(upper) || BLOCKED_CLIENT_SIGNALS.includes(key)) {
        violations.push(`blocked_signal:${fullPath}`);
      }
      if (BLOCKED_CANONICAL_FIELDS.includes(key)) {
        violations.push(`blocked_canonical_field:${fullPath}`);
      }
      for (const pattern of INTERNAL_ONLY_FIELD_PATTERNS) {
        if (pattern.test(key)) {
          violations.push(`internal_only_field:${fullPath}`);
        }
      }
      if (val != null && typeof val === "object") {
        stack.push({ path: fullPath, value: val });
      }
    }
  }
  return { ok: violations.length === 0, violations };
}
