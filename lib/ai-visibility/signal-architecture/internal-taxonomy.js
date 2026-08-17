/**
 * Internal 10-class recommendation taxonomy — preserved, not client contract.
 * Old multiclass production gate retired → INTERNAL_RESEARCH_VALIDATION.
 */

export const INTERNAL_TAXONOMY_VERSION = "ai_visibility_recommendation_status_10_class_v1";

export const INTERNAL_TAXONOMY_STATUS = Object.freeze({
  RESEARCH_ONLY_NOT_PRODUCTION_CONTRACT: "RESEARCH_ONLY / NOT_PRODUCTION_CONTRACT",
});

/** Canonical internal roles (immutable vocabulary for audit/GT). */
export const INTERNAL_RECOMMENDATION_ROLES = Object.freeze([
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

export const OLD_10_CLASS_PRODUCTION_GATE = Object.freeze({
  retiredAsProductionReleaseControl: true,
  preservedAs: "INTERNAL_RESEARCH_VALIDATION",
  historicalMetricsPreserved: true,
  note:
    "Do not use multiclass recommendation accuracy as the client release gate. Production gates are per-signal: PRESENCE / RECOMMENDED / FIRST_REC / NEGATIVE / COMPARATOR.",
});

/**
 * Mark classification-threshold recommendation multiclass checks as research-only.
 */
export function classifyOld10ClassGateRole() {
  return {
    PRODUCTION_CONTRACT: false,
    INTERNAL_RESEARCH_ONLY: true,
    status: INTERNAL_TAXONOMY_STATUS.RESEARCH_ONLY_NOT_PRODUCTION_CONTRACT,
    gate: OLD_10_CLASS_PRODUCTION_GATE,
    rolesPreserved: [...INTERNAL_RECOMMENDATION_ROLES],
  };
}
