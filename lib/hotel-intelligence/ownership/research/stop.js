/**
 * Ownership research stop conditions.
 */

export const OWNERSHIP_STOP_VERSION = "ownership-stop-v1";

export const STOP_REASONS = Object.freeze({
  EXISTING_STRONG: "existing_verified_or_high_owned_by",
  STAGE_B_HIGH: "stage_b_high_owned_by",
  AGREEMENT: "multiple_strong_sources_agree",
  CONFLICT: "material_ownership_conflict",
  STAGE_C_DONE: "stage_c_complete",
  STAGE_C_DISABLED: "stage_c_serpapi_unavailable_or_disabled",
  EXHAUSTED: "bounded_research_exhausted",
  UNKNOWN: "no_reliable_ownership_evidence",
});

/**
 * Detect OWNED_BY conflicts among staged/current relationships.
 * @param {object[]} relationships
 */
export function detectOwnedByConflict(relationships) {
  const owned = (relationships || []).filter(
    (r) =>
      r.relationship_type === "OWNED_BY" &&
      r.is_current !== false &&
      r.verification_status !== "unknown"
  );
  const entityIds = [...new Set(owned.map((r) => r.object_entity_id))];
  if (entityIds.length <= 1) {
    return { conflict: false, entity_ids: entityIds };
  }
  const strong = owned.filter((r) =>
    ["verified", "high", "probable"].includes(r.verification_status)
  );
  const strongIds = [...new Set(strong.map((r) => r.object_entity_id))];
  if (strongIds.length > 1) {
    return { conflict: true, entity_ids: strongIds };
  }
  return { conflict: false, entity_ids: entityIds };
}

/**
 * @param {object} state
 */
export function decideStop(state = {}) {
  if (state.force_stop_reason) {
    return { stop: true, reason: state.force_stop_reason };
  }
  if (state.conflict) {
    return { stop: true, reason: STOP_REASONS.CONFLICT };
  }
  if (state.existing_strong) {
    return { stop: true, reason: STOP_REASONS.EXISTING_STRONG };
  }
  if (state.agreement) {
    return { stop: true, reason: STOP_REASONS.AGREEMENT };
  }
  if (state.stage_b_high) {
    return { stop: true, reason: STOP_REASONS.STAGE_B_HIGH };
  }
  if (state.stage_c_done) {
    const hasOwned = Boolean(state.has_owned_by);
    return {
      stop: true,
      reason: hasOwned ? STOP_REASONS.STAGE_C_DONE : STOP_REASONS.UNKNOWN,
    };
  }
  return { stop: false, reason: null };
}
