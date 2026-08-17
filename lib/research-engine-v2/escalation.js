/**
 * External research escalation — recommendations only; never auto-calls Webhound.
 */

export const ESCALATION_TYPES = Object.freeze([
  "opaque_ownership",
  "bot_blocked_official",
  "government_project",
  "unclear_affiliation_conflict",
  "long_tail_no_directory",
  "conflicting_high_authority",
  "parser_failure",
  "missing_identity_codes",
]);

export const ESCALATION_ACTIONS = Object.freeze([
  "Native retry",
  "Human review",
  "Webhound candidate",
  "Specialist registry/data source",
  "Manual source retrieval",
]);

/**
 * @param {object} ctx
 */
export function recommendEscalation(ctx = {}) {
  const reasons = [];
  /** @type {string[]} */
  const actions = [];

  if (ctx.sourceState === "Blocked" || ctx.sourceState === "Failed") {
    reasons.push("bot_blocked_official");
    actions.push("Native retry", "Manual source retrieval", "Webhound candidate");
  }
  if (ctx.opaqueOwnership) {
    reasons.push("opaque_ownership");
    actions.push("Specialist registry/data source", "Webhound candidate");
  }
  if (ctx.governmentProject) {
    reasons.push("government_project");
    actions.push("Webhound candidate", "Manual source retrieval");
  }
  if (ctx.affiliationConflict) {
    reasons.push("unclear_affiliation_conflict");
    actions.push("Human review", "Native retry");
  }
  if (ctx.longTailNoDirectory) {
    reasons.push("long_tail_no_directory");
    actions.push("Webhound candidate", "Human review");
  }
  if (ctx.conflictingHighAuthority) {
    reasons.push("conflicting_high_authority");
    actions.push("Human review", "Webhound candidate");
  }
  if (ctx.missingIdentityCodes) {
    reasons.push("missing_identity_codes");
    actions.push("Native retry", "Human review");
  }

  const uniqueActions = [...new Set(actions)];
  return {
    needsEscalation: reasons.length > 0,
    types: [...new Set(reasons)],
    recommendedActions: uniqueActions.length ? uniqueActions : ["Human review"],
    webhoundAuthorized: false,
    note: "Webhound must remain explicitly authorized — never auto-called",
    entityId: ctx.entityId || null,
    entityName: ctx.entityName || null,
  };
}
