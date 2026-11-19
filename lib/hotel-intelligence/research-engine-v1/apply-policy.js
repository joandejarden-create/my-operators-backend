/**
 * Dry-run vs apply separation. P1: Airtable apply always disabled.
 */

import { FAILURE_CLASS, P1_POLICY, writeGuaranteesP1 } from "./contract.js";

export const APPLY_MODE = Object.freeze({
  DRY_RUN: "DRY_RUN",
  REVIEW_REQUIRED: "REVIEW_REQUIRED",
  APPLY: "APPLY",
});

/**
 * Resolve apply mode. P1 forces non-apply even if caller requests APPLY.
 */
export function resolveApplyMode(requested = APPLY_MODE.DRY_RUN, opts = {}) {
  const req = String(requested || APPLY_MODE.DRY_RUN).toUpperCase();
  if (P1_POLICY.airtable_apply_enabled !== true || opts.forceDisableApply !== false) {
    if (req === APPLY_MODE.APPLY) {
      return {
        ok: false,
        mode: APPLY_MODE.DRY_RUN,
        error: FAILURE_CLASS.APPLY_DISABLED,
        message: "Airtable apply disabled in P1 foundation",
        write_guarantees: writeGuaranteesP1(),
      };
    }
  }
  if (req === APPLY_MODE.APPLY && opts.founderAuth !== true) {
    return {
      ok: false,
      mode: APPLY_MODE.REVIEW_REQUIRED,
      error: FAILURE_CLASS.APPLY_DISABLED,
      message: "founder_auth_required_for_apply",
      write_guarantees: writeGuaranteesP1(),
    };
  }
  return {
    ok: true,
    mode: Object.values(APPLY_MODE).includes(req) ? req : APPLY_MODE.DRY_RUN,
    write_guarantees: writeGuaranteesP1(),
  };
}

export function assertNoAirtableMutations(sideEffects = []) {
  const writes = (sideEffects || []).filter(
    (s) => s && (s.type === "airtable_write" || s.airtable_write === true)
  );
  return {
    ok: writes.length === 0,
    writes,
    error: writes.length ? FAILURE_CLASS.APPLY_DISABLED : null,
  };
}
