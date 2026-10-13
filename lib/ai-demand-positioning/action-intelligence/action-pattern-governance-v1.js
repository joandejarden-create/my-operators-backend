/**
 * Human governance for ADP action patterns.
 * LLM may propose DISCOVERED/DRAFT patterns; promotion requires founder/product approval.
 */

import { ACTION_PATTERN_LIFECYCLE } from "./action-executability-contract-v1.js";

export const ACTION_PATTERN_GOVERNANCE_VERSION = "ADP_ACTION_PATTERN_GOVERNANCE_V1";

export const GOVERNANCE_RULES = Object.freeze({
  llmMayPropose: true,
  llmMayAutoPromoteToGoverned: false,
  requiredApprover: "founder_or_product",
  lifecycleOrder: [
    ACTION_PATTERN_LIFECYCLE.DISCOVERED,
    ACTION_PATTERN_LIFECYCLE.DRAFT,
    ACTION_PATTERN_LIFECYCLE.REVIEWED,
    ACTION_PATTERN_LIFECYCLE.GOVERNED,
    ACTION_PATTERN_LIFECYCLE.REUSABLE,
  ],
  startSmall: true,
  note: "New hotels create evidence-backed patterns. Do not invent a large generic library.",
});

export function assertPatternPromotionAllowed({ fromLifecycle, toLifecycle, approvedByHuman }) {
  if (
    toLifecycle === ACTION_PATTERN_LIFECYCLE.GOVERNED ||
    toLifecycle === ACTION_PATTERN_LIFECYCLE.REUSABLE
  ) {
    if (!approvedByHuman) {
      return {
        ok: false,
        reason: "Founder/product approval required before GOVERNED/REUSABLE promotion.",
      };
    }
  }
  return { ok: true, fromLifecycle, toLifecycle };
}
