/**
 * Future Existing Hotel ADP period pipeline (governed order).
 * Orchestration helpers — does not create periods or call LLMs.
 */

import {
  buildFullCompetitiveRankingSnapshot,
  finalizeRankSnapshot,
  saveCompetitiveHistorySnapshot,
} from "../competitive-history/rank-history-ledger-v1.js";

export const ADP_PERIOD_PIPELINE_VERSION = "adp_existing_hotel_period_pipeline_v1";

export const ADP_PERIOD_PIPELINE_STAGES = Object.freeze([
  "RAW_OBSERVATIONS",
  "GOVERNED_INTERPRETATION",
  "PROVIDER_COVERAGE_RECOVERY",
  "MEASUREMENT_ASSURANCE",
  "MISSING_EVIDENCE_INDEX",
  "POSITIVE_EVIDENCE_INDEX",
  "COMPETITIVE_RANKING",
  "PRIOR_COMPARABLE_PERIOD_MATCH",
  "RANK_MOVEMENT_HISTORY",
  "ANALYTICAL_COHERENCE",
  "PUBLICATION_INTEGRITY",
  "CUSTOMER_PUBLICATION",
]);

export const PERMANENT_DEFECT_CLASSES = Object.freeze([
  "MISSING_EVIDENCE_CONTEXT_MISMATCH",
  "POSITIVE_EVIDENCE_CONTEXT_MISMATCH",
  "PROPRIETARY_PROMPT_LEAKAGE",
  "NON_DETERMINISTIC_EVIDENCE_SAMPLING",
  "INCORRECT_RANK_DELTA",
  "FALSE_NEW_TO_RANKING",
  "FALSE_RETURNED",
  "ENTITY_HISTORY_BREAK",
  "NON_COMPARABLE_PERIOD_MOVEMENT",
  "STALE_COMPETITIVE_HISTORY",
  "UNRECOVERED_PROVIDER_COVERAGE_GAP",
]);

/**
 * Persist immutable full competitive ranking after provider coverage allows finalize.
 * Does NOT enable customer rank arrows / movement UI.
 */
export function persistCompetitiveHistoryAfterCertification({
  period,
  scenarios,
  propertyProfile,
  certificationStatus,
  write = true,
}) {
  const draft = buildFullCompetitiveRankingSnapshot({
    period,
    scenarios,
    propertyProfile,
    certificationStatus,
  });
  const fin = finalizeRankSnapshot(draft, { certificationStatus });
  if (!fin.ok) {
    return {
      ok: false,
      reason: fin.reason,
      snapshot: draft,
      stages: ADP_PERIOD_PIPELINE_STAGES,
    };
  }
  let path = null;
  if (write) {
    path = saveCompetitiveHistorySnapshot(fin.snapshot);
  }
  return {
    ok: true,
    path,
    snapshot: fin.snapshot,
    stages: ADP_PERIOD_PIPELINE_STAGES,
  };
}
