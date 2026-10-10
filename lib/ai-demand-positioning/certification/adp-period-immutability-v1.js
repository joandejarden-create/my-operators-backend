/**
 * ADP period immutability + correction workflow (Parts 25–26).
 * Once CERTIFIED: do not mutate raw metrics. Corrections create a new period.
 */

import { ADP_PERIOD_PIPELINE_STATES } from "./adp-period-pipeline-states-v1.js";

export const ADP_PERIOD_IMMUTABILITY_VERSION = "adp_period_immutability_v1";

/**
 * Refuse in-place mutation of certified period metric payloads.
 */
export function assertPeriodMutableForMetricWrite(period, { force = false } = {}) {
  const status = String(period?.certificationStatus || "").toUpperCase();
  const certified =
    period?.certified === true || status === ADP_PERIOD_PIPELINE_STATES.CERTIFIED;
  if (certified && !force) {
    return {
      allowed: false,
      reason: "CERTIFIED_PERIOD_IMMUTABLE",
      guidance:
        "Create a REPROCESSED / new official period with supersedesPeriodId + correctionReason. Do not overwrite certified history.",
    };
  }
  return { allowed: true, reason: null };
}

/**
 * Build metadata for a correction / reprocessed period linked to a prior certified period.
 */
export function buildCorrectionPeriodLinkage({
  oldPeriodId,
  newPeriodId,
  correctionReason,
  bugType = null,
  affectedMetrics = [],
} = {}) {
  return {
    immutabilityVersion: ADP_PERIOD_IMMUTABILITY_VERSION,
    oldPeriod: {
      periodId: oldPeriodId,
      certificationStatus: ADP_PERIOD_PIPELINE_STATES.SUPERSEDED,
      supersededByPeriodId: newPeriodId,
    },
    newPeriod: {
      periodId: newPeriodId,
      supersedesPeriodId: oldPeriodId,
      correctionReason: correctionReason || "CORRECTION",
      bugType,
      affectedMetrics,
      reprocessed: true,
    },
  };
}
