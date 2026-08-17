/**
 * Per-signal validation scorecard — no composite score.
 */

import { DEV_SIGNAL_VALIDATION_SNAPSHOT } from "./readiness.js";
import { SIGNAL_KEYS } from "./production-signals.js";
import { INTERNAL_TAXONOMY_STATUS } from "./internal-taxonomy.js";

export const SCORECARD_VERSION = "ai_intelligence_signal_scorecard_v1";

/**
 * @param {{ snapshot?: typeof DEV_SIGNAL_VALIDATION_SNAPSHOT }} [opts]
 */
export function buildSignalValidationScorecard(opts = {}) {
  const snap = opts.snapshot || DEV_SIGNAL_VALIDATION_SNAPSHOT;
  const rows = [];

  const order = [
    SIGNAL_KEYS.PRESENCE,
    SIGNAL_KEYS.RECOMMENDED,
    SIGNAL_KEYS.FIRST_RECOMMENDATION,
    SIGNAL_KEYS.NEGATIVE_OR_QUALIFIED,
    SIGNAL_KEYS.COMPARATOR,
  ];

  for (const key of order) {
    const s = snap.signals[key];
    rows.push({
      signal: key,
      DEV_N: s.N,
      precision: s.precision,
      recall: s.recall,
      f1: s.f1,
      accuracy: s.accuracy,
      holdoutStatus: key === SIGNAL_KEYS.PRESENCE ? s.holdoutStatus || "FAIL" : "NOT_RUN",
      holdoutV1: key === SIGNAL_KEYS.PRESENCE ? s.holdoutV1Status || null : null,
      holdoutV2: key === SIGNAL_KEYS.PRESENCE ? s.holdoutV2Status || null : null,
      holdoutV3: key === SIGNAL_KEYS.PRESENCE ? s.holdoutV3Status || null : null,
      readiness:
        key === SIGNAL_KEYS.PRESENCE && s.productionReadinessAfterHoldout
          ? s.productionReadinessAfterHoldout
          : s.readiness,
      gateStatus: s.gateStatus,
      sparse: s.sparse === true,
      DEV: key === SIGNAL_KEYS.PRESENCE ? "PASS" : s.readiness === "VALIDATED" ? "PASS" : "NOT_READY",
      HOLDOUT: key === SIGNAL_KEYS.PRESENCE ? s.holdoutStatus || "FAIL" : "NOT_RUN",
      HOLDOUT_V1:
        key === SIGNAL_KEYS.PRESENCE
          ? s.holdoutV1Status || "INSPECTED_DIAGNOSTIC_HOLDOUT"
          : "NOT_RUN",
      HOLDOUT_V2: key === SIGNAL_KEYS.PRESENCE ? s.holdoutV2Status || "NOT_CREATED" : "NOT_RUN",
      HOLDOUT_V3: key === SIGNAL_KEYS.PRESENCE ? s.holdoutV3Status || "NOT_CREATED" : "NOT_RUN",
      PRODUCTION_READINESS:
        key === SIGNAL_KEYS.PRESENCE
          ? s.productionReadinessAfterHoldout || "NOT_READY"
          : s.readiness,
    });
  }

  return {
    version: SCORECARD_VERSION,
    PER_SIGNAL: true,
    COMPOSITE_SCORE: false,
    HOLDOUT_ACCESSED: snap.HOLDOUT_ACCESSED === true,
    rows,
    internal10ClassTaxonomy: {
      status: INTERNAL_TAXONOMY_STATUS.RESEARCH_ONLY_NOT_PRODUCTION_CONTRACT,
      productionContract: false,
      internalResearchOnly: true,
    },
  };
}
