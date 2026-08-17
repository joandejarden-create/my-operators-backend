/**
 * Internal metric readiness flags (admin QA only).
 * Not client-facing. No numeric confidence scores.
 * Aligned with adopted signal/flag architecture (per-signal readiness).
 */

import {
  DEV_SIGNAL_VALIDATION_SNAPSHOT,
  getSignalReadiness,
  SIGNAL_KEYS,
  SIGNAL_READINESS,
} from "./signal-architecture/index.js";

export const METRIC_READINESS_VERSION = "ai_visibility_metric_readiness_v2_signal";

function mapSignalReadinessToLegacy(status) {
  if (status === SIGNAL_READINESS.VALIDATED) return "READY";
  if (status === SIGNAL_READINESS.PROVISIONAL) return "PARTIAL";
  if (status === SIGNAL_READINESS.RESEARCH_BLOCKED) return "BLOCKED";
  return "NOT_READY";
}

/**
 * @param {{
 *   classificationIntegrity?: boolean,
 *   citationAssociationCompleteness?: "high"|"partial"|"low",
 *   testCoverage?: boolean,
 *   parentBrandCollisions?: number,
 *   manualClassificationAccuracy?: number|null,
 * }} evidence
 */
export function assessMetricReadiness(evidence = {}) {
  const presence = getSignalReadiness(SIGNAL_KEYS.PRESENCE);
  const recommended = getSignalReadiness(SIGNAL_KEYS.RECOMMENDED);
  const first = getSignalReadiness(SIGNAL_KEYS.FIRST_RECOMMENDATION);
  const cite = evidence.citationAssociationCompleteness || "low";
  const testsOk = evidence.testCoverage !== false;
  const noParentFp = (evidence.parentBrandCollisions || 0) === 0;

  const presenceOk =
    presence.productionReadinessAfterHoldout === SIGNAL_READINESS.VALIDATED &&
    noParentFp &&
    testsOk;

  return {
    version: METRIC_READINESS_VERSION,
    signalArchitecture: DEV_SIGNAL_VALIDATION_SNAPSHOT.signals,
    AI_PRESENCE_RATE: {
      status: presenceOk ? "READY" : mapSignalReadinessToLegacy(presence.readiness),
      reason: presenceOk
        ? "PRESENCE signal PRODUCTION_VALIDATED (Holdout v3 PASS); parent/brand collisions not observed"
        : "Presence depends on entity match / holdout production certification",
      drivingSignal: SIGNAL_KEYS.PRESENCE,
    },
    COMPETITIVE_POSITION: {
      status: presenceOk ? "READY" : mapSignalReadinessToLegacy(presence.readiness),
      reason: "Ranks by AI Presence Rate; inherits PRESENCE production certification",
      drivingSignal: SIGNAL_KEYS.PRESENCE,
    },
    RECOMMENDATION_SHARE: {
      status: "BLOCKED",
      reason:
        "RESEARCH_BLOCKED_NOT_PRODUCTION_READY — Recommendation Share remains blocked; Brand AI Visibility v1 does not depend on Recommended",
      drivingSignal: SIGNAL_KEYS.RECOMMENDED,
    },
    FIRST_RECOMMENDATION_RATE: {
      status: mapSignalReadinessToLegacy(first.readiness),
      reason: "NOT_READY — blocked with Recommended research cycle",
      drivingSignal: SIGNAL_KEYS.FIRST_RECOMMENDATION,
    },
    QUESTIONS_WON: {
      status: "BLOCKED",
      reason: "Blocked — depends on FIRST_RECOMMENDATION; not part of Presence-led Brand v1",
      drivingSignal: SIGNAL_KEYS.FIRST_RECOMMENDATION,
    },
    QUESTIONS_MISSING: {
      status: presenceOk ? "READY" : mapSignalReadinessToLegacy(presence.readiness),
      reason: "Absence detection follows PRESENCE matching (production-validated)",
      drivingSignal: SIGNAL_KEYS.PRESENCE,
    },
    CITATION_RATE: {
      status:
        cite === "high" ? "READY" : cite === "partial" ? "PARTIAL" : "NOT_READY",
      reason:
        cite === "high"
          ? "Citation-entity association reliable"
          : "Brand first-party domains often missing; third-party association remains partial",
    },
    note:
      "Client publication uses per-signal gates; Presence PRODUCTION_VALIDATED. Recommended = RESEARCH_BLOCKED_NOT_PRODUCTION_READY (non-blocking for Brand v1). Recommendation Share / Questions Won remain BLOCKED.",
  };
}
