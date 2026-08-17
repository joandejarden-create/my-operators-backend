/**
 * Per-signal readiness — independent; one failed signal must not block validated signals.
 */

import {
  SIGNAL_GATE_IDS,
  SIGNAL_KEYS,
  SIGNAL_ARCHITECTURE_VERSION,
} from "./production-signals.js";

export const SIGNAL_READINESS = Object.freeze({
  VALIDATED: "VALIDATED",
  PROVISIONAL: "PROVISIONAL",
  NOT_READY: "NOT_READY",
  /** Research cycle closed without production certification — not abandoned. */
  RESEARCH_BLOCKED: "RESEARCH_BLOCKED_NOT_PRODUCTION_READY",
  NOT_GOVERNED: "NOT_GOVERNED",
});

export const SIGNAL_GATE_STATUS = Object.freeze({
  PASS: "PASS",
  FAIL: "FAIL",
  FAIL_SPARSE: "FAIL_SPARSE",
  NOT_RUN: "NOT_RUN",
});

/**
 * Current DEV validation snapshot (v4.1, Clean DEV Golden Set n=290).
 * Presence Holdout v3 one-time certification PASS (2026-08-15) → PRODUCTION_VALIDATED.
 * Holdout v2 remains SCORED_FAIL (not for reuse/tuning).
 */
export const DEV_SIGNAL_VALIDATION_SNAPSHOT = Object.freeze({
  source: "production-signal-taxonomy-study.json#candidateE.benchmarks.v4.1",
  holdoutSource: "presence-holdout-v3-one-time-score.json",
  holdoutV3CertificationArtifact: "presence-holdout-v3-one-time-score",
  classifierVersion: "recommendation-classifier-v4_1",
  resolverVersion: "ai_visibility_entity_resolver_v2_1_contextual",
  DEV_N: 290,
  HOLDOUT_ACCESSED: true,
  HOLDOUT_N: 100,
  signals: Object.freeze({
    PRESENCE: Object.freeze({
      key: SIGNAL_KEYS.PRESENCE,
      gate: SIGNAL_GATE_IDS.PRESENCE_GATE,
      N: 290,
      accuracy: 1,
      precision: 1,
      recall: 1,
      f1: 1,
      gateStatus: SIGNAL_GATE_STATUS.PASS,
      readiness: SIGNAL_READINESS.VALIDATED,
      sparse: false,
      holdoutStatus: "PASS",
      holdoutV1Status: "INSPECTED_DIAGNOSTIC_HOLDOUT",
      holdoutV2Status: "SCORED_FAIL",
      holdoutV3Status: "PASS",
      productionReadinessAfterHoldout: SIGNAL_READINESS.VALIDATED,
      productionCertificationStatus: "PRODUCTION_VALIDATED",
      note:
        "DEV PASS. Holdout v1 INSPECTED_DIAGNOSTIC. Holdout v2 SCORED_FAIL (untouched). Holdout v3 certification PASS (TP=60 TN=40 FP=0 FN=0; P/R 100%). AI_SIGNAL_PRESENCE = PRODUCTION_VALIDATED for client Presence surfaces. Recommended/First/Negative/Comparator remain NOT_READY.",
    }),
    RECOMMENDED: Object.freeze({
      key: SIGNAL_KEYS.RECOMMENDED,
      gate: SIGNAL_GATE_IDS.RECOMMENDED_GATE,
      N: 290,
      accuracy: 0.8896551724137931,
      precision: 0.9791666666666666,
      recall: 0.6025641025641025,
      f1: 0.7460317460317459,
      gateStatus: SIGNAL_GATE_STATUS.FAIL,
      readiness: SIGNAL_READINESS.RESEARCH_BLOCKED,
      productionCertificationStatus: "RESEARCH_BLOCKED_NOT_PRODUCTION_READY",
      sparse: false,
      researchCycleClosed: true,
      abandoned: false,
      recurringResearchScheduled: false,
      note:
        "Research cycle closed 2026-08-15 (deterministic ~P82/R60; semantic feasibility ~P85/R53). Non-blocking for Brand AI Visibility v1. Reopen only under REOPEN_RESEARCH_IF — no recurring research.",
    }),
    FIRST_RECOMMENDATION: Object.freeze({
      key: SIGNAL_KEYS.FIRST_RECOMMENDATION,
      gate: SIGNAL_GATE_IDS.FIRST_REC_GATE,
      N: 290,
      accuracy: 0.9413793103448276,
      precision: 1,
      recall: 0.43333333333333335,
      f1: 0.6046511627906976,
      gateStatus: SIGNAL_GATE_STATUS.FAIL,
      readiness: SIGNAL_READINESS.NOT_READY,
      productionCertificationStatus: "NOT_READY",
      sparse: false,
      note: "NOT_READY — blocked with Recommended research cycle; no First work while Recommended research-blocked",
    }),
    NEGATIVE_OR_QUALIFIED: Object.freeze({
      key: SIGNAL_KEYS.NEGATIVE_OR_QUALIFIED,
      gate: SIGNAL_GATE_IDS.NEGATIVE_GATE,
      N: 290,
      accuracy: 0.996551724137931,
      precision: 1,
      recall: 0.8571428571428571,
      f1: 0.923076923076923,
      gateStatus: SIGNAL_GATE_STATUS.FAIL_SPARSE,
      readiness: SIGNAL_READINESS.NOT_READY,
      productionCertificationStatus: "NOT_READY",
      sparse: true,
      note: "NOT_READY — sparse positive class; defer",
    }),
    COMPARATOR: Object.freeze({
      key: SIGNAL_KEYS.COMPARATOR,
      gate: SIGNAL_GATE_IDS.COMPARATOR_GATE,
      N: 290,
      accuracy: 0.9862068965517241,
      precision: 1,
      recall: 0.42857142857142855,
      f1: 0.6,
      gateStatus: SIGNAL_GATE_STATUS.FAIL_SPARSE,
      readiness: SIGNAL_READINESS.NOT_READY,
      productionCertificationStatus: "NOT_READY",
      sparse: true,
      note: "NOT_READY — sparse positive class; defer",
    }),
  }),
});

/**
 * @param {string} signalKey
 * @param {{ snapshot?: typeof DEV_SIGNAL_VALIDATION_SNAPSHOT }} [opts]
 */
export function getSignalReadiness(signalKey, opts = {}) {
  const snap = opts.snapshot || DEV_SIGNAL_VALIDATION_SNAPSHOT;
  const row = snap.signals?.[signalKey];
  if (!row) {
    return {
      key: signalKey,
      readiness: SIGNAL_READINESS.NOT_GOVERNED,
      gateStatus: SIGNAL_GATE_STATUS.NOT_RUN,
      clientPublishable: false,
    };
  }
  // Presence requires holdout PRODUCTION_VALIDATED before client publish.
  const productionReady =
    signalKey === SIGNAL_KEYS.PRESENCE
      ? row.productionReadinessAfterHoldout === SIGNAL_READINESS.VALIDATED
      : row.readiness === SIGNAL_READINESS.VALIDATED &&
        row.gateStatus === SIGNAL_GATE_STATUS.PASS;
  return {
    ...row,
    DEV_GATE: row.gateStatus,
    clientPublishable: productionReady === true,
  };
}

/**
 * Independence: recommended/first failure must not force Presence DEV gate to fail.
 */
export function assertReadinessIndependence(snapshot = DEV_SIGNAL_VALIDATION_SNAPSHOT) {
  const presence = getSignalReadiness(SIGNAL_KEYS.PRESENCE, { snapshot });
  const recommended = getSignalReadiness(SIGNAL_KEYS.RECOMMENDED, { snapshot });
  const first = getSignalReadiness(SIGNAL_KEYS.FIRST_RECOMMENDATION, { snapshot });

  const presenceDevPass = presence.gateStatus === SIGNAL_GATE_STATUS.PASS;
  const recommendedNotProduction =
    recommended.readiness === SIGNAL_READINESS.NOT_READY ||
    recommended.readiness === SIGNAL_READINESS.RESEARCH_BLOCKED ||
    recommended.productionCertificationStatus ===
      "RESEARCH_BLOCKED_NOT_PRODUCTION_READY";
  const failedRecommendedDoesNotBlockPresenceDev =
    recommendedNotProduction && presenceDevPass;
  const failedFirstDoesNotBlockPresenceDev =
    first.readiness === SIGNAL_READINESS.NOT_READY && presenceDevPass;
  const brandV1DoesNotRequireRecommended =
    presence.clientPublishable === true && recommendedNotProduction;

  return {
    version: SIGNAL_ARCHITECTURE_VERSION,
    SIGNAL_READINESS_INDEPENDENT: true,
    FAILED_RECOMMENDED_DOES_NOT_BLOCK_PRESENCE: failedRecommendedDoesNotBlockPresenceDev,
    FAILED_FIRST_DOES_NOT_BLOCK_PRESENCE: failedFirstDoesNotBlockPresenceDev,
    BRAND_AI_VISIBILITY_CAN_ADVANCE_WITHOUT_RECOMMENDED:
      brandV1DoesNotRequireRecommended,
    RECOMMENDED_REQUIRED_FOR_V1: false,
    ok:
      failedRecommendedDoesNotBlockPresenceDev &&
      failedFirstDoesNotBlockPresenceDev &&
      brandV1DoesNotRequireRecommended,
  };
}

/** Client-visible only when production-ready for that signal. */
export function isSignalClientPublishable(signalKey, opts = {}) {
  return getSignalReadiness(signalKey, opts).clientPublishable === true;
}
