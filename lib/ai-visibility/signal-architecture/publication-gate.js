/**
 * Per-signal publication gate for Brand AI Visibility client surfaces.
 * Never replace unavailable with zero.
 */

import { AVAILABILITY } from "../availability-states.js";
import { SIGNAL_KEYS } from "./production-signals.js";
import {
  getSignalReadiness,
  isSignalClientPublishable,
  SIGNAL_READINESS,
} from "./readiness.js";

export const SIGNAL_PUBLICATION_GATE_VERSION =
  "ai_intelligence_signal_publication_gate_v1";

/** Governed copy — do not invent alternate zero placeholders. */
export const SIGNAL_UNAVAILABLE_MESSAGE =
  "Validated monitoring data is not currently available.";

/**
 * Map product metrics → driving production signal.
 */
export const METRIC_TO_SIGNAL = Object.freeze({
  aiPresence: SIGNAL_KEYS.PRESENCE,
  aiPresenceRate: SIGNAL_KEYS.PRESENCE,
  aiPresenceChange: SIGNAL_KEYS.PRESENCE,
  competitivePosition: SIGNAL_KEYS.PRESENCE,
  regionalPresence: SIGNAL_KEYS.PRESENCE,
  presenceTrend: SIGNAL_KEYS.PRESENCE,
  questionsMissing: SIGNAL_KEYS.PRESENCE,
  recommendationShare: SIGNAL_KEYS.RECOMMENDED,
  recommendationRate: SIGNAL_KEYS.RECOMMENDED,
  top3RecommendationRate: SIGNAL_KEYS.RECOMMENDED,
  firstRecommendationRate: SIGNAL_KEYS.FIRST_RECOMMENDATION,
  questionsWon: SIGNAL_KEYS.FIRST_RECOMMENDATION,
});

/**
 * Unavailable metric shell — value always null (never 0).
 * @param {{ signalKey: string, metric?: string }} args
 */
export function unavailableSignalMetric(args = {}) {
  const readiness = getSignalReadiness(args.signalKey);
  return {
    availability: AVAILABILITY.UNAVAILABLE,
    value: null,
    display: SIGNAL_UNAVAILABLE_MESSAGE,
    message: SIGNAL_UNAVAILABLE_MESSAGE,
    signalKey: args.signalKey,
    metric: args.metric || null,
    readiness: readiness.readiness,
    gateStatus: readiness.gateStatus,
    signalGateBlocked: true,
    note: "Do not coerce unavailable → 0",
  };
}

/**
 * Apply signal publication gate on top of an already-classified metric object.
 * If signal is client-publishable, returns original. Else unavailable (not zero).
 *
 * @param {string} metricKey
 * @param {object|null|undefined} metric
 * @param {{ snapshot?: object }} [opts]
 */
export function applySignalPublicationGate(metricKey, metric, opts = {}) {
  const signalKey = METRIC_TO_SIGNAL[metricKey];
  if (!signalKey) {
    return metric;
  }
  if (isSignalClientPublishable(signalKey, opts)) {
    return metric;
  }
  const blocked = unavailableSignalMetric({ signalKey, metric: metricKey });
  // Preserve structural helpers when present, but never numeric value.
  if (metric && typeof metric === "object") {
    return {
      ...blocked,
      helper: metric.helper,
      unit: metric.unit,
      rank: null,
      peerCount: metricKey === "competitivePosition" ? null : undefined,
      delta: null,
    };
  }
  return blocked;
}

/**
 * Gate a raw numeric before classifyMetricAvailability.
 * @returns {{ unavailable: true, value: null } | { unavailable: false, value: number|null|undefined }}
 */
export function gateNumericForSignal(metricKey, value, opts = {}) {
  const signalKey = METRIC_TO_SIGNAL[metricKey];
  if (!signalKey || isSignalClientPublishable(signalKey, opts)) {
    return { unavailable: false, value };
  }
  return { unavailable: true, value: null };
}

/**
 * Publication decision summary for admin/scorecard.
 */
export function evaluateSignalPublicationPlan(opts = {}) {
  const keys = Object.values(SIGNAL_KEYS);
  const plan = {};
  for (const key of keys) {
    const r = getSignalReadiness(key, opts);
    plan[key] = {
      clientVisible: r.clientPublishable === true,
      readiness: r.readiness,
      gateStatus: r.gateStatus,
      ifNotVisible: SIGNAL_UNAVAILABLE_MESSAGE,
      neverShowAsZero: true,
    };
  }
  return {
    version: SIGNAL_PUBLICATION_GATE_VERSION,
    plan,
    presenceMayPublish:
      plan.PRESENCE.clientVisible === true &&
      plan.PRESENCE.readiness === SIGNAL_READINESS.VALIDATED,
    recommendedMustHide: plan.RECOMMENDED.clientVisible === false,
    firstMustHide: plan.FIRST_RECOMMENDATION.clientVisible === false,
  };
}
