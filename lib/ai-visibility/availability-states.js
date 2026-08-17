/**
 * Explicit data-availability / failure-state grammar for AI Visibility UI/API.
 * Never coerce missing → 0.
 *
 * Brand v1 controlled-release states (client-facing):
 * - NOT_MONITORED — provider/cohort not in requested monitoring set
 * - PROVIDER_ERROR — scheduled/attempted but request failed
 * - PARTIAL_MONITORING — some required observations succeeded, others failed
 * - NOT_COMPARABLE — observations exist but cannot validly aggregate/compare
 * - INSUFFICIENT_HISTORY — not enough comparable periods for trend
 * - NO_PRESENCE_OBSERVED / ZERO — monitoring succeeded; brand not observed (valid zero)
 * - OBSERVED — monitoring succeeded; presence rate > 0
 */

export const AVAILABILITY = Object.freeze({
  OBSERVED: "observed",
  /** Valid zero Presence — monitoring succeeded, denominator > 0, entity not observed. */
  ZERO: "zero",
  NO_PRESENCE_OBSERVED: "no_presence_observed",
  NOT_MONITORED: "not_monitored",
  PROVIDER_ERROR: "provider_error",
  PARTIAL: "partial",
  PARTIAL_MONITORING: "partial_monitoring",
  NOT_COMPARABLE: "not_comparable",
  INSUFFICIENT_HISTORY: "insufficient_history",
  UNAVAILABLE: "unavailable",
  FUTURE_READY: "future_ready",
});

export const AVAILABILITY_VERSION = "ai_visibility_availability_v2_controlled_release";

export const CLIENT_FAILURE_STATE_COPY = Object.freeze({
  [AVAILABILITY.NOT_MONITORED]:
    "Not Monitored — provider or cohort was not part of the requested monitoring set.",
  [AVAILABILITY.PROVIDER_ERROR]:
    "Provider Error — monitoring was attempted but the provider request failed.",
  [AVAILABILITY.PARTIAL_MONITORING]:
    "Partial Monitoring — some required observations succeeded and others failed.",
  [AVAILABILITY.NOT_COMPARABLE]:
    "Not Comparable — observations exist but cannot validly be aggregated or compared.",
  [AVAILABILITY.INSUFFICIENT_HISTORY]:
    "Insufficient History — not enough comparable periods exist for trend.",
  [AVAILABILITY.NO_PRESENCE_OBSERVED]:
    "No Presence Observed — monitoring succeeded and the brand was not observed.",
  [AVAILABILITY.ZERO]:
    "No Presence Observed — monitoring succeeded and the brand was not observed.",
  [AVAILABILITY.OBSERVED]: "Observed Presence.",
  [AVAILABILITY.UNAVAILABLE]: "Validated monitoring data is not currently available.",
  [AVAILABILITY.FUTURE_READY]: "Future ready — not activated.",
});

/**
 * Valid zero Presence only when monitoring succeeded, cohort valid, denominator > 0.
 * @param {{
 *   monitored?: boolean,
 *   success?: boolean,
 *   denominator?: number|null,
 *   presenceCount?: number|null,
 *   value?: number|null,
 *   providerError?: boolean,
 *   partial?: boolean,
 *   notComparable?: boolean,
 *   insufficientHistory?: boolean,
 *   unavailable?: boolean,
 * }} args
 */
export function classifyMetricAvailability(args = {}) {
  if (args.unavailable) {
    return shell(AVAILABILITY.UNAVAILABLE, null, CLIENT_FAILURE_STATE_COPY[AVAILABILITY.UNAVAILABLE]);
  }
  if (args.providerError) {
    return shell(
      AVAILABILITY.PROVIDER_ERROR,
      null,
      CLIENT_FAILURE_STATE_COPY[AVAILABILITY.PROVIDER_ERROR]
    );
  }
  if (args.notComparable) {
    return shell(
      AVAILABILITY.NOT_COMPARABLE,
      null,
      CLIENT_FAILURE_STATE_COPY[AVAILABILITY.NOT_COMPARABLE]
    );
  }
  if (args.insufficientHistory) {
    return shell(
      AVAILABILITY.INSUFFICIENT_HISTORY,
      null,
      CLIENT_FAILURE_STATE_COPY[AVAILABILITY.INSUFFICIENT_HISTORY]
    );
  }
  if (!args.monitored) {
    return shell(
      AVAILABILITY.NOT_MONITORED,
      null,
      CLIENT_FAILURE_STATE_COPY[AVAILABILITY.NOT_MONITORED]
    );
  }
  if (args.partial || args.partialMonitoring) {
    return {
      availability: AVAILABILITY.PARTIAL_MONITORING,
      value: args.value ?? null,
      display:
        args.value == null
          ? CLIENT_FAILURE_STATE_COPY[AVAILABILITY.PARTIAL_MONITORING]
          : formatRate(args.value),
      message: CLIENT_FAILURE_STATE_COPY[AVAILABILITY.PARTIAL_MONITORING],
      ZERO_STATE_INTEGRITY: true,
    };
  }

  const denom =
    typeof args.denominator === "number" && Number.isFinite(args.denominator)
      ? args.denominator
      : null;
  const value = args.value;

  // Valid zero: succeeded monitoring + positive denominator + rate 0 / count 0
  if (
    args.success !== false &&
    ((typeof value === "number" && value === 0) ||
      args.presenceCount === 0 ||
      args.noPresenceObserved === true)
  ) {
    if (denom != null && denom <= 0) {
      return shell(
        AVAILABILITY.NOT_MONITORED,
        null,
        "Not Monitored — no eligible denominator for Presence."
      );
    }
    return {
      availability: AVAILABILITY.NO_PRESENCE_OBSERVED,
      value: 0,
      display: "0%",
      message: CLIENT_FAILURE_STATE_COPY[AVAILABILITY.NO_PRESENCE_OBSERVED],
      ZERO_STATE_INTEGRITY: true,
      validZero: true,
    };
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    if (value === 0) {
      return {
        availability: AVAILABILITY.NO_PRESENCE_OBSERVED,
        value: 0,
        display: "0%",
        message: CLIENT_FAILURE_STATE_COPY[AVAILABILITY.NO_PRESENCE_OBSERVED],
        ZERO_STATE_INTEGRITY: true,
        validZero: true,
      };
    }
    return {
      availability: AVAILABILITY.OBSERVED,
      value,
      display: formatRate(value),
      ZERO_STATE_INTEGRITY: true,
    };
  }
  return shell(
    AVAILABILITY.NOT_MONITORED,
    null,
    CLIENT_FAILURE_STATE_COPY[AVAILABILITY.NOT_MONITORED]
  );
}

function shell(availability, value, display) {
  return {
    availability,
    value,
    display,
    message: display,
    ZERO_STATE_INTEGRITY: true,
  };
}

function formatRate(v) {
  if (v == null || !Number.isFinite(v)) return "—";
  const n = v <= 1 ? v * 100 : v;
  return `${(Math.round(n * 10) / 10).toFixed(1)}%`;
}

/**
 * Normalize stored metric names (snake_case from Phase 2E + camelCase aliases).
 */
export function normalizeMetricKey(metric) {
  const m = String(metric || "").trim();
  const map = {
    ai_presence_rate: "aiPresenceRate",
    aiPresenceRate: "aiPresenceRate",
    recommendation_share: "recommendationShare",
    recommendationShare: "recommendationShare",
    recommendation_rate: "recommendationRate",
    recommendationRate: "recommendationRate",
    top3_recommendation_rate: "top3RecommendationRate",
    top3RecommendationRate: "top3RecommendationRate",
    first_recommendation_rate: "firstRecommendationRate",
    firstRecommendationRate: "firstRecommendationRate",
    citation_rate: "citationRate",
    citationRate: "citationRate",
    questions_won: "questionsWon",
    questionsWon: "questionsWon",
    questions_missing: "questionsMissing",
    questionsMissing: "questionsMissing",
    competitive_position: "competitivePosition",
    competitivePosition: "competitivePosition",
  };
  return map[m] || m;
}

/** Alias retained for older PARTIAL consumers. */
export function normalizePartialAvailability(availability) {
  if (availability === AVAILABILITY.PARTIAL) return AVAILABILITY.PARTIAL_MONITORING;
  if (availability === AVAILABILITY.ZERO) return AVAILABILITY.NO_PRESENCE_OBSERVED;
  return availability;
}
