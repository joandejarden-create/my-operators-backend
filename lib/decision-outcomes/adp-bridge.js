/**
 * ADP ↔ Decision & Outcome bridge.
 * Creates Decisions for actionable positioning recommendations.
 * Outcome reconciliation observes measurement change without claiming causation.
 */

import {
  PRODUCT_MODULE,
  DECISION_TYPE,
  SUBJECT_TYPE,
  ADP_VALIDATION_TYPE,
  ADP_OUTCOME_TYPE,
  CAUSAL_CONFIDENCE,
  ACTOR_ROLE,
} from "./types.js";
import {
  createDecision,
  getSubjectDecision,
  recordValidation,
  recordAction,
  recordOutcome,
} from "./service.js";

/**
 * Ensure Decision for an actionable ADP finding/recommendation.
 * Idempotent on hotel + finding id + recommendation version.
 */
export async function ensureAdpFindingDecision({
  hotelId,
  findingId,
  recommendation,
  recommendationSummary = null,
  demandSegment = null,
  evidenceSnapshotSummary = null,
  confidence = null,
  recommendationVersion = 1,
  sourceSystem = "ADP",
} = {}) {
  if (!hotelId || !findingId || !recommendation) {
    return { decision: null, created: false, skipped: "missing_fields" };
  }

  return createDecision({
    hotelId,
    productModule: PRODUCT_MODULE.ADP,
    decisionType: DECISION_TYPE.POSITIONING_ACTION,
    subjectType: demandSegment
      ? SUBJECT_TYPE.DEMAND_SEGMENT
      : SUBJECT_TYPE.ADP_FINDING,
    subjectId: findingId,
    entityId: demandSegment || null,
    recommendation,
    recommendationSummary:
      recommendationSummary || String(recommendation).slice(0, 200),
    recommendationVersion,
    evidenceSnapshotSummary: evidenceSnapshotSummary || {
      findingId,
      frozenAt: new Date().toISOString(),
    },
    confidence,
    confidenceMethodologyVersion: "adp_finding_confidence_v1",
    sourceSystem,
    createdBy: { role: ACTOR_ROLE.SYSTEM, userId: "adp_system" },
  });
}

export async function recordAdpHotelResponse({
  hotelId,
  findingId,
  validationValue,
  note = null,
  actor = {},
} = {}) {
  const bundle = await getSubjectDecision(hotelId, {
    productModule: PRODUCT_MODULE.ADP,
    subjectId: findingId,
    decisionType: DECISION_TYPE.POSITIONING_ACTION,
  });
  if (!bundle?.decision) {
    const err = new Error("adp_decision_not_found");
    err.code = "adp_decision_not_found";
    throw err;
  }
  return recordValidation(hotelId, bundle.decision.decisionId, {
    validationType: ADP_VALIDATION_TYPE.HOTEL_RESPONSE,
    validationValue,
    validationNote: note,
    userId: actor.userId,
    role: actor.role || ACTOR_ROLE.OTHER,
    sourceSurface: "adp_ui",
  });
}

export async function recordAdpAction({
  hotelId,
  findingId,
  actionType,
  actionDescription = null,
  actor = {},
} = {}) {
  const bundle = await getSubjectDecision(hotelId, {
    productModule: PRODUCT_MODULE.ADP,
    subjectId: findingId,
  });
  if (!bundle?.decision) {
    const err = new Error("adp_decision_not_found");
    err.code = "adp_decision_not_found";
    throw err;
  }
  return recordAction(hotelId, bundle.decision.decisionId, {
    actionType,
    actionDescription,
    userId: actor.userId,
    role: actor.role || ACTOR_ROLE.OTHER,
    sourceSurface: "adp_ui",
  });
}

/**
 * Suggest outcome from later ADP measurement. Never asserts causality by default.
 */
export async function suggestAdpMeasurementOutcome({
  hotelId,
  findingId,
  beforeMetric = null,
  afterMetric = null,
  metricName = null,
  tooEarly = false,
  write = false,
  actor = {},
} = {}) {
  let outcomeType = ADP_OUTCOME_TYPE.UNKNOWN;
  let measurementChange = null;

  if (tooEarly) {
    outcomeType = ADP_OUTCOME_TYPE.TOO_EARLY_TO_MEASURE;
  } else if (
    beforeMetric != null &&
    afterMetric != null &&
    Number.isFinite(Number(beforeMetric)) &&
    Number.isFinite(Number(afterMetric))
  ) {
    const delta = Number(afterMetric) - Number(beforeMetric);
    measurementChange = { metricName, beforeMetric, afterMetric, delta };
    if (delta > 0) outcomeType = ADP_OUTCOME_TYPE.PRESENCE_IMPROVED;
    else if (delta === 0) outcomeType = ADP_OUTCOME_TYPE.NO_MEASURABLE_CHANGE;
    else outcomeType = ADP_OUTCOME_TYPE.DECLINED;
  }

  const suggestion = {
    outcomeType,
    measurementChange,
    temporalAssociation: measurementChange
      ? "movement_observed_after_action"
      : null,
    causalConfidence: CAUSAL_CONFIDENCE.UNKNOWN,
    language:
      "Movement observed after action — does not prove the recommendation caused the change.",
  };

  if (!write) return { suggestion, written: null };

  const bundle = await getSubjectDecision(hotelId, {
    productModule: PRODUCT_MODULE.ADP,
    subjectId: findingId,
  });
  if (!bundle?.decision) {
    return { suggestion, written: null, error: "adp_decision_not_found" };
  }

  const written = await recordOutcome(hotelId, bundle.decision.decisionId, {
    outcomeType: suggestion.outcomeType,
    outcomeNote: suggestion.language,
    measurementChange: suggestion.measurementChange,
    temporalAssociation: suggestion.temporalAssociation,
    causalConfidence: suggestion.causalConfidence,
    userId: actor.userId || "adp_system",
    role: actor.role || ACTOR_ROLE.SYSTEM,
    sourceSurface: "adp_measurement_reconciliation",
  });

  return { suggestion, written };
}
