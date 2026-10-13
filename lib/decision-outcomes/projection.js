/**
 * Current-state projection derived from append-only event history.
 * Latest state is never the sole stored truth.
 */

import {
  DECISION_LIFECYCLE,
  EVENT_KIND,
  GDI_ACTION_TYPE,
  GDI_OUTCOME_TYPE,
  GDI_COMMERCIAL_FUNNEL_STAGE,
  labelGdiActionType,
  labelGdiOutcomeType,
  labelGdiCommercialFunnelStage,
} from "./types.js";

function eventTime(e) {
  return (
    e.validatedAt ||
    e.reportedAt ||
    e.completedAt ||
    e.actionDate ||
    e.createdAt ||
    e.outcomeDate ||
    ""
  );
}

function latestOf(events, kind) {
  const rows = events.filter((e) => e.eventKind === kind);
  if (!rows.length) return null;
  return rows
    .slice()
    .sort((a, b) => String(eventTime(a)).localeCompare(String(eventTime(b))))
    .at(-1);
}

/**
 * Map latest action/outcome into commercial funnel stage.
 * Prefer stronger progression outcomes over mere contact actions.
 */
export function deriveCommercialFunnelStage(events = []) {
  const latestOutcome = latestOf(events, EVENT_KIND.OUTCOME);
  const latestAction = latestOf(events, EVENT_KIND.ACTION);
  const o = latestOutcome?.outcomeType;
  const a = latestAction?.actionType;

  if (o === GDI_OUTCOME_TYPE.WON || o === GDI_OUTCOME_TYPE.BOOKED) {
    return GDI_COMMERCIAL_FUNNEL_STAGE.WON;
  }
  if (o === GDI_OUTCOME_TYPE.LOST) return GDI_COMMERCIAL_FUNNEL_STAGE.LOST;
  if (
    o === GDI_OUTCOME_TYPE.OPPORTUNITY_CLOSED ||
    o === GDI_OUTCOME_TYPE.NOT_QUALIFIED
  ) {
    return GDI_COMMERCIAL_FUNNEL_STAGE.CLOSED_NO_DECISION;
  }
  if (o === GDI_OUTCOME_TYPE.ADDED_TO_SOURCED_PROPERTIES) {
    return GDI_COMMERCIAL_FUNNEL_STAGE.ADDED_TO_SOURCING;
  }
  if (
    a === GDI_ACTION_TYPE.PROPOSAL_SUBMITTED ||
    a === GDI_ACTION_TYPE.NEGOTIATING
  ) {
    return GDI_COMMERCIAL_FUNNEL_STAGE.PROPOSAL_SUBMITTED;
  }
  if (
    a === GDI_ACTION_TYPE.RFP_RECEIVED ||
    a === GDI_ACTION_TYPE.RFP_REQUESTED
  ) {
    return GDI_COMMERCIAL_FUNNEL_STAGE.RFP_RECEIVED;
  }
  if (
    a === GDI_ACTION_TYPE.CONTACTED ||
    a === GDI_ACTION_TYPE.FOLLOW_UP_REQUIRED ||
    a === GDI_ACTION_TYPE.SITE_VISIT_REQUESTED ||
    a === GDI_ACTION_TYPE.SITE_VISIT_COMPLETED
  ) {
    return GDI_COMMERCIAL_FUNNEL_STAGE.CONTACTED;
  }
  return GDI_COMMERCIAL_FUNNEL_STAGE.IDENTIFIED;
}

/** Customer-safe progression projection for GDI UI (auth + share). */
export function projectCommercialProgression(decision, events = []) {
  const latestAction = latestOf(events, EVENT_KIND.ACTION);
  const latestOutcome = latestOf(events, EVENT_KIND.OUTCOME);
  const stage = deriveCommercialFunnelStage(events);
  const stageLabel = labelGdiCommercialFunnelStage(stage) || "Identified";
  const currentStatusLabel =
    (latestOutcome && labelGdiOutcomeType(latestOutcome.outcomeType)) ||
    stageLabel;
  const compactStatusLabel =
    stage === GDI_COMMERCIAL_FUNNEL_STAGE.ADDED_TO_SOURCING
      ? "Added to sourcing"
      : stageLabel;

  return {
    decisionId: decision?.decisionId || null,
    funnelStage: stage,
    funnelStageLabel: stageLabel,
    currentStatusLabel,
    compactStatusLabel,
    latestActionType: latestAction?.actionType || null,
    latestActionLabel: latestAction
      ? labelGdiActionType(latestAction.actionType) || latestAction.actionType
      : null,
    latestActionDate: latestAction
      ? latestAction.actionDate || latestAction.createdAt || null
      : null,
    latestOutcomeType: latestOutcome?.outcomeType || null,
    latestOutcomeLabel: latestOutcome
      ? labelGdiOutcomeType(latestOutcome.outcomeType) ||
        latestOutcome.outcomeType
      : null,
    latestOutcomeDate: latestOutcome
      ? latestOutcome.outcomeDate ||
        latestOutcome.reportedAt ||
        latestOutcome.createdAt ||
        null
      : null,
  };
}

export function projectCurrentState(decision, events = []) {
  const latestValidation = latestOf(events, EVENT_KIND.VALIDATION);
  const latestAction = latestOf(events, EVENT_KIND.ACTION);
  const latestOutcome = latestOf(events, EVENT_KIND.OUTCOME);
  const validations = events.filter((e) => e.eventKind === EVENT_KIND.VALIDATION);
  const actions = events.filter((e) => e.eventKind === EVENT_KIND.ACTION);
  const outcomes = events.filter((e) => e.eventKind === EVENT_KIND.OUTCOME);
  const commercialProgression = projectCommercialProgression(decision, events);

  return {
    decisionId: decision.decisionId,
    hotelId: decision.hotelId,
    subjectId: decision.subjectId,
    productModule: decision.productModule,
    decisionLifecycleStage: deriveLifecycle(decision, events),
    latestValidation,
    latestAction,
    latestOutcome,
    validationCount: validations.length,
    actionCount: actions.length,
    outcomeCount: outcomes.length,
    commercialProgression,
    // Convenience maps for multi-type GDI validations
    latestValidationsByType: Object.fromEntries(
      Object.entries(
        validations.reduce((acc, v) => {
          const t = v.validationType || "UNKNOWN";
          const prev = acc[t];
          if (!prev || String(eventTime(v)) >= String(eventTime(prev))) acc[t] = v;
          return acc;
        }, {})
      )
    ),
  };
}

export function deriveLifecycle(decision, events = []) {
  const hasOutcome = events.some((e) => e.eventKind === EVENT_KIND.OUTCOME);
  const actions = events.filter((e) => e.eventKind === EVENT_KIND.ACTION);
  const hasAction = actions.length > 0;
  const hasValidation = events.some((e) => e.eventKind === EVENT_KIND.VALIDATION);

  if (hasOutcome) return DECISION_LIFECYCLE.OUTCOME_RECORDED;

  if (hasAction) {
    const last = actions
      .slice()
      .sort((a, b) => String(eventTime(a)).localeCompare(String(eventTime(b))))
      .at(-1);
    if (
      last?.actionType === GDI_ACTION_TYPE.PLANNED_TO_CONTACT ||
      last?.actionStatus === "PLANNED"
    ) {
      return DECISION_LIFECYCLE.ACTION_PLANNED;
    }
    if (last?.actionType === GDI_ACTION_TYPE.NO_ACTION) {
      return hasValidation ? DECISION_LIFECYCLE.VALIDATED : DECISION_LIFECYCLE.CLOSED;
    }
    return DECISION_LIFECYCLE.ACTION_TAKEN;
  }

  if (hasValidation) return DECISION_LIFECYCLE.VALIDATED;
  return decision.decisionStatus || DECISION_LIFECYCLE.RECOMMENDED;
}
