/**
 * Current-state projection derived from append-only event history.
 * Latest state is never the sole stored truth.
 */

import { DECISION_LIFECYCLE, EVENT_KIND, GDI_ACTION_TYPE } from "./types.js";

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
  return rows.slice().sort((a, b) => String(eventTime(a)).localeCompare(String(eventTime(b)))).at(-1);
}

export function projectCurrentState(decision, events = []) {
  const latestValidation = latestOf(events, EVENT_KIND.VALIDATION);
  const latestAction = latestOf(events, EVENT_KIND.ACTION);
  const latestOutcome = latestOf(events, EVENT_KIND.OUTCOME);
  const validations = events.filter((e) => e.eventKind === EVENT_KIND.VALIDATION);
  const actions = events.filter((e) => e.eventKind === EVENT_KIND.ACTION);
  const outcomes = events.filter((e) => e.eventKind === EVENT_KIND.OUTCOME);

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
