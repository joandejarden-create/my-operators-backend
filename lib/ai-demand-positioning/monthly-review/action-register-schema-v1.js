/**
 * Persistent Action Register model for ADP Monthly Executive Review.
 * PDF/live view are projections — this schema is the system of record shape.
 */

import { ACTION_STATUS, DEFAULT_OWNER_ROLE } from "./monthly-review-contract-v1.js";

export const ACTION_REGISTER_SCHEMA = "ADP_MONTHLY_REVIEW_ACTION_REGISTER_V1";

export const ACTION_REGISTER_FIELDS = Object.freeze([
  "actionId",
  "propertyId",
  "monitoringPeriodId",
  "actionTitle",
  "observedIssue",
  "recommendedAction",
  "rationale",
  "evidenceRefs",
  "accountableOwnerRole",
  "supportingTeam",
  "createdDate",
  "targetDate",
  "status",
  "completionDate",
  "expectedSignal",
  "nextPeriodResult",
  "followUpAssessment",
  "carriedForward",
  "founderCustomerNotes",
  "impactTracking",
]);

/**
 * @param {object} partial
 */
export function createActionRegisterEntry(partial = {}) {
  const status = partial.status || ACTION_STATUS.OPEN;
  return {
    actionId: partial.actionId || null,
    propertyId: partial.propertyId || null,
    monitoringPeriodId: partial.monitoringPeriodId || null,
    actionTitle: partial.actionTitle || "",
    observedIssue: partial.observedIssue || "",
    recommendedAction: partial.recommendedAction || "",
    rationale: partial.rationale || "",
    evidenceRefs: Array.isArray(partial.evidenceRefs) ? partial.evidenceRefs : [],
    accountableOwnerRole: partial.accountableOwnerRole || DEFAULT_OWNER_ROLE,
    supportingTeam: partial.supportingTeam || null,
    createdDate: partial.createdDate || null,
    targetDate: partial.targetDate ?? null, // editable / unset until management confirms
    status,
    completionDate: partial.completionDate || null,
    expectedSignal: partial.expectedSignal || "",
    nextPeriodResult: partial.nextPeriodResult || null,
    followUpAssessment: partial.followUpAssessment || null,
    carriedForward: Boolean(partial.carriedForward),
    founderCustomerNotes: partial.founderCustomerNotes || null,
    impactTracking: partial.impactTracking || {
      schema: "MONTHLY_REVIEW_ACTION_IMPACT_NONCAUSAL_TRACKING",
      before: null,
      actionDate: null,
      nextRun: null,
      change: null,
      assessment: null,
      causationClaimed: false,
      note: "Later periods may record before/after measurement. Monitoring alone does not establish causation.",
    },
  };
}

export function assertActionAccountabilityComplete(actions) {
  const failures = [];
  for (const a of actions || []) {
    if (!a.actionTitle) failures.push(`${a.actionId || "?"}: missing title`);
    if (!a.observedIssue) failures.push(`${a.actionId || "?"}: missing observedIssue`);
    if (!a.recommendedAction) failures.push(`${a.actionId || "?"}: missing recommendedAction`);
    if (!a.accountableOwnerRole) failures.push(`${a.actionId || "?"}: missing owner`);
    if (!a.status) failures.push(`${a.actionId || "?"}: missing status`);
    if (a.targetDate === undefined) {
      failures.push(`${a.actionId || "?"}: targetDate field missing`);
    }
  }
  return { ok: failures.length === 0, failures };
}
