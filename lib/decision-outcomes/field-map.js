/**
 * Canonical Decision & Outcome — Airtable field map (module-neutral).
 * Tables live on canonical intelligence base (appa2cE7FTRmIbB32), not Deal Capture MVP.
 */

export const DECISIONS_TABLE_NAME = "Decisions";
export const DECISION_EVENTS_TABLE_NAME = "Decision Events";

/** Env: AIRTABLE_INTELLIGENCE_BASE_ID || AIRTABLE_DECISION_OUTCOME_BASE_ID || … */
export const MAP_DECISION = Object.freeze({
  decisionId: "decisionId",
  idempotencyKey: "idempotencyKey",
  hotelId: "hotelId",
  organizationId: "organizationId",
  entityId: "entityId",
  productModule: "productModule",
  decisionType: "decisionType",
  subjectType: "subjectType",
  subjectId: "subjectId",
  recommendation: "recommendation",
  recommendationSummary: "recommendationSummary",
  recommendationDate: "recommendationDate",
  recommendationVersion: "recommendationVersion",
  decisionStatus: "decisionStatus",
  confidenceScore: "confidenceScore",
  confidenceBand: "confidenceBand",
  confidenceMethodologyVersion: "confidenceMethodologyVersion",
  evidenceSnapshotSummary: "evidenceSnapshotSummary",
  evidenceReferenceIds: "evidenceReferenceIds",
  sourceSystem: "sourceSystem",
  schemaVersion: "schemaVersion",
  createdAt: "createdAt",
  updatedAt: "updatedAt",
  createdBy: "createdBy",
  createdByRole: "createdByRole",
  supersedesDecisionId: "supersedesDecisionId",
});

export const MAP_DECISION_EVENT = Object.freeze({
  eventId: "eventId",
  decisionId: "decisionId",
  hotelId: "hotelId",
  organizationId: "organizationId",
  productModule: "productModule",
  subjectId: "subjectId",
  eventType: "eventType",
  eventSubtype: "eventSubtype",
  eventValue: "eventValue",
  reason: "reason",
  note: "note",
  userId: "userId",
  userRole: "userRole",
  eventDate: "eventDate",
  reportedAt: "reportedAt",
  createdAt: "createdAt",
  financialValue: "financialValue",
  currency: "currency",
  roomNights: "roomNights",
  revenueValue: "revenueValue",
  sourceSurface: "sourceSurface",
  schemaVersion: "schemaVersion",
  causalConfidence: "causalConfidence",
  measurementChange: "measurementChange",
  temporalAssociation: "temporalAssociation",
  /** Lossless extras for migration / forward-compat (JSON string). */
  rawEventJson: "rawEventJson",
});

export const VAL_PRODUCT_MODULE = Object.freeze([
  "GDI",
  "ADP",
  "BRAND_EXPLORER",
  "OPERATOR_ALIGNMENT",
  "OWNERSHIP_INTELLIGENCE",
]);

export const VAL_DECISION_TYPE = Object.freeze([
  "OPPORTUNITY_PURSUIT",
  "POSITIONING_ACTION",
  "CONTACT_SELECTION",
  "BRAND_SELECTION",
  "OPERATOR_SELECTION",
  "OWNERSHIP_VALIDATION",
]);

export const VAL_SUBJECT_TYPE = Object.freeze([
  "GROUP_OPPORTUNITY",
  "ADP_FINDING",
  "DEMAND_SEGMENT",
  "CONTACT",
  "BRAND",
  "OPERATOR",
  "PROPERTY",
]);

export const VAL_DECISION_STATUS = Object.freeze([
  "RECOMMENDED",
  "VALIDATED",
  "ACTION_PLANNED",
  "ACTION_TAKEN",
  "OUTCOME_PENDING",
  "OUTCOME_RECORDED",
  "CLOSED",
]);

export const VAL_EVENT_TYPE = Object.freeze(["VALIDATION", "ACTION", "OUTCOME"]);
