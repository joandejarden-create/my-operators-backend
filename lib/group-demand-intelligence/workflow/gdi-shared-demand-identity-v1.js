/**
 * Cross-hotel shared demand identity V1 (CD-3 / CD-4).
 *
 * Shared: Demand Generator / Program / Signal ids.
 * Hotel-private: GDO row, Hotel Generator Fit, sales feedback, contacts notes.
 */

import { MAP_GDI_OPPORTUNITY_DG_IDENTITY as DG } from "../opportunity-field-map.js";
import { stripHotelPrivateKeys } from "./gdi-commercial-payload-durability-v1.js";

export const GDI_SHARED_DEMAND_IDENTITY_V1 = "gdi-shared-demand-identity-v1";

export const RECURRENCE_WRITE_POLICY = Object.freeze({
  /** CD-4-A: only meaningful recurring programs with evidence. */
  ALLOWED_STATUSES: Object.freeze([
    "RECURRING_CONFIRMED",
    "RECURRING_HISTORICAL",
  ]),
  FORBIDDEN: Object.freeze([
    "ONBOARDING_MIRROR",
    "GENERIC_SEED",
    "POSSIBLE_RECURRING_WITHOUT_EVIDENCE",
  ]),
});

export function extractSharedDemandIdentity(opp = {}) {
  return {
    dgGeneratorId: opp.dgGeneratorId || null,
    dgProgramId: opp.dgProgramId || null,
    dgSignalId: opp.dgSignalId || null,
    hotelGeneratorFitId: opp.hotelGeneratorFitId || null,
    eventSeriesId: opp.eventSeriesId || null,
    eventCycleId: opp.eventCycleId || null,
    organizationName: opp.organizationName || null,
  };
}

/**
 * Apply shared ids onto a hotel opportunity without leaking hotel-private keys
 * into the shared identity projection.
 */
export function applySharedDemandIdentityToOpportunity(opp = {}, identity = {}) {
  const next = { ...(opp && typeof opp === "object" ? opp : {}) };
  if (identity.dgGeneratorId) next.dgGeneratorId = identity.dgGeneratorId;
  if (identity.dgProgramId) next.dgProgramId = identity.dgProgramId;
  if (identity.dgSignalId) next.dgSignalId = identity.dgSignalId;
  if (identity.hotelGeneratorFitId) {
    next.hotelGeneratorFitId = identity.hotelGeneratorFitId;
  }
  if (identity.eventSeriesId) next.eventSeriesId = identity.eventSeriesId;
  if (identity.eventCycleId) next.eventCycleId = identity.eventCycleId;
  if (Array.isArray(identity.dgGeneratorAirtableRecordIds)) {
    next.dgGeneratorAirtableRecordIds = identity.dgGeneratorAirtableRecordIds;
  }
  if (Array.isArray(identity.dgProgramAirtableRecordIds)) {
    next.dgProgramAirtableRecordIds = identity.dgProgramAirtableRecordIds;
  }
  if (Array.isArray(identity.dgSignalAirtableRecordIds)) {
    next.dgSignalAirtableRecordIds = identity.dgSignalAirtableRecordIds;
  }
  return next;
}

/**
 * Two hotels may share the same dgGeneratorId / dgProgramId / dgSignalId while
 * keeping separate opportunityId + hotelId + private feedback.
 */
export function assertCrossHotelSharedIdentityIsolation({
  hotelAOpportunity = {},
  hotelBOpportunity = {},
} = {}) {
  const a = extractSharedDemandIdentity(hotelAOpportunity);
  const b = extractSharedDemandIdentity(hotelBOpportunity);
  const sharedOk =
    a.dgGeneratorId &&
    a.dgGeneratorId === b.dgGeneratorId &&
    String(hotelAOpportunity.hotelId) !== String(hotelBOpportunity.hotelId) &&
    String(hotelAOpportunity.opportunityId || hotelAOpportunity.id) !==
      String(hotelBOpportunity.opportunityId || hotelBOpportunity.id);

  const privateLeakKeys = [];
  for (const key of [
    "hotelSalesFeedback",
    "salesFeedbackNotes",
    "hotelTeamNotes",
    "decisionId",
  ]) {
    const sharedProjection = stripHotelPrivateKeys({
      [key]: hotelAOpportunity[key],
      dgGeneratorId: a.dgGeneratorId,
    });
    if (key in sharedProjection) privateLeakKeys.push(key);
  }

  return {
    ok: Boolean(sharedOk) && privateLeakKeys.length === 0,
    sharedOk: Boolean(sharedOk),
    privateLeakKeys,
    fieldNames: DG,
  };
}

/**
 * CD-4 gate — Demand Program upserts require confirmed/historical recurrence.
 */
export function mayUpsertDemandProgram({
  recurrenceStatus = null,
  evidenceConfirmed = false,
  onboardingMirror = false,
} = {}) {
  if (onboardingMirror) {
    return { allowed: false, reason: "ONBOARDING_MIRROR_FORBIDDEN" };
  }
  if (!evidenceConfirmed) {
    return { allowed: false, reason: "RECURRENCE_EVIDENCE_REQUIRED" };
  }
  if (!RECURRENCE_WRITE_POLICY.ALLOWED_STATUSES.includes(recurrenceStatus)) {
    return { allowed: false, reason: "RECURRENCE_STATUS_NOT_ELIGIBLE" };
  }
  return { allowed: true, reason: "RECURRENCE_EVIDENCE_OK" };
}
