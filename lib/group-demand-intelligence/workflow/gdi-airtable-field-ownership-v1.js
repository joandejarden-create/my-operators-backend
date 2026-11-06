/**
 * Explicit field ownership for GDI Airtable sync.
 *
 * Research-owned: updated by GDI research / SyncAirtable research path.
 * Sales-owned: never overwritten by research sync (hotel team / feedback).
 * Publication-owned: updated only after successful publish verification.
 *
 * Uses existing Airtable column names from opportunity-field-map.js —
 * does not invent new schema.
 */
import { MAP_GDI_OPPORTUNITY as F } from "../opportunity-field-map.js";

export const GDI_AIRTABLE_FIELD_OWNERSHIP_V1 = "gdi-airtable-field-ownership-v1";

/** Conceptual lifecycle → existing actionStatus / qualification vocabulary. */
export const GDI_WORKFLOW_LIFECYCLE = Object.freeze({
  DISCOVERED: "DISCOVERED",
  RESEARCHING: "RESEARCHING",
  QUALIFIED: "QUALIFIED",
  READY_FOR_REVIEW: "READY_FOR_REVIEW",
  APPROVED: "APPROVED",
  PUBLISHED: "PUBLISHED",
  SALES_FOLLOW_UP: "SALES_FOLLOW_UP",
  REJECTED: "REJECTED",
  EXCLUDED: "EXCLUDED",
  NEEDS_MORE_RESEARCH: "NEEDS_MORE_RESEARCH",
  ARCHIVED: "ARCHIVED",
});

/**
 * Sales-owned payload keys preserved inside opportunityPayloadJson merge
 * and flat actionStatus when already human-set.
 */
export const SALES_OWNED_PAYLOAD_KEYS = Object.freeze([
  "hotelSalesFeedback",
  "salesFeedback",
  "salesFeedbackStatus",
  "salesFeedbackReason",
  "salesFeedbackNotes",
  "salesOwner",
  "assignedSalesperson",
  "lastSalesActivityAt",
  "nextFollowUpDate",
  "contactedAt",
  "workingStatus",
  "usefulnessRating",
  "alreadyKnown",
  "notAFitReason",
  "rejectionReason",
  "hotelTeamNotes",
]);

/** Flat Airtable columns that research sync must not clobber when present. */
export const SALES_OWNED_AIRTABLE_COLUMNS = Object.freeze([
  F.actionStatus, // may hold sales progression when set by hotel team
  F.decisionId, // owned by decision-outcomes bridge
]);

/** Flat columns research sync may update. */
export const RESEARCH_OWNED_AIRTABLE_COLUMNS = Object.freeze([
  F.opportunityName,
  F.organizationName,
  F.organizationEntityId,
  F.opportunityType,
  F.priority,
  F.eventStartDate,
  F.eventEndDate,
  F.segment,
  F.territory,
  F.venueStatus,
  F.sourcingStatus,
  F.attendance,
  F.attendanceStatus,
  F.peakRooms,
  F.roomDemandStatus,
  F.hotelFit,
  F.qualification,
  F.evidenceConfidence,
  F.whyNow,
  F.whyThisMatters,
  F.hotelWinThesis,
  F.recommendedAction,
  F.primaryContactId,
  F.primaryContactName,
  F.primaryContactRole,
  F.primaryContactEmail,
  F.primaryContactPhone,
  F.contactQuality,
  F.sourceCount,
  F.sourceSummary,
  F.lastResearchedAt,
  F.researchVersion,
  F.opportunityPayloadJson,
  F.runId,
  F.demandFamily,
  F.demandSignalType,
  F.peVenueId,
  F.peSignalId,
  F.hotelVenueFitId,
  F.isTestData,
  F.updatedAt,
]);

/** Flat columns updated only after verified publication. */
export const PUBLICATION_OWNED_PAYLOAD_KEYS = Object.freeze([
  "gdiPublicationStatus",
  "gdiPublicationVersion",
  "gdiPublicationId",
  "gdiPublishedAt",
  "gdiPublishedOpportunityIds",
  "gdiCustomerShareReady",
  "gdiFullContentParityStatus",
  "gdiFullContentParityVerifiedAt",
  "gdiPublicationProvenance",
]);

/**
 * Merge research payload onto existing Airtable payload without wiping sales keys.
 */
export function mergeResearchPayloadPreservingSales(
  existingPayload = null,
  researchOpp = {},
) {
  const base =
    existingPayload && typeof existingPayload === "object"
      ? { ...existingPayload }
      : {};
  const preserved = {};
  for (const key of SALES_OWNED_PAYLOAD_KEYS) {
    if (base[key] != null && base[key] !== "") {
      preserved[key] = base[key];
    }
  }
  for (const key of PUBLICATION_OWNED_PAYLOAD_KEYS) {
    if (base[key] != null && base[key] !== "") {
      preserved[key] = base[key];
    }
  }
  const next = {
    ...base,
    ...researchOpp,
    ...preserved,
    id: researchOpp.id || researchOpp.opportunityId || base.id,
    hotelId: researchOpp.hotelId || base.hotelId,
  };
  return next;
}

/**
 * Strip sales-owned flat fields from a research field patch when existing
 * Airtable row already has a human sales value.
 */
export function applySalesOwnershipToAirtableFields(
  nextFields = {},
  existingFields = {},
  opts = {},
) {
  const out = { ...nextFields };
  if (opts.overwriteSalesOwned === true) return out;

  for (const col of SALES_OWNED_AIRTABLE_COLUMNS) {
    const existing = existingFields?.[col];
    if (existing != null && existing !== "" && col in out) {
      // Keep hotel/decision values; research does not reset them.
      out[col] = existing;
    }
  }
  return out;
}

/**
 * Map conceptual lifecycle to existing actionStatus string when empty.
 * Does not invent new Airtable select options — writes free-text actionStatus
 * only when the column already accepts arbitrary strings (current schema).
 */
export function mapLifecycleToActionStatus(lifecycle, existingActionStatus) {
  if (existingActionStatus != null && String(existingActionStatus).trim()) {
    return existingActionStatus;
  }
  const key = String(lifecycle || "").toUpperCase();
  if (!key || !GDI_WORKFLOW_LIFECYCLE[key]) return existingActionStatus || null;
  return key;
}
