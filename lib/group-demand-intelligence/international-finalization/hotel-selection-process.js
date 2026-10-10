/**
 * Canonical hotel-selection process model.
 */

import { SELECTION_MODEL, SELECTION_STATUS } from "./constants.js";

const MODELS = new Set(Object.values(SELECTION_MODEL));
const STATUSES = new Set(Object.values(SELECTION_STATUS));

export function normalizeSelectionModel(raw) {
  const t = String(raw || "")
    .trim()
    .toUpperCase()
    .replace(/\s+/g, "_");
  const aliases = {
    ATTENDEE_SELF_BOOKING: SELECTION_MODEL.SELF_BOOKING_RECOMMENDED_LIST,
    SELF_BOOKING: SELECTION_MODEL.SELF_BOOKING_RECOMMENDED_LIST,
    HOST_INSTITUTION_SELECTION: SELECTION_MODEL.HOST_INSTITUTION_MANAGED,
    HOST_HOTEL: SELECTION_MODEL.OFFICIAL_HOTEL,
    PREFERRED_LIST: SELECTION_MODEL.PREFERRED_HOTEL_LIST,
    RFP: SELECTION_MODEL.PROCUREMENT_RFP,
    PROCUREMENT: SELECTION_MODEL.PROCUREMENT_RFP,
    DIRECT: SELECTION_MODEL.DIRECT_NEGOTIATION,
  };
  if (MODELS.has(t)) return t;
  return aliases[t] || SELECTION_MODEL.UNKNOWN;
}

export function normalizeSelectionStatus(raw) {
  const t = String(raw || "")
    .trim()
    .toUpperCase()
    .replace(/\s+/g, "_");
  return STATUSES.has(t) ? t : SELECTION_STATUS.UNKNOWN;
}

/**
 * Build hotelSelectionProcess record from evidenced fields only.
 */
export function buildHotelSelectionProcess(input = {}) {
  const selectionModel = normalizeSelectionModel(input.selectionModel || input.process);
  let selectionStatus = normalizeSelectionStatus(input.selectionStatus);

  // Derive status carefully from evidence flags — never invent OPEN
  if (selectionStatus === SELECTION_STATUS.UNKNOWN) {
    if (input.hotelListClosed === true || input.selectionClosed === true) {
      selectionStatus = SELECTION_STATUS.CLOSED;
    } else if (input.officialHostLocked && !input.secondaryPartnerOpen) {
      selectionStatus = SELECTION_STATUS.PARTIALLY_PLACED;
    } else if (input.recommendedListPublished && input.subjectOnList === false) {
      selectionStatus = SELECTION_STATUS.UNDER_REVIEW;
    } else if (input.rateSubmissionsAccepted === true || input.rfpOpen === true) {
      selectionStatus = SELECTION_STATUS.OPEN;
    } else if (input.hotelListExpected === true || input.convenioCircularExpected === true) {
      selectionStatus = SELECTION_STATUS.EXPECTED;
    } else if (input.housingPageExistsWithoutHotels === true) {
      selectionStatus = SELECTION_STATUS.EXPECTED;
    } else if (input.procurementNoticeFound === true) {
      selectionStatus = SELECTION_STATUS.EXPECTED;
    }
  }

  return {
    selectionStatus,
    selectionControllerId: input.selectionControllerId || input.demandControllerId || null,
    selectionModel,
    selectionOpenDate: input.selectionOpenDate || null,
    selectionCloseDate: input.selectionCloseDate || null,
    hotelListPublishDate: input.hotelListPublishDate || null,
    rateSubmissionDeadline: input.rateSubmissionDeadline || null,
    RFPStatus: input.RFPStatus || input.rfpStatus || null,
    submissionPath: input.submissionPath || input.publicContactPath || null,
    selectionCriteria: input.selectionCriteria || null,
    knownOfficialHotels: Array.isArray(input.knownOfficialHotels) ? input.knownOfficialHotels : [],
    roomBlockModel: input.roomBlockModel || null,
    overflowModel: input.overflowModel || null,
    selfBookingModel: input.selfBookingModel || null,
    lastVerifiedAt: input.lastVerifiedAt || new Date().toISOString(),
    evidenceSource: input.evidenceSource || null,
    evidenceSnippet: input.evidenceSnippet || null,
    historicalVsCurrent: input.historicalVsCurrent || "CURRENT",
  };
}

/**
 * Map legacy LDI process labels → selection model + status hints.
 */
export function fromLodgingDecisionIntelligenceRow(row = {}) {
  const process = String(row.process || "").toUpperCase();
  const inclusion = String(row.inclusionClass || "").toUpperCase();
  const model = normalizeSelectionModel(process);

  let selectionStatus = SELECTION_STATUS.UNKNOWN;
  let flags = {};
  if (inclusion === "CURRENT_HOTEL_LIST_PUBLISHED") {
    flags.recommendedListPublished = true;
    selectionStatus = SELECTION_STATUS.UNDER_REVIEW;
  }
  if (inclusion === "PAST_HOTEL_LIST_AVAILABLE") {
    flags.historicalSecondaryPartner = true;
    selectionStatus = SELECTION_STATUS.PARTIALLY_PLACED;
  }
  if (inclusion === "NO_HOUSING_EVIDENCE_YET") {
    if (process === "ATTENDEE_SELF_BOOKING") {
      flags.housingPageExistsWithoutHotels = true;
      selectionStatus = SELECTION_STATUS.EXPECTED;
    } else if (process === "DIRECT_NEGOTIATION") {
      flags.convenioCircularExpected = true;
      selectionStatus = SELECTION_STATUS.EXPECTED;
    } else if (process === "UNKNOWN") {
      selectionStatus = SELECTION_STATUS.NOT_STARTED;
    }
  }
  if (process === "HOST_INSTITUTION_SELECTION" || process === "HOST_INSTITUTION_MANAGED") {
    flags.officialHostLocked = true;
    selectionStatus = SELECTION_STATUS.PARTIALLY_PLACED;
  }

  return buildHotelSelectionProcess({
    selectionModel: model,
    selectionStatus,
    evidenceSource: row.evidence || row.evidenceSource,
    evidenceSnippet: row.evidence,
    ...flags,
    knownOfficialHotels: row.knownOfficialHotels || [],
    selfBookingModel:
      model === SELECTION_MODEL.SELF_BOOKING_RECOMMENDED_LIST ? "ATTENDEE_SELF_BOOK" : null,
  });
}
