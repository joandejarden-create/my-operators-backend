/**
 * GDI Live Commercial Quality V1 — reusable customer-facing commercial layer.
 * Applies to ALL GDI hotels. Bethesda is canary only (no hotel-specific rules).
 * Reuses commercial-evidence-v4 guards; does not modify Hygiene V3 / V11 / V12.
 */

import {
  COMMERCIAL_EVIDENCE_V4,
  EVIDENCE_STATUS,
  CONTACT_PATH_CLASS,
  buildEventSeriesIdentity,
  detectLocalNoRoomProgram,
  assessOverflowMarketRealism,
  hasMeaningfulSource,
  classifyContactPath,
  isUnconfirmedFutureCycleInvention,
} from "./commercial-evidence-v4.js";
import {
  buildCustomerExportCsv,
  toCustomerExportRow,
  GDI_CUSTOMER_CSV_HEADERS,
  customerCsvContentDisposition,
  buildCustomerCsvFilename,
} from "./customer-csv-export.js";

export const LIVE_COMMERCIAL_QUALITY_V1 = "gdi_live_commercial_quality_v1";

export const DATE_GRANULARITY = Object.freeze({
  EXACT: "EXACT",
  RANGE: "RANGE",
  MONTH: "MONTH",
  YEAR: "YEAR",
  UNKNOWN: "UNKNOWN",
});

export const NEXT_CYCLE_STATUS = Object.freeze({
  CONFIRMED: "CONFIRMED",
  UNCONFIRMED: "UNCONFIRMED",
  UNKNOWN: "UNKNOWN",
});

export const ROOM_DEMAND_LIVE = Object.freeze({
  LODGING_DEMAND_CONFIRMED: "LODGING_DEMAND_CONFIRMED",
  LODGING_DEMAND_LIKELY: "LODGING_DEMAND_LIKELY",
  LODGING_DEMAND_UNKNOWN: "LODGING_DEMAND_UNKNOWN",
  LOCAL_NO_ROOM_SIGNAL: "LOCAL_NO_ROOM_SIGNAL",
});

export const HOTEL_VALIDATION_REASON = Object.freeze({
  GOOD_LEAD: "GOOD_LEAD",
  NOT_RELEVANT: "NOT_RELEVANT",
  ALREADY_KNOWN_OR_IN_SYSTEM: "ALREADY_KNOWN_OR_IN_SYSTEM",
  WRONG_GEOGRAPHY: "WRONG_GEOGRAPHY",
  NO_ROOM_DEMAND: "NO_ROOM_DEMAND",
  WRONG_CONTACT: "WRONG_CONTACT",
  BAD_DATE: "BAD_DATE",
  DUPLICATE_OR_RELATED: "DUPLICATE_OR_RELATED",
  BAD_SOURCE: "BAD_SOURCE",
  OTHER: "OTHER",
});

export const HOTEL_VALIDATION_REASON_LABEL = Object.freeze({
  GOOD_LEAD: "Good lead",
  NOT_RELEVANT: "Not relevant",
  ALREADY_KNOWN_OR_IN_SYSTEM: "Already known / in our system",
  WRONG_GEOGRAPHY: "Wrong geography",
  NO_ROOM_DEMAND: "No room demand",
  WRONG_CONTACT: "Wrong contact",
  BAD_DATE: "Bad date",
  DUPLICATE_OR_RELATED: "Duplicate or related",
  BAD_SOURCE: "Bad source",
  OTHER: "Other",
});

const VENUE_PURSUE_RE =
  /\b(?:pursue\s+as\s+(?:the\s+)?venue|become\s+(?:the\s+)?venue|bid\s+(?:as\s+)?venue|host\s+the\s+event|primary\s+venue)\b/i;
const OVERFLOW_RE =
  /\b(?:overflow|housing\s+partner|room\s+block\s+partner|stay[- ]to[- ]play|lodging\s+partner)\b/i;
const YEAR_FLOOR_RE = /^20\d{2}-01-01$/;

function unknownLabel(v) {
  if (v == null || v === "") return "Not publicly found";
  const s = String(v).trim();
  if (!s || /^(unknown|unk|n\/?a|tbd|desconocido|—|-)$/i.test(s)) {
    return "Unknown";
  }
  return s;
}

function isYearFloorDate(iso) {
  return YEAR_FLOOR_RE.test(String(iso || "").trim());
}

function titleImpliesYearOnly(opp, year) {
  const t = String(opp.title || opp.eventName || "");
  if (!year) return false;
  if (!t.includes(String(year))) return false;
  // Explicit month/day in title → not year-only
  if (/\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|january|february|march|april|june|july|august|september|october|november|december)\b/i.test(t)) {
    return false;
  }
  if (/\b\d{1,2}[\/\-]\d{1,2}([\/\-]\d{2,4})?\b/.test(t)) return false;
  if (/\b(?:sep|sept|oct|nov|dec|jan|feb|mar|apr|may|jun|jul|aug)\s+\d{1,2}\b/i.test(t)) {
    return false;
  }
  return true;
}

/**
 * Normalize date model. Never leave year-only as YYYY-01-01 for customer display.
 */
export function normalizeDateModel(opp = {}, { nowDate = null } = {}) {
  const corrections = [];
  let eventStartDate = opp.eventStartDate || null;
  let eventEndDate = opp.eventEndDate || null;
  let eventDateStatus = opp.eventDateStatus || null;
  let eventDateGranularity = opp.eventDateGranularity || null;
  let eventYear = opp.eventYear || null;

  const startStr = String(eventStartDate || "").trim();
  const yearMatch = startStr.match(/^(20\d{2})/);
  const year = yearMatch ? yearMatch[1] : null;

  if (isYearFloorDate(startStr) && titleImpliesYearOnly(opp, year)) {
    eventYear = year;
    eventDateGranularity = DATE_GRANULARITY.YEAR;
    eventDateStatus = EVIDENCE_STATUS.INFERRED;
    eventStartDate = null; // do not keep fake Jan 1
    eventEndDate = null;
    corrections.push("DATE_CORRECTION");
  } else if (isYearFloorDate(startStr) && !opp.eventDateEvidenceUrl && !opp.eventDateEvidenceText) {
    // Year-floor without day evidence → treat as YEAR
    eventYear = year;
    eventDateGranularity = DATE_GRANULARITY.YEAR;
    eventDateStatus = EVIDENCE_STATUS.INFERRED;
    eventStartDate = null;
    eventEndDate = null;
    corrections.push("DATE_CORRECTION");
  } else if (eventStartDate && eventEndDate && eventStartDate !== eventEndDate) {
    eventDateGranularity = eventDateGranularity || DATE_GRANULARITY.RANGE;
    eventDateStatus = eventDateStatus || EVIDENCE_STATUS.CONFIRMED;
  } else if (eventStartDate && /^\d{4}-\d{2}-\d{2}$/.test(eventStartDate)) {
    eventDateGranularity = eventDateGranularity || DATE_GRANULARITY.EXACT;
    eventDateStatus = eventDateStatus || EVIDENCE_STATUS.CONFIRMED;
  } else if (year && !eventStartDate) {
    eventYear = year;
    eventDateGranularity = DATE_GRANULARITY.YEAR;
    eventDateStatus = eventDateStatus || EVIDENCE_STATUS.UNKNOWN;
  } else if (!eventStartDate) {
    // Title may carry a year without a day — store YEAR, never invent Jan 1
    const titleYear = String(opp.title || opp.eventName || "").match(/\b(20\d{2})\b/);
    if (titleYear) {
      eventYear = titleYear[1];
      eventDateGranularity = DATE_GRANULARITY.YEAR;
      eventDateStatus = eventDateStatus || EVIDENCE_STATUS.INFERRED;
      if (!corrections.includes("DATE_CORRECTION")) corrections.push("DATE_CORRECTION");
    } else {
      eventDateGranularity = eventDateGranularity || DATE_GRANULARITY.UNKNOWN;
      eventDateStatus = eventDateStatus || EVIDENCE_STATUS.UNKNOWN;
    }
  }

  if (nowDate && eventStartDate) {
    const now = new Date(`${String(nowDate).slice(0, 10)}T00:00:00Z`);
    const start = new Date(`${String(eventStartDate).slice(0, 10)}T00:00:00Z`);
    if (!Number.isNaN(now.getTime()) && !Number.isNaN(start.getTime()) && start < now) {
      if (eventDateStatus !== EVIDENCE_STATUS.PAST) {
        eventDateStatus = EVIDENCE_STATUS.PAST;
        corrections.push("DATE_CORRECTION");
      }
    }
  }

  const displayDate =
    eventDateGranularity === DATE_GRANULARITY.YEAR && eventYear
      ? String(eventYear)
      : eventDateGranularity === DATE_GRANULARITY.UNKNOWN || (!eventStartDate && !eventYear)
        ? "Date not yet confirmed"
        : eventStartDate && eventEndDate && eventStartDate !== eventEndDate
          ? `${eventStartDate} – ${eventEndDate}`
          : eventStartDate || "Date not yet confirmed";

  return {
    eventStartDate,
    eventEndDate,
    eventYear,
    eventDateStatus,
    eventDateGranularity,
    eventDateDisplay: displayDate,
    eventDateEvidenceUrl: opp.eventDateEvidenceUrl || null,
    eventDateEvidenceText: opp.eventDateEvidenceText || null,
    eventDateCheckedAt: opp.eventDateCheckedAt || new Date().toISOString(),
    corrections,
  };
}

/**
 * Historical recurrence alone must not invent active future dates.
 */
export function normalizeFutureCycle(opp = {}) {
  const corrections = [];
  const invented = Boolean(opp.futureCycleInvented) ||
    isUnconfirmedFutureCycleInvention({
      evidenceYears: opp.evidenceYears || [],
      claimedStartDate: opp.eventStartDate,
    });

  const type = String(opp.opportunityType || "").toUpperCase();
  const isFutureType = type === "FUTURE_CYCLE" || type === "FUTURE_WATCH";
  let opportunityType = opp.opportunityType;
  let nextCycleStatus = opp.nextCycleStatus || null;
  let futureCycleEvidenceState = opp.futureCycleEvidenceState || null;

  if (invented || (isFutureType && futureCycleEvidenceState === "FUTURE_CYCLE_UNCONFIRMED")) {
    opportunityType = "FUTURE_WATCH";
    nextCycleStatus = NEXT_CYCLE_STATUS.UNCONFIRMED;
    futureCycleEvidenceState = "FUTURE_CYCLE_UNCONFIRMED";
    corrections.push("FUTURE_CYCLE_CORRECTION");
  } else if (isFutureType && !nextCycleStatus) {
    nextCycleStatus = NEXT_CYCLE_STATUS.UNCONFIRMED;
    corrections.push("FUTURE_CYCLE_CORRECTION");
  }

  const nextCycleDisplay =
    nextCycleStatus === NEXT_CYCLE_STATUS.UNCONFIRMED
      ? "Next cycle not yet confirmed"
      : nextCycleStatus === NEXT_CYCLE_STATUS.CONFIRMED
        ? "Next cycle confirmed"
        : null;

  return {
    opportunityType,
    nextCycleStatus,
    futureCycleEvidenceState,
    nextCycleDisplay,
    corrections,
  };
}

export function normalizeAttendancePeak(opp = {}) {
  const attendance =
    opp.attendance ??
    (known(opp.estimatedAttendance) ? opp.estimatedAttendance : null) ??
    opp.publishedAttendance ??
    null;
  const peakRooms =
    opp.peakRooms ??
    (known(opp.estimatedPeakRooms) ? opp.estimatedPeakRooms : null) ??
    opp.publishedPeakRooms ??
    null;

  const attendanceStatus =
    opp.attendanceStatus ||
    (known(attendance) ? EVIDENCE_STATUS.ESTIMATED : EVIDENCE_STATUS.UNKNOWN);
  const peakRoomsStatus =
    opp.peakRoomsStatus ||
    (known(peakRooms) ? EVIDENCE_STATUS.ESTIMATED : EVIDENCE_STATUS.UNKNOWN);

  // Never auto-derive rooms from attendance
  return {
    attendance,
    attendanceStatus,
    attendanceEvidenceUrl: opp.attendanceEvidenceUrl || null,
    attendanceEvidenceText: opp.attendanceEvidenceText || null,
    attendanceMethodology: opp.attendanceMethodology || null,
    peakRooms,
    peakRoomsStatus,
    peakRoomsEvidenceUrl: opp.peakRoomsEvidenceUrl || null,
    peakRoomsEvidenceText: opp.peakRoomsEvidenceText || null,
    peakRoomsMethodology: opp.peakRoomsMethodology || null,
    estimatedAttendance: attendance ?? opp.estimatedAttendance ?? null,
    estimatedPeakRooms: peakRooms ?? opp.estimatedPeakRooms ?? null,
  };
}

function known(v) {
  if (v == null) return false;
  const s = String(v).trim();
  return Boolean(s) && !/^(unknown|unk|n\/?a|tbd|desconocido)$/i.test(s);
}

export function normalizeRoomDemand(opp = {}) {
  const local = detectLocalNoRoomProgram(opp);
  let roomDemandLive = opp.roomDemandLive || null;
  const corrections = [];

  if (local.localNoRoom) {
    roomDemandLive = ROOM_DEMAND_LIVE.LOCAL_NO_ROOM_SIGNAL;
    corrections.push("ROOM_DEMAND_CORRECTION");
  } else if (
    String(opp.roomDemandStatus || "").toUpperCase().includes("CONFIRMED") ||
    String(opp.roomDemandStatus || "").toUpperCase() === "PUBLISHED_ROOM_BLOCK"
  ) {
    roomDemandLive = ROOM_DEMAND_LIVE.LODGING_DEMAND_CONFIRMED;
  } else if (
    String(opp.roomDemandStatus || "").toUpperCase().includes("ESTIMATED") ||
    String(opp.roomDemandStatus || "").toUpperCase().includes("LIKELY")
  ) {
    roomDemandLive = ROOM_DEMAND_LIVE.LODGING_DEMAND_LIKELY;
  } else {
    roomDemandLive = roomDemandLive || ROOM_DEMAND_LIVE.LODGING_DEMAND_UNKNOWN;
  }

  const hotelDemandThesis =
    opp.hotelDemandThesis ||
    opp.hotelOpportunityThesis ||
    opp.hotelWinThesis ||
    opp.bethesdaWinThesis ||
    null;

  return {
    roomDemandLive,
    roomDemandStatus: opp.roomDemandStatus || null,
    roomDemandConfidence: opp.roomDemandConfidence || "UNKNOWN",
    hotelDemandThesis,
    hotelOpportunityThesis: hotelDemandThesis,
    localAttendanceOnly: local.localNoRoom || Boolean(opp.localAttendanceOnly),
    corrections,
  };
}

/**
 * Single reusable action reconciler for all GDI hotels.
 */
export function reconcileSuggestedAction(opp = {}, catchment = {}) {
  const corrections = [];
  const raw = String(opp.recommendedAction || opp.suggestedAction || "").trim();
  const venueSelected =
    /FULLY_PLACED|VENUE_LOCKED|HOST_CONFIRMED|SELECTED/i.test(
      String(opp.venueSourcingStatus || opp.venueStatus || "")
    ) || opp.venueFullyPlaced === true;

  const local = detectLocalNoRoomProgram(opp);
  const overflowAssess = assessOverflowMarketRealism({
    eventSubmarket: catchment.eventSubmarket || opp.eventSubmarket || opp.eventLocationSummary,
    hotelSubmarkets: catchment.hotelSubmarkets || opp.hotelSubmarkets || [],
    distanceKm: catchment.distanceKm ?? opp.overflowDistanceKm ?? null,
    housingClusterMentionsHotelCorridor: catchment.housingClusterMentionsHotelCorridor ??
      opp.housingClusterMentionsHotelCorridor,
    overflowEvidence: catchment.overflowEvidence ?? opp.overflowEvidence,
  });

  const dateModel = normalizeDateModel(opp, { nowDate: catchment.nowDate });
  const isPast = dateModel.eventDateStatus === EVIDENCE_STATUS.PAST;
  const future = normalizeFutureCycle(opp);
  const hasSource = hasMeaningfulSource(opp);
  const contactClass = classifyContactPath(opp.primaryContact || {}, opp.backupContacts || []);
  const hasActionPath =
    contactClass !== CONTACT_PATH_CLASS.NO_CONTACT ||
    hasSource ||
    Boolean(opp.officialHousingUrl || opp.officialSourcingUrl);

  let recommendedAction = raw || null;
  let actionBasis = opp.actionBasis || null;
  let customerFacingState = opp.customerFacingState || "ACTIVE";

  if (!hasSource && !hasActionPath) {
    customerFacingState = "INSUFFICIENT_EVIDENCE";
    recommendedAction = recommendedAction || "Monitor — insufficient actionable path";
    actionBasis = "sourceless_and_no_action_path";
    corrections.push("SOURCE_CORRECTION", "ACTION_CORRECTION");
  }

  if (venueSelected && VENUE_PURSUE_RE.test(raw)) {
    recommendedAction =
      "Do not pursue as venue — primary venue already selected. Evaluate overflow/housing only if evidence supports lodging need.";
    actionBasis = "venue_already_selected";
    corrections.push("VENUE_CORRECTION", "ACTION_CORRECTION");
  }

  if (OVERFLOW_RE.test(raw) && !overflowAssess.plausible) {
    recommendedAction =
      "Do not recommend overflow for this hotel — geographic / housing-cluster fit not supported.";
    actionBasis = `overflow_not_plausible:${(overflowAssess.reasons || []).join(",")}`;
    corrections.push("OVERFLOW_CORRECTION", "GEOGRAPHY_CORRECTION", "ACTION_CORRECTION");
  }

  if (local.localNoRoom && OVERFLOW_RE.test(raw + " " + (opp.opportunityType || ""))) {
    recommendedAction =
      "Do not pursue room block — local / no-guestroom program signals.";
    actionBasis = "local_no_room";
    corrections.push("ROOM_DEMAND_CORRECTION", "ACTION_CORRECTION");
  }

  if (isPast) {
    recommendedAction =
      "Past cycle — do not pursue this cycle. Monitor for confirmed next cycle / relationship only.";
    actionBasis = "past_event_cycle";
    corrections.push("ACTION_CORRECTION");
    customerFacingState = customerFacingState === "ACTIVE" ? "WATCH" : customerFacingState;
  }

  if (future.nextCycleStatus === NEXT_CYCLE_STATUS.UNCONFIRMED) {
    recommendedAction =
      recommendedAction ||
      "Watch — next cycle not yet confirmed. Do not treat as active pursuit date.";
    actionBasis = actionBasis || "future_cycle_unconfirmed";
    customerFacingState = "FUTURE_WATCH";
  }

  return {
    recommendedAction,
    suggestedAction: recommendedAction,
    actionBasis,
    actionReconciledV1: true,
    customerFacingState,
    overflowPlausible: overflowAssess.plausible,
    overflowReasons: overflowAssess.reasons,
    contactPathClass: contactClass,
    hasMeaningfulSource: hasSource,
    hasActionPath,
    corrections: [...new Set(corrections)],
  };
}

export function buildRelatedOpportunityGroups(opportunities = []) {
  const bySeries = new Map();
  for (const o of opportunities) {
    const id = buildEventSeriesIdentity(o);
    const key = id.eventSeriesId || `solo:${o.id}`;
    if (!bySeries.has(key)) bySeries.set(key, []);
    bySeries.get(key).push(o.id);
  }
  const relatedById = {};
  for (const [, ids] of bySeries) {
    if (ids.length < 2) continue;
    for (const id of ids) {
      relatedById[id] = ids.filter((x) => x !== id);
    }
  }
  return relatedById;
}

/**
 * Apply full live commercial quality projection (read-path safe; also used for writes).
 */
export function applyLiveCommercialQuality(opp = {}, ctx = {}) {
  const date = normalizeDateModel(opp, { nowDate: ctx.nowDate });
  const future = normalizeFutureCycle({ ...opp, ...date });
  const attPeak = normalizeAttendancePeak(opp);
  const room = normalizeRoomDemand(opp);
  const identity = buildEventSeriesIdentity({
    ...opp,
    ...date,
    eventStartDate: date.eventStartDate || (date.eventYear ? `${date.eventYear}-06-15` : null),
  });
  // Prefer explicit year for cycle identity when granularity is YEAR
  if (date.eventYear && identity.eventSeriesId) {
    identity.eventCycleId = `cycle:${identity.eventSeriesId}|${date.eventYear}`;
  }
  const action = reconcileSuggestedAction(
    { ...opp, ...date, ...future, ...room },
    ctx.catchment || { nowDate: ctx.nowDate }
  );

  const correctionClasses = [
    ...date.corrections,
    ...future.corrections,
    ...room.corrections,
    ...action.corrections,
  ];

  if (!opp.eventSeriesId && identity.eventSeriesId) {
    correctionClasses.push("SERIES_GROUPING");
  }

  const aliases = Array.isArray(opp.eventAliases) ? opp.eventAliases : [];
  const canonicalEventName =
    opp.canonicalEventName || identity.normalizedEventName || opp.title || null;

  return {
    ...opp,
    ...date,
    ...future,
    ...attPeak,
    ...room,
    ...identity,
    ...action,
    canonicalEventName,
    eventAliases: aliases,
    organizationAliases: opp.organizationAliases || [],
    newToGdi: opp.newToGdi !== false,
    newToHotel: null,
    commercialQualityV1: {
      version: LIVE_COMMERCIAL_QUALITY_V1,
      evidenceVersion: COMMERCIAL_EVIDENCE_V4,
      correctionClasses: [...new Set(correctionClasses)],
      appliedAt: new Date().toISOString(),
    },
  };
}

export function classifyBethesdaStyleCorrections(before, after) {
  const classes = new Set(after?.commercialQualityV1?.correctionClasses || []);
  if (!classes.size) {
    // Diff heuristics
    if (String(before.eventStartDate) !== String(after.eventStartDate)) {
      classes.add("DATE_CORRECTION");
    }
    if (String(before.opportunityType) !== String(after.opportunityType)) {
      classes.add("FUTURE_CYCLE_CORRECTION");
    }
    if (String(before.recommendedAction) !== String(after.recommendedAction)) {
      classes.add("ACTION_CORRECTION");
    }
    if (!before.eventSeriesId && after.eventSeriesId) classes.add("SERIES_GROUPING");
  }
  if (!classes.size) return ["NO_CHANGE"];
  return [...classes];
}

export function formatCustomerDate(opp) {
  if (opp.eventDateDisplay) return opp.eventDateDisplay;
  const n = normalizeDateModel(opp);
  return n.eventDateDisplay;
}

export function unknownDisplay(v) {
  return unknownLabel(v);
}

/** @deprecated Use toCustomerExportRow — kept as alias for older callers. */
export function toExportRow(opp = {}, _hotel = {}) {
  return toCustomerExportRow(opp);
}

export function rowsToCsv(rows = []) {
  if (!rows.length) {
    return GDI_CUSTOMER_CSV_HEADERS.join(",") + "\r\n";
  }
  const headers = Object.keys(rows[0]);
  const escCell = (v) => {
    let s = v == null ? "" : String(v);
    if (/^[=+\-@|\t\r]/.test(s)) {
      s = `'${s}`;
    }
    if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  };
  return (
    [headers.join(","), ...rows.map((r) => headers.map((h) => escCell(r[h])).join(","))].join(
      "\r\n"
    ) + "\r\n"
  );
}

/**
 * Customer sales CSV (UTF-8 BOM). hotel/filterState kept for call-site compat;
 * hotelId/schema are NOT written into the file body.
 */
export function buildExportCsv(opportunities, _hotel = {}, _filterState = {}) {
  return buildCustomerExportCsv(opportunities);
}

export {
  buildCustomerExportCsv,
  toCustomerExportRow,
  GDI_CUSTOMER_CSV_HEADERS,
  customerCsvContentDisposition,
  buildCustomerCsvFilename,
};

