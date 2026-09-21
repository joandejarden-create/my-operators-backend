/**
 * GDI Discovery Hygiene V3 — fixes Wave 2 false-actionable discovery.
 * Does not break V2 gates; imports V2 helpers + venue-locked V11.
 * V3 owns the final TRUE_ACTIONABLE decision.
 */

import {
  HYGIENE_STATE,
  classifyGeography,
  classifySourceAuthority,
  classifyRoomDemand,
  GEO_CLASS,
  SOURCE_TIER,
  evidenceBackedSourcingStatus,
} from "./discovery-hygiene-v2.js";
import {
  classifyVenueLockedOpportunity,
  detectOverflowEvidence,
  VENUE_LOCKED_SUBSTATE,
} from "./venue-locked-classification-v11.js";

export const DISCOVERY_HYGIENE_V3 = "gdi_discovery_hygiene_v3";

export const EVENT_SEMANTIC_TYPE = Object.freeze({
  EVENT: "EVENT",
  EVENT_SERIES: "EVENT_SERIES",
  EVENT_CYCLE: "EVENT_CYCLE",
  PROGRAM: "PROGRAM",
  PLAN: "PLAN",
  REPORT: "REPORT",
  INITIATIVE: "INITIATIVE",
  ORGANIZATION: "ORGANIZATION",
  VENUE: "VENUE",
  OTHER: "OTHER",
});

export const DATE_CONFIDENCE = Object.freeze({
  DATE_CONFIRMED: "DATE_CONFIRMED",
  DATE_INFERRED: "DATE_INFERRED",
  DATE_UNKNOWN: "DATE_UNKNOWN",
  DATE_PAST: "DATE_PAST",
});

export const ACTIONABILITY_V3 = Object.freeze({
  TRUE_ACTIONABLE: "TRUE_ACTIONABLE",
  VALID_WATCH: "VALID_WATCH",
  VALID_FUTURE: "VALID_FUTURE",
  INSUFFICIENT: "INSUFFICIENT",
  INVALID: "INVALID",
});

export const FAILURE_CLASS_V3 = Object.freeze({
  NO_OPEN_SOURCING_EVIDENCE: "NO_OPEN_SOURCING_EVIDENCE",
  COMPETITOR_HOST_LOCKED: "COMPETITOR_HOST_LOCKED",
  PAST_EVENT: "PAST_EVENT",
  NON_EVENT_CONTENT: "NON_EVENT_CONTENT",
  NO_HOTEL_ROOM_THESIS: "NO_HOTEL_ROOM_THESIS",
  NO_OVERFLOW_EVIDENCE: "NO_OVERFLOW_EVIDENCE",
  EVENT_IDENTITY_WEAK: "EVENT_IDENTITY_WEAK",
  DATE_UNCERTAIN: "DATE_UNCERTAIN",
  SOURCE_SEMANTICS_FAILURE: "SOURCE_SEMANTICS_FAILURE",
  OTHER: "OTHER",
});

/** Conceptual map of V3 actionability → V2 HYGIENE_STATE. */
export const ACTIONABILITY_TO_HYGIENE_STATE = Object.freeze({
  [ACTIONABILITY_V3.TRUE_ACTIONABLE]: HYGIENE_STATE.VALID_ACTIONABLE,
  [ACTIONABILITY_V3.VALID_WATCH]: HYGIENE_STATE.VALID_WATCH,
  [ACTIONABILITY_V3.VALID_FUTURE]: HYGIENE_STATE.VALID_FUTURE,
  [ACTIONABILITY_V3.INSUFFICIENT]: HYGIENE_STATE.INSUFFICIENT,
  [ACTIONABILITY_V3.INVALID]: HYGIENE_STATE.INVALID,
});

const NON_EVENT_TITLE_RE =
  /\b(?:action\s+plan|open\s+government|strategic\s+plan|annual\s+report|policy\s+(?:brief|paper|framework)|campaign\s+overview|research\s+project)\b/i;
const STANDALONE_INITIATIVE_RE = /\b(?:initiative|program\s+overview)\b/i;
const EVENT_MARKER_RE =
  /\b(?:conference|congress|symposium|forum|summit|convention|expo|exhibition|meeting|workshop|seminar|tradeshow|trade\s+show|annual\s+meeting|assembly)\b/i;
const SERIES_CYCLE_RE =
  /\b(?:series|annual\s+cycle|edition|biennial|recurring|year(?:ly)?\s+event)\b/i;
const WEAK_DEMAND_PHRASE_RE =
  /\b(?:likely\s+needs?\s+rooms?|potential\s+for\s+rooms?|indicating\s+a\s+need|can\s+cater|well[- ]positioned\s+to\s+accommodate)\b/i;
const STRONG_HOUSING_LANG_RE =
  /\b(?:room\s+block|housing\s+program|official\s+hotels?|peak\s+rooms?|sleeping\s+rooms?|hotel\s+block|verified\s+housing|estimated\s+room\s+demand|attendee\s+housing)\b/i;

const OPEN_CODES = new Set([
  "RFP_ACTIVE_SOURCING",
  "HOTEL_VENUE_TBD",
  "OPEN_UNRESOLVED",
  "PARTIALLY_PLACED",
  "PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE",
]);

const GEO_FIT = new Set([
  GEO_CLASS.PRIMARY_MARKET,
  GEO_CLASS.LOCAL,
  GEO_CLASS.SECONDARY_FEEDER,
  GEO_CLASS.DESTINATION_RELEVANT,
  GEO_CLASS.REGIONAL_RELEVANT,
]);

const NON_EVENT_TYPES = new Set([
  EVENT_SEMANTIC_TYPE.PLAN,
  EVENT_SEMANTIC_TYPE.REPORT,
  EVENT_SEMANTIC_TYPE.INITIATIVE,
  EVENT_SEMANTIC_TYPE.PROGRAM,
  EVENT_SEMANTIC_TYPE.ORGANIZATION,
  EVENT_SEMANTIC_TYPE.VENUE,
]);

const OVERLAY_FIELDS = [
  "venue",
  "hostHotel",
  "meetingVenue",
  "sleepingRoomHotels",
  "eventStartDate",
  "eventEndDate",
  "semanticTypeHint",
  "overflowEvidence",
  "openSourcingEvidence",
  "hotelDemandEvidence",
  "pastEventConfirmed",
  "recurrenceEvidence",
  "fullyPlaced",
  "meetingVenueRoomsOpen",
];

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function collectText(opp = {}) {
  const bits = [
    opp.title,
    opp.organizationName,
    opp.destinationStatus,
    opp.location,
    opp.venue,
    opp.hostHotel,
    opp.meetingVenue,
    opp.whyNow,
    opp.hotelOpportunityThesis,
    opp.summaryWhyHotel,
    opp.housingEvidence,
    opp.roomDemandEvidence,
    opp.commercialThesis,
    opp.officialSource,
  ];
  for (const e of opp.evidence || opp.evidenceSources || []) {
    if (!e) continue;
    if (typeof e === "string") bits.push(e);
    else bits.push(e.url, e.title, e.value, e.sourceTitle);
  }
  return bits.filter(Boolean).join(" \n ");
}

/** Evidence-only blob — excludes sales thesis copy that invents "room block" language. */
function collectEvidenceText(opp = {}) {
  const bits = [
    opp.title,
    opp.organizationName,
    opp.destinationStatus,
    opp.location,
    opp.venue,
    opp.hostHotel,
    opp.meetingVenue,
    opp.housingEvidence,
    opp.roomDemandEvidence,
    opp.officialSource,
    opp.venueWatch,
  ];
  for (const e of opp.evidence || opp.evidenceSources || []) {
    if (!e) continue;
    if (typeof e === "string") bits.push(e);
    else bits.push(e.url, e.title, e.value, e.sourceTitle, e.snippet);
  }
  return bits.filter(Boolean).join(" \n ");
}

function parseDateOnly(value) {
  if (!value) return null;
  const s = String(value).slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return null;
  const d = new Date(`${s}T00:00:00Z`);
  return Number.isNaN(d.getTime()) ? null : d;
}

function todayUtc(nowDate) {
  const raw = parseDateOnly(nowDate) || new Date();
  return new Date(Date.UTC(raw.getUTCFullYear(), raw.getUTCMonth(), raw.getUTCDate()));
}

function isMultiDay(opp = {}) {
  const a = parseDateOnly(opp.eventStartDate);
  const b = parseDateOnly(opp.eventEndDate);
  if (a && b) {
    const days = Math.floor((b - a) / 86400000) + 1;
    if (days >= 2) return true;
  }
  return /\bmulti[- ]?day|several\s+days|\d+\s*[-–]\s*\d+\s+(?:day|night)/i.test(
    collectText(opp)
  );
}

function mergeEvidenceOverlay(opp = {}, overlay = null) {
  if (!overlay || typeof overlay !== "object") return { ...opp };
  const working = { ...opp };
  for (const k of OVERLAY_FIELDS) {
    if (overlay[k] !== undefined && overlay[k] !== null) working[k] = overlay[k];
  }
  if (Array.isArray(overlay.sourceUrls) && overlay.sourceUrls.length) {
    const existing = Array.isArray(working.evidenceSources)
      ? [...working.evidenceSources]
      : [];
    for (const url of overlay.sourceUrls) existing.push({ url });
    working.evidenceSources = existing;
    if (!working.officialSource) working.officialSource = overlay.sourceUrls[0];
  }
  return working;
}

function isRealEventType(t) {
  return (
    t === EVENT_SEMANTIC_TYPE.EVENT ||
    t === EVENT_SEMANTIC_TYPE.EVENT_CYCLE ||
    t === EVENT_SEMANTIC_TYPE.EVENT_SERIES
  );
}

export function classifyEventSemanticType(opp = {}) {
  const hint = String(opp.semanticTypeHint || "").toUpperCase();
  if (hint && EVENT_SEMANTIC_TYPE[hint]) return EVENT_SEMANTIC_TYPE[hint];

  const title = norm(opp.title);
  const blob = `${title} ${collectText(opp)}`;

  if (NON_EVENT_TITLE_RE.test(title) || NON_EVENT_TITLE_RE.test(blob)) {
    if (/\bannual\s+report\b/i.test(blob)) return EVENT_SEMANTIC_TYPE.REPORT;
    if (/\bresearch\s+project\b/i.test(blob)) return EVENT_SEMANTIC_TYPE.OTHER;
    return EVENT_SEMANTIC_TYPE.PLAN;
  }
  if (STANDALONE_INITIATIVE_RE.test(title) && !EVENT_MARKER_RE.test(title)) {
    return /\binitiative\b/i.test(title)
      ? EVENT_SEMANTIC_TYPE.INITIATIVE
      : EVENT_SEMANTIC_TYPE.PROGRAM;
  }
  if (/\b(?:venue|hotel)\s+(?:profile|directory|listing)\b/i.test(title)) {
    return EVENT_SEMANTIC_TYPE.VENUE;
  }
  if (
    /\b(?:association|chamber|society|institute)\b/i.test(title) &&
    !EVENT_MARKER_RE.test(title) &&
    !opp.eventStartDate
  ) {
    return EVENT_SEMANTIC_TYPE.ORGANIZATION;
  }
  if (EVENT_MARKER_RE.test(title) || EVENT_MARKER_RE.test(blob)) {
    if (SERIES_CYCLE_RE.test(blob) || /\bcycle\b/i.test(String(opp.opportunityType || ""))) {
      return EVENT_SEMANTIC_TYPE.EVENT_CYCLE;
    }
    if (/\bseries\b/i.test(blob)) return EVENT_SEMANTIC_TYPE.EVENT_SERIES;
    return EVENT_SEMANTIC_TYPE.EVENT;
  }
  if (opp.eventStartDate || opp.eventEndDate) return EVENT_SEMANTIC_TYPE.EVENT;
  return EVENT_SEMANTIC_TYPE.OTHER;
}

export function classifyDateConfidence(opp = {}, { nowDate } = {}) {
  const now = todayUtc(nowDate);

  if (opp.pastEventConfirmed === true) {
    return {
      confidence: DATE_CONFIDENCE.DATE_PAST,
      eventStartDate: opp.eventStartDate || null,
      eventEndDate: opp.eventEndDate || null,
      reasons: ["overlay_past_event_confirmed"],
      highSupport: false,
    };
  }

  const end = parseDateOnly(opp.eventEndDate);
  const start = parseDateOnly(opp.eventStartDate);
  const compare = end || start;
  if (compare && compare.getTime() < now.getTime()) {
    return {
      confidence: DATE_CONFIDENCE.DATE_PAST,
      eventStartDate: opp.eventStartDate || null,
      eventEndDate: opp.eventEndDate || null,
      reasons: ["event_date_before_now"],
      highSupport: false,
    };
  }

  if (start && /^\d{4}-\d{2}-\d{2}$/.test(String(opp.eventStartDate || "").slice(0, 10))) {
    const inferredOnly = /INFERRED|APPROX|TBD|estimated/i.test(
      String(opp.dateConfidence || opp.dateStatus || "")
    );
    if (inferredOnly) {
      return {
        confidence: DATE_CONFIDENCE.DATE_INFERRED,
        eventStartDate: opp.eventStartDate,
        eventEndDate: opp.eventEndDate || null,
        reasons: ["date_marked_inferred"],
        highSupport: Boolean(opp.eventEndDate) || isMultiDay(opp),
      };
    }
    return {
      confidence: DATE_CONFIDENCE.DATE_CONFIRMED,
      eventStartDate: opp.eventStartDate,
      eventEndDate: opp.eventEndDate || null,
      reasons: ["iso_start_present"],
      highSupport: true,
    };
  }

  if (
    /\b(?:q[1-4]\s*20\d{2}|20\d{2}\s*[-–]\s*20\d{2}|(?:fall|spring|summer|winter)\s*20\d{2})\b/i.test(
      collectText(opp)
    )
  ) {
    return {
      confidence: DATE_CONFIDENCE.DATE_INFERRED,
      eventStartDate: opp.eventStartDate || null,
      eventEndDate: opp.eventEndDate || null,
      reasons: ["season_or_quarter_inferred"],
      highSupport: false,
    };
  }

  return {
    confidence: DATE_CONFIDENCE.DATE_UNKNOWN,
    eventStartDate: opp.eventStartDate || null,
    eventEndDate: opp.eventEndDate || null,
    reasons: ["no_usable_event_date"],
    highSupport: false,
  };
}

export function classifyHotelDemandThesis(opp = {}) {
  const reasons = [];
  const blob = collectEvidenceText(opp);
  const thesisBlob = collectText(opp);
  const status = String(opp.roomDemandStatus || "");
  const roomsClass = classifyRoomDemand(opp);
  const multiDay = isMultiDay(opp);
  const title = norm(opp.title);
  const housingStatusEvidenceBacked =
    /VERIFIED_HOUSING|VERIFIED_ROOM_BLOCK|ESTIMATED_ROOM_DEMAND/i.test(status) &&
    (Boolean(opp.hotelDemandEvidence) ||
      Boolean(opp.housingEvidence) ||
      Boolean(opp.roomDemandEvidence) ||
      STRONG_HOUSING_LANG_RE.test(blob));

  if (opp.hotelDemandEvidence) {
    return { ok: true, strength: "STRONG", reasons: ["overlay_hotel_demand_evidence"] };
  }

  // Weak phrases do not create demand by themselves — but they must not
  // veto demand when independent event/housing signals already exist.
  if (WEAK_DEMAND_PHRASE_RE.test(thesisBlob)) {
    const hasIndependentDemand =
      STRONG_HOUSING_LANG_RE.test(blob) ||
      housingStatusEvidenceBacked ||
      EVENT_MARKER_RE.test(title) ||
      multiDay ||
      roomsClass === "ROOMS_CONFIRMED" ||
      roomsClass === "ROOMS_ESTIMATED";
    if (!hasIndependentDemand) {
      return { ok: false, strength: "NONE", reasons: ["weak_demand_phrase_only"] };
    }
    reasons.push("weak_phrase_present_but_overridden");
  }

  let strength = "NONE";
  if (housingStatusEvidenceBacked && /VERIFIED_HOUSING|VERIFIED_ROOM_BLOCK/i.test(status)) {
    strength = "STRONG";
    reasons.push("verified_housing_program_evidence_backed");
  }
  if (STRONG_HOUSING_LANG_RE.test(blob)) {
    strength = "STRONG";
    reasons.push("explicit_housing_or_room_block_language");
  }
  if (multiDay) {
    reasons.push("multi_day_span");
    if (strength !== "STRONG") strength = "WEAK";
  }
  if (
    housingStatusEvidenceBacked &&
    /ESTIMATED_ROOM_DEMAND|ESTIMATED|STRONG_ROOM/i.test(status) &&
    multiDay
  ) {
    strength = "STRONG";
    reasons.push("estimated_room_demand_multi_day");
  }
  if (/\b(?:congress|conference|symposium|forum|summit)\b/i.test(title) && multiDay) {
    strength = "STRONG";
    reasons.push("named_event_type_multi_day");
  }
  if (roomsClass === "ROOMS_CONFIRMED" || roomsClass === "ROOMS_ESTIMATED") {
    if (strength !== "STRONG") strength = multiDay ? "STRONG" : "WEAK";
    reasons.push(`room_demand_class:${roomsClass}`);
  }
  // Named event identity with no housing → WEAK (watch-eligible), not NONE.
  if (strength === "NONE" && EVENT_MARKER_RE.test(title)) {
    strength = "WEAK";
    reasons.push("named_event_identity_weak_demand");
  }
  if (strength === "NONE" && !reasons.length) reasons.push("no_hotel_room_thesis");

  return { ok: strength === "STRONG" || strength === "WEAK", strength, reasons };
}

/**
 * CRITICAL: UNKNOWN stays UNKNOWN — never map UNKNOWN → OPEN.
 * Unsupported engine RFP labels without evidence are NOT open (V2).
 */
export function classifyOpenSourcingEvidence(opp = {}) {
  const reasons = [];

  if (opp.openSourcingEvidence) {
    return {
      open: true,
      status: "OPEN_UNRESOLVED",
      supported: true,
      reasons: ["overlay_open_sourcing_evidence"],
    };
  }

  const sourcing = evidenceBackedSourcingStatus(opp);
  const status = sourcing.status || "UNKNOWN";

  if (status === "UNKNOWN") {
    reasons.push("status_unknown_not_open");
    if (sourcing.downgraded) {
      reasons.push(`downgraded_from:${sourcing.priorStatus || "open_label"}`);
    }
    return {
      open: false,
      status: "UNKNOWN",
      supported: false,
      reasons,
      priorStatus: sourcing.priorStatus || null,
      downgraded: Boolean(sourcing.downgraded),
    };
  }

  const isOpenCode = OPEN_CODES.has(status);
  const open = Boolean(isOpenCode && sourcing.supported === true);
  if (isOpenCode && !sourcing.supported) {
    reasons.push("open_label_unsupported_by_evidence");
  } else if (open) {
    reasons.push(`supported_open:${status}`);
  } else {
    reasons.push(`non_open_status:${status}`);
  }

  return {
    open,
    status,
    supported: Boolean(sourcing.supported) && status !== "UNKNOWN",
    reasons,
    priorStatus: sourcing.priorStatus || null,
    downgraded: Boolean(sourcing.downgraded),
  };
}

const MEETING_VENUE_NO_SLEEP_RE =
  /\b(?:convention\s+cent(?:er|re)|centro\s+de\s+convenciones|universidad|university|udem|estoa|stadium|estadio|arena|auditorium|expo\s+center|exhibition\s+(?:hall|centre|center))\b/i;

export function detectCompetitorOrHostLock(opp = {}, subjectHotel = {}) {
  const reasons = [];
  const v11 = classifyVenueLockedOpportunity(opp, subjectHotel);
  const hostName =
    String(opp.hostHotel || opp.venue || v11.venueEvidence || "").trim() || null;
  const meetingVenueName = String(opp.meetingVenue || "").trim();
  const fullyPlaced =
    v11.substate === VENUE_LOCKED_SUBSTATE.FULLY_PLACED ||
    Boolean(opp.fullyPlaced) ||
    /FULLY_PLACED/i.test(String(opp.venueSourcingStatus || ""));
  const meetingVenueOnly =
    v11.substate === VENUE_LOCKED_SUBSTATE.MEETING_VENUE_ROOMS_OPEN ||
    Boolean(opp.meetingVenueRoomsOpen) ||
    (Boolean(meetingVenueName) && MEETING_VENUE_NO_SLEEP_RE.test(meetingVenueName)) ||
    /\bmeeting\s+(?:space|venue)\s+only\b/i.test(collectText(opp));
  const namedHostNotSubject =
    Boolean(hostName) &&
    subjectHotel?.name &&
    !norm(hostName).includes(norm(subjectHotel.name).slice(0, 12)) &&
    !norm(subjectHotel.name).includes(norm(hostName).slice(0, 12));
  const locked =
    v11.substate !== VENUE_LOCKED_SUBSTATE.NOT_VENUE_LOCKED ||
    Boolean(meetingVenueName) ||
    Boolean(hostName) ||
    namedHostNotSubject ||
    /PRIMARY_VENUE_SELECTED|PRIMARY_SELECTED/i.test(
      String(opp.venueSourcingStatus || "")
    );

  if (v11.reasons?.length) reasons.push(...v11.reasons);
  if (fullyPlaced) reasons.push("fully_placed");
  if (meetingVenueOnly) reasons.push("meeting_venue_rooms_open");
  if (meetingVenueName) reasons.push(`meeting_venue:${meetingVenueName}`);
  if (namedHostNotSubject) reasons.push("competitor_or_named_host");

  return {
    locked: Boolean(locked || fullyPlaced || meetingVenueOnly),
    hostName: hostName || meetingVenueName || null,
    meetingVenueOnly,
    fullyPlaced,
    reasons,
    v11Substate: v11.substate,
    opportunityTypeHint: v11.opportunityType || null,
  };
}

export function classifyOverflowThesis(opp = {}) {
  // Explicit false from evidence overlay wins (verified no-overflow).
  if (opp.overflowEvidence === false) {
    return { ok: false, reasons: ["overlay_no_overflow_evidence"] };
  }
  if (opp.overflowEvidence) {
    return { ok: true, reasons: ["overlay_overflow_evidence"] };
  }
  const status = String(
    opp.venueSourcingStatus || opp.sourcingStatus || opp.venueStatus || ""
  );
  // Explicit no-overflow / closed statuses win (avoid V11 substring false-positive
  // where NO_OVERFLOW matches /OVERFLOW/).
  if (
    /NO_OVERFLOW_EVIDENCE|FULLY_PLACED|CURRENT_CYCLE_CLOSED/i.test(status) &&
    !/OVERFLOW_POSSIBLE/i.test(status)
  ) {
    return { ok: false, reasons: ["status_asserts_no_overflow"] };
  }
  // Only evidence fields — never sales thesis — may assert overflow/housing.
  // Do not trust engine opportunityType=OVERFLOW_HOUSING alone.
  const evidenceOpp = {
    ...opp,
    whyNow: undefined,
    hotelOpportunityThesis: undefined,
    summaryWhyHotel: undefined,
    commercialThesis: undefined,
    opportunityType: undefined,
    venueSourcingStatus: /NO_OVERFLOW/i.test(status) ? "" : opp.venueSourcingStatus,
  };
  // Engine VERIFIED_HOUSING labels alone are not overflow evidence.
  if (
    /VERIFIED_HOUSING|HOUSING_PROGRAM/i.test(String(opp.roomDemandStatus || "")) &&
    !opp.housingEvidence &&
    !opp.roomDemandEvidence &&
    !STRONG_HOUSING_LANG_RE.test(collectEvidenceText(opp))
  ) {
    evidenceOpp.roomDemandStatus = "UNKNOWN";
  }
  const det = detectOverflowEvidence(evidenceOpp);
  if (det.present) {
    return { ok: true, reasons: [det.evidence || "overflow_evidence_present"] };
  }
  return { ok: false, reasons: ["no_overflow_evidence"] };
}

function isFarFuture(opp, nowDate) {
  const now = todayUtc(nowDate);
  const start = parseDateOnly(opp.eventStartDate);
  if (!start) return false;
  const nowYear = now.getUTCFullYear();
  const startYear = start.getUTCFullYear();
  // Jan+: start year >= nowYear+1 is far future; when now is 2026, start >= 2027.
  return startYear >= nowYear + 1 || (nowYear === 2026 && startYear >= 2027);
}

function hasNearTermOpenHousing(opp, openSourcing) {
  return Boolean(
    openSourcing.open ||
      opp.openSourcingEvidence ||
      /\b(?:housing\s+(?:still\s+)?open|room\s+block\s+(?:still\s+)?open|accepting\s+hotel)\b/i.test(
        collectText(opp)
      )
  );
}

function eventIdentityKey(opp = {}) {
  const title = norm(opp.title)
    .replace(/\s*[-–|].*$/, "")
    .replace(/\b20\d{2}\b/g, "")
    .replace(/\b(12th|xviii|annual)\b/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
  const org = norm(opp.organizationName).slice(0, 40);
  const date = String(opp.eventStartDate || "").slice(0, 10);
  const market = norm(opp.destinationStatus || opp.location)
    .replace(/[^a-z0-9]+/g, " ")
    .slice(0, 40);
  return `${title}|${date}|${market}|${org}`;
}

function looseTitleKey(opp = {}) {
  const loose = norm(opp.title)
    .replace(/\s*[-–].*$/, "")
    .replace(/\b20\d{2}\b/g, "")
    .replace(/\b(12th|xviii)\b/g, "")
    .trim();
  return `loose:${loose}|${String(opp.eventStartDate || "").slice(0, 10)}`;
}

function credibleSource(source) {
  return (
    source.tier === SOURCE_TIER.TIER_1 ||
    source.tier === SOURCE_TIER.TIER_2 ||
    (source.tier === SOURCE_TIER.TIER_3 && !source.aggregatorOnly)
  );
}

function pack(partial) {
  const actionability = partial.actionability;
  return {
    actionability,
    hygieneState:
      ACTIONABILITY_TO_HYGIENE_STATE[actionability] || HYGIENE_STATE.INSUFFICIENT,
    failureClass: partial.failureClass || null,
    semanticType: partial.semanticType || null,
    date: partial.dateInfo || null,
    geo: partial.geo || null,
    source: partial.source || null,
    demand: partial.demand || null,
    openSourcing: partial.openSourcing || null,
    hostLock: partial.hostLock || null,
    overflow: partial.overflow || null,
    failures: partial.failures || [],
    notes: partial.notes || [],
    duplicate: Boolean(partial.duplicate),
    opportunityType: partial.opportunityType || null,
    identityKey: eventIdentityKey(partial.working || {}),
  };
}

/**
 * Qualify one opportunity. evidenceOverlay fields win when present.
 * Pipeline: semantic → real event → date → geo → source → demand →
 * venue lock / open / overflow → actionability.
 */
export function qualifyOpportunityV3(hotelId, opp = {}, opts = {}) {
  const {
    nowDate,
    subjectHotel = {},
    evidenceOverlay = null,
    seenKeys = null,
  } = opts;
  const working = mergeEvidenceOverlay(opp, evidenceOverlay);
  const failures = [];
  const notes = [];
  const semanticType = classifyEventSemanticType(working);
  notes.push(`semantic:${semanticType}`);

  // Non-event content → INVALID
  if (
    (NON_EVENT_TYPES.has(semanticType) && !EVENT_MARKER_RE.test(norm(working.title))) ||
    (semanticType === EVENT_SEMANTIC_TYPE.OTHER &&
      !working.eventStartDate &&
      !EVENT_MARKER_RE.test(norm(working.title)))
  ) {
    return pack({
      actionability: ACTIONABILITY_V3.INVALID,
      failureClass: FAILURE_CLASS_V3.NON_EVENT_CONTENT,
      semanticType,
      failures: [{ class: FAILURE_CLASS_V3.NON_EVENT_CONTENT, detail: semanticType }],
      notes,
      working,
    });
  }

  const dateInfo = classifyDateConfidence(working, { nowDate });
  notes.push(`date:${dateInfo.confidence}`);

  if (dateInfo.confidence === DATE_CONFIDENCE.DATE_PAST) {
    const reactivation = Boolean(
      working.recurrenceEvidence || evidenceOverlay?.recurrenceEvidence
    );
    return pack({
      actionability: reactivation
        ? ACTIONABILITY_V3.VALID_WATCH
        : ACTIONABILITY_V3.INVALID,
      failureClass: FAILURE_CLASS_V3.PAST_EVENT,
      semanticType,
      dateInfo,
      failures: [{ class: FAILURE_CLASS_V3.PAST_EVENT, detail: "past_date" }],
      notes: reactivation ? [...notes, "past_with_recurrence_watch"] : notes,
      working,
    });
  }

  const geo = classifyGeography(hotelId, working);
  notes.push(`geo:${geo.class}`);
  if (geo.class === GEO_CLASS.OUT_OF_MARKET) {
    return pack({
      actionability: ACTIONABILITY_V3.INVALID,
      failureClass: FAILURE_CLASS_V3.OTHER,
      semanticType,
      dateInfo,
      geo,
      failures: [{ class: FAILURE_CLASS_V3.OTHER, detail: `geo_out:${geo.detail}` }],
      notes,
      working,
    });
  }

  const source = classifySourceAuthority(working);
  notes.push(`source:${source.tier}`);
  if (source.aggregatorOnly || source.tier === SOURCE_TIER.AGGREGATOR) {
    return pack({
      actionability: ACTIONABILITY_V3.INVALID,
      failureClass: FAILURE_CLASS_V3.SOURCE_SEMANTICS_FAILURE,
      semanticType,
      dateInfo,
      geo,
      source,
      failures: [
        { class: FAILURE_CLASS_V3.SOURCE_SEMANTICS_FAILURE, detail: "aggregator_only" },
      ],
      notes,
      working,
    });
  }

  const demand = classifyHotelDemandThesis(working);
  const openSourcing = classifyOpenSourcingEvidence(working);
  const hostLock = detectCompetitorOrHostLock(working, subjectHotel);
  const overflow = classifyOverflowThesis(working);
  notes.push(
    `demand:${demand.strength}`,
    `open:${openSourcing.open}/${openSourcing.status}`,
    `lock:${hostLock.locked}`,
    `overflow:${overflow.ok}`
  );

  if (!demand.ok || demand.strength === "NONE") {
    failures.push({
      class: FAILURE_CLASS_V3.NO_HOTEL_ROOM_THESIS,
      detail: demand.reasons.join(","),
    });
  }

  let duplicate = false;
  const key = eventIdentityKey(working);
  const loose = looseTitleKey(working);
  if (seenKeys) {
    if (seenKeys.has(key) || seenKeys.has(loose)) {
      duplicate = true;
      failures.push({ class: FAILURE_CLASS_V3.OTHER, detail: "duplicate" });
    } else {
      seenKeys.add(key);
      seenKeys.add(loose);
    }
  }
  if (!String(working.title || "").trim()) {
    failures.push({
      class: FAILURE_CLASS_V3.EVENT_IDENTITY_WEAK,
      detail: "missing_title",
    });
  }

  const base = {
    semanticType,
    dateInfo,
    geo,
    source,
    demand,
    openSourcing,
    hostLock,
    overflow,
    notes,
    working,
    duplicate,
  };

  if (hostLock.fullyPlaced) {
    return pack({
      ...base,
      actionability: ACTIONABILITY_V3.INVALID,
      failureClass: FAILURE_CLASS_V3.COMPETITOR_HOST_LOCKED,
      failures: [
        ...failures,
        { class: FAILURE_CLASS_V3.COMPETITOR_HOST_LOCKED, detail: "fully_placed" },
      ],
      opportunityType: "CLOSED_DISQUALIFIED",
    });
  }

  const meetingVenueRoomsOpen = Boolean(hostLock.meetingVenueOnly);
  // Overflow thesis itself is a placement path (housing/overflow evidence).
  // Do not also require host-lock — host-lock without overflow stays WATCH above.
  const placementOk =
    (openSourcing.open && openSourcing.supported) ||
    meetingVenueRoomsOpen ||
    overflow.ok;

  // Host locked + no overflow → never TRUE PRIMARY
  if (hostLock.locked && !overflow.ok && !meetingVenueRoomsOpen && !openSourcing.open) {
    return pack({
      ...base,
      actionability: ACTIONABILITY_V3.VALID_WATCH,
      failureClass: FAILURE_CLASS_V3.NO_OVERFLOW_EVIDENCE,
      failures: [
        ...failures,
        { class: FAILURE_CLASS_V3.COMPETITOR_HOST_LOCKED, detail: "host_locked" },
        { class: FAILURE_CLASS_V3.NO_OVERFLOW_EVIDENCE, detail: "no_overflow" },
      ],
      notes: [...notes, "host_locked_no_overflow_watch"],
      opportunityType: "FUTURE_CYCLE",
    });
  }

  if (isFarFuture(working, nowDate) && !hasNearTermOpenHousing(working, openSourcing)) {
    return pack({
      ...base,
      actionability: ACTIONABILITY_V3.VALID_FUTURE,
      failureClass: FAILURE_CLASS_V3.OTHER,
      failures,
      notes: [...notes, "far_future"],
    });
  }

  const dateOk =
    dateInfo.confidence === DATE_CONFIDENCE.DATE_CONFIRMED ||
    (dateInfo.confidence === DATE_CONFIDENCE.DATE_INFERRED && dateInfo.highSupport);
  if (!dateOk) {
    failures.push({
      class: FAILURE_CLASS_V3.DATE_UNCERTAIN,
      detail: dateInfo.confidence,
    });
  }

  const geoOk = GEO_FIT.has(geo.class);
  const sourceOk = credibleSource(source) && !source.aggregatorOnly;
  const demandOk = demand.ok && demand.strength !== "NONE";
  // Demand must be STRONG for TRUE_ACTIONABLE (WEAK alone is watch/insufficient)
  const demandStrong = demand.ok && demand.strength === "STRONG";
  const realEvent = isRealEventType(semanticType);

  if (
    realEvent &&
    dateOk &&
    geoOk &&
    sourceOk &&
    demandStrong &&
    placementOk &&
    !hostLock.fullyPlaced &&
    !duplicate
  ) {
    const asOverflow =
      meetingVenueRoomsOpen ||
      hostLock.locked ||
      (overflow.ok && !(openSourcing.open && openSourcing.supported));
    return pack({
      ...base,
      actionability: ACTIONABILITY_V3.TRUE_ACTIONABLE,
      failureClass: null,
      failures,
      notes: [...notes, "true_actionable"],
      opportunityType: asOverflow ? "OVERFLOW_HOUSING" : "PRIMARY_PURSUIT",
    });
  }

  if (!demandOk) {
    return pack({
      ...base,
      actionability: ACTIONABILITY_V3.INSUFFICIENT,
      failureClass: FAILURE_CLASS_V3.NO_HOTEL_ROOM_THESIS,
      failures,
    });
  }
  if (!placementOk) {
    return pack({
      ...base,
      actionability: ACTIONABILITY_V3.VALID_WATCH,
      failureClass: FAILURE_CLASS_V3.NO_OPEN_SOURCING_EVIDENCE,
      failures: [
        ...failures,
        {
          class: FAILURE_CLASS_V3.NO_OPEN_SOURCING_EVIDENCE,
          detail: "no_placement_path",
        },
      ],
    });
  }

  const failureClass = !dateOk
    ? FAILURE_CLASS_V3.DATE_UNCERTAIN
    : !realEvent
      ? FAILURE_CLASS_V3.NON_EVENT_CONTENT
      : !sourceOk
        ? FAILURE_CLASS_V3.SOURCE_SEMANTICS_FAILURE
        : FAILURE_CLASS_V3.EVENT_IDENTITY_WEAK;

  return pack({
    ...base,
    actionability:
      geo.class === GEO_CLASS.UNKNOWN
        ? ACTIONABILITY_V3.INSUFFICIENT
        : ACTIONABILITY_V3.VALID_WATCH,
    failureClass,
    failures,
  });
}

export function applyDiscoveryHygieneV3(hotelId, opportunities = [], opts = {}) {
  const seenKeys = new Set();
  const filterCounts = Object.fromEntries(
    Object.values(FAILURE_CLASS_V3).map((c) => [c, 0])
  );

  const ordered = [...opportunities].sort(
    (a, b) => String(b.title || "").length - String(a.title || "").length
  );

  const rows = [];
  for (const opp of ordered) {
    const overlay =
      opts.evidenceOverlays?.[opp.id || opp.opportunityId] ||
      opts.evidenceOverlay ||
      null;
    const result = qualifyOpportunityV3(hotelId, opp, {
      nowDate: opts.nowDate,
      subjectHotel: opts.subjectHotel || {},
      evidenceOverlay: overlay,
      seenKeys,
    });
    for (const f of result.failures || []) {
      const code = f.class || FAILURE_CLASS_V3.OTHER;
      filterCounts[code] = (filterCounts[code] || 0) + 1;
    }
    rows.push({
      ...opp,
      hygieneV3: result,
      actionabilityV3: result.actionability,
      hygieneState: result.hygieneState,
      opportunityType: result.opportunityType || opp.opportunityType,
      venueSourcingStatus: result.openSourcing?.status || opp.venueSourcingStatus,
    });
  }

  const byState = {};
  for (const r of rows) {
    byState[r.actionabilityV3] = (byState[r.actionabilityV3] || 0) + 1;
  }
  const trueActionable = rows.filter(
    (r) => r.actionabilityV3 === ACTIONABILITY_V3.TRUE_ACTIONABLE
  );

  return {
    version: DISCOVERY_HYGIENE_V3,
    hotelId,
    rows,
    trueActionable,
    byState,
    filterCounts,
    metrics: {
      total: rows.length,
      trueActionable: trueActionable.length,
      validWatch: byState[ACTIONABILITY_V3.VALID_WATCH] || 0,
      validFuture: byState[ACTIONABILITY_V3.VALID_FUTURE] || 0,
      insufficient: byState[ACTIONABILITY_V3.INSUFFICIENT] || 0,
      invalid: byState[ACTIONABILITY_V3.INVALID] || 0,
    },
  };
}

/** Self-check: UNKNOWN sourcing is never treated as open. */
export function assertUnknownNotOpenRegression() {
  const failures = [];
  const cases = [
    { name: "raw_unknown", opp: { title: "Sample Conference", venueSourcingStatus: "UNKNOWN" } },
    {
      name: "unsupported_rfp",
      opp: {
        title: "Sample Conference 2026",
        venueSourcingStatus: "RFP_ACTIVE_SOURCING",
        hotelOpportunityThesis: "request RFP for hotel",
      },
    },
    {
      name: "unsupported_open",
      opp: { title: "Industry Forum", venueSourcingStatus: "OPEN_UNRESOLVED" },
    },
  ];

  for (const c of cases) {
    const r = classifyOpenSourcingEvidence(c.opp);
    if (r.open === true) {
      failures.push({ case: c.name, detail: "open_true_on_unknown_or_unsupported" });
    }
    if (r.status === "UNKNOWN" && (r.open !== false || r.supported === true)) {
      failures.push({ case: c.name, detail: "unknown_treated_as_open_or_supported" });
    }
  }

  const backed = classifyOpenSourcingEvidence({
    title: "Citywide Congress",
    venueSourcingStatus: "OPEN_UNRESOLVED",
    housingEvidence: "housing still open for overflow hotels",
  });
  if (!backed.open || !backed.supported) {
    failures.push({ case: "positive_open_control", detail: "evidence_backed_open_failed" });
  }

  return { ok: failures.length === 0, failures };
}
