/**
 * Venue-locked commercial classification (V11).
 *
 * If primary host hotel/venue is already selected:
 *   default must NOT be PRIMARY_PURSUIT for venue-win.
 * OVERFLOW requires plausible overflow/housing thesis evidence.
 * Preserve: selected / no-overflow cannot be HIGH PRIMARY.
 *
 * Structural rules — no event-name hardcoding.
 */

import {
  OPPORTUNITY_TYPE,
  VENUE_SOURCING_STATUS,
  PRIORITY,
} from "./claim-types.js";

export const VENUE_LOCKED_SUBSTATE = Object.freeze({
  PRIMARY_SELECTED_OVERFLOW_POSSIBLE: "PRIMARY_SELECTED_OVERFLOW_POSSIBLE",
  PRIMARY_SELECTED_NO_OVERFLOW_EVIDENCE: "PRIMARY_SELECTED_NO_OVERFLOW_EVIDENCE",
  FULLY_PLACED: "FULLY_PLACED",
  MEETING_VENUE_ROOMS_OPEN: "MEETING_VENUE_ROOMS_OPEN",
  NOT_VENUE_LOCKED: "NOT_VENUE_LOCKED",
});

const HOTEL_VENUE_NAME =
  /\b(?:hotel|resort|marriott|hilton|hyatt|ihg|westin|sheraton|st\.?\s*regis|barcel[oó]|hard\s*rock|curio|autograph|renaissance|four\s+seasons|ritz|intercontinental|holiday\s+inn|courtyard|embassy\s+suites|doubletree|hampton|aloft|w\s+hotel|conrad|waldorf)\b/i;

const PRIMARY_SELECTED_RE =
  /\b(?:hosted\s+at|host\s+hotel|official\s+hotel|headquarters\s+hotel|primary\s+(?:hotel|venue)|venue\s*(?:is|:)|already\s+(?:at|booked|selected|placed)|will\s+be\s+held\s+at|taking\s+place\s+at)\b/i;

const OVERFLOW_EVIDENCE_RE =
  /\b(?:overflow|housing\s+program|room\s+block|official\s+hotels?|multi-?hotel|citywide|compression|alternate\s+(?:hotel|accommodation)|additional\s+hotels?|sleeping\s+rooms?\s+(?:open|available|tbd)|rooms?\s+still\s+open|attendee\s+volume|peak\s+rooms?|spillover)\b/i;

const ACTIVE_SOURCING_RE =
  /\b(?:rfp|request\s+for\s+proposal|sourcing\s+hotels?|seeking\s+(?:hotels?|proposals?)|hotel\s+bids?\s+open|reopened\s+sourcing|non-?exclusive|multiple\s+primary\s+hotels?|meeting\s+(?:space|venue)\s+only)\b/i;

const FULLY_PLACED_RE =
  /\b(?:fully\s+placed|sold\s+out\s+block|no\s+(?:additional|further)\s+(?:rooms?|hotels?)|housing\s+closed|registration\s+closed\s+for\s+housing)\b/i;

function evidenceBlob(opp = {}) {
  return [
    opp.venue,
    opp.venueStatus,
    opp.venueSourcingStatus,
    opp.sourcingStatus,
    opp.housingEvidence,
    opp.roomDemandEvidence,
    opp.whyRelevantToHotel,
    opp.whyNow,
    opp.venueWatch,
    opp.commercialThesis,
    ...(opp.evidence || []).map((e) =>
      typeof e === "string" ? e : e?.value || e?.title || ""
    ),
    ...(opp.evidenceSources || []).map((e) =>
      typeof e === "string" ? e : e?.value || e?.title || ""
    ),
  ]
    .filter(Boolean)
    .join(" ");
}

/**
 * Detect whether a named primary venue/hotel is already selected
 * (and is not the subject hotel).
 */
export function detectPrimaryVenueSelected(opp = {}, subjectHotel = {}) {
  const blob = evidenceBlob(opp);
  const venueField = String(opp.venue || opp.primaryVenue || opp.hostHotel || "").trim();
  const status = String(
    opp.venueSourcingStatus || opp.sourcingStatus || opp.venueStatus || ""
  ).toUpperCase();

  if (
    /PRIMARY_VENUE_SELECTED|FULLY_PLACED|PRIMARY_SELECTED/i.test(status) ||
    status === "CONFIRMED"
  ) {
    // Confirmed alone is weak — require hotel-like venue name or selected language
  }

  const namedVenue =
    (venueField && HOTEL_VENUE_NAME.test(venueField)) ||
    (PRIMARY_SELECTED_RE.test(blob) && HOTEL_VENUE_NAME.test(blob));

  const statusSelected =
    /PRIMARY_VENUE_SELECTED|FULLY_PLACED|PRIMARY_SELECTED/i.test(status);

  if (!namedVenue && !statusSelected && !/Confirmed/i.test(String(opp.venueStatus || ""))) {
    // venueWatch / rationale may still assert competitor host
    if (!HOTEL_VENUE_NAME.test(blob) || !PRIMARY_SELECTED_RE.test(blob + " " + (opp.venueWatch || ""))) {
      // Soft: venueWatch mentioning competitor hotel counts
      if (opp.venueWatch && HOTEL_VENUE_NAME.test(String(opp.venueWatch))) {
        return {
          selected: true,
          venueEvidence: String(opp.venueWatch),
          reason: "venue_watch_named_host",
        };
      }
      return { selected: false, venueEvidence: null, reason: null };
    }
  }

  // Exclude subject hotel as "selected elsewhere"
  const subject = String(
    subjectHotel.name || subjectHotel.hotelName || opp.subjectHotelName || ""
  ).toLowerCase();
  const venueLower = (venueField || blob).toLowerCase();
  if (subject && venueLower.includes(subject.slice(0, Math.min(12, subject.length)))) {
    return { selected: false, venueEvidence: venueField || null, reason: "subject_hotel_is_host" };
  }

  if (namedVenue || statusSelected || (opp.venueWatch && HOTEL_VENUE_NAME.test(String(opp.venueWatch)))) {
    return {
      selected: true,
      venueEvidence: venueField || opp.venueWatch || blob.slice(0, 160),
      reason: statusSelected ? "status_primary_selected" : "named_host_venue",
    };
  }

  return { selected: false, venueEvidence: null, reason: null };
}

export function detectOverflowEvidence(opp = {}) {
  const blob = evidenceBlob(opp);
  if (OVERFLOW_EVIDENCE_RE.test(blob)) {
    return { present: true, evidence: "overflow_or_housing_language" };
  }
  if (/OVERFLOW|PRIMARY_VENUE_SELECTED_OVERFLOW|VERIFIED_HOUSING|HOUSING_PROGRAM/i.test(
    String(opp.venueSourcingStatus || opp.roomDemandStatus || opp.roomDemandEvidence || "")
  )) {
    return { present: true, evidence: "status_overflow_or_housing" };
  }
  if (/OVERFLOW_HOUSING/i.test(String(opp.opportunityType || ""))) {
    return { present: true, evidence: "already_typed_overflow" };
  }
  return { present: false, evidence: null };
}

export function detectActiveHotelSourcing(opp = {}) {
  const blob = evidenceBlob(opp);
  return ACTIVE_SOURCING_RE.test(blob);
}

/**
 * Reclassify opportunity commercial type given venue-lock evidence.
 *
 * @returns {{
 *   opportunityType: string,
 *   venueSourcingStatus: string,
 *   substate: string,
 *   priorityCap: string|null,
 *   changed: boolean,
 *   oldType: string,
 *   commercialThesis: string,
 *   venueEvidence: string|null,
 *   reasons: string[]
 * }}
 */
export function classifyVenueLockedOpportunity(opp = {}, subjectHotel = {}) {
  const oldType = String(opp.opportunityType || OPPORTUNITY_TYPE.PRIMARY_PURSUIT);
  const reasons = [];
  const selected = detectPrimaryVenueSelected(opp, subjectHotel);
  const overflow = detectOverflowEvidence(opp);
  const blob = evidenceBlob(opp);

  if (FULLY_PLACED_RE.test(blob) || /FULLY_PLACED/i.test(String(opp.venueSourcingStatus || ""))) {
    return {
      opportunityType: OPPORTUNITY_TYPE.CLOSED_DISQUALIFIED,
      venueSourcingStatus: VENUE_SOURCING_STATUS.FULLY_PLACED,
      substate: VENUE_LOCKED_SUBSTATE.FULLY_PLACED,
      priorityCap: PRIORITY.DISQUALIFIED,
      changed: oldType !== OPPORTUNITY_TYPE.CLOSED_DISQUALIFIED,
      oldType,
      commercialThesis: "Fully placed — no hotel sourcing path this cycle",
      venueEvidence: selected.venueEvidence,
      reasons: ["fully_placed"],
    };
  }

  if (!selected.selected) {
    // Venue TBD / RFP — PRIMARY possible
    const status = /RFP/i.test(String(opp.sourcingStatus || opp.venueSourcingStatus || ""))
      ? VENUE_SOURCING_STATUS.RFP_ACTIVE_SOURCING
      : /TBD/i.test(String(opp.venueStatus || opp.venueSourcingStatus || ""))
        ? VENUE_SOURCING_STATUS.HOTEL_VENUE_TBD
        : opp.venueSourcingStatus || VENUE_SOURCING_STATUS.OPEN_UNRESOLVED;
    return {
      opportunityType: oldType,
      venueSourcingStatus: status,
      substate: VENUE_LOCKED_SUBSTATE.NOT_VENUE_LOCKED,
      priorityCap: null,
      changed: false,
      oldType,
      commercialThesis: opp.commercialThesis || "Venue not locked — pursuit type unchanged",
      venueEvidence: null,
      reasons: ["not_venue_locked"],
    };
  }

  reasons.push(selected.reason || "primary_venue_selected");

  // Meeting venue selected but sleeping rooms open
  if (
    /\bmeeting\s+(?:space|venue)\s+only\b|\bsleeping\s+rooms?\s+(?:open|available|tbd)\b/i.test(
      blob
    )
  ) {
    return {
      opportunityType: OPPORTUNITY_TYPE.PRIMARY_PURSUIT,
      venueSourcingStatus:
        VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE,
      substate: VENUE_LOCKED_SUBSTATE.MEETING_VENUE_ROOMS_OPEN,
      priorityCap: null,
      changed: false,
      oldType,
      commercialThesis:
        "Meeting venue selected; sleeping-room sourcing remains open",
      venueEvidence: selected.venueEvidence,
      reasons: [...reasons, "meeting_venue_rooms_open"],
    };
  }

  // Primary exception: active additional hotel sourcing
  if (detectActiveHotelSourcing(opp) && overflow.present) {
    return {
      opportunityType: OPPORTUNITY_TYPE.PRIMARY_PURSUIT,
      venueSourcingStatus:
        VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE,
      substate: VENUE_LOCKED_SUBSTATE.PRIMARY_SELECTED_OVERFLOW_POSSIBLE,
      priorityCap: PRIORITY.MEDIUM,
      changed: false,
      oldType,
      commercialThesis:
        "Primary venue selected but credible active multi-hotel / overflow sourcing",
      venueEvidence: selected.venueEvidence,
      reasons: [...reasons, "primary_exception_active_sourcing"],
    };
  }

  if (overflow.present) {
    const newType = OPPORTUNITY_TYPE.OVERFLOW_HOUSING;
    return {
      opportunityType: newType,
      venueSourcingStatus:
        VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE,
      substate: VENUE_LOCKED_SUBSTATE.PRIMARY_SELECTED_OVERFLOW_POSSIBLE,
      priorityCap: PRIORITY.MEDIUM,
      changed: oldType !== newType,
      oldType,
      commercialThesis:
        "Primary host already selected — pursue as overflow / housing / alternate hotel",
      venueEvidence: selected.venueEvidence,
      reasons: [...reasons, "overflow_evidence_present"],
    };
  }

  // Selected, no overflow evidence → not PRIMARY pursuit
  const newType =
    oldType === OPPORTUNITY_TYPE.FUTURE_CYCLE
      ? OPPORTUNITY_TYPE.FUTURE_CYCLE
      : OPPORTUNITY_TYPE.FUTURE_CYCLE; // WATCH / FUTURE when no overflow thesis
  return {
    opportunityType: newType,
    venueSourcingStatus:
      VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE,
    substate: VENUE_LOCKED_SUBSTATE.PRIMARY_SELECTED_NO_OVERFLOW_EVIDENCE,
    priorityCap: PRIORITY.WATCHLIST,
    changed: oldType !== newType,
    oldType,
    commercialThesis:
      "Primary venue selected with no overflow/housing evidence — watch / future, not HIGH primary",
    venueEvidence: selected.venueEvidence,
    reasons: [...reasons, "no_overflow_evidence", "cannot_high_primary"],
    watchLabel: "WATCH",
  };
}

/**
 * Ranking rule: selected + no overflow cannot be HIGH PRIMARY.
 */
export function applyVenueLockedPriorityCap(priority, classification) {
  if (
    classification.substate ===
      VENUE_LOCKED_SUBSTATE.PRIMARY_SELECTED_NO_OVERFLOW_EVIDENCE ||
    classification.venueSourcingStatus ===
      VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE
  ) {
    if (priority === PRIORITY.HIGH || priority === "HIGH" || priority === "HIGH_PRIORITY") {
      return classification.priorityCap || PRIORITY.WATCHLIST;
    }
  }
  if (classification.priorityCap && priority === PRIORITY.HIGH) {
    return classification.priorityCap;
  }
  return priority;
}
