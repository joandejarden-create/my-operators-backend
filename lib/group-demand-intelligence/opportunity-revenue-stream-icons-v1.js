/**
 * GDI opportunity tile — revenue-stream icon presentation contract V1.
 *
 * Maps canonical HOTEL_REVENUE_STREAM_CLASS → two compact SVG indicators
 * (Guestrooms · Meetings, Events & Catering). Does not invent revenue theses
 * from demand segment alone. Prefer persisted revenueStreamClass; otherwise
 * derive via multi-revenue-qualification-v1 + explicit legacy fields only.
 */

import {
  classifyHotelRevenueStream,
  HOTEL_REVENUE_STREAM_CLASS,
  HOTEL_REVENUE_STREAM_LABEL,
} from "./multi-revenue-qualification-v1.js";

export const GDI_OPPORTUNITY_REVENUE_STREAM_ICONS_V1 =
  "GDI_OPPORTUNITY_REVENUE_STREAM_ICONS_V1";

export const REVENUE_STREAM_ICON_STATE = Object.freeze({
  ACTIVE: "active",
  MUTED: "muted",
});

export const REVENUE_STREAM_ICON_LABEL = Object.freeze({
  /** Founder / customer a11y labels — Guest Rooms · Local Catering */
  guestrooms: "Guest Rooms",
  eventFnb: "Local Catering",
});

const VALID_CLASSES = new Set(Object.values(HOTEL_REVENUE_STREAM_CLASS));

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

function thesisUnsupported(t) {
  const s = text(t);
  if (!s) return true;
  return (
    /^(unsupported|not supported|no evidence|none|n\/a|null|unverified)\b/i.test(s) ||
    /^unsupported\b/i.test(s.slice(0, 24))
  );
}

function normalizeRevenueStreamClass(raw) {
  const c = upper(raw).replace(/\s+/g, "_");
  if (VALID_CLASSES.has(c)) return c;
  // Tolerate label-ish aliases from older packs
  if (c === "GUESTROOMS" || c === "ROOMS_ONLY" || c === "LODGING_ONLY") {
    return HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY;
  }
  if (
    c === "EVENT_ONLY" ||
    c === "FNB_ONLY" ||
    c === "CATERING_ONLY" ||
    c === "MEETINGS_ONLY" ||
    c === "EVENT_FNB"
  ) {
    return HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY;
  }
  if (c === "COMBINED" || c === "BOTH" || c === "GUESTROOMS_AND_EVENT_FNB") {
    return HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB;
  }
  if (c === "UNCONFIRMED" || c === "UNKNOWN" || c === "NONE") {
    return HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED;
  }
  return null;
}

/**
 * Map canonical class → icon active/muted states (no invention).
 */
export function mapRevenueStreamClassToIconStates(revenueStreamClass) {
  const cls =
    normalizeRevenueStreamClass(revenueStreamClass) ||
    HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED;
  const A = REVENUE_STREAM_ICON_STATE.ACTIVE;
  const M = REVENUE_STREAM_ICON_STATE.MUTED;
  switch (cls) {
    case HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY:
      return { guestrooms: A, eventFnb: M };
    case HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY:
      return { guestrooms: M, eventFnb: A };
    case HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB:
      return { guestrooms: A, eventFnb: A };
    default:
      return { guestrooms: M, eventFnb: M };
  }
}

function hasExplicitEventFnbEvidence(opp = {}) {
  const hyp = opp.hotelDemandHypotheses || {};
  const candidates = [
    hyp.meetingsHypothesis,
    hyp.cateringHypothesis,
    opp.eventFnbThesis,
    opp.meetingsThesis,
    opp.potentialMeetingDemand,
    opp.potentialCateringDemand,
    opp.hotelDemandEvidence?.meetingOrTrainingSignals,
  ];
  return candidates.some((c) => text(c) && !thesisUnsupported(c));
}

function hasExplicitGuestroomEvidence(opp = {}) {
  const hyp = opp.hotelDemandHypotheses || {};
  const candidates = [
    hyp.guestroomHypothesis,
    opp.guestroomThesis,
    opp.potentialGuestroomDemand,
    opp.hotelDemandEvidence?.potentialGuestroomDemand,
  ];
  if (candidates.some((c) => text(c) && !thesisUnsupported(c))) return true;
  const gs = upper(opp.guestroomStatus);
  const gds = upper(opp.guestroomDemandStatus || opp.lodgingPath);
  return (
    gs === "REQUIRED_EVIDENCED" ||
    gds === "EVIDENCE_SUPPORTED" ||
    gds === "REQUIRED" ||
    gds === "REQUIRED_EVIDENCED"
  );
}

function legacyGuestroomNotRequired(opp = {}) {
  const gs = upper(opp.guestroomStatus);
  const gds = upper(opp.guestroomDemandStatus || opp.lodgingPath);
  if (opp.cateringOnly === true) return true;
  if (gs === "NOT_REQUIRED") return true;
  return (
    gds === "NONE" ||
    gds === "NONE_CONFIRMED" ||
    gds === "NOT_APPLICABLE" ||
    gds.includes("CATERING_ONLY")
  );
}

/**
 * Build classifyHotelRevenueStream input from customer / research opportunity fields.
 * Never seeds theses from demandSegmentId alone.
 */
export function buildRevenueStreamClassifyInput(opp = {}) {
  const hyp = { ...(opp.hotelDemandHypotheses || {}) };
  const guestroomHypothesis =
    text(hyp.guestroomHypothesis) ||
    text(opp.guestroomThesis) ||
    text(opp.potentialGuestroomDemand) ||
    text(opp.hotelDemandEvidence?.potentialGuestroomDemand) ||
    null;
  const meetingsHypothesis =
    text(hyp.meetingsHypothesis) ||
    text(hyp.cateringHypothesis) ||
    text(opp.eventFnbThesis) ||
    text(opp.meetingsThesis) ||
    text(opp.potentialMeetingDemand) ||
    text(opp.potentialCateringDemand) ||
    text(opp.hotelDemandEvidence?.meetingOrTrainingSignals) ||
    null;

  // Explicit not-required: attach negative guestroom language only when an
  // event/F&B thesis already exists (does not invent catering from the flag alone).
  let meetings = meetingsHypothesis;
  if (legacyGuestroomNotRequired(opp) && meetings && !/guestrooms? not required/i.test(meetings)) {
    meetings = `${meetings}; guestrooms not required`;
  }

  return {
    hotelDemandHypotheses: {
      ...hyp,
      guestroomHypothesis: guestroomHypothesis || null,
      meetingsHypothesis: meetings || null,
      cateringHypothesis: hyp.cateringHypothesis || null,
      primaryMechanism:
        hyp.primaryMechanism ||
        opp.primaryDemandHypothesis ||
        opp.hotelOpportunityThesis ||
        null,
    },
    hotelDemandEvidence: opp.hotelDemandEvidence || {},
    guestroomThesis: guestroomHypothesis,
    eventFnbThesis: meetings,
    potentialGuestroomDemand: opp.potentialGuestroomDemand,
    potentialMeetingDemand: opp.potentialMeetingDemand,
    potentialCateringDemand: opp.potentialCateringDemand,
    summary: opp.summary || opp.summaryWhat || opp.whatIsHappening || opp.tileCustomerSummary,
    recommendedAction: opp.recommendedAction,
    thesis: opp.hotelOpportunityThesis || opp.thesis,
  };
}

/**
 * Drawer consistency: never highlight a stream without supporting evidence.
 * Trusts classify() detection when present; never invents missing explanations.
 */
function applyDrawerConsistencyGuard(cls, opp, icons, detected = null) {
  const next = { ...icons };
  const M = REVENUE_STREAM_ICON_STATE.MUTED;

  const guestOk =
    hasExplicitGuestroomEvidence(opp) || detected?.guestroomCredible === true;
  const eventOk =
    hasExplicitEventFnbEvidence(opp) || detected?.eventFnbCredible === true;

  if (next.guestrooms === REVENUE_STREAM_ICON_STATE.ACTIVE && !guestOk) {
    next.guestrooms = M;
  }
  if (next.eventFnb === REVENUE_STREAM_ICON_STATE.ACTIVE && !eventOk) {
    next.eventFnb = M;
  }

  if (cls === HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED) {
    next.guestrooms = M;
    next.eventFnb = M;
  }

  return next;
}

/**
 * Resolve presentation revenue-stream class + icon states for a customer opportunity.
 */
export function resolveOpportunityRevenueStreamIcons(opp = {}) {
  const persisted = normalizeRevenueStreamClass(
    opp.revenueStreamClass ||
      opp.applicableHotelRevenueStreams ||
      opp.multiRevenue?.revenueStreamClass,
  );

  let classification;
  if (persisted) {
    classification = {
      revenueStreamClass: persisted,
      revenueStreamLabel: HOTEL_REVENUE_STREAM_LABEL[persisted],
      guestroomThesis:
        text(opp.guestroomThesis) ||
        text(opp.hotelDemandHypotheses?.guestroomHypothesis) ||
        text(opp.potentialGuestroomDemand) ||
        null,
      eventFnbThesis:
        text(opp.eventFnbThesis) ||
        text(opp.hotelDemandHypotheses?.meetingsHypothesis) ||
        text(opp.hotelDemandHypotheses?.cateringHypothesis) ||
        text(opp.potentialMeetingDemand) ||
        text(opp.potentialCateringDemand) ||
        null,
      source: "persisted",
    };
  } else {
    const input = buildRevenueStreamClassifyInput(opp);
    const classified = classifyHotelRevenueStream(input);
    let cls = classified.revenueStreamClass;

    // Legacy explicit statuses — only when classify stayed unconfirmed and
    // evidence fields (not segment) support promotion.
    if (cls === HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED) {
      const rooms = hasExplicitGuestroomEvidence(opp);
      const events = hasExplicitEventFnbEvidence(opp);
      const roomsBanned = legacyGuestroomNotRequired(opp);
      if (rooms && events && !roomsBanned) {
        cls = HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB;
      } else if (rooms && !roomsBanned && !events) {
        cls = HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY;
      } else if (events && (roomsBanned || !rooms)) {
        cls = HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY;
      }
    } else if (
      legacyGuestroomNotRequired(opp) &&
      (cls === HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY ||
        cls === HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB)
    ) {
      // Explicit NOT_REQUIRED / cateringOnly wins over incidental room keywords
      cls = hasExplicitEventFnbEvidence(opp)
        ? HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY
        : HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED;
    }

    classification = {
      revenueStreamClass: cls,
      revenueStreamLabel: HOTEL_REVENUE_STREAM_LABEL[cls],
      guestroomThesis: classified.guestroomThesis,
      eventFnbThesis: classified.eventFnbThesis,
      source: "derived",
      detected: classified.detected,
    };
  }

  let icons = mapRevenueStreamClassToIconStates(classification.revenueStreamClass);
  icons = applyDrawerConsistencyGuard(
    classification.revenueStreamClass,
    {
      ...opp,
      guestroomThesis: classification.guestroomThesis,
      eventFnbThesis: classification.eventFnbThesis,
      hotelDemandHypotheses: {
        ...(opp.hotelDemandHypotheses || {}),
        guestroomHypothesis:
          classification.guestroomThesis ||
          opp.hotelDemandHypotheses?.guestroomHypothesis,
        meetingsHypothesis:
          classification.eventFnbThesis ||
          opp.hotelDemandHypotheses?.meetingsHypothesis,
      },
    },
    icons,
    classification.detected || null,
  );

  // Reconcile class if consistency muted a combined stream down to one side
  let finalClass = classification.revenueStreamClass;
  if (
    finalClass === HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB &&
    icons.guestrooms === REVENUE_STREAM_ICON_STATE.MUTED &&
    icons.eventFnb === REVENUE_STREAM_ICON_STATE.ACTIVE
  ) {
    finalClass = HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY;
  } else if (
    finalClass === HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB &&
    icons.guestrooms === REVENUE_STREAM_ICON_STATE.ACTIVE &&
    icons.eventFnb === REVENUE_STREAM_ICON_STATE.MUTED
  ) {
    finalClass = HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY;
  } else if (
    icons.guestrooms === REVENUE_STREAM_ICON_STATE.MUTED &&
    icons.eventFnb === REVENUE_STREAM_ICON_STATE.MUTED
  ) {
    finalClass = HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED;
  }

  const guestroomsLabel = REVENUE_STREAM_ICON_LABEL.guestrooms;
  const eventLabel = REVENUE_STREAM_ICON_LABEL.eventFnb;
  // Muted streams must not emit customer-facing "not indicated" copy — icons
  // alone carry state. Active streams keep a short applicable label for a11y.
  const guestAria =
    icons.guestrooms === REVENUE_STREAM_ICON_STATE.ACTIVE
      ? `${guestroomsLabel} — applicable`
      : guestroomsLabel;
  const eventAria =
    icons.eventFnb === REVENUE_STREAM_ICON_STATE.ACTIVE
      ? `${eventLabel} — applicable`
      : eventLabel;

  return {
    version: GDI_OPPORTUNITY_REVENUE_STREAM_ICONS_V1,
    revenueStreamClass: finalClass,
    revenueStreamLabel: HOTEL_REVENUE_STREAM_LABEL[finalClass],
    guestroomThesis: classification.guestroomThesis || null,
    eventFnbThesis: classification.eventFnbThesis || null,
    icons: {
      guestrooms: icons.guestrooms,
      eventFnb: icons.eventFnb,
    },
    aria: {
      guestrooms: guestAria,
      eventFnb: eventAria,
      group: `Revenue streams: ${guestAria}; ${eventAria}`,
    },
    source: classification.source || "derived",
  };
}

/**
 * Attach serialization fields onto a customer opportunity DTO (non-mutating Airtable).
 */
export function attachOpportunityRevenueStreamIcons(opportunity = {}) {
  const resolved = resolveOpportunityRevenueStreamIcons(opportunity);
  return {
    ...opportunity,
    revenueStreamClass: resolved.revenueStreamClass,
    revenueStreamLabel: resolved.revenueStreamLabel,
    guestroomThesis: resolved.guestroomThesis || opportunity.guestroomThesis || null,
    eventFnbThesis: resolved.eventFnbThesis || opportunity.eventFnbThesis || null,
    revenueStreamIcons: resolved.icons,
    revenueStreamIconsAria: resolved.aria,
    revenueStreamIconsVersion: GDI_OPPORTUNITY_REVENUE_STREAM_ICONS_V1,
  };
}

/**
 * Permanent regression fixtures for icon states (includes wedding / corporate cases).
 */
export function revenueStreamIconRegressionCases() {
  return [
    {
      id: "guestrooms_only",
      expectClass: HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY,
      expectIcons: { guestrooms: "active", eventFnb: "muted" },
      input: {
        hotelDemandHypotheses: {
          guestroomHypothesis: "Nonlocal executives need overnight stays",
          meetingsHypothesis: null,
        },
      },
    },
    {
      id: "event_fnb_only",
      expectClass: HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY,
      expectIcons: { guestrooms: "muted", eventFnb: "active" },
      input: {
        hotelDemandHypotheses: {
          meetingsHypothesis: "Day meeting with catering; guestrooms not required",
          guestroomHypothesis: null,
        },
      },
    },
    {
      id: "combined",
      expectClass: HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB,
      expectIcons: { guestrooms: "active", eventFnb: "active" },
      input: {
        hotelDemandHypotheses: {
          guestroomHypothesis: "Peak of 40 rooms for nonlocal attendees",
          meetingsHypothesis: "General session and closing banquet",
        },
      },
    },
    {
      id: "unconfirmed",
      expectClass: HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED,
      expectIcons: { guestrooms: "muted", eventFnb: "muted" },
      input: {
        summary: "Local business listing with no hospitality demand mechanism",
        hotelDemandHypotheses: {
          guestroomHypothesis: null,
          meetingsHypothesis: null,
        },
      },
    },
    {
      id: "wedding_catering_only",
      expectClass: HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY,
      expectIcons: { guestrooms: "muted", eventFnb: "active" },
      input: {
        demandSegmentId: "social_special_events",
        summary: "Local wedding reception seeking banquet; no overnight room block",
        hotelDemandHypotheses: {
          primaryMechanism: "Wedding reception banquet",
          meetingsHypothesis: "Ceremony+reception banquet catering for local guests",
          guestroomHypothesis: null,
        },
        cateringOnly: true,
        guestroomStatus: "NOT_REQUIRED",
      },
    },
    {
      id: "corporate_meeting_guestrooms_only",
      expectClass: HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY,
      expectIcons: { guestrooms: "active", eventFnb: "muted" },
      input: {
        demandSegmentId: "corporate_meetings_incentives",
        hotelDemandHypotheses: {
          primaryMechanism: "Corporate visitor lodging",
          guestroomHypothesis: "Visiting engineers need multi-night stays near campus",
          meetingsHypothesis: null,
        },
      },
    },
    {
      id: "missing_legacy_fields",
      expectClass: HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED,
      expectIcons: { guestrooms: "muted", eventFnb: "muted" },
      input: {
        title: "Legacy opportunity without revenue fields",
        demandSegmentId: "corporate_meetings_incentives",
      },
    },
    {
      id: "persisted_class_wins",
      expectClass: HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB,
      expectIcons: { guestrooms: "active", eventFnb: "active" },
      input: {
        revenueStreamClass: "COMBINED_GUESTROOMS_EVENT_FNB",
        guestroomThesis: "Room block for nonlocal board members",
        eventFnbThesis: "Board dinner with private dining",
      },
    },
  ];
}

export function runOpportunityRevenueStreamIconsGate() {
  const cases = revenueStreamIconRegressionCases();
  const results = cases.map((c) => {
    const got = resolveOpportunityRevenueStreamIcons(c.input);
    const pass =
      got.revenueStreamClass === c.expectClass &&
      got.icons.guestrooms === c.expectIcons.guestrooms &&
      got.icons.eventFnb === c.expectIcons.eventFnb;
    return {
      id: c.id,
      pass,
      expectClass: c.expectClass,
      gotClass: got.revenueStreamClass,
      expectIcons: c.expectIcons,
      gotIcons: got.icons,
      aria: got.aria,
    };
  });
  const failures = results.filter((r) => !r.pass);
  return {
    gate: GDI_OPPORTUNITY_REVENUE_STREAM_ICONS_V1,
    pass: failures.length === 0,
    results,
    failures,
  };
}
