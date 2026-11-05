/**
 * GDI multi-revenue opportunity qualification V1.
 *
 * Guestrooms are NOT mandatory. An opportunity may qualify on:
 *   - guestroom revenue only
 *   - meetings / catering / banquets / private dining / event revenue only
 *   - combined guestrooms + event/F&B
 *
 * Aligns with commercial-demand-thesis-standard-v1 and commercial-eligibility-contract-v1
 * ("guestrooms and/or local catering").
 *
 * These are revenue opportunity classifications — not new demand segments.
 */

export const GDI_MULTI_REVENUE_QUALIFICATION = "GDI_MULTI_REVENUE_QUALIFICATION";

export const HOTEL_REVENUE_STREAM_CLASS = Object.freeze({
  GUESTROOMS_ONLY: "GUESTROOMS_ONLY",
  EVENT_FNB_ONLY: "EVENT_FNB_ONLY",
  COMBINED_GUESTROOMS_EVENT_FNB: "COMBINED_GUESTROOMS_EVENT_FNB",
  REVENUE_STREAM_UNCONFIRMED: "REVENUE_STREAM_UNCONFIRMED",
});

export const HOTEL_REVENUE_STREAM_LABEL = Object.freeze({
  [HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY]: "Guestroom revenue only",
  [HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY]:
    "Meetings / catering / banquet / private dining / event revenue only",
  [HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB]:
    "Combined guestrooms and meetings/F&B/event revenue",
  [HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED]:
    "Hotel revenue stream unconfirmed",
});

/** Qualifying classes (may support sales-ready / Track A/B when other gates pass). */
export const QUALIFYING_REVENUE_STREAM_CLASSES = Object.freeze([
  HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY,
  HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY,
  HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB,
]);

export const MULTI_REVENUE_NOT_REQUIRED = Object.freeze([
  "confirmed_room_night_volume",
  "immediate_rfp",
  "assumed_overnight_block_for_every_meeting",
  "assumed_room_block_for_every_wedding",
  "assumed_group_accommodation_for_every_sporting_event",
]);

export const MULTI_REVENUE_ASSUMPTION_BANS = Object.freeze([
  "Never assume a corporate meeting requires guestrooms",
  "Never assume a wedding necessarily requires an overnight room block",
  "Never assume a sporting event creates available group accommodation demand",
]);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function blobOf(...parts) {
  return parts
    .map((p) => {
      if (p == null) return "";
      if (typeof p === "string") return p;
      if (Array.isArray(p)) return p.map((x) => text(typeof x === "string" ? x : x?.claim || x?.text || JSON.stringify(x))).join(" ");
      if (typeof p === "object") return JSON.stringify(p);
      return String(p);
    })
    .join(" ")
    .toLowerCase();
}

const GUESTROOM_RE =
  /\b(guestroom|guest rooms?|room block|room.?nights?|overnight|lodging|accommodation|housing block|hotel stay|sleeping rooms?|transient stay|extended stay|\d+\s+rooms?)\b/i;

const EVENT_FNB_RE =
  /\b(meeting|meetings|day.?meeting|day use|conference room|boardroom|catering|banquet|banquets|private dining|reception|f\s*&\s*b|food and beverage|event space|ballroom|workshop venue|training room|offsite|celebration|gala|wedding reception|rehearsal dinner|welcome reception)\b/i;

const NEGATIVE_GUESTROOM_RE =
  /\b(no guestroom|guestrooms? not required|rooms? not required|day.?only|no overnight|local attendees only|no lodging|without (?:an? )?overnight|catering.?only|meeting.?only)\b/i;

const NEGATIVE_EVENT_RE =
  /\b(no meeting|no catering|no banquet|lodging only|rooms? only|no f\s*&\s*b|no event space)\b/i;

/**
 * Detect credible guestroom and event/F&B thesis signals from ORS / opportunity fields.
 * Does not invent volume. Does not assume rooms from meetings or weddings.
 */
export function detectRevenueStreamSignals(input = {}) {
  const hyp = input.hotelDemandHypotheses || {};
  const evidence = input.hotelDemandEvidence || {};
  const guestroomThesis = text(
    hyp.guestroomHypothesis ||
      input.guestroomThesis ||
      input.potentialGuestroomDemand ||
      evidence.potentialGuestroomDemand,
  );
  const meetingsThesis = text(
    hyp.meetingsHypothesis ||
      hyp.cateringHypothesis ||
      input.meetingsThesis ||
      input.eventFnbThesis ||
      input.potentialMeetingDemand ||
      input.potentialCateringDemand ||
      evidence.meetingOrTrainingSignals,
  );
  const primary = text(
    hyp.primaryMechanism ||
      input.primaryDemandHypothesis ||
      input.thesis ||
      input.hotelOpportunityThesis ||
      input.summary,
  );
  const blob = blobOf(
    guestroomThesis,
    meetingsThesis,
    primary,
    input.summary,
    input.recommendedAction,
    evidence.documentedSignals,
    evidence.travelerTypes,
  );

  const thesisUnsupported = (t) =>
    /^(unsupported|not supported|no evidence|none|n\/a|null|unverified)\b/i.test(text(t)) ||
    /\bunsupported\b/i.test(text(t).slice(0, 24));

  let guestroomCredible =
    (Boolean(guestroomThesis) && !thesisUnsupported(guestroomThesis)) ||
    (GUESTROOM_RE.test(blob) && !NEGATIVE_GUESTROOM_RE.test(blob));
  let eventFnbCredible =
    (Boolean(meetingsThesis) && !thesisUnsupported(meetingsThesis)) ||
    (EVENT_FNB_RE.test(blob) && !NEGATIVE_EVENT_RE.test(blob));

  // Explicit negatives win for that stream unless an explicit positive thesis exists
  if (
    NEGATIVE_GUESTROOM_RE.test(blob) &&
    !guestroomThesis &&
    !GUESTROOM_RE.test(guestroomThesis)
  ) {
    guestroomCredible = false;
  }
  if (NEGATIVE_EVENT_RE.test(blob) && !EVENT_FNB_RE.test(meetingsThesis)) {
    eventFnbCredible = false;
  }

  // Wedding / sport / meeting without lodging language → do NOT auto-add guestrooms
  if (
    /\b(wedding|bridal|sport|tournament|meeting|training|workshop)\b/i.test(blob) &&
    !GUESTROOM_RE.test(blob) &&
    !guestroomThesis
  ) {
    guestroomCredible = false;
  }

  // Day meeting / catering language without lodging thesis → event/F&B only is OK
  // Never clear an explicit guestroomHypothesis (combined programmes include banquets).
  if (
    /\b(day.?meeting|day use|local catering|private dining|banquet)\b/i.test(blob) &&
    !GUESTROOM_RE.test(blob) &&
    !guestroomThesis
  ) {
    guestroomCredible = false;
    if (EVENT_FNB_RE.test(blob) || meetingsThesis) eventFnbCredible = true;
  }

  // Explicit unsupported guestroom/event theses win over incidental keyword hits in the same field
  if (guestroomThesis && thesisUnsupported(guestroomThesis)) guestroomCredible = false;
  if (meetingsThesis && thesisUnsupported(meetingsThesis) && !EVENT_FNB_RE.test(meetingsThesis)) {
    // keep eventFnb if the meetings field itself still carries positive F&B language after a soft hedge
  }
  if (meetingsThesis && /^unsupported\b/i.test(meetingsThesis)) eventFnbCredible = false;

  return {
    guestroomCredible,
    eventFnbCredible,
    guestroomThesis: guestroomThesis || null,
    eventFnbThesis: meetingsThesis || null,
    primaryMechanism: primary || null,
    signals: {
      guestroomLanguage: GUESTROOM_RE.test(blob),
      eventFnbLanguage: EVENT_FNB_RE.test(blob),
      negativeGuestroomLanguage: NEGATIVE_GUESTROOM_RE.test(blob),
      negativeEventLanguage: NEGATIVE_EVENT_RE.test(blob),
    },
  };
}

/**
 * Classify hotel revenue stream opportunity (not a demand segment).
 */
export function classifyHotelRevenueStream(input = {}) {
  const detected = detectRevenueStreamSignals(input);
  const { guestroomCredible, eventFnbCredible } = detected;

  let revenueStreamClass = HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED;
  if (guestroomCredible && eventFnbCredible) {
    revenueStreamClass = HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB;
  } else if (guestroomCredible) {
    revenueStreamClass = HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY;
  } else if (eventFnbCredible) {
    revenueStreamClass = HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY;
  }

  const qualifiesOnRevenue =
    QUALIFYING_REVENUE_STREAM_CLASSES.includes(revenueStreamClass);

  return {
    version: GDI_MULTI_REVENUE_QUALIFICATION,
    revenueStreamClass,
    revenueStreamLabel: HOTEL_REVENUE_STREAM_LABEL[revenueStreamClass],
    qualifiesOnRevenue,
    researchContinuationRequired:
      revenueStreamClass === HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED,
    guestroomThesis: detected.guestroomThesis,
    eventFnbThesis: detected.eventFnbThesis,
    primaryMechanism: detected.primaryMechanism,
    detected,
    assumptionBans: [...MULTI_REVENUE_ASSUMPTION_BANS],
    notRequired: [...MULTI_REVENUE_NOT_REQUIRED],
    note:
      revenueStreamClass === HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY
        ? "Catering/events-only opportunities must NOT be demoted for absent guestrooms"
        : revenueStreamClass === HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY
          ? "Guestroom-only opportunities do not require an event/F&B thesis"
          : revenueStreamClass === HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED
            ? "Remain Research Continuation until a credible hotel revenue mechanism is established"
            : "Investigate guestroom and event/F&B pathways independently",
  };
}

/**
 * Attach multi-revenue fields onto a qualification / CIB object.
 */
export function attachMultiRevenueClassification(target = {}, input = {}) {
  const classification = classifyHotelRevenueStream({
    ...input,
    thesis: input.thesis || target.thesis,
    hotelDemandHypotheses: input.hotelDemandHypotheses || target.hotelDemandHypotheses,
    hotelDemandEvidence: input.hotelDemandEvidence || target.hotelDemandEvidence,
    summary: input.summary || target.summary,
    recommendedAction: input.recommendedAction || target.recommendedAction,
  });
  return {
    ...target,
    revenueStreamClass: classification.revenueStreamClass,
    revenueStreamLabel: classification.revenueStreamLabel,
    multiRevenue: classification,
    guestroomThesis: classification.guestroomThesis,
    eventFnbThesis: classification.eventFnbThesis,
  };
}

/**
 * CIB / ten-section helper fields for customer-facing intelligence.
 */
export function buildMultiRevenueCibFields(classification) {
  const c = classification || classifyHotelRevenueStream({});
  return {
    applicableHotelRevenueStreams: c.revenueStreamClass,
    applicableHotelRevenueStreamsLabel: c.revenueStreamLabel,
    potentialBusinessGuestroomThesis: c.guestroomThesis,
    potentialBusinessEventFnbThesis: c.eventFnbThesis,
    revenueStreamFactsVsInferencesNote:
      "Distinguish established facts from informed inferences; do not invent room nights or event spend.",
    revenueSpecificNextActionGuidance:
      c.revenueStreamClass === HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY
        ? "Lead with meeting/catering/banquet/private-dining qualification; do not force a room-block ask."
        : c.revenueStreamClass === HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY
          ? "Lead with lodging/block qualification; event/F&B is optional unless evidenced."
          : c.revenueStreamClass === HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB
            ? "Qualify guestroom and F&B/meeting pathways independently; name decision-makers per stream when known."
            : "Do not treat as sales-ready until a credible Waterstone revenue mechanism is established.",
  };
}

/**
 * Permanent regression fixtures — positive and negative examples.
 */
export function multiRevenueQualificationRegressionCases() {
  return [
    {
      id: "guestrooms_only_positive",
      expect: HOTEL_REVENUE_STREAM_CLASS.GUESTROOMS_ONLY,
      input: {
        hotelDemandHypotheses: {
          primaryMechanism: "Corporate visitor lodging for Boca HQ teams",
          guestroomHypothesis: "Nonlocal executives need overnight stays near Boca campus",
          meetingsHypothesis: null,
          evidenceStatus: "plausible_inferred",
        },
      },
    },
    {
      id: "event_fnb_only_positive",
      expect: HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY,
      input: {
        hotelDemandHypotheses: {
          primaryMechanism: "Local board meeting with lunch catering — day use only",
          guestroomHypothesis: null,
          meetingsHypothesis: "Half-day board meeting needing meeting room and catering; guestrooms not required",
          cateringHypothesis: "Plated lunch for 18 local directors",
          evidenceStatus: "plausible_inferred",
        },
      },
    },
    {
      id: "combined_positive",
      expect: HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB,
      input: {
        hotelDemandHypotheses: {
          primaryMechanism: "Multi-day offsite with sleeping rooms and banquet dinner",
          guestroomHypothesis: "Peak of 40 rooms for nonlocal attendees",
          meetingsHypothesis: "General session and breakouts on property",
          cateringHypothesis: "Welcome reception and closing banquet",
          evidenceStatus: "documented",
        },
      },
    },
    {
      id: "unconfirmed_negative",
      expect: HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED,
      input: {
        hotelDemandHypotheses: {
          primaryMechanism: "Generic company exists in Boca",
          guestroomHypothesis: null,
          meetingsHypothesis: null,
          evidenceStatus: "unsupported",
        },
        summary: "Local business listing with no hospitality demand mechanism",
      },
    },
    {
      id: "wedding_without_assumed_rooms",
      expect: HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY,
      input: {
        summary: "Local wedding reception seeking banquet and private dining; no overnight room block mentioned",
        hotelDemandHypotheses: {
          primaryMechanism: "Wedding reception banquet",
          meetingsHypothesis: "Ceremony+reception banquet catering for local guests",
          guestroomHypothesis: null,
          evidenceStatus: "plausible_inferred",
        },
      },
    },
    {
      id: "day_meeting_without_assumed_guestrooms",
      expect: HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY,
      input: {
        hotelDemandHypotheses: {
          primaryMechanism: "Corporate day meeting",
          meetingsHypothesis: "Day meeting with AV and coffee service; local attendees only; guestrooms not required",
          guestroomHypothesis: null,
          evidenceStatus: "plausible_inferred",
        },
      },
    },
  ];
}

export function runMultiRevenueQualificationGate() {
  const cases = multiRevenueQualificationRegressionCases();
  const results = cases.map((c) => {
    const got = classifyHotelRevenueStream(c.input);
    const pass = got.revenueStreamClass === c.expect;
    return {
      id: c.id,
      expect: c.expect,
      got: got.revenueStreamClass,
      pass,
      qualifiesOnRevenue: got.qualifiesOnRevenue,
      researchContinuationRequired: got.researchContinuationRequired,
    };
  });
  const failures = results.filter((r) => !r.pass);
  return {
    gate: GDI_MULTI_REVENUE_QUALIFICATION,
    pass: failures.length === 0,
    results,
    failures,
    asserts: {
      cateringOnlyNotDemotedForMissingGuestrooms: results.some(
        (r) =>
          r.id === "event_fnb_only_positive" &&
          r.pass &&
          r.qualifiesOnRevenue === true,
      ),
      weddingDoesNotAssumeRooms: results.some(
        (r) => r.id === "wedding_without_assumed_rooms" && r.pass,
      ),
      dayMeetingDoesNotAssumeGuestrooms: results.some(
        (r) => r.id === "day_meeting_without_assumed_guestrooms" && r.pass,
      ),
      unconfirmedStaysContinuation: results.some(
        (r) =>
          r.id === "unconfirmed_negative" &&
          r.pass &&
          r.researchContinuationRequired === true,
      ),
    },
  };
}
