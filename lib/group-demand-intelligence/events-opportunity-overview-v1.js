/**
 * Association / Conference / Events — structured Opportunity overview + drawer header.
 * Presentation fallbacks only; never invent dates, attendance, or peak rooms.
 */
import { classifyOpportunityTypeFilter, OPPORTUNITY_TYPE_FILTER } from "./opportunity-type-filter-v1.js";
import { EVENT_COMMERCIAL_TRACK } from "./research/event-discovery-contract-v1.js";
import { cleanAssociationOrganizationName } from "./research/association-events-buyer-recovery-v1.js";
import { composeCustomerFacingHotelSourcingLabel } from "./commercial-consistency/commercial-status-dimensions-v1.js";

export const EVENTS_OPPORTUNITY_OVERVIEW_V1 = "gdi-events-opportunity-overview-v1";

/** Exactly four customer-facing Hotel Sourcing statuses (tile + drawer). */
export const HOTEL_SOURCING_DISPLAY = Object.freeze({
  HOTEL_SOURCING_OPEN: "Hotel Sourcing Open",
  HOTEL_SOURCING_UNCONFIRMED: "Hotel Sourcing Unconfirmed",
  VENUE_CONFIRMED: "Venue Confirmed",
  HISTORICAL_VENUE_ONLY: "Historical Venue Only",
});

/** Evidence-supported event type labels for tile meta line. */
export const EVENT_TYPE_DISPLAY = Object.freeze({
  association_conference: "Association Conference",
  association_meeting: "Association Meeting",
  association_led: "Association Conference",
  medical_scientific: "Association Conference",
  trade_show: "Trade Show",
  corporate_meeting: "Corporate Meeting",
  corporate_retreat: "Leadership Retreat",
  leadership_retreat: "Leadership Retreat",
  incentive_recognition: "Corporate Incentive",
  training_programme: "Training Event",
  training_event: "Training Event",
  industry_conference: "Industry Conference",
  corporate_event: "Corporate Event",
  other_event_led: "Corporate Event",
  unknown: "Corporate Event",
});

export const DEMAND_CYCLE_DISPLAY = Object.freeze({
  CONFIRMED_UPCOMING: "Confirmed upcoming edition",
  FUTURE_CYCLE_EXPECTED: "Future cycle expected",
  PAST_EDITION_NEXT_CYCLE_EXPECTED: "Past edition — next cycle expected",
  RECURRING_COMPETITIVE_CAPTURE: "Recurring competitive capture",
  HISTORICAL_RELATIONSHIP: "Historical relationship development",
  ONE_TIME: "One-time event",
  DISCONTINUED_OR_CANCELED: "Discontinued or canceled",
});

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

function isBlank(v) {
  const s = text(v);
  return !s || /^[—\-–]$/.test(s) || /^n\/?a$/i.test(s) || /^unknown$/i.test(s);
}

export function isAssociationConferenceEventsOpportunity(o = {}) {
  // Segment 8 specialist subtypes use shared ten-section drawers + Venue/Planner
  // Intelligence — never the Association/Conference Events overview path.
  // (PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP contains the substring "EVENT".)
  if (
    o.professionalEventPlannerPartnershipV1 === true ||
    o.preferredLodgingPartnershipV1 === true ||
    o.demandOpportunitySubtype === "PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP" ||
    o.demandOpportunitySubtype === "PREFERRED_LODGING_PARTNERSHIP" ||
    o.opportunityType === "PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP" ||
    o.opportunityType === "PREFERRED_LODGING_PARTNERSHIP" ||
    classifyOpportunityTypeFilter(o) ===
      OPPORTUNITY_TYPE_FILTER.PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP ||
    classifyOpportunityTypeFilter(o) ===
      OPPORTUNITY_TYPE_FILTER.PREFERRED_LODGING_PARTNERSHIP
  ) {
    return false;
  }
  if (classifyOpportunityTypeFilter(o) === OPPORTUNITY_TYPE_FILTER.ASSOCIATION_CONFERENCE_GROUP) {
    return true;
  }
  const seg = text(o.researchSegment || o.segment).toLowerCase();
  if (seg.includes("assoc") || seg.includes("conference")) return true;
  if (o.associationEventsGoldBrief && !/scaling/i.test(seg)) return true;
  if (o.segmentReclassificationPreview === true && seg === "associations_government") {
    return true;
  }
  // MICE / incentive / dated programmes — never leave these on the legacy
  // Account/occasion drawer; they are Events-shaped for Section 1 + ten sections.
  if (/\bmice\b|\bincentive/.test(seg)) return true;
  if (o.eventsDrawerModel || o.eventsOpportunityOverview) return true;
  if (
    (o.eventStartDate || o.eventEndDate || o.eventDateDisplay) &&
    !/scaling/i.test(seg) &&
    (o.opportunityType === "CLOSED_DISQUALIFIED" ||
      o.opportunityType === "ASSOCIATION_CONFERENCE" ||
      /event|conference|congress|summit|incentive/i.test(
        String(o.opportunityTypeLabel || o.title || ""),
      ))
  ) {
    return true;
  }
  return false;
}

function isGenericProgrammeLabel(name) {
  return /^(historical (corporate incentive|event) programme|corporate recognition programme|programme TBD|organizer TBD)$/i.test(
    text(name),
  );
}

/**
 * Drawer header: Event Name when verified; else Company / Organization.
 * Company-led relationship rows keep the organization name (not a generic programme label).
 */
export function resolveEventsDrawerHeaderTitle(o = {}) {
  const overviewEvent = text(o.eventName || o.canonicalEventName);
  const eventName = overviewEvent && !isTechnicalSlug(overviewEvent)
    ? overviewEvent
    : pickEventName(o) || extractEventNameFromEvidenceProse(o);
  const org = pickOrganizerName(o);

  // Company-led historical / competitive-capture: organization is the drawer title
  const track = text(o.commercialOpportunityTrack);
  if (
    org &&
    (o.historicalEventOnly === true ||
      track === EVENT_COMMERCIAL_TRACK.TRACK_D_HISTORICAL_RELATIONSHIP ||
      (track === EVENT_COMMERCIAL_TRACK.TRACK_B_RECURRING_COMPETITIVE_CAPTURE &&
        text(o.historicalProgrammeLabel)))
  ) {
    if (isGenericProgrammeLabel(eventName) || text(o.historicalProgrammeLabel)) {
      // Named competitor programmes (President's Club) may still headline when
      // they are distinct verified event labels without a generic programme stamp.
      if (
        text(o.historicalProgrammeLabel) &&
        (!eventName ||
          isGenericProgrammeLabel(eventName) ||
          namesEffectivelyEqual(eventName, text(o.historicalProgrammeLabel)) ||
          /gold trip/i.test(eventName))
      ) {
        return org;
      }
    }
  }
  if (eventName && isGenericProgrammeLabel(eventName) && org) {
    return org;
  }
  if (eventName && !isTechnicalSlug(eventName) && isDistinctNamedEvent(eventName, org)) {
    return eventName;
  }
  if (org) return org;
  if (eventName && !isTechnicalSlug(eventName)) return eventName;
  const fallback = text(o.displayTitle || o.title);
  if (fallback && !isTechnicalSlug(fallback)) return fallback;
  return org || "Opportunity";
}

function namesEffectivelyEqual(a, b) {
  const na = text(a).toLowerCase();
  const nb = text(b).toLowerCase();
  if (!na || !nb) return false;
  return na === nb || na.includes(nb) || nb.includes(na);
}

/** True when eventName is a real programme title, not merely the org repeated. */
function isDistinctNamedEvent(eventName, org) {
  const ev = text(eventName);
  const o = text(org);
  if (!ev) return false;
  if (!o) return true;
  if (ev.toLowerCase() === o.toLowerCase()) return false;
  // "LivePURE Brand Partner Incentive" contains org but is still the Event Name
  if (ev.length > o.length + 3) return true;
  if (/\b(20\d{2}|congress|conference|incentive|club|trip|retreat|summit|expo|forum)\b/i.test(ev)) {
    return true;
  }
  return !namesEffectivelyEqual(ev, o);
}

function pickOrganizerName(o = {}) {
  const eventHint = text(
    o.eventDisplayName || o.eventName || o.title || o.displayTitle || "",
  ).replace(/\s*\((?:confirmed call|call for papers|cfp|tentative|tbd)[^)]*\)\s*$/i, "");
  const candidates = [
    o.displayCompanyName,
    o.organizationName,
    o.organizer,
    o.organizingEntity,
    o.corporateOrAssociationOwner,
    o.companyName,
    o.legalEntityName,
  ]
    .map((raw) => cleanAssociationOrganizationName(raw) || text(raw))
    .map(text)
    .filter(Boolean);
  for (const c of candidates) {
    if (isTechnicalSlug(c)) continue;
    // Reject truncated discovery blurbs used as organizer
    if (c.length > 90 && /\s/.test(c)) continue;
    if (/\bis\s+(?:a|an|the)\b/i.test(c)) continue;
    if (/\b(anunci[oó]|llev[oó]|CONMEMORACI[OÓ]N)\b/i.test(c)) continue;
    // Reject slogan/tagline rows that embed the event title (not an org name)
    if (
      eventHint &&
      !isTechnicalSlug(eventHint) &&
      c.toLowerCase().includes(eventHint.toLowerCase()) &&
      c.length > eventHint.length + 5
    ) {
      continue;
    }
    // Event name reused as organizer with no distinct legal identity
    if (
      eventHint &&
      !isTechnicalSlug(eventHint) &&
      namesEffectivelyEqual(c, eventHint) &&
      !/\b(Asociaci[oó]n|Association|Sociedad|C[aá]mara|College|Institute|Federation)\b/i.test(c)
    ) {
      continue;
    }
    if (/^(de |the |a |el |la |design:)/i.test(c) && c.length > 20) continue;
    return c;
  }
  // Shared fallback: Cumbre/Expo/Congreso + acronym (AMEXCAP) when org was event-title pollution
  const acronym = eventHint.match(
    /^(?:Cumbre|Expo|Congreso|Congress|Forum|Summit)\s+([A-ZÁÉÍÓÚÑ]{3,}[A-ZÁÉÍÓÚÑ0-9]*)\b/u,
  );
  if (acronym?.[1]) return acronym[1];
  return "";
}

/** Series-identity slugs (normalizedEventName) are not customer display names. */
export function isTechnicalSlug(s) {
  const raw = String(s || "").trim();
  if (!raw) return true;
  if (/^gdi[_-]/i.test(raw)) return true;
  if (/^series:/i.test(raw)) return true;
  // underscore_case identifiers (ASCII + Latin accents)
  if (/^[\p{Ll}\p{Nd}]+(?:_[\p{Ll}\p{Nd}]+)+$/u.test(raw)) return true;
  // compact lowercase tokens with no spaces (lasa2027, icuh, expo_…)
  if (
    !/\s/.test(raw) &&
    !/\p{Lu}/u.test(raw) &&
    /^[\p{Ll}\p{Nd}_.-]{2,80}$/u.test(raw)
  ) {
    return true;
  }
  return false;
}

function cleanEventDisplayName(s) {
  let t = text(s);
  if (!t) return "";
  // Drop trailing parenthetical research notes
  t = t.replace(/\s*\((?:confirmed call|call for papers|cfp|tentative|tbd)[^)]*\)\s*$/i, "").trim();
  t = t.replace(/\s*[-–—,:;]+\s*$/g, "").trim();
  return t;
}

/**
 * Extract a verified programme/event label from research prose when title is org-only.
 * Does not invent names — only surfaces patterns already present in evidence text.
 */
export function extractEventNameFromEvidenceProse(o = {}) {
  const blob = [
    o.eventDisplayName,
    o.historicalProgrammeLabel,
    o.segmentReclassificationRationale,
    o.activityHeadline,
    o.whatIsHappening,
    o.summaryWhat,
    o.summary,
  ]
    .map(text)
    .filter(Boolean)
    .join(" · ");
  if (!blob) return "";

  const patterns = [
    /(Passport to Paradise[:\s]*Canc[uú]n\s*20\d{2})/i,
    /(Passport to Paradise[^·."]{0,40}?20\d{2})/i,
    /(President'?s Club(?:\s+at\s+[A-Z][A-Za-z0-9 &'-]{2,40})?)/i,
    /(\d+(?:st|nd|rd|th)?\s+annual\s+GOLD\s+Trip)/i,
    /(GOLD\s+Trip)/i,
    /(Qcells\s+North\s+America\s*20\d{2})/i,
    /(ACMG'?s?\s+20\d{2}\s+Annual Clinical Genetics Meeting)/i,
    /(Annual Clinical Genetics Meeting(?:\s+20\d{2})?)/i,
    /(Cumbre Empresarial(?:\s+CANACO(?:\s+CDMX)?)?)/i,
    /\b((?:Expo|Cumbre|Congreso|Congress|Conference|Forum|Summit|Retreat)\s+[A-Za-zÁÉÍÓÚÑáéíóúñ0-9 &/'°.-]{2,50}?\s*20\d{2})\b/i,
    /\b([A-Z]{2,}[A-Za-z0-9]*\s*20\d{2})\b/, // LASA2027 / ICUH 2026 style
  ];
  for (const re of patterns) {
    const m = blob.match(re);
    if (m && m[1] && !isTechnicalSlug(m[1])) {
      return cleanEventDisplayName(m[1]);
    }
  }
  return "";
}

function pickEventName(o = {}) {
  const org = pickOrganizerName(o);
  const candidates = [
    o.eventDisplayName,
    o.displayEventName,
    o.eventEditionName,
    o.eventName,
    o.eventIdentity,
    // Human titles before series-identity slugs
    o.title,
    o.displayTitle,
    // Slugs last — only if somehow human-readable
    o.canonicalEventName,
    o.normalizedEventName,
  ]
    .map(cleanEventDisplayName)
    .filter(Boolean);

  for (const c of candidates) {
    if (isTechnicalSlug(c)) continue;
    if (/opportunity/i.test(c) && c.length < 20) continue;
    // Skip pure legal-entity dumps that equal the org without event tokens
    if (
      org &&
      namesEffectivelyEqual(c, org) &&
      !/\b(20\d{2}|congress|conference|cumbre|expo|congreso|incentive|club|trip|meeting|retreat|paradise|forum|summit)\b/i.test(
        c,
      )
    ) {
      continue;
    }
    return c;
  }

  const fromProse = extractEventNameFromEvidenceProse(o);
  if (fromProse) return fromProse;
  return "";
}

function formatIsoDate(iso) {
  const s = text(iso);
  if (!s) return null;
  // Keep YYYY-MM-DD when already that shape
  const m = s.match(/^(\d{4}-\d{2}-\d{2})/);
  if (m) return m[1];
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function buildTiming(o = {}) {
  const track = text(o.commercialOpportunityTrack || o.demandSegmentRouting?.commercialOpportunityTrack);
  const futureStatus = text(o.futureEditionStatus || o.futureDateStatus).toLowerCase();
  const historicalOnly =
    track === EVENT_COMMERCIAL_TRACK.TRACK_D_HISTORICAL_RELATIONSHIP ||
    track === EVENT_COMMERCIAL_TRACK.TRACK_B_RECURRING_COMPETITIVE_CAPTURE ||
    /historical_completed|historical/i.test(text(o.eventStatus)) ||
    o.historicalEventOnly === true ||
    (futureStatus === "unverified" && /historical/i.test(text(o.eventStatus)));

  // Confirmed upcoming may use verifiedStartDate from Exa; never promote historical-only dates.
  const allowLiveDates =
    (!historicalOnly && futureStatus !== "canceled_discontinued") ||
    track === EVENT_COMMERCIAL_TRACK.TRACK_A_UPCOMING_VENUE ||
    /^(confirmed|announced_incomplete)$/i.test(futureStatus);

  const futureStart = formatIsoDate(
    o.futureEditionDate ||
      (allowLiveDates ? o.verifiedStartDate : null) ||
      (allowLiveDates ? o.eventStartDate : null),
  );
  const futureEnd = formatIsoDate(
    o.futureEditionEndDate ||
      (allowLiveDates ? o.verifiedEndDate : null) ||
      (allowLiveDates ? o.eventEndDate : null),
  );

  // Never promote historical dates as upcoming
  if (!futureStart && !futureEnd) {
    return {
      label: "Dates TBD",
      start: null,
      end: null,
      isHistoricalDisplayedAsUpcoming: false,
      historicalStart: historicalOnly ? formatIsoDate(o.eventStartDate || o.historicalEventStartDate) : null,
      historicalEnd: historicalOnly ? formatIsoDate(o.eventEndDate || o.historicalEventEndDate) : null,
    };
  }
  const label =
    futureStart && futureEnd && futureStart !== futureEnd
      ? `${futureStart} → ${futureEnd}`
      : futureStart || futureEnd;
  return {
    label,
    start: futureStart,
    end: futureEnd,
    isHistoricalDisplayedAsUpcoming: false,
    historicalStart: null,
    historicalEnd: null,
  };
}

/** Venue-commitment / sourcing phrases are not geographic locations. */
function isVenueStatusAsLocation(summary) {
  return /host\s+hotel\s+contracted|competitor\s+(?:venue|hotel|booking)|venue\s+confirmed|hotel\s+sourcing|overflow\s+sourcing|room\s+block\s+at|confirmed\s+(?:at|host)/i.test(
    text(summary),
  );
}

function buildLocation(o = {}) {
  const summary = text(o.eventLocationSummary);
  if (
    summary &&
    !/tbd|unknown|unverified/i.test(summary) &&
    !isVenueStatusAsLocation(summary)
  ) {
    return summary;
  }
  const competitor = text(
    o.cmiCompetitorHotel || o.competitorHotel || o.likelyCompetitor || o.verifiedVenue,
  );
  // Prefer city / market geography; never put "Host hotel contracted — X" in Location.
  const parts = [
    o.eventLocationCity || o.eventCity || o.venueCity || o.city || o.destination,
    o.eventLocationRegion || o.eventState || o.region,
    o.eventLocationCountry || o.eventCountry || o.country,
  ]
    .map(text)
    .filter(Boolean);
  if (parts.length) return parts.join(", ");
  if (competitor && isVenueStatusAsLocation(summary)) {
    // Geography unknown; surface competitor as venue context only via note, not Location label.
    return "Location TBD";
  }
  return "Location TBD";
}

/**
 * Normalize research / enum / legacy labels into the four customer-facing statuses.
 * Silence / unannounced venue alone must never become Hotel Sourcing Open.
 */
export function normalizeHotelSourcingStatus(o = {}) {
  const rawLabel = text(
    o.hotelSourcingStatusLabel ||
      o.venueSourcingStatusLabel ||
      o.venueSourcingStatus ||
      o.hotelSourcingStatus ||
      o.sourcingStatus ||
      o.venueCommitmentStatus ||
      o.currentEditionVenueCommitment ||
      "",
  );
  const raw = rawLabel.toUpperCase().replace(/[-\s]+/g, "_");
  const track = text(o.commercialOpportunityTrack);
  const evidenceBlob = `${o.verifiedVenue || ""} ${o.incumbentVenue || ""} ${o.summaryWhat || ""} ${o.activityHeadline || ""} ${o.segmentReclassificationRationale || ""} ${o.whatIsHappening || ""}`;
  const historicalOnly =
    o.historicalEventOnly === true ||
    track === EVENT_COMMERCIAL_TRACK.TRACK_D_HISTORICAL_RELATIONSHIP ||
    /historical_completed|historical_venue_only/i.test(text(o.eventStatus)) ||
    /^historical_venue_only$/i.test(rawLabel);

  // Already one of the four approved display strings
  for (const v of Object.values(HOTEL_SOURCING_DISPLAY)) {
    if (rawLabel.toLowerCase() === v.toLowerCase()) return v;
  }

  const venueNamed =
    /COMMITTED_ELSEWHERE|CONFIRMED_COMPETING|CONFIRMED_COMPETING_HOTEL|HOST_CONTRACTED|HOTEL_CONFIRMED_ALREADY|VENUE_COMMITTED|LIKELY_CLOSED_INCUMBENT|VENUE_CONFIRMED|CONFIRMED_THIS_HOTEL|CONFIRMED_VENUE|FULLY_PLACED|FULLYPLACED|CLOSED_DISQUALIFIED|CLOSED\/DISQUALIFIED/.test(
      raw,
    ) ||
    /fully\s*placed|venue\s*confirmed|sourcing\s*closed/i.test(rawLabel) ||
    /nizuc|ava resort|majestic elegance/i.test(evidenceBlob);

  if (venueNamed) {
    // Past edition host ≠ future venue commitment
    if (historicalOnly) return HOTEL_SOURCING_DISPLAY.HISTORICAL_VENUE_ONLY;
    return HOTEL_SOURCING_DISPLAY.VENUE_CONFIRMED;
  }

  if (
    /HISTORICAL_VENUE|PRIOR_HOST|PAST_VENUE|HISTORICAL_VENUE_ONLY/.test(raw) ||
    historicalOnly
  ) {
    return HOTEL_SOURCING_DISPLAY.HISTORICAL_VENUE_ONLY;
  }

  // Affirmative open only — never OPEN_OR_UNANNOUNCED / silence
  if (
    /HOTEL_SOURCING_OPEN|SOURCING_CONFIRMED_OPEN|SOURCING_OPEN|SELECTION_OPEN|RFP_ACTIVE_SOURCING|ACTIVE_SOURCING/.test(
      raw,
    ) &&
    !/UNANNOUNCED|POTENTIALLY|UNKNOWN|UNCONFIRMED/.test(raw)
  ) {
    return HOTEL_SOURCING_DISPLAY.HOTEL_SOURCING_OPEN;
  }

  return HOTEL_SOURCING_DISPLAY.HOTEL_SOURCING_UNCONFIRMED;
}

/**
 * Prefer the commercial-consistency composer so competitor venue confirmation
 * never collapses into a single "Venue Confirmed" label that contradicts
 * target-hotel overflow/unknown sourcing.
 */
function buildHotelSourcing(o = {}) {
  const composed = text(composeCustomerFacingHotelSourcingLabel(o));
  if (
    composed &&
    (/Competitor venue confirmed|target-hotel overflow|Historical competitor|Overflow sourcing confirmed|Sourcing closed for this edition|Target-hotel sourcing not established/i.test(
      composed,
    ) ||
      composed === HOTEL_SOURCING_DISPLAY.HOTEL_SOURCING_OPEN ||
      composed === HOTEL_SOURCING_DISPLAY.HOTEL_SOURCING_UNCONFIRMED)
  ) {
    return composed;
  }
  // Preserve an already-composed customer label from sanitizer / DTO.
  const prior = text(o.hotelSourcingStatusLabel || o.targetHotelSourcingStatusLabel);
  if (
    prior &&
    /Competitor venue confirmed|target-hotel overflow|Historical competitor/i.test(prior)
  ) {
    return prior;
  }
  return normalizeHotelSourcingStatus(o);
}

/** Drawer note when a competitor (or unnamed) venue is confirmed. */
export function buildVenueConfirmationNote(o = {}, hotelSourcing) {
  const sourcing = text(hotelSourcing);
  const competitorConfirmed =
    /Competitor venue confirmed/i.test(sourcing) ||
    sourcing === HOTEL_SOURCING_DISPLAY.VENUE_CONFIRMED;
  if (!competitorConfirmed) return null;
  const venue = text(
    o.cmiCompetitorHotel ||
      o.competitorHotel ||
      o.likelyCompetitor ||
      o.verifiedVenue ||
      o.incumbentVenue ||
      o.confirmedVenueName,
  );
  const hotelName = text(o.hotelDisplayName || o.hotelName);
  if (venue && hotelName && venue.toLowerCase().includes(hotelName.toLowerCase().slice(0, 12))) {
    return `Confirmed venue appears to be this hotel (${venue}).`;
  }
  if (venue) {
    return `Competitor venue confirmed (${venue}). Target-hotel overflow or secondary lodging is not confirmed — do not treat as an open host RFP.`;
  }
  return "A venue is confirmed for this edition; verify whether it is this hotel, a competitor, or a non-hotel facility before pitching contracted space.";
}

export function resolveEventTypeLabel(o = {}) {
  const explicit = text(o.eventTypeLabel || o.eventTypeDisplayLabel);
  if (explicit && !/^[a-z0-9_]+$/.test(explicit)) return explicit;
  const key = text(o.eventSubtype || o.eventType || o.demandFamily || "")
    .toLowerCase()
    .replace(/[\s-]+/g, "_");
  if (EVENT_TYPE_DISPLAY[key]) return EVENT_TYPE_DISPLAY[key];
  if (/incentive|gold trip|passport to paradise|president'?s club/i.test(
    `${o.summaryWhat || ""} ${o.activityHeadline || ""} ${o.segmentReclassificationRationale || ""} ${o.eventDisplayName || ""}`,
  )) {
    return EVENT_TYPE_DISPLAY.incentive_recognition;
  }
  if (/trade\s*show|expo/i.test(`${o.title || ""} ${o.eventName || ""}`)) {
    return EVENT_TYPE_DISPLAY.trade_show;
  }
  if (/conference|congress|cumbre|congreso/i.test(`${o.title || ""} ${o.eventName || ""}`)) {
    return EVENT_TYPE_DISPLAY.association_conference;
  }
  if (/retreat|leadership/i.test(`${o.title || ""} ${o.summaryWhat || ""}`)) {
    return EVENT_TYPE_DISPLAY.leadership_retreat;
  }
  if (/training/i.test(`${o.title || ""} ${o.summaryWhat || ""}`)) {
    return EVENT_TYPE_DISPLAY.training_event;
  }
  return EVENT_TYPE_DISPLAY.corporate_event;
}

const ROOM_DEMAND_CUSTOMER_LABEL = Object.freeze({
  LODGING_DEMAND_CONFIRMED: "Lodging demand confirmed",
  LODGING_DEMAND_LIKELY: "Lodging demand likely (unconfirmed)",
  LODGING_DEMAND_UNKNOWN: "Room demand not yet published",
  LODGING_DEMAND_UNLIKELY: "Lodging demand unlikely",
  LODGING_DEMAND_NONE: "No lodging demand indicated",
});

function buildRoomDemand(o = {}) {
  const live = text(o.roomDemandLive || o.roomDemandStatusLabel || o.roomDemandStatus);
  if (live && !/not yet|unknown|unpublished|not established|null/i.test(live)) {
    // Reject invented-looking pure numbers without claim kind
    if (/^\d+$/.test(live) && !o.roomDemandClaimKind && !o.peakRoomsClaimKind) {
      return "Room demand not yet published";
    }
    // Never expose internal LODGING_DEMAND_* enums to customers
    const enumKey = live.toUpperCase().replace(/[\s-]+/g, "_");
    if (ROOM_DEMAND_CUSTOMER_LABEL[enumKey]) {
      return ROOM_DEMAND_CUSTOMER_LABEL[enumKey];
    }
    if (/^LODGING_DEMAND_/i.test(live)) {
      return "Room demand not yet published";
    }
    return live;
  }
  if (o.roomDemandVerifiedZero === true) return "Verified: no room demand published (zero)";
  return "Room demand not yet published";
}

function buildDemandCycle(o = {}) {
  const action = text(o.recommendedAction || o.suggestedAction || o.customerRecommendedAction);
  // When commercial action is past-cycle, do not label the overview as a plain
  // "Future cycle expected" — that reads as an open chase against past-cycle policy.
  if (/Past cycle\s*[—–-]\s*do not pursue/i.test(action)) {
    return DEMAND_CYCLE_DISPLAY.PAST_EDITION_NEXT_CYCLE_EXPECTED;
  }
  const track = text(o.commercialOpportunityTrack || o.demandSegmentRouting?.commercialOpportunityTrack);
  const labeled = text(
    o.demandCycleStatus ||
      o.futureCycleEvidenceLabel ||
      o.demandCycleLabel ||
      o.futureCycleEvidenceState,
  );
  if (labeled && !/^unknown$/i.test(labeled)) {
    // Normalize common internal enums + Exa demandCycleStatus
    const u = labeled.toUpperCase().replace(/[-\s]+/g, "_");
    if (/DISCONTINUED|CANCELED|CANCELLED/.test(u)) return DEMAND_CYCLE_DISPLAY.DISCONTINUED_OR_CANCELED;
    if (/CONFIRMED_UPCOMING|UPCOMING_CONFIRMED|CONFIRMED_UPCOMING_EDITION/.test(u)) {
      return DEMAND_CYCLE_DISPLAY.CONFIRMED_UPCOMING;
    }
    if (/FUTURE_CYCLE|CYCLE_EXPECTED/.test(u)) return DEMAND_CYCLE_DISPLAY.FUTURE_CYCLE_EXPECTED;
    if (/COMPETITIVE_CAPTURE|RECURRING/.test(u)) return DEMAND_CYCLE_DISPLAY.RECURRING_COMPETITIVE_CAPTURE;
    if (/HISTORICAL|RELATIONSHIP/.test(u)) return DEMAND_CYCLE_DISPLAY.HISTORICAL_RELATIONSHIP;
    if (/ONE[_ ]?TIME/.test(u)) return DEMAND_CYCLE_DISPLAY.ONE_TIME;
    // Already a display label
    if (Object.values(DEMAND_CYCLE_DISPLAY).includes(labeled)) return labeled;
    return labeled;
  }
  if (track === EVENT_COMMERCIAL_TRACK.TRACK_A_UPCOMING_VENUE) {
    return DEMAND_CYCLE_DISPLAY.CONFIRMED_UPCOMING;
  }
  if (track === EVENT_COMMERCIAL_TRACK.TRACK_B_RECURRING_COMPETITIVE_CAPTURE) {
    return DEMAND_CYCLE_DISPLAY.RECURRING_COMPETITIVE_CAPTURE;
  }
  if (track === EVENT_COMMERCIAL_TRACK.TRACK_C_FUTURE_EDITION_VENUE_CONSTRAINT) {
    return DEMAND_CYCLE_DISPLAY.FUTURE_CYCLE_EXPECTED;
  }
  if (track === EVENT_COMMERCIAL_TRACK.TRACK_D_HISTORICAL_RELATIONSHIP) {
    return DEMAND_CYCLE_DISPLAY.HISTORICAL_RELATIONSHIP;
  }
  // Recurrence evidence without a confirmed date ≠ confirmed upcoming
  if (o.recurrenceEvidence && !o.futureEditionDate && !o.verifiedStartDate) {
    return DEMAND_CYCLE_DISPLAY.FUTURE_CYCLE_EXPECTED;
  }
  if (o.verifiedStartDate || o.futureEditionDate) return DEMAND_CYCLE_DISPLAY.CONFIRMED_UPCOMING;
  if (o.eventStartDate && !/historical/i.test(text(o.eventStatus))) {
    return DEMAND_CYCLE_DISPLAY.CONFIRMED_UPCOMING;
  }
  return DEMAND_CYCLE_DISPLAY.FUTURE_CYCLE_EXPECTED;
}

function buildAttendance(o = {}) {
  const status = text(o.attendanceStatus).toUpperCase();
  const val = o.attendancePublished ?? o.attendance ?? o.publishedAttendance ?? o.estimatedAttendance;
  if (val != null && val !== "" && !Number.isNaN(Number(val))) {
    const n = Number(val);
    if (status.includes("HISTORICAL") || o.attendanceIsHistorical === true) {
      return `Historical: ${n}`;
    }
    if (status.includes("EXPECTED") || o.attendanceIsExpected === true) {
      return `Expected: ${n}`;
    }
    if (status.includes("CONFIRMED") || o.attendanceClaimKind === "VERIFIED") {
      return `Confirmed: ${n}`;
    }
    // Unlabeled numeric — only show if claim kind / Exa published field says published
    if (
      o.attendanceClaimKind === "PUBLISHED" ||
      o.publishedAttendance != null ||
      o.attendancePublished != null
    ) {
      return String(n);
    }
  }
  return "Not yet published";
}

function buildPeakRooms(o = {}) {
  const status = text(o.peakRoomsStatus).toUpperCase();
  const val = o.peakRoomsPublished ?? o.peakRooms ?? o.publishedPeakRooms ?? o.estimatedPeakRooms;
  if (val != null && val !== "" && !Number.isNaN(Number(val))) {
    const n = Number(val);
    // Never present estimated-from-attendance without methodology
    if (
      (/INFERRED|ESTIMATED_FROM_ATTENDANCE|INFERRED_WITH_METHODOLOGY/i.test(
        String(o.peakRoomsClaimKind || o.peakRoomsStatus || ""),
      ) &&
        !o.peakRoomsInferenceMethodology) ||
      status.includes("INFERRED_WITH_METHODOLOGY") && !o.peakRoomsInferenceMethodology
    ) {
      return "Not yet published";
    }
    if (status.includes("HISTORICAL") || o.peakRoomsIsHistorical === true) {
      return `Historical: ${n}`;
    }
    if (
      o.peakRoomsClaimKind === "VERIFIED" ||
      o.publishedPeakRooms != null ||
      o.peakRoomsPublished != null ||
      status.includes("VERIFIED")
    ) {
      return String(n);
    }
    if (o.peakRoomsInferenceMethodology) {
      return `Inferred: ${n} (${text(o.peakRoomsInferenceMethodology)})`;
    }
  }
  return "Not yet published";
}

/**
 * Authoritative structured overview for Events Section 1 + CIB embedding.
 */
export function buildEventsOpportunityOverview(o = {}) {
  const organizerName = pickOrganizerName(o) || "Organizer TBD";
  const track = text(o.commercialOpportunityTrack);
  let eventName = pickEventName(o);

  // Prefer canonical historical programme label for company-led relationship rows
  if (
    text(o.historicalProgrammeLabel) &&
    (o.historicalEventOnly === true ||
      track === EVENT_COMMERCIAL_TRACK.TRACK_D_HISTORICAL_RELATIONSHIP ||
      track === EVENT_COMMERCIAL_TRACK.TRACK_B_RECURRING_COMPETITIVE_CAPTURE)
  ) {
    // Keep a verified named edition (President's Club) when eventDisplayName is set
    if (text(o.eventDisplayName) && !isTechnicalSlug(o.eventDisplayName)) {
      eventName = text(o.eventDisplayName);
    } else {
      eventName = text(o.historicalProgrammeLabel);
    }
  } else if (!eventName || namesEffectivelyEqual(eventName, organizerName)) {
    // Company-led historical without distinct event name
    const prose = `${o.summaryWhat || ""} ${o.title || ""} ${o.activityHeadline || ""} ${o.segmentReclassificationRationale || ""}`;
    if (
      track === EVENT_COMMERCIAL_TRACK.TRACK_D_HISTORICAL_RELATIONSHIP ||
      track === EVENT_COMMERCIAL_TRACK.TRACK_B_RECURRING_COMPETITIVE_CAPTURE ||
      /incentive|gold trip|historical|president'?s club|passport to paradise/i.test(prose)
    ) {
      eventName =
        text(o.historicalProgrammeLabel) ||
        extractEventNameFromEvidenceProse(o) ||
        (/gold trip/i.test(prose)
          ? "Historical Corporate Incentive Programme"
          : /president'?s club/i.test(prose)
            ? "Corporate Recognition Programme"
            : text(o.eventDisplayName) || "Historical event programme");
    } else if (!eventName) {
      const title = text(o.title);
      eventName =
        title && !isTechnicalSlug(title) && !namesEffectivelyEqual(title, organizerName)
          ? title
          : "Programme TBD";
    }
  }
  // Final slug guard — never surface series identity tokens in the drawer
  if (isTechnicalSlug(eventName)) {
    eventName =
      extractEventNameFromEvidenceProse(o) ||
      text(o.historicalProgrammeLabel) ||
      (organizerName !== "Organizer TBD" ? "" : "Programme TBD");
    if (!eventName) eventName = organizerName !== "Organizer TBD" ? organizerName : "Programme TBD";
  }

  // Avoid exact "Org — Org" duplicates; allow "Org — Org 2027" style event editions
  const identityLine =
    !organizerName ||
    organizerName === "Organizer TBD" ||
    text(organizerName).toLowerCase() === text(eventName).toLowerCase()
      ? eventName || organizerName
      : `${organizerName} — ${eventName}`;

  const timing = buildTiming(o);
  const location = buildLocation(o);
  const hotelSourcing = buildHotelSourcing(o);
  const roomDemand = buildRoomDemand(o);
  const demandCycle = buildDemandCycle(o);
  const attendance = buildAttendance(o);
  const peakRooms = buildPeakRooms(o);
  const eventTypeLabel = resolveEventTypeLabel(o);
  const venueConfirmationNote = buildVenueConfirmationNote(o, hotelSourcing);
  const drawerHeaderTitle = resolveEventsDrawerHeaderTitle({
    ...o,
    eventName,
    canonicalEventName: eventName,
    organizationName: organizerName,
  });

  return {
    version: EVENTS_OPPORTUNITY_OVERVIEW_V1,
    organizerName,
    eventName,
    eventTypeLabel,
    identityLine,
    timing: timing.label,
    timingStart: timing.start,
    timingEnd: timing.end,
    timingHistoricalStart: timing.historicalStart,
    timingHistoricalEnd: timing.historicalEnd,
    location,
    hotelSourcing,
    venueConfirmationNote,
    roomDemand,
    demandCycle,
    attendance,
    peakRooms,
    drawerHeaderTitle,
    tileClassificationLine: [organizerName, eventTypeLabel, hotelSourcing]
      .filter((p) => p && p !== "Organizer TBD")
      .join(" · "),
    safeguards: {
      historicalNotShownAsUpcoming: timing.isHistoricalDisplayedAsUpcoming === false,
      sourcingOpenNotInferredFromSilence:
        hotelSourcing !== HOTEL_SOURCING_DISPLAY.HOTEL_SOURCING_OPEN ||
        Boolean(
          text(o.venueSourcingStatus || o.venueSourcingStatusLabel || o.hotelSourcingStatus).match(
            /SOURCING_CONFIRMED_OPEN|HOTEL_SOURCING_OPEN|RFP_ACTIVE|SELECTION_OPEN/i,
          ),
        ),
      peakRoomsNotInventedFromAttendance: peakRooms === "Not yet published" || !/attendance/i.test(peakRooms),
    },
  };
}

/**
 * Attach overview onto opportunity / CIB without deleting history.
 */
export function attachEventsOpportunityOverview(opportunity = {}) {
  if (!isAssociationConferenceEventsOpportunity(opportunity)) {
    return opportunity;
  }
  const overview = buildEventsOpportunityOverview(opportunity);
  const cib = opportunity.commercialIntelligenceBrief
    ? { ...opportunity.commercialIntelligenceBrief }
    : {};
  cib.eventsOpportunityOverview = overview;
  // Keep prose overview for Scaling-compat consumers; structured wins in Events UI
  if (!cib.opportunityOverview) {
    cib.opportunityOverview = overview.identityLine;
  }
  const aegb = opportunity.associationEventsGoldBrief
    ? { ...opportunity.associationEventsGoldBrief, eventsOpportunityOverview: overview }
    : opportunity.associationEventsGoldBrief;
  return {
    ...opportunity,
    eventsOpportunityOverview: overview,
    displayTitle: overview.drawerHeaderTitle,
    drawerHeaderTitle: overview.drawerHeaderTitle,
    // Preserve raw identity; expose cleaned organizer for customer tile/meta.
    organizationNameRaw:
      opportunity.organizationNameRaw || opportunity.organizationName || null,
    // Always publish cleaned organizer (or empty) — never re-leak article prose.
    organizationName:
      overview.organizerName !== "Organizer TBD" ? overview.organizerName : "",
    displayCompanyName:
      overview.organizerName !== "Organizer TBD" ? overview.organizerName : null,
    eventName: overview.eventName || opportunity.eventName,
    eventDisplayName: overview.eventName || opportunity.eventDisplayName,
    // Tile meta contract fields (shared with drawer)
    eventTypeLabel: overview.eventTypeLabel,
    venueSourcingStatusLabel: overview.hotelSourcing,
    hotelSourcingStatusLabel: overview.hotelSourcing,
    hotelSourcingStatus: overview.hotelSourcing,
    commercialIntelligenceBrief: cib,
    associationEventsGoldBrief: aegb || opportunity.associationEventsGoldBrief,
  };
}

/** HTML for Section 1 — label/value separated, no run-on concatenation. */
export function renderEventsOpportunityOverviewHtml(overview, { esc } = {}) {
  const e =
    typeof esc === "function"
      ? esc
      : (s) =>
          String(s ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;");
  const ov = overview || {};
  const row = (label, value) =>
    `<div class="gdi-events-overview-item"><span class="gdi-events-overview-label">${e(label)}</span><span class="gdi-events-overview-value">${e(value || "—")}</span></div>`;

  return (
    `<div class="gdi-events-opportunity-overview" data-overview-version="${e(EVENTS_OPPORTUNITY_OVERVIEW_V1)}">` +
    `<p class="gdi-events-overview-identity">${e(ov.identityLine || "—")}</p>` +
    `<div class="gdi-events-overview-grid">` +

    row("Timing", ov.timing) +
    row("Location", ov.location) +
    row("Hotel sourcing", ov.hotelSourcing) +
    row("Room demand", ov.roomDemand) +
    row("Demand cycle", ov.demandCycle) +
    row("Attendance", ov.attendance) +
    row("Peak rooms", ov.peakRooms) +
    `</div>` +
    (ov.venueConfirmationNote
      ? `<p class="gdi-events-overview-venue-note gdi-muted">${e(ov.venueConfirmationNote)}</p>`
      : "") +
    `</div>`
  );
}
