/**
 * Events customer tile pills v1 — segment-aware Priority · Commercial Stage · Timing.
 *
 * Visual contract matches Scaling three-pill tiles. Stage vocabulary is Events-specific:
 *   Pursue Now | Future Capture | Relationship Development | Research Needed
 *
 * Does not invent High priority or Pursue Now without evidence.
 * Missing/unreliable priority → Watch (never customer-facing Unclassified).
 * Historical dates are never shown as upcoming timing pills.
 */

import { PRIORITY, canonicalizePriorityKey } from "./claim-types.js";
import { EVENT_COMMERCIAL_TRACK } from "./research/event-discovery-contract-v1.js";
import { HOTEL_SOURCING_DISPLAY } from "./events-opportunity-overview-v1.js";
import {
  resolveUniversalTileTiming,
  TIMING_TYPE,
} from "./universal-timing-intelligence-v1.js";

export const EVENTS_TILE_COMMERCIAL_PILLS_V1 = "gdi-events-tile-commercial-pills-v1";

export const EVENTS_COMMERCIAL_STAGE = Object.freeze({
  PURSUE_NOW: "PURSUE_NOW",
  FUTURE_CAPTURE: "FUTURE_CAPTURE",
  RELATIONSHIP_DEVELOPMENT: "RELATIONSHIP_DEVELOPMENT",
  RESEARCH_NEEDED: "RESEARCH_NEEDED",
});

export const EVENTS_COMMERCIAL_STAGE_LABEL = Object.freeze({
  [EVENTS_COMMERCIAL_STAGE.PURSUE_NOW]: "Pursue Now",
  [EVENTS_COMMERCIAL_STAGE.FUTURE_CAPTURE]: "Future Capture",
  [EVENTS_COMMERCIAL_STAGE.RELATIONSHIP_DEVELOPMENT]: "Relationship Development",
  [EVENTS_COMMERCIAL_STAGE.RESEARCH_NEEDED]: "Research Needed",
});

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

function isoDay(v) {
  const m = text(v).match(/^(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : null;
}

function trackOf(o = {}) {
  return text(
    o.commercialOpportunityTrack ||
      o.demandSegmentRouting?.commercialOpportunityTrack ||
      "",
  );
}

function sourcingLabel(o = {}) {
  return text(
    o.eventsOpportunityOverview?.hotelSourcing ||
      o.hotelSourcingStatusLabel ||
      o.venueSourcingStatusLabel ||
      o.hotelSourcingStatus ||
      o.venueSourcingStatus ||
      "",
  );
}

function isHistoricalCommercial(o = {}) {
  const track = trackOf(o);
  return (
    o.historicalEventOnly === true ||
    track === EVENT_COMMERCIAL_TRACK.TRACK_D_HISTORICAL_RELATIONSHIP ||
    track === EVENT_COMMERCIAL_TRACK.TRACK_B_RECURRING_COMPETITIVE_CAPTURE ||
    /historical_completed|historical/i.test(text(o.eventStatus))
  );
}

/**
 * Verified *upcoming* event dates only — never past historical dates as next demand.
 * Uses Universal Timing Intelligence V1 (structured + saved-research narrative).
 */
export function resolveEventsTileTiming(o = {}) {
  if (isHistoricalCommercial(o) && !isoDay(o.futureEditionDate)) {
    const futureOnly = {
      ...o,
      eventStartDate: null,
      eventEndDate: null,
      verifiedStartDate: null,
      verifiedEndDate: null,
    };
    // Still allow explicit futureEditionDate / narrative future edition recovery.
    if (!isoDay(o.futureEditionDate) && !o.whatIsHappening && !o.summaryWhat) {
      return {
        start: null,
        end: null,
        label: "Dates TBD",
        displayLabel: "Dates TBD",
        hasVerifiedUpcomingDate: false,
      };
    }
    const recovered = resolveUniversalTileTiming({
      ...futureOnly,
      futureEditionDate: o.futureEditionDate,
      whatIsHappening: o.whatIsHappening,
      summaryWhat: o.summaryWhat,
      establishedFacts: o.establishedFacts,
      title: o.title,
      programmeName: o.programmeName,
    });
    if (!recovered.hasVerifiedUpcomingDate) {
      return {
        start: null,
        end: null,
        label: "Dates TBD",
        displayLabel: "Dates TBD",
        hasVerifiedUpcomingDate: false,
      };
    }
    return {
      start: recovered.start,
      end: recovered.end,
      label:
        recovered.timingType === TIMING_TYPE.UNKNOWN
          ? "Dates TBD"
          : recovered.hasVerifiedUpcomingDate
            ? "VERIFIED_DATE"
            : recovered.displayLabel,
      displayLabel: recovered.displayLabel,
      hasVerifiedUpcomingDate: true,
      timingType: recovered.timingType,
      canonicalTiming: recovered.canonicalTiming,
    };
  }

  const resolved = resolveUniversalTileTiming(o);
  return {
    start: resolved.start,
    end: resolved.end,
    label: resolved.hasVerifiedUpcomingDate
      ? "VERIFIED_DATE"
      : resolved.displayLabel || "Dates TBD",
    displayLabel: resolved.displayLabel || "Dates TBD",
    hasVerifiedUpcomingDate: resolved.hasVerifiedUpcomingDate === true,
    timingType: resolved.timingType,
    precision: resolved.precision,
    canonicalTiming: resolved.canonicalTiming,
  };
}

function hasCertifiedNamedBuyer(o = {}) {
  const who =
    o.associationEventsGoldBrief?.whoToContact ||
    o.commercialIntelligenceBrief?.whoToContact ||
    null;
  const primary = who?.primary;
  if (!primary || !text(primary.name)) return false;
  if (primary.isDirectBuyer === true) return true;
  if (primary.isRelevantCommercialEntryPoint === true) return true;
  const cls = upper(primary.contactEligibilityClass);
  if (cls && !/INELIGIBLE|UNSPECIFIED|UNKNOWN/.test(cls) && /ELIGIBLE|VERIFIED|BUYER/.test(cls)) {
    return true;
  }
  return false;
}

function isWrongGeographyForHotel(o = {}) {
  const narrative = [o.whatIsHappening, o.summaryWhat, o.whyActNow]
    .map(text)
    .join(" · ");
  // Explicit research disqualifiers — do not let hotel catchment boilerplate cancel these.
  if (/resolves to ACMG|not a Mexico City event|outside the hotel|wrong city|not in the current Guadalajara/i.test(narrative)) {
    return true;
  }
  if (
    /\b(Baltimore|Guadalajara|Tijuana)\b/i.test(narrative) &&
    !/\b(Mexico City|Ciudad de México|CDMX|Canc[uú]n).{0,40}\b(edition|congress|event|summit|venue|host)/i.test(
      narrative,
    )
  ) {
    return true;
  }
  return false;
}

/**
 * Evidence-grounded Events priority. Never returns UNCLASSIFIED for customer pills.
 */
export function deriveEventsCommercialPriority(o = {}) {
  const explicit = canonicalizePriorityKey(
    o.priorityFilterKey || o.priority || o.priorityRaw,
  );
  // Commercial eligibility overlay is newer than research defaults — never let
  // derivation or a stale Watch stamp override an audited High/Medium.
  if (
    o._commercialEligibilityOverlayLocked === true &&
    (explicit === PRIORITY.HIGH || explicit === PRIORITY.MEDIUM)
  ) {
    return explicit;
  }

  if (isWrongGeographyForHotel(o)) return PRIORITY.WATCHLIST;

  const sourcing = sourcingLabel(o);
  const timing = resolveEventsTileTiming(o);
  const track = trackOf(o);
  const buyer = hasCertifiedNamedBuyer(o);
  const lodgingOpen = sourcing === HOTEL_SOURCING_DISPLAY.HOTEL_SOURCING_OPEN;
  const venueConfirmed = sourcing === HOTEL_SOURCING_DISPLAY.VENUE_CONFIRMED;
  const historicalOnly = sourcing === HOTEL_SOURCING_DISPLAY.HISTORICAL_VENUE_ONLY;

  let derived = PRIORITY.WATCHLIST;

  // High — strong hotel-specific demand + buyer + actionable path (strict)
  if (
    lodgingOpen &&
    buyer &&
    (timing.hasVerifiedUpcomingDate || track === EVENT_COMMERCIAL_TRACK.TRACK_A_UPCOMING_VENUE)
  ) {
    derived = PRIORITY.HIGH;
  } else if (
    !isWrongGeographyForHotel(o) &&
    (timing.hasVerifiedUpcomingDate ||
      track === EVENT_COMMERCIAL_TRACK.TRACK_A_UPCOMING_VENUE ||
      track === EVENT_COMMERCIAL_TRACK.TRACK_B_RECURRING_COMPETITIVE_CAPTURE ||
      (historicalOnly &&
        /Expo Reforma|UNAM|JW Marriott|recurring|announced|2027|next edition/i.test(
          `${o.whatIsHappening || ""} ${o.summaryWhat || ""}`,
        )))
  ) {
    // Competing venue alone with future dates can still be Medium (overflow/future capture)
    if (
      venueConfirmed ||
      lodgingOpen ||
      historicalOnly ||
      sourcing === HOTEL_SOURCING_DISPLAY.HOTEL_SOURCING_UNCONFIRMED
    ) {
      derived = PRIORITY.MEDIUM;
    }
  }

  // Explicit High/Medium from research wins over weaker derivation.
  if (explicit === PRIORITY.HIGH || explicit === PRIORITY.MEDIUM) {
    return explicit;
  }
  // Stale Watch / missing must not block evidence-backed High/Medium.
  return derived;
}

/**
 * Events commercial stage (pill 2). Never implies current edition available when venue is competitor-confirmed.
 */
export function deriveEventsCommercialStage(o = {}) {
  const sourcing = sourcingLabel(o);
  const timing = resolveEventsTileTiming(o);
  const track = trackOf(o);
  const buyer = hasCertifiedNamedBuyer(o);
  const wrongGeo = isWrongGeographyForHotel(o);
  const lodgingOpen = sourcing === HOTEL_SOURCING_DISPLAY.HOTEL_SOURCING_OPEN;
  const venueConfirmed = sourcing === HOTEL_SOURCING_DISPLAY.VENUE_CONFIRMED;

  if (wrongGeo) {
    return {
      key: EVENTS_COMMERCIAL_STAGE.RESEARCH_NEEDED,
      label: EVENTS_COMMERCIAL_STAGE_LABEL.RESEARCH_NEEDED,
    };
  }

  // Pursue Now — credible immediate action toward accessible business
  if (lodgingOpen && (buyer || timing.hasVerifiedUpcomingDate)) {
    return {
      key: EVENTS_COMMERCIAL_STAGE.PURSUE_NOW,
      label: EVENTS_COMMERCIAL_STAGE_LABEL.PURSUE_NOW,
    };
  }

  // Future Capture — recurrence / future edition / competitor displacement pathway
  if (
    track === EVENT_COMMERCIAL_TRACK.TRACK_B_RECURRING_COMPETITIVE_CAPTURE ||
    (venueConfirmed && timing.hasVerifiedUpcomingDate) ||
    (sourcing === HOTEL_SOURCING_DISPLAY.HISTORICAL_VENUE_ONLY &&
      /announced|2027|next edition|recurring|preferential hotel|future/i.test(
        `${o.whatIsHappening || ""} ${o.whyActNow || ""} ${o.summaryWhat || ""}`,
      ))
  ) {
    // Never label locked current host inventory as Pursue Now — Future Capture only
    return {
      key: EVENTS_COMMERCIAL_STAGE.FUTURE_CAPTURE,
      label: EVENTS_COMMERCIAL_STAGE_LABEL.FUTURE_CAPTURE,
    };
  }

  // Relationship Development — relevant organizer relationship, no verified active sourcing cycle
  if (
    track === EVENT_COMMERCIAL_TRACK.TRACK_D_HISTORICAL_RELATIONSHIP ||
    sourcing === HOTEL_SOURCING_DISPLAY.HISTORICAL_VENUE_ONLY
  ) {
    return {
      key: EVENTS_COMMERCIAL_STAGE.RELATIONSHIP_DEVELOPMENT,
      label: EVENTS_COMMERCIAL_STAGE_LABEL.RELATIONSHIP_DEVELOPMENT,
    };
  }

  // Preferential-hotel / unconfirmed lodging with confirmed nearby programme
  if (
    venueConfirmed &&
    !timing.hasVerifiedUpcomingDate &&
    /preferential|Expo Reforma|hotel-rate/i.test(`${o.whatIsHappening || ""} ${o.whyActNow || ""}`)
  ) {
    return {
      key: EVENTS_COMMERCIAL_STAGE.FUTURE_CAPTURE,
      label: EVENTS_COMMERCIAL_STAGE_LABEL.FUTURE_CAPTURE,
    };
  }

  return {
    key: EVENTS_COMMERCIAL_STAGE.RESEARCH_NEEDED,
    label: EVENTS_COMMERCIAL_STAGE_LABEL.RESEARCH_NEEDED,
  };
}

/**
 * Full Events customer badge plan fields (merged into planCustomerTileBadges).
 */
export function planEventsCustomerTileBadges(opportunity = {}) {
  const priorityFilterKey = deriveEventsCommercialPriority(opportunity);
  const stage = deriveEventsCommercialStage(opportunity);
  const timing = resolveEventsTileTiming(opportunity);

  return {
    version: EVENTS_TILE_COMMERCIAL_PILLS_V1,
    segment: "events",
    priorityFilterKey,
    priorityPillLabel:
      priorityFilterKey === PRIORITY.HIGH
        ? "High"
        : priorityFilterKey === PRIORITY.MEDIUM
          ? "Medium"
          : "Watch",
    showPriorityPill: true,
    commercialStageKey: stage.key,
    commercialStagePillLabel: stage.label,
    showCommercialStagePill: true,
    showDatePill: true,
    datePillLabel: timing.label,
    datePillDisplayLabel: timing.displayLabel || timing.label || "Dates TBD",
    hasVerifiedEventDate: timing.hasVerifiedUpcomingDate,
    tileEventStartDate: timing.start,
    tileEventEndDate: timing.end,
    canonicalTiming: timing.canonicalTiming || null,
    timingType: timing.timingType || null,
    // Suppress Scaling/legacy Exploratory / Unclassified as customer pills
    suppressedCustomerPills: [
      "Unclassified",
      "Exploratory",
      "Excluded",
      "OPEN_ENDED",
      "Corporate Account Relationship",
    ],
  };
}
