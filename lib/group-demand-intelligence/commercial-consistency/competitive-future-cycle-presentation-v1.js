/**
 * Competitive vs future-cycle presentation V1.
 *
 * Separates confirmed competitor-booked business from potential future-cycle
 * opportunity without inventing dates, open RFPs, overflow, or contacts.
 * Preserves independent dimensions: competitor venue, target-hotel sourcing,
 * future-cycle opportunity status, commercial actionability.
 */

import {
  COMPETITOR_VENUE_STATUS,
  COMPETITOR_VENUE_STATUS_LABEL,
  TARGET_HOTEL_SOURCING_STATUS,
  TARGET_HOTEL_SOURCING_STATUS_LABEL,
  COMMERCIAL_ACTIONABILITY,
  normalizeTargetHotelSourcingFromLegacy,
  recommendCommercialAction,
} from "./commercial-status-dimensions-v1.js";

export const COMPETITIVE_COMMERCIAL_ACTION = Object.freeze({
  MONITOR_FUTURE_CYCLE: "MONITOR_FUTURE_CYCLE",
  DEVELOP_ORGANIZER_RELATIONSHIP: "DEVELOP_ORGANIZER_RELATIONSHIP",
  QUALIFY_OVERFLOW: "QUALIFY_OVERFLOW",
  PURSUE_OPEN_BUSINESS: "PURSUE_OPEN_BUSINESS",
});

export const COMPETITIVE_COMMERCIAL_ACTION_LABEL = Object.freeze({
  [COMPETITIVE_COMMERCIAL_ACTION.MONITOR_FUTURE_CYCLE]: "Monitor Future Cycle",
  [COMPETITIVE_COMMERCIAL_ACTION.DEVELOP_ORGANIZER_RELATIONSHIP]:
    "Develop Organizer Relationship",
  [COMPETITIVE_COMMERCIAL_ACTION.QUALIFY_OVERFLOW]: "Qualify Overflow",
  [COMPETITIVE_COMMERCIAL_ACTION.PURSUE_OPEN_BUSINESS]: "Pursue Open Business",
});

/** Map onto the eight canonical sales-action pill keys (no new demand segment). */
export const COMPETITIVE_ACTION_TO_SALES_PILL = Object.freeze({
  [COMPETITIVE_COMMERCIAL_ACTION.MONITOR_FUTURE_CYCLE]: "MONITOR",
  [COMPETITIVE_COMMERCIAL_ACTION.DEVELOP_ORGANIZER_RELATIONSHIP]: "BUILD_ACCOUNT",
  [COMPETITIVE_COMMERCIAL_ACTION.QUALIFY_OVERFLOW]: "QUALIFY_OPPORTUNITY",
  [COMPETITIVE_COMMERCIAL_ACTION.PURSUE_OPEN_BUSINESS]: "CONTACT_BUYER",
});

export const FUTURE_CYCLE_OPPORTUNITY_STATUS = Object.freeze({
  MONITOR: "MONITOR",
  NOT_APPLICABLE: "NOT_APPLICABLE",
  UNKNOWN: "UNKNOWN",
});

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

function uniqueUrls(list) {
  const out = [];
  const seen = new Set();
  for (const u of list || []) {
    const s = text(u);
    if (!s || !/^https?:\/\//i.test(s) || seen.has(s)) continue;
    seen.add(s);
    out.push(s);
  }
  return out;
}

function collectEvidenceUrls(opp = {}) {
  const urls = [];
  for (const u of opp.cmiEvidenceUrls || []) urls.push(u);
  const claims = opp.cmiEvidenceLedger?.claims || opp.claims || [];
  for (const c of claims) {
    for (const u of c?.evidenceUrls || []) urls.push(u);
  }
  for (const s of opp.sources || []) {
    if (typeof s === "string") urls.push(s);
    else if (s && typeof s === "object") urls.push(s.url || s.href || s.sourceUrl);
  }
  urls.push(opp.sourceUrl, opp.primarySourceUrl, opp.lodgingEvidenceUrl);
  return uniqueUrls(urls);
}

function isSlugToken(v) {
  const t = text(v);
  if (!t) return true;
  if (/^[a-z0-9]+(_[a-z0-9]+)+$/.test(t)) return true;
  if (!/\s/.test(t) && !/[A-ZÁÉÍÓÚÑ]/.test(t) && /^[a-z0-9_.-]{2,64}$/.test(t)) {
    return true;
  }
  return false;
}

function eventNameEdition(opp = {}) {
  const org = text(opp.organizationName || opp.displayCompanyName);
  const candidates = [
    opp.eventDisplayName,
    opp.displayTitle,
    opp.opportunityName,
    opp.title,
    opp.canonicalEventName,
    opp.eventName,
  ];
  let raw = "";
  for (const c of candidates) {
    const t = text(c);
    if (!t || isSlugToken(t)) continue;
    raw = t;
    break;
  }
  // Prefer human titles; strip remediations that already embed competitor host wording.
  const stripped = raw
    .replace(/\s*[—–-]\s*.*\b(host|confirmed|overflow|verify)\b.*$/i, "")
    .trim();
  const year =
    (raw.match(/\b(20\d{2})\b/) ||
      String(opp.eventStartDate || "").match(/\b(20\d{2})\b/) ||
      [])[1];
  if (stripped && !isSlugToken(stripped)) return stripped;
  if (org) return year ? `${org} — ${year}` : org;
  return raw || "This event edition";
}

function bookingDatesLabel(opp = {}) {
  const start = text(opp.eventStartDate || opp.startDate);
  const end = text(opp.eventEndDate || opp.endDate);
  if (start && end && start !== end) return `${start} – ${end}`;
  if (start) return start;
  const timing = text(opp.eventTiming || opp.eventDates || opp.datesSummary);
  return timing || null;
}

function isConfirmedCompetitorVenue(status) {
  const s = upper(status);
  return (
    s === COMPETITOR_VENUE_STATUS.CONFIRMED_CONTRACTED_BLOCK ||
    s === COMPETITOR_VENUE_STATUS.CONFIRMED_EVENT_VENUE ||
    s === COMPETITOR_VENUE_STATUS.VERIFIED_HISTORICAL_VENUE
  );
}

/** Lightweight display names for GDI hotel IDs — presentation only, not a new segment. */
const HOTEL_ID_DISPLAY_NAME = Object.freeze({
  recLuxvwwxID7U2B8: "Bethesda Marriott",
});

function targetHotelDisplayName(opp = {}) {
  return (
    text(
      opp.targetHotelName ||
        opp.hotelName ||
        opp.propertyName ||
        opp.gdiHotelName ||
        HOTEL_ID_DISPLAY_NAME[String(opp.hotelId || opp.gdiHotelId || "")]
    ) || "the target hotel"
  );
}

/**
 * Resolve the four founder commercial action labels from evidence dimensions.
 */
export function resolveCompetitiveCommercialAction(opp = {}) {
  const sourcing = normalizeTargetHotelSourcingFromLegacy(opp);
  const venue = upper(
    opp.competitorVenueStatus || opp.cmiCompetitorVenueStatus || ""
  );
  const competitiveStatus = upper(opp.competitiveStatus || "");
  const verified =
    opp.cmiVerified === true || competitiveStatus === "VERIFIED";

  if (sourcing === TARGET_HOTEL_SOURCING_STATUS.OPEN_CONFIRMED) {
    return COMPETITIVE_COMMERCIAL_ACTION.PURSUE_OPEN_BUSINESS;
  }
  if (sourcing === TARGET_HOTEL_SOURCING_STATUS.OVERFLOW_CONFIRMED) {
    return COMPETITIVE_COMMERCIAL_ACTION.QUALIFY_OVERFLOW;
  }
  if (sourcing === TARGET_HOTEL_SOURCING_STATUS.FUTURE_CYCLE_TO_MONITOR) {
    return COMPETITIVE_COMMERCIAL_ACTION.MONITOR_FUTURE_CYCLE;
  }
  if (
    verified ||
    venue === COMPETITOR_VENUE_STATUS.CONFIRMED_CONTRACTED_BLOCK ||
    venue === COMPETITOR_VENUE_STATUS.CONFIRMED_EVENT_VENUE
  ) {
    // Confirmed competitor booking without open/overflow → qualify residual need,
    // not host RFP.
    if (
      sourcing === TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED ||
      sourcing === TARGET_HOTEL_SOURCING_STATUS.UNKNOWN
    ) {
      return COMPETITIVE_COMMERCIAL_ACTION.QUALIFY_OVERFLOW;
    }
    if (sourcing === TARGET_HOTEL_SOURCING_STATUS.SOURCING_CLOSED) {
      return COMPETITIVE_COMMERCIAL_ACTION.MONITOR_FUTURE_CYCLE;
    }
  }
  if (competitiveStatus === "HISTORICAL") {
    return COMPETITIVE_COMMERCIAL_ACTION.MONITOR_FUTURE_CYCLE;
  }
  if (
    opp.commercialActionability ===
      COMMERCIAL_ACTIONABILITY.ACCOUNT_DEVELOPMENT ||
    competitiveStatus === "NEEDS_VERIFICATION"
  ) {
    return COMPETITIVE_COMMERCIAL_ACTION.DEVELOP_ORGANIZER_RELATIONSHIP;
  }
  return COMPETITIVE_COMMERCIAL_ACTION.DEVELOP_ORGANIZER_RELATIONSHIP;
}

function buildConfirmedSection(opp = {}, actionKey) {
  const venue = upper(opp.competitorVenueStatus || "");
  const competitiveStatus = upper(opp.competitiveStatus || "");
  const competitorHotel = text(
    opp.cmiCompetitorHotel ||
      opp.competitorHotel ||
      opp.likelyCompetitor ||
      opp.competitorVenue
  );
  const verified =
    opp.cmiVerified === true || competitiveStatus === "VERIFIED";
  const historical = competitiveStatus === "HISTORICAL";
  const applicable =
    Boolean(competitorHotel) &&
    (verified ||
      historical ||
      isConfirmedCompetitorVenue(venue) ||
      competitiveStatus === "NEEDS_VERIFICATION");

  if (!applicable) {
    return {
      applicable: false,
      headline: null,
      eventNameEdition: null,
      competitorHotel: null,
      bookingDates: null,
      evidenceUrls: [],
      competitorVenueStatus: venue || null,
      competitorVenueStatusLabel:
        COMPETITOR_VENUE_STATUS_LABEL[venue] || null,
      currentSourcingStatus: null,
      currentSourcingStatusLabel: null,
    };
  }

  const sourcing = normalizeTargetHotelSourcingFromLegacy(opp);
  const target = targetHotelDisplayName(opp);
  const edition = eventNameEdition(opp);
  const dates = bookingDatesLabel(opp);
  let headline;
  if (historical || venue === COMPETITOR_VENUE_STATUS.VERIFIED_HISTORICAL_VENUE) {
    headline = competitorHotel
      ? `A prior edition was hosted / lodged at ${competitorHotel}. That historical booking is not available business for ${target}.`
      : `A prior competitor venue is evidenced historically. That booking is not available business for ${target}.`;
  } else if (verified || isConfirmedCompetitorVenue(venue)) {
    headline = competitorHotel
      ? `${edition} accommodation / venue is confirmed at ${competitorHotel}. This business is not currently available to ${target}.`
      : `${edition} is confirmed at a competitor venue. This business is not currently available to ${target}.`;
  } else {
    headline = competitorHotel
      ? `Competitor association with ${competitorHotel} is under investigation — not a confirmed booking for ${target} to pursue as host.`
      : `Competitor association is under investigation — not confirmed open business for ${target}.`;
  }

  return {
    applicable: true,
    headline,
    eventNameEdition: edition,
    competitorHotel: competitorHotel || null,
    bookingDates: dates,
    evidenceUrls: collectEvidenceUrls(opp),
    competitorVenueStatus: venue || null,
    competitorVenueStatusLabel:
      COMPETITOR_VENUE_STATUS_LABEL[venue] ||
      (verified ? "Verified competitor booking" : null),
    currentSourcingStatus: sourcing,
    currentSourcingStatusLabel:
      TARGET_HOTEL_SOURCING_STATUS_LABEL[sourcing] || null,
    commercialActionKey: actionKey,
  };
}

function buildFutureSection(opp = {}, actionKey) {
  const sourcing = normalizeTargetHotelSourcingFromLegacy(opp);
  const competitiveStatus = upper(opp.competitiveStatus || "");
  const verified =
    opp.cmiVerified === true || competitiveStatus === "VERIFIED";
  const historical = competitiveStatus === "HISTORICAL";
  const target = targetHotelDisplayName(opp);
  const competitorHotel = text(
    opp.cmiCompetitorHotel || opp.competitorHotel || opp.likelyCompetitor
  );

  const futureCycle =
    sourcing === TARGET_HOTEL_SOURCING_STATUS.FUTURE_CYCLE_TO_MONITOR ||
    historical ||
    (verified &&
      (sourcing === TARGET_HOTEL_SOURCING_STATUS.SOURCING_CLOSED ||
        sourcing === TARGET_HOTEL_SOURCING_STATUS.NOT_ESTABLISHED ||
        sourcing === TARGET_HOTEL_SOURCING_STATUS.UNKNOWN));

  if (!futureCycle && actionKey === COMPETITIVE_COMMERCIAL_ACTION.PURSUE_OPEN_BUSINESS) {
    return {
      applicable: false,
      futureCycleOpportunityStatus: FUTURE_CYCLE_OPPORTUNITY_STATUS.NOT_APPLICABLE,
      headline: null,
      nextEditionHint: null,
      expectedTiming: null,
      futureHotelSelectionKnown: false,
      whyTargetHotelCandidate: null,
      recommendedSalesAction: null,
    };
  }

  if (!futureCycle && !verified && !historical) {
    // Needs-verification competitive claims: relationship development, not invented future edition.
    if (competitiveStatus === "NEEDS_VERIFICATION") {
      return {
        applicable: true,
        futureCycleOpportunityStatus: FUTURE_CYCLE_OPPORTUNITY_STATUS.UNKNOWN,
        headline:
          "Competitor-related intelligence still needs verification. Do not treat this as available host business until housing / venue evidence is confirmed.",
        nextEditionHint: null,
        expectedTiming: null,
        futureHotelSelectionKnown: false,
        whyTargetHotelCandidate: null,
        recommendedSalesAction: recommendCommercialAction({
          competitorVenueStatus: opp.competitorVenueStatus,
          targetHotelSourcingStatus: sourcing,
          commercialActionability: opp.commercialActionability,
          competitorHotel,
          contactHint:
            opp.primaryContact?.name ||
            opp.primaryContactName ||
            opp.organizationContactRoute ||
            "",
          targetHotelName: target,
        }),
      };
    }
    return {
      applicable: false,
      futureCycleOpportunityStatus: FUTURE_CYCLE_OPPORTUNITY_STATUS.NOT_APPLICABLE,
      headline: null,
      nextEditionHint: null,
      expectedTiming: null,
      futureHotelSelectionKnown: false,
      whyTargetHotelCandidate: null,
      recommendedSalesAction: null,
    };
  }

  // Never invent a next edition year — only state monitoring posture.
  const nextEditionHint =
    sourcing === TARGET_HOTEL_SOURCING_STATUS.FUTURE_CYCLE_TO_MONITOR || historical
      ? "Subsequent edition (timing not independently confirmed)"
      : null;

  let headline;
  if (sourcing === TARGET_HOTEL_SOURCING_STATUS.OVERFLOW_CONFIRMED) {
    headline = `Overflow sourcing for ${target} is independently confirmed. Qualify residual guestroom need — do not assume host rights.`;
  } else if (sourcing === TARGET_HOTEL_SOURCING_STATUS.OPEN_CONFIRMED) {
    headline = `Open sourcing for ${target} is independently confirmed for this opportunity.`;
  } else if (
    sourcing === TARGET_HOTEL_SOURCING_STATUS.FUTURE_CYCLE_TO_MONITOR ||
    historical
  ) {
    headline = `Monitor future event editions and identify when the organizer will next evaluate hotel accommodations. Future sourcing has not been confirmed for ${target}.`;
  } else {
    headline = `Competitor holds the current evidenced edition${
      competitorHotel ? ` at ${competitorHotel}` : ""
    }. Qualify whether any overflow or secondary lodging is needed for ${target} — do not pursue as an open host RFP.`;
  }

  const whyCandidate =
    text(opp.summaryWhyHotel || opp.whyThisHotel || opp.hotelOpportunityThesis) ||
    null;

  return {
    applicable: true,
    futureCycleOpportunityStatus:
      sourcing === TARGET_HOTEL_SOURCING_STATUS.FUTURE_CYCLE_TO_MONITOR ||
      historical
        ? FUTURE_CYCLE_OPPORTUNITY_STATUS.MONITOR
        : FUTURE_CYCLE_OPPORTUNITY_STATUS.UNKNOWN,
    headline,
    nextEditionHint,
    expectedTiming: null, // evidence-backed timing only — none invented here
    futureHotelSelectionKnown: false,
    whyTargetHotelCandidate: whyCandidate,
    recommendedSalesAction: recommendCommercialAction({
      competitorVenueStatus: opp.competitorVenueStatus,
      targetHotelSourcingStatus: sourcing,
      commercialActionability: opp.commercialActionability,
      competitorHotel,
      contactHint:
        opp.primaryContact?.name ||
        opp.primaryContactName ||
        opp.organizationContactRoute ||
        "",
      targetHotelName: target,
    }),
  };
}

/**
 * Build presentation payload for competitive / future-cycle opportunities.
 */
export function buildCompetitiveFutureCyclePresentation(opp = {}) {
  const competitiveStatus = upper(opp.competitiveStatus || "");
  const isCompetitiveSurface =
    opp.competitiveMarketScopeEligible === true ||
    opp.cmiVerified === true ||
    competitiveStatus === "VERIFIED" ||
    competitiveStatus === "NEEDS_VERIFICATION" ||
    competitiveStatus === "HISTORICAL" ||
    Boolean(
      opp.cmiCompetitorHotel ||
        opp.competitorHotel ||
        opp.competitorVenueStatus
    );

  if (!isCompetitiveSurface) {
    return {
      showSplitPresentation: false,
      commercialActionKey: null,
      commercialActionLabel: null,
      salesActionKeyMapped: null,
      confirmedCompetitorBusiness: { applicable: false },
      potentialFutureOpportunity: { applicable: false },
      futureCycleOpportunityStatus: FUTURE_CYCLE_OPPORTUNITY_STATUS.NOT_APPLICABLE,
    };
  }

  const actionKey = resolveCompetitiveCommercialAction(opp);
  const confirmed = buildConfirmedSection(opp, actionKey);
  const future = buildFutureSection(opp, actionKey);
  const showSplit = confirmed.applicable === true || future.applicable === true;

  return {
    showSplitPresentation: showSplit,
    commercialActionKey: actionKey,
    commercialActionLabel: COMPETITIVE_COMMERCIAL_ACTION_LABEL[actionKey] || null,
    salesActionKeyMapped: COMPETITIVE_ACTION_TO_SALES_PILL[actionKey] || "MONITOR",
    confirmedCompetitorBusiness: confirmed,
    potentialFutureOpportunity: future,
    futureCycleOpportunityStatus:
      future.futureCycleOpportunityStatus ||
      FUTURE_CYCLE_OPPORTUNITY_STATUS.UNKNOWN,
  };
}

/**
 * Attach presentation fields onto an opportunity (non-mutating copy).
 * Also remaps sales-action pill for competitive rows away from Pursue Now / RFP.
 */
export function applyCompetitiveFutureCyclePresentation(opp = {}) {
  const presentation = buildCompetitiveFutureCyclePresentation(opp);
  if (!presentation.showSplitPresentation) {
    return {
      ...opp,
      competitiveFutureCyclePresentation: presentation,
    };
  }

  const actionLabel = presentation.commercialActionLabel;
  const mappedPill = presentation.salesActionKeyMapped;
  const next = {
    ...opp,
    competitiveFutureCyclePresentation: presentation,
    competitiveCommercialActionKey: presentation.commercialActionKey,
    competitiveCommercialActionLabel: actionLabel,
    futureCycleOpportunityStatus: presentation.futureCycleOpportunityStatus,
  };

  // Do not leave Pursue Now / Win Room Block on confirmed competitor-only rows.
  const sourcing = normalizeTargetHotelSourcingFromLegacy(opp);
  const openOrOverflow =
    sourcing === TARGET_HOTEL_SOURCING_STATUS.OPEN_CONFIRMED ||
    sourcing === TARGET_HOTEL_SOURCING_STATUS.OVERFLOW_CONFIRMED;
  if (!openOrOverflow && mappedPill) {
    next.salesActionKey = mappedPill;
    next.salesActionPillLabel =
      mappedPill === "MONITOR"
        ? "Monitor"
        : mappedPill === "BUILD_ACCOUNT"
          ? "Build Account"
          : mappedPill === "CONTACT_BUYER"
            ? "Contact Buyer"
            : "Qualify Opportunity";
    if (next.tileBadgePlan && typeof next.tileBadgePlan === "object") {
      next.tileBadgePlan = {
        ...next.tileBadgePlan,
        salesActionKey: next.salesActionKey,
        salesActionPillLabel: next.salesActionPillLabel,
      };
    }
  }

  // Keep recommendedAction aligned with evidence-safe template when CMI split applies.
  if (
    presentation.potentialFutureOpportunity?.recommendedSalesAction ||
    presentation.confirmedCompetitorBusiness?.applicable
  ) {
    const safe =
      presentation.potentialFutureOpportunity?.recommendedSalesAction ||
      recommendCommercialAction({
        competitorVenueStatus: opp.competitorVenueStatus,
        targetHotelSourcingStatus: sourcing,
        commercialActionability: opp.commercialActionability,
        competitorHotel:
          opp.cmiCompetitorHotel || opp.competitorHotel || "",
        targetHotelName: targetHotelDisplayName(opp),
      });
    if (safe) {
      next.recommendedAction = safe;
      next.suggestedAction = safe;
    }
  }

  return next;
}
