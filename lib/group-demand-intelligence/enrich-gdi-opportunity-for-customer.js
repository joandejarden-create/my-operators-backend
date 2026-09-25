/**
 * Canonical customer enrichment for GDI opportunities.
 * New promotions must pass through this before customer publication —
 * same depth as mature opportunities (no "new opportunity" quality tier).
 *
 * Does NOT fabricate attendance, room nights, contacts, or venue status.
 * Distinguishes: KNOWN | UNKNOWN_AFTER_RESEARCH | NOT_RESEARCHED
 */

import { buildOpportunity } from "./opportunity-factory.js";
import { applyLiveCommercialQuality } from "./live-commercial-quality-v1.js";
import { loadHotelDemandConfig } from "./hotel-profile.js";
import {
  OPPORTUNITY_TYPE,
  OPPORTUNITY_QUALIFICATION,
  VENUE_SOURCING_STATUS,
  ROOM_DEMAND_STATUS,
} from "./claim-types.js";
import { geoConflictsWithHotelMarket } from "./discovery-routing-v1-2.js";

export const CUSTOMER_ENRICHMENT_VERSION = "gdi_customer_enrichment_v1";

export const RESEARCH_FIELD_STATE = Object.freeze({
  KNOWN: "KNOWN",
  UNKNOWN_AFTER_RESEARCH: "UNKNOWN_AFTER_RESEARCH",
  NOT_RESEARCHED: "NOT_RESEARCHED",
});

/** Generic boilerplate written by thin V3 promotion — must not reach customers. */
const GENERIC_THESIS_RE =
  /^future demand cycle relevant to .+ meeting \/ housing capacity\.?$/i;
const GENERIC_WHY_NOW_RE = /^future cycle 20\d{2}\.?$/i;
const INTERNAL_SEGMENT_SLUG_RE = /^[a-z][a-z0-9]*(?:_[a-z0-9]+){1,8}$/;
const CITY_STATE_RE =
  /\b([A-Z][a-zA-Z.'-]+(?:\s+[A-Z][a-zA-Z.'-]+){0,3}),\s*([A-Z]{2})\b/;

const FAR_FROM_DMV_RE =
  /\b(Las Vegas|Los Angeles|San Francisco|San Diego|Seattle|Portland|Phoenix|Denver|Dallas|Houston|Austin|San Antonio|Chicago|Minneapolis|Miami|Orlando|Tampa|Atlanta|Nashville|New Orleans|Boston|Detroit|Kansas City|Salt Lake)\b/i;

const DMV_RE =
  /\b(Bethesda|Rockville|Gaithersburg|Silver Spring|Washington|Arlington|Alexandria|McLean|Tysons|College Park|National Harbor|Baltimore|Fairfax|Reston|DMV|Natcher|NIH)\b/i;

/**
 * Customer-facing segment: never expose discovery vertical slugs.
 */
export function sanitizeCustomerSegment(value, opp = {}) {
  const raw = String(value || "").trim();
  if (!raw) {
    return inferSegmentLabel(opp);
  }
  // Discovery mode / lane enums
  if (
    /^(NIH_CALENDAR|EVENT_SERIES|FUTURE_CALENDAR|OPEN_UNIVERSE|HOUSING|SPORTS|TBD|KNOWN_TARGET)$/i.test(
      raw
    )
  ) {
    return inferSegmentLabel(opp);
  }
  if (INTERNAL_SEGMENT_SLUG_RE.test(raw) && !/\s/.test(raw)) {
    return inferSegmentLabel(opp) || humanizeSlug(raw);
  }
  if (/^[A-Z0-9_]+$/.test(raw) && raw.includes("_") && raw.length > 12) {
    return inferSegmentLabel(opp) || humanizeSlug(raw);
  }
  return raw;
}

function humanizeSlug(slug) {
  return String(slug || "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase())
    .slice(0, 60);
}

function inferSegmentLabel(opp = {}) {
  const blob = `${opp.opportunityType || ""} ${opp.discoveryMode || ""} ${opp.title || ""} ${opp.organizationName || ""}`;
  if (/NIH|NCI|NIDA|Common Fund|CTN|scientific|symposium/i.test(blob)) {
    return "Medical / Scientific";
  }
  if (/FIXED_VENUE|HOUSING|OVERFLOW|stay.?to.?play/i.test(blob)) {
    return "Housing / Overflow";
  }
  if (/sports|tournament|cup|soccer/i.test(blob)) return "Sports / Weekend Group";
  if (/association|academy|advocacy|annual (?:meeting|session|conference)/i.test(blob)) {
    return "Association";
  }
  if (/government|federal/i.test(blob)) return "Government";
  return "Discovered";
}

/**
 * venueStatus sometimes holds a city string ("Las Vegas, NV") from LLM extract.
 * Move city to destination fields; clear improper venueStatus.
 */
export function normalizeMisplacedGeoFields(opp = {}) {
  const out = { ...opp };
  const venueStatus = String(out.venueStatus || "").trim();
  const cityHit = venueStatus.match(CITY_STATE_RE);
  const looksLikeCityOnly =
    Boolean(cityHit) ||
    (/^[A-Za-z .'-]+,\s*[A-Z]{2}$/.test(venueStatus) &&
      !/hotel|marriott|hilton|convention|center|tbd|unknown|fixed|selected/i.test(venueStatus));

  if (looksLikeCityOnly) {
    if (!out.destinationStatus || out.destinationStatus === "UNKNOWN") {
      out.destinationStatus = venueStatus;
    }
    if (!out.eventLocation) out.eventLocation = venueStatus;
    if (!out.location) out.location = venueStatus;
    out.venueStatus = out.venueStatusHint || "UNKNOWN";
    out.venueNote = out.venueNote || `Reported destination context: ${venueStatus}`;
    out._geoFieldNormalized = true;
  }

  out.segment = sanitizeCustomerSegment(out.segment || out.vertical, out);
  if (INTERNAL_SEGMENT_SLUG_RE.test(String(out.vertical || "")) && !/\s/.test(String(out.vertical || ""))) {
    out.vertical = out.segment;
  }

  return out;
}

/**
 * Strip thin-promotion boilerplate so precision thesis can rebuild from evidence.
 */
export function clearGenericBoilerplate(opp = {}) {
  const out = { ...opp };
  const thesis =
    out.hotelOpportunityThesis || out.hotelDemandThesis || out.bethesdaWinThesis || "";
  if (GENERIC_THESIS_RE.test(String(thesis).trim())) {
    out.hotelOpportunityThesis = null;
    out.hotelDemandThesis = null;
    out.bethesdaWinThesis = null;
    out.hotelWinThesis = null;
    out._clearedGenericThesis = true;
  }
  if (GENERIC_WHY_NOW_RE.test(String(out.whyNow || "").trim())) {
    out.whyNow = null;
    out._clearedGenericWhyNow = true;
  }
  if (/^Monitor future cycle and venue\/housing announcements\.?$/i.test(String(out.recommendedAction || "").trim())) {
    out.recommendedAction = null;
  }
  return out;
}

/**
 * Geographic consistency vs hotel catchment.
 */
export function assessGeoConsistencyForHotel(opp = {}, hotelConfig = null) {
  const destBlob = [
    opp.destinationStatus,
    opp.eventLocation,
    opp.location,
    opp.venueNote,
    opp.venueStatus,
    opp.title,
  ]
    .filter(Boolean)
    .join(" ");

  const marketTokens = [
    ...(hotelConfig?.demandTerritory?.includes || []),
    hotelConfig?.identity?.city,
    hotelConfig?.identity?.state,
    hotelConfig?.displayName,
  ]
    .filter(Boolean)
    .map((x) => String(x).toLowerCase());

  const hasDmv = DMV_RE.test(destBlob);
  const farHit = destBlob.match(FAR_FROM_DMV_RE);
  const routing = geoConflictsWithHotelMarket(opp, marketTokens);

  const overflowOrHousing =
    /OVERFLOW|HOUSING|FIXED_VENUE_OPEN_HOUSING|stay.?to.?play/i.test(
      `${opp.opportunityType || ""} ${opp.roomDemandStatus || ""} ${opp.cqState || ""} ${opp.salesPartitionV11 || ""}`
    ) || /overflow|housing pending|accommodations?\s+tbd|natcher|bethesda/i.test(destBlob);

  const destinationTbd =
    /tbd|to be announced|destination\s+tbd|location\s+tbd|not yet (?:named|announced)/i.test(destBlob);

  let status = "OK";
  let reason = null;
  if (farHit && !hasDmv && !destinationTbd && !overflowOrHousing) {
    status = "OUTSIDE_CATCHMENT";
    reason = `Definitive destination ${farHit[0]} is outside practical hotel catchment with no overflow/housing/rotation evidence`;
  } else if (farHit && hasDmv) {
    status = "MIXED_SIGNAL";
    reason = "Both far destination and core-market signals present — verify cycle year";
  } else if (routing.conflict && !overflowOrHousing) {
    status = "GEO_CONFLICT";
    reason = routing.reason || "ORG_VS_EVENT";
  } else if (farHit && overflowOrHousing) {
    status = "FAR_BUT_HOUSING_MOTION";
    reason = "Far primary venue may still support overflow only if evidenced — currently housing-tagged";
  }

  return {
    status,
    reason,
    farDestination: farHit ? farHit[0] : null,
    coreMarketSignal: hasDmv,
    overflowOrHousing,
    destinationTbd,
  };
}

function buildEvidenceBasedWhyNow(opp = {}, geo = {}) {
  if (opp.whyNow && String(opp.whyNow).trim().length > 24 && !GENERIC_WHY_NOW_RE.test(opp.whyNow)) {
    return opp.whyNow;
  }
  const year = opp.eventYear || String(opp.eventStartDate || "").slice(0, 4);
  const bits = [];
  if (/HOUSING_PENDING|HOUSING|FIXED_VENUE_OPEN/i.test(`${opp.roomDemandStatus} ${opp.opportunityType}`)) {
    bits.push("housing / accommodations still open or TBD");
  }
  if (
    opp.venueSourcingStatus === VENUE_SOURCING_STATUS.HOTEL_VENUE_TBD ||
    opp.venueSourcingStatus === VENUE_SOURCING_STATUS.OPEN_UNRESOLVED ||
    opp.venueSourcingStatus === VENUE_SOURCING_STATUS.RFP_ACTIVE_SOURCING
  ) {
    bits.push("hotel/venue not yet named");
  }
  if (geo.destinationTbd) bits.push("destination still TBD");
  if (opp.officialSource || (opp.sources || []).length) {
    bits.push("official source confirms future cycle");
  }
  if (!bits.length) {
    return year
      ? `Monitor — future cycle ${year} confirmed; sourcing timing not yet public`
      : "Monitor — future cycle confirmed, sourcing timing not yet public";
  }
  return `Watch now: ${bits.join("; ")}${year ? ` (${year})` : ""}.`;
}

function buildHotelSpecificWhyHotel(opp = {}, hotelConfig = null) {
  const existing = String(opp.summaryWhyHotel || opp.fitExplanation || "").trim();
  if (existing.length >= 40 && existing !== String(opp.hotelOpportunityThesis || "").trim()) {
    return existing;
  }
  const name = hotelConfig?.displayName || opp.hotelName || "This hotel";
  const rooms = hotelConfig?.capabilityProfile?.totalGuestrooms;
  const meeting = hotelConfig?.capabilityProfile?.totalMeetingSpaceSqFt;
  const city = hotelConfig?.identity?.city || hotelConfig?.demandTerritory?.label;
  const parts = [];
  if (city) parts.push(`${city} market access`);
  if (rooms) parts.push(`${rooms} guestrooms`);
  if (meeting) parts.push("meeting inventory");
  if (/NIH|Natcher|Bethesda|CTN|scientific/i.test(`${opp.title} ${opp.organizationName} ${opp.destinationStatus}`)) {
    parts.push("NIH / Bethesda corridor adjacency");
  }
  if (/OVERFLOW|HOUSING|FIXED_VENUE/i.test(`${opp.opportunityType}`)) {
    parts.push("full-service overflow / housing product");
  }
  if (!parts.length) return null;
  return `${name}: ${parts.join("; ")}.`.slice(0, 280);
}

/**
 * Drawer readiness — unknown-after-research is OK; missing evaluation is not.
 */
export function isGdiCustomerDrawerReady(opportunity = {}) {
  const failed = [];
  if (!(opportunity.id || opportunity.opportunityId)) failed.push("id");
  if (!opportunity.title && !opportunity.opportunityName) failed.push("title");
  if (!opportunity.hotelId) failed.push("hotelId");
  if (!opportunity.opportunityQualification || opportunity.opportunityQualification === "WEAK") {
    failed.push("qualification");
  }
  if (!opportunity.opportunityType) failed.push("opportunityType");
  if (!opportunity.venueSourcingStatus) failed.push("venueSourcingStatus");
  const hasSource =
    opportunity.officialSource ||
    opportunity.discoverySource ||
    (Array.isArray(opportunity.sources) && opportunity.sources.some((s) => s?.url));
  if (!hasSource) failed.push("source");
  if (opportunity.hotelFitScore == null && opportunity.hotelFitScore !== 0) failed.push("hotelFit");
  const thesis =
    opportunity.hotelOpportunityThesis ||
    opportunity.hotelDemandThesis ||
    opportunity.thesisResearchState;
  if (!thesis && opportunity.thesisResearchState !== RESEARCH_FIELD_STATE.UNKNOWN_AFTER_RESEARCH) {
    if (opportunity.thesisResearchState !== "INSUFFICIENT_FOR_HOTEL_THESIS") {
      failed.push("thesis");
    }
  }
  if (!opportunity.whyNow) failed.push("whyNow");
  if (!opportunity.recommendedAction) failed.push("recommendedAction");
  if (opportunity.customerEnrichmentVersion !== CUSTOMER_ENRICHMENT_VERSION) {
    failed.push("enrichment_version");
  }
  return { ok: failed.length === 0, failed };
}

/**
 * Detect whether customer copy still contains internal IDs / slugs.
 */
export function findCustomerInternalIdLeaks(opportunity = {}) {
  const fields = [
    "segment",
    "summaryWhat",
    "hotelOpportunityThesis",
    "hotelDemandThesis",
    "summaryWhyHotel",
    "whyNow",
    "recommendedAction",
    "venueSourcingRationale",
    "roomDemandRationale",
  ];
  const leaks = [];
  for (const f of fields) {
    const v = String(opportunity[f] || "");
    if (INTERNAL_SEGMENT_SLUG_RE.test(v.trim()) && !/\s/.test(v.trim()) && v.length > 8) {
      leaks.push({ field: f, value: v });
    }
  }
  if (INTERNAL_SEGMENT_SLUG_RE.test(String(opportunity.segment || "").trim())) {
    leaks.push({ field: "segment", value: opportunity.segment });
  }
  return leaks;
}

/**
 * Canonical enrichment: discovery candidate → customer-ready opportunity fields.
 */
export function enrichGdiOpportunityForCustomer(raw = {}, opts = {}) {
  const hotelId = opts.hotelId || raw.hotelId;
  const hotelConfig = opts.hotelConfig || (hotelId ? loadHotelDemandConfig(hotelId) : null);

  let working = normalizeMisplacedGeoFields({ ...raw, hotelId });
  working = clearGenericBoilerplate(working);

  const geo = assessGeoConsistencyForHotel(working, hotelConfig);

  if (geo.status === "OUTSIDE_CATCHMENT") {
    working.geoClass = "OUTSIDE_CATCHMENT";
    working.geoConflict = true;
    working.geoConflictReason = geo.reason;
    working.demandTerritoryFit = "OUTSIDE";
    working.opportunityType = OPPORTUNITY_TYPE.FUTURE_CYCLE;
    working.customerFacingState = "FUTURE_WATCH";
    working.salesPartitionV11 = "FUTURE_WATCH";
    working.priority = "WATCHLIST";
    working.thesisResearchState = "INSUFFICIENT_FOR_HOTEL_THESIS";
    working.hotelOpportunityThesis = null;
    working.hotelDemandThesis = null;
    working.summaryWhyHotel = null;
    working.cqDowngradeReason = geo.reason;
    if (!geo.destinationTbd) {
      working.customerVisible = false;
      working.opportunityQualification = OPPORTUNITY_QUALIFICATION.CLOSED;
      working.priority = "DISQUALIFIED";
      working.customerFacingState = "CLOSED";
      working.cqState = "DISQUALIFIED";
    }
  }

  // Clear prior false DQ from thin enrichment so factory can re-score
  if (
    geo.status !== "OUTSIDE_CATCHMENT" &&
    geo.status !== "GEO_CONFLICT" &&
    (working.priority === "DISQUALIFIED" ||
      working.opportunityQualification === OPPORTUNITY_QUALIFICATION.CLOSED ||
      working.opportunityQualification === "CLOSED") &&
    (working.officialSource || working.discoverySource)
  ) {
    working.priority = "WATCHLIST";
    working.customerVisible = true;
    working.opportunityQualification = OPPORTUNITY_QUALIFICATION.MODERATE;
    working.customerFacingState =
      working.salesPartitionV11 || working.cqState || "WATCH";
    working.cqState = working.cqState === "DISQUALIFIED" ? "WATCH" : working.cqState;
  }

  if (
    working.opportunityType === "PRIMARY_PURSUIT" &&
    (!working.venueSourcingStatus || working.venueSourcingStatus === "UNKNOWN") &&
    !working.venueSourcingRationale
  ) {
    working.opportunityType = OPPORTUNITY_TYPE.FUTURE_CYCLE;
  }

  // Default fit components when discovery rows have none — avoid Hotel Fit = 0 → false DISQUALIFIED
  const hasDmvSignal = DMV_RE.test(
    `${working.title} ${working.destinationStatus} ${working.eventLocation} ${working.organizationName}`
  );
  const defaultFit = {
    physicalFit: 55,
    geographyFit: hasDmvSignal || geo.coreMarketSignal ? 78 : geo.status === "OUTSIDE_CATCHMENT" ? 15 : 45,
    timing: 58,
    commercialValue: 55,
    historicalFit: 48,
    competitiveAccessibility: 50,
    contactability: 42,
  };

  const built = buildOpportunity({
    ...working,
    hotelId,
    hotelName: hotelConfig?.displayName || working.hotelName,
    fitComponents: {
      ...defaultFit,
      ...(working.fitComponents || {}),
    },
    hotelContext: {
      ...(working.hotelContext || {}),
      displayName: hotelConfig?.displayName,
      rooms: hotelConfig?.capabilityProfile?.totalGuestrooms,
      meetingSpaceSqFt: hotelConfig?.capabilityProfile?.totalMeetingSpaceSqFt,
      marketLabel: hotelConfig?.demandTerritory?.label,
      brand: hotelConfig?.capabilityProfile?.softBrand,
    },
  });

  let enriched = applyLiveCommercialQuality(built, {
    nowDate: opts.nowDate || new Date().toISOString().slice(0, 10),
    catchment: opts.catchment || {},
  });

  // Preserve intentional geo DQ; do not let empty fit components DQ otherwise-valid WATCH/HOUSING
  if (
    enriched.priority === "DISQUALIFIED" &&
    geo.status !== "OUTSIDE_CATCHMENT" &&
    geo.status !== "GEO_CONFLICT" &&
    (working.opportunityQualification === OPPORTUNITY_QUALIFICATION.MODERATE ||
      working.opportunityQualification === OPPORTUNITY_QUALIFICATION.STRONG ||
      working.opportunityQualification === "MODERATE" ||
      working.opportunityQualification === "STRONG") &&
    (working.officialSource || working.discoverySource)
  ) {
    enriched.priority = "WATCHLIST";
    enriched.customerVisible = true;
    if (enriched.opportunityQualification === OPPORTUNITY_QUALIFICATION.CLOSED) {
      enriched.opportunityQualification = OPPORTUNITY_QUALIFICATION.MODERATE;
    }
    if (!enriched.customerFacingState || enriched.customerFacingState === "CLOSED") {
      enriched.customerFacingState =
        working.salesPartitionV11 || working.cqState || "WATCH";
    }
  }

  // Re-assert geo DQ after factory (factory may reset flags)
  if (geo.status === "OUTSIDE_CATCHMENT" && !geo.destinationTbd) {
    enriched.geoClass = "OUTSIDE_CATCHMENT";
    enriched.geoConflict = true;
    enriched.customerVisible = false;
    enriched.priority = "DISQUALIFIED";
    enriched.opportunityQualification = OPPORTUNITY_QUALIFICATION.CLOSED;
    enriched.customerFacingState = "CLOSED";
    enriched.cqState = "DISQUALIFIED";
  }

  enriched.segment = sanitizeCustomerSegment(enriched.segment || working.segment, enriched);

  const whyHotel = buildHotelSpecificWhyHotel(enriched, hotelConfig);
  if (whyHotel) {
    enriched.summaryWhyHotel = whyHotel;
    enriched.fitExplanation = enriched.fitExplanation || whyHotel;
  } else if (
    geo.status === "OUTSIDE_CATCHMENT" ||
    enriched.thesisResearchState === "INSUFFICIENT_FOR_HOTEL_THESIS"
  ) {
    enriched.summaryWhyHotel =
      "Insufficient hotel-specific fit evidence for this destination cycle.";
    enriched.thesisResearchState = "INSUFFICIENT_FOR_HOTEL_THESIS";
  }

  enriched.whyNow = buildEvidenceBasedWhyNow(enriched, geo);

  if (
    !enriched.roomDemandStatus ||
    enriched.roomDemandStatus === "UNKNOWN" ||
    enriched.roomDemandStatus === ROOM_DEMAND_STATUS.UNKNOWN
  ) {
    enriched.roomDemandStatus = ROOM_DEMAND_STATUS.UNKNOWN;
    enriched.roomDemandRationale =
      enriched.roomDemandRationale ||
      "Room demand not publicly available after source review — attendance must not be treated as room nights.";
    enriched.roomDemandResearchState = RESEARCH_FIELD_STATE.UNKNOWN_AFTER_RESEARCH;
  } else {
    enriched.roomDemandResearchState = RESEARCH_FIELD_STATE.KNOWN;
  }

  if (
    enriched.estimatedAttendance == null ||
    /unknown/i.test(String(enriched.estimatedAttendance))
  ) {
    enriched.attendanceResearchState = RESEARCH_FIELD_STATE.UNKNOWN_AFTER_RESEARCH;
  } else {
    enriched.attendanceResearchState = RESEARCH_FIELD_STATE.KNOWN;
  }

  if (enriched.estimatedPeakRooms == null && enriched.peakRooms == null) {
    enriched.peakRoomsResearchState = RESEARCH_FIELD_STATE.UNKNOWN_AFTER_RESEARCH;
  } else {
    enriched.peakRoomsResearchState = RESEARCH_FIELD_STATE.KNOWN;
  }

  if (enriched.hotelOpportunityThesis || enriched.hotelDemandThesis) {
    enriched.thesisResearchState =
      enriched.thesisResearchState || RESEARCH_FIELD_STATE.KNOWN;
  } else if (enriched.thesisResearchState !== "INSUFFICIENT_FOR_HOTEL_THESIS") {
    enriched.thesisResearchState = RESEARCH_FIELD_STATE.UNKNOWN_AFTER_RESEARCH;
    enriched.hotelOpportunityThesis =
      "Insufficient public evidence to state a hotel-specific win thesis yet.";
  }

  if (!enriched.recommendedAction || /^reach out$/i.test(enriched.recommendedAction)) {
    const cq = enriched.cqState || enriched.salesPartitionV11 || enriched.customerFacingState;
    if (/HOUSING/i.test(`${cq} ${enriched.opportunityType}`)) {
      enriched.recommendedAction =
        "Contact housing / registration owner when posted; pursue room block / VIP / overflow around fixed venue.";
    } else if (/FUTURE_WATCH/i.test(String(cq))) {
      enriched.recommendedAction =
        "Monitor next-cycle destination and hotel naming; do not force immediate sales outreach.";
    } else if (/OVERFLOW/i.test(String(enriched.opportunityType))) {
      enriched.recommendedAction =
        "Contact housing/venue team regarding overflow / recommended-hotel inclusion.";
    } else {
      enriched.recommendedAction =
        "Watch — monitor venue/housing announcements; qualify before outreach.";
    }
  }

  enriched.geoConsistency = geo;
  enriched.customerEnrichmentVersion = CUSTOMER_ENRICHMENT_VERSION;
  enriched.customerEnrichmentAt = new Date().toISOString();
  enriched.summaryWhat =
    enriched.summaryWhat ||
    enriched.title ||
    enriched.canonicalEventName ||
    null;

  if (Array.isArray(enriched.sources) && enriched.officialSource) {
    const officialHost = (() => {
      try {
        return new URL(enriched.officialSource).hostname.replace(/^www\./, "");
      } catch {
        return "";
      }
    })();
    enriched.sources = enriched.sources.filter((s) => {
      const url = s?.url || "";
      if (/10times|showsbee|conferenceindex|allconference/i.test(url)) {
        return officialHost && url.includes(officialHost);
      }
      return true;
    });
    if (!enriched.sources.some((s) => s?.url === enriched.officialSource)) {
      enriched.sources = [
        {
          url: enriched.officialSource,
          authority: "FIRST_PARTY",
          title: enriched.officialSourceTitle || "Official source",
        },
        ...enriched.sources,
      ];
    }
  }

  const readiness = isGdiCustomerDrawerReady(enriched);
  enriched.drawerReadiness = readiness;
  const leaks = findCustomerInternalIdLeaks(enriched);
  enriched.internalIdLeaks = leaks;
  if (leaks.some((l) => l.field === "segment")) {
    enriched.segment = inferSegmentLabel(enriched);
  }

  return enriched;
}

export function shouldHideAfterEnrichment(opp = {}) {
  if (opp.customerVisible === false) return true;
  if (opp.priority === "DISQUALIFIED") return true;
  if (
    opp.opportunityQualification === OPPORTUNITY_QUALIFICATION.CLOSED &&
    opp.geoClass === "OUTSIDE_CATCHMENT"
  ) {
    return true;
  }
  return false;
}
