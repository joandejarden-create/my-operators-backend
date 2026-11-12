/**
 * GDI Sales Opportunities quick-filters V1
 *
 * Canonical predicates for hotel sales-leader browse. Grounded in structured
 * opportunity fields — never text-search of rendered drawer HTML.
 *
 * Faceting rule: each quick-filter count = opportunities matching that filter
 * given all OTHER active filter groups (AND across groups). Within Sales
 * Opportunities, chips are single-select (OR only via Hotel Sourcing subgroup).
 */
import { HOTEL_SOURCING_DISPLAY } from "./events-opportunity-overview-v1.js";
import { isAssociationConferenceEventsOpportunity } from "./events-opportunity-overview-v1.js";
import { isPreferredLodgingPartnershipOpportunity } from "./preferred-lodging-partnership/preferred-lodging-partnership-contract-v1.js";
import { isProfessionalEventPlannerOpportunity } from "./professional-event-planners/professional-event-planner-contract-v1.js";
import {
  HOTEL_REVENUE_TYPE,
  normalizeRevenueTypeTags,
} from "./demand-segments/hotel-revenue-mechanism-v1.js";
import { OPPORTUNITY_TYPE_FILTER, classifyOpportunityTypeFilter } from "./opportunity-type-filter-v1.js";
import { PRIORITY } from "./claim-types.js";
import {
  COMMERCIAL_DEMAND_TYPE_FILTER,
  matchesCommercialDemandTypeFilter,
} from "./commercial-demand-type-v1.js";

export const SALES_OPPORTUNITY_QUICK_FILTERS_V1 =
  "gdi-sales-opportunity-quick-filters-v1";

/** Sales Opportunities chip keys (single-select). */
export const SALES_OPPORTUNITY_FILTER = Object.freeze({
  ALL: "",
  HOTEL_SOURCING_NOT_CONFIRMED: "HOTEL_SOURCING_NOT_CONFIRMED",
  PURSUE_NOW: "PURSUE_NOW",
  FUTURE_GROUP_BUSINESS: "FUTURE_GROUP_BUSINESS",
  COMPETITOR_CAPTURE: "COMPETITOR_CAPTURE",
  PARTNERSHIP_OPPORTUNITIES: "PARTNERSHIP_OPPORTUNITIES",
  LOCAL_CATERING: "LOCAL_CATERING",
  /** Commercial demand type — Guest Rooms (includes BOTH). */
  GUEST_ROOMS: "GUEST_ROOMS",
  /** Commercial demand type — Both only. */
  BOTH: "BOTH",
  /** Scoped Need-Date view — not a permanent fourth tile pill. */
  MATCHES_NEED_DATES: "MATCHES_NEED_DATES",
});

/** Compact customer-facing chip labels (IDs/predicates unchanged). */
export const SALES_OPPORTUNITY_FILTER_LABEL = Object.freeze({
  [SALES_OPPORTUNITY_FILTER.ALL]: "All",
  [SALES_OPPORTUNITY_FILTER.HOTEL_SOURCING_NOT_CONFIRMED]: "Hotel Unconfirmed",
  [SALES_OPPORTUNITY_FILTER.PURSUE_NOW]: "Pursue Now",
  [SALES_OPPORTUNITY_FILTER.FUTURE_GROUP_BUSINESS]: "Future Groups",
  [SALES_OPPORTUNITY_FILTER.COMPETITOR_CAPTURE]: "Competitor",
  [SALES_OPPORTUNITY_FILTER.PARTNERSHIP_OPPORTUNITIES]: "Partnerships",
  [SALES_OPPORTUNITY_FILTER.LOCAL_CATERING]: "Catering",
  [SALES_OPPORTUNITY_FILTER.GUEST_ROOMS]: "Guest Rooms",
  [SALES_OPPORTUNITY_FILTER.BOTH]: "Both",
  [SALES_OPPORTUNITY_FILTER.MATCHES_NEED_DATES]: "Need Dates",
});

/** Full accessible names for tooltips / aria-label (display chips stay compact). */
export const SALES_OPPORTUNITY_FILTER_LABEL_FULL = Object.freeze({
  [SALES_OPPORTUNITY_FILTER.ALL]: "All Opportunities",
  [SALES_OPPORTUNITY_FILTER.HOTEL_SOURCING_NOT_CONFIRMED]:
    "Hotel Sourcing Not Confirmed",
  [SALES_OPPORTUNITY_FILTER.PURSUE_NOW]: "Pursue Now",
  [SALES_OPPORTUNITY_FILTER.FUTURE_GROUP_BUSINESS]: "Future Group Business",
  [SALES_OPPORTUNITY_FILTER.COMPETITOR_CAPTURE]: "Competitor Capture",
  [SALES_OPPORTUNITY_FILTER.PARTNERSHIP_OPPORTUNITIES]:
    "Partnership Opportunities",
  [SALES_OPPORTUNITY_FILTER.LOCAL_CATERING]: "Local Catering",
  [SALES_OPPORTUNITY_FILTER.GUEST_ROOMS]: "Guest Rooms",
  [SALES_OPPORTUNITY_FILTER.BOTH]: "Both Guest Rooms and Local Catering",
  [SALES_OPPORTUNITY_FILTER.MATCHES_NEED_DATES]: "Matches Need Dates",
});

/** Subgroup for Hotel Sourcing Not Confirmed. */
export const HOTEL_SOURCING_SUBGROUP = Object.freeze({
  BOTH: "BOTH",
  OPEN: "OPEN",
  UNCONFIRMED: "UNCONFIRMED",
});

export const HOTEL_SOURCING_SUBGROUP_LABEL = Object.freeze({
  [HOTEL_SOURCING_SUBGROUP.BOTH]: "Both",
  [HOTEL_SOURCING_SUBGROUP.OPEN]: "Confirmed Open",
  [HOTEL_SOURCING_SUBGROUP.UNCONFIRMED]: "Unconfirmed",
});

/**
 * Commercial Stage browse filter (distinct from Priority and from sourcing).
 * Qualification Needed = Events Research Needed / Scaling Exploratory.
 */
export const COMMERCIAL_STAGE_FILTER = Object.freeze({
  ALL: "",
  PURSUE_NOW: "PURSUE_NOW",
  FUTURE_CAPTURE: "FUTURE_CAPTURE",
  RELATIONSHIP_DEVELOPMENT: "RELATIONSHIP_DEVELOPMENT",
  QUALIFICATION_NEEDED: "QUALIFICATION_NEEDED",
});

export const COMMERCIAL_STAGE_FILTER_LABEL = Object.freeze({
  [COMMERCIAL_STAGE_FILTER.PURSUE_NOW]: "Pursue Now",
  [COMMERCIAL_STAGE_FILTER.FUTURE_CAPTURE]: "Future Capture",
  [COMMERCIAL_STAGE_FILTER.RELATIONSHIP_DEVELOPMENT]:
    "Relationship Development",
  [COMMERCIAL_STAGE_FILTER.QUALIFICATION_NEEDED]: "Qualification Needed",
});

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

/** Exclude from filter counts / customer sales browse. */
export function isExcludedFromSalesFilterInventory(o = {}) {
  if (!o || typeof o !== "object") return true;
  if (o.archived === true || o.isArchived === true) return true;
  if (o.certificationFixtureOnly === true) return true;
  if (o.isTestData === true) return true;
  if (o.customerVisible === false) return true;
  if (o.prospectSuppressed === true) return true;
  if (o.localActiveInventory === false || o.localInventorySuperseded === true) {
    return true;
  }
  return false;
}

/**
 * Normalize Hotel Sourcing to the four approved display statuses, or null
 * when sourcing is not conceptually relevant (non-event partnership accounts).
 */
export function normalizeHotelSourcingStatus(o = {}) {
  const raw = text(
    o.eventsOpportunityOverview?.hotelSourcing ||
      o.hotelSourcingStatusLabel ||
      o.hotelSourcingStatus ||
      o.venueSourcingStatusLabel ||
      o.venueSourcingStatus ||
      "",
  );
  if (!raw) {
    // Sourcing only conceptually relevant for event-led / association inventory.
    if (
      isAssociationConferenceEventsOpportunity(o) ||
      classifyOpportunityTypeFilter(o) ===
        OPPORTUNITY_TYPE_FILTER.ASSOCIATION_CONFERENCE_GROUP
    ) {
      return HOTEL_SOURCING_DISPLAY.HOTEL_SOURCING_UNCONFIRMED;
    }
    return null;
  }
  const u = raw.toUpperCase().replace(/\s+/g, "_");
  if (
    /HOTEL_SOURCING_OPEN|SOURCING_CONFIRMED_OPEN|RFP_ACTIVE|SOURCING OPEN/i.test(
      raw,
    ) ||
    u === "HOTEL_SOURCING_OPEN"
  ) {
    return HOTEL_SOURCING_DISPLAY.HOTEL_SOURCING_OPEN;
  }
  if (
    /VENUE_CONFIRMED|VENUE CONFIRMED/i.test(raw) ||
    u === "VENUE_CONFIRMED"
  ) {
    return HOTEL_SOURCING_DISPLAY.VENUE_CONFIRMED;
  }
  if (
    /HISTORICAL_VENUE|HISTORICAL VENUE/i.test(raw) ||
    u === "HISTORICAL_VENUE_ONLY"
  ) {
    return HOTEL_SOURCING_DISPLAY.HISTORICAL_VENUE_ONLY;
  }
  if (
    /HOTEL_SOURCING_UNCONFIRMED|SOURCING_UNCONFIRMED|UNCONFIRMED|HOTEL.?VENUE.?TBD|OPEN.?UNRESOLVED|REGISTRATION_OPEN/i.test(
      raw,
    )
  ) {
    return HOTEL_SOURCING_DISPLAY.HOTEL_SOURCING_UNCONFIRMED;
  }
  // Unknown string on an event opportunity — do not invent Open.
  if (isAssociationConferenceEventsOpportunity(o)) {
    return HOTEL_SOURCING_DISPLAY.HOTEL_SOURCING_UNCONFIRMED;
  }
  return null;
}

/**
 * Map segment-aware commercialStageKey / inventoryLane → browse filter key.
 */
export function normalizeCommercialStageForFilter(o = {}) {
  const key = upper(
    o.commercialStageKey || o.tileBadgePlan?.commercialStageKey || "",
  );
  if (key === "PURSUE_NOW" || key === "SALES_OPPORTUNITY") {
    return COMMERCIAL_STAGE_FILTER.PURSUE_NOW;
  }
  if (key === "FUTURE_CAPTURE" || key === "FUTURE_OPPORTUNITY") {
    return COMMERCIAL_STAGE_FILTER.FUTURE_CAPTURE;
  }
  if (key === "RELATIONSHIP_DEVELOPMENT" || key === "ACCOUNT_DEVELOPMENT") {
    return COMMERCIAL_STAGE_FILTER.RELATIONSHIP_DEVELOPMENT;
  }
  if (
    key === "RESEARCH_NEEDED" ||
    key === "RESEARCH_WATCHLIST" ||
    key === "QUALIFICATION_NEEDED" ||
    key === "EXCLUDED"
  ) {
    return COMMERCIAL_STAGE_FILTER.QUALIFICATION_NEEDED;
  }
  // Scaling Exploratory via lane
  const lane = upper(o.inventoryLane || "");
  if (lane === "SALES_OPPORTUNITY") return COMMERCIAL_STAGE_FILTER.PURSUE_NOW;
  if (lane === "FUTURE_OPPORTUNITY") return COMMERCIAL_STAGE_FILTER.FUTURE_CAPTURE;
  if (lane === "ACCOUNT_DEVELOPMENT") {
    return COMMERCIAL_STAGE_FILTER.RELATIONSHIP_DEVELOPMENT;
  }
  if (lane === "RESEARCH_WATCHLIST" || lane === "EXCLUDED") {
    return COMMERCIAL_STAGE_FILTER.QUALIFICATION_NEEDED;
  }
  return null;
}

function hasFutureDemandEvidence(o = {}) {
  if (o.futureDemandEvidence === true) return true;
  if (o.futureEditionDate && /^\d{4}-\d{2}-\d{2}/.test(String(o.futureEditionDate))) {
    return true;
  }
  const start = text(o.eventStartDate || o.tileEventStartDate || "");
  if (/^\d{4}-\d{2}-\d{2}/.test(start)) {
    return start >= new Date().toISOString().slice(0, 10);
  }
  if (o.standingOpportunity === true && o.recurrenceClass) return true;
  const cycle = text(
    o.eventsOpportunityOverview?.demandCycle || o.demandCycle || "",
  );
  if (/CONFIRMED_UPCOMING|FUTURE_CYCLE|RECURRING/i.test(cycle)) return true;
  return false;
}

function hasCompetitiveVenueEvidence(o = {}) {
  if (o.competitiveVenueEvidence === true) return true;
  if (o.competitorVenueConfirmed === true) return true;
  const sourcing = normalizeHotelSourcingStatus(o);
  if (sourcing === HOTEL_SOURCING_DISPLAY.VENUE_CONFIRMED) {
    // Venue confirmed elsewhere — competitive capture thesis only with explicit flags
    return (
      o.competitiveCaptureEligible === true ||
      /competitor|alternative.?venue|displacement|recurring.?competitive/i.test(
        text(o.commercialOpportunityTrack || o.demandMechanism || ""),
      )
    );
  }
  if (sourcing === HOTEL_SOURCING_DISPLAY.HISTORICAL_VENUE_ONLY) {
    return (
      o.competitiveCaptureEligible === true ||
      hasFutureDemandEvidence(o) ||
      /TRACK_B_RECURRING_COMPETITIVE|competitive.?capture/i.test(
        text(o.commercialOpportunityTrack || ""),
      )
    );
  }
  return false;
}

function isPartnershipOpportunity(o = {}) {
  if (isPreferredLodgingPartnershipOpportunity(o)) return true;
  if (isProfessionalEventPlannerOpportunity(o)) return true;
  const subtype = upper(
    o.demandOpportunitySubtype || o.opportunitySubtype || "",
  );
  if (
    subtype.includes("PARTNERSHIP") ||
    subtype.includes("TRAVEL_TRADE") ||
    subtype.includes("INTERMEDIARY")
  ) {
    return true;
  }
  if (o.partnershipStatus || o.preferredLodgingPartnershipV1) return true;
  if (o.professionalEventPlannerPartnershipV1) return true;
  const type = classifyOpportunityTypeFilter(o);
  if (
    type === OPPORTUNITY_TYPE_FILTER.TRAVEL_TRADE_GROUP_INTERMEDIARIES ||
    type === OPPORTUNITY_TYPE_FILTER.PREFERRED_LODGING_PARTNERSHIP ||
    type === OPPORTUNITY_TYPE_FILTER.PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP
  ) {
    return true;
  }
  return false;
}

function isLocalCateringOpportunity(o = {}) {
  // Canonical commercial demand type: Local Catering filter includes EVENT_FNB_ONLY + BOTH
  if (
    matchesCommercialDemandTypeFilter(
      o,
      COMMERCIAL_DEMAND_TYPE_FILTER.LOCAL_CATERING,
    )
  ) {
    return true;
  }
  const tags = normalizeRevenueTypeTags(
    o.revenueTypes || o.revenueTypeTags || o.revenueMechanisms || [],
  );
  const cateringTags = new Set([
    HOTEL_REVENUE_TYPE.LOCAL_CATERING,
    HOTEL_REVENUE_TYPE.BANQUETS,
    HOTEL_REVENUE_TYPE.PRIVATE_DINING,
    HOTEL_REVENUE_TYPE.MEETING_SPACE_RENTAL,
  ]);
  if (tags.some((t) => cateringTags.has(t))) return true;
  // Structured segment id — secondary, not sole signal
  const seg = text(o.demandSegmentId || o.researchSegment || "").toLowerCase();
  if (seg === "local_catering_day_meetings") {
    // Require at least one revenue signal or explicit local catering posture
    if (tags.length) return true;
    if (o.guestroomDemandStatus === "NONE_CONFIRMED") return true;
    if (Array.isArray(o.revenueTypes) && o.revenueTypes.length) return true;
    // Segment alone with day-meeting overview is acceptable when segment is authoritative
    return Boolean(o.segmentOverviewModel || o.demandSegmentId === "local_catering_day_meetings");
  }
  // Mechanism enum aliases from planner/partnership research
  const mechanisms = (Array.isArray(o.revenueMechanisms) ? o.revenueMechanisms : [])
    .map((m) => upper(m));
  if (
    mechanisms.some((m) =>
      /BANQUET|CATERING|PRIVATE_DINING|MEETING.?EVENT.?SPACE|LOCAL_CATERING/.test(
        m,
      ),
    )
  ) {
    return true;
  }
  return false;
}

function isGuestRoomsDemandOpportunity(o = {}) {
  return matchesCommercialDemandTypeFilter(
    o,
    COMMERCIAL_DEMAND_TYPE_FILTER.GUEST_ROOMS,
  );
}

function isBothDemandOpportunity(o = {}) {
  return matchesCommercialDemandTypeFilter(
    o,
    COMMERCIAL_DEMAND_TYPE_FILTER.BOTH,
  );
}

function isPursueNowSalesOpportunity(o = {}) {
  const stage = normalizeCommercialStageForFilter(o);
  if (stage === COMMERCIAL_STAGE_FILTER.PURSUE_NOW) {
    // Require commercial eligibility — do not surface exploratory research as Pursue Now sales filter
    const ready =
      o.inventorySalesReady === true ||
      o.commercialReadiness === "READY" ||
      o.commercialReadiness === "ACTIONABLE" ||
      o.buyerEligibility === "ELIGIBLE" ||
      o.hasCredibleOutreachRoute === true ||
      o.hasVerifiedNamedRelevantContact === true ||
      upper(o.priorityFilterKey || o.priority) === PRIORITY.HIGH ||
      upper(o.priorityFilterKey || o.priority) === PRIORITY.MEDIUM;
    return ready;
  }
  // Action timing CONTACT_NOW with credible route
  if (
    upper(o.actionTimingFilterKey || o.bookingWindowStatus) === "CONTACT_NOW"
  ) {
    return (
      o.hasCredibleOutreachRoute === true ||
      o.hasVerifiedNamedRelevantContact === true ||
      o.inventorySalesReady === true
    );
  }
  return false;
}

function isFutureGroupBusiness(o = {}) {
  if (isPartnershipOpportunity(o) && !hasFutureDemandEvidence(o)) {
    // Standing partnerships are Partnership filter, not Future Group unless dated/recurring
    if (o.standingOpportunity === true && !o.eventStartDate) return false;
  }
  if (!hasFutureDemandEvidence(o) && !o.standingOpportunity) return false;
  const type = classifyOpportunityTypeFilter(o);
  const groupTypes = new Set([
    OPPORTUNITY_TYPE_FILTER.ASSOCIATION_CONFERENCE_GROUP,
    OPPORTUNITY_TYPE_FILTER.SCALING_COMPANY,
    OPPORTUNITY_TYPE_FILTER.GOVERNMENT_PUBLIC_SECTOR,
    OPPORTUNITY_TYPE_FILTER.SPORTS_ENTERTAINMENT,
    OPPORTUNITY_TYPE_FILTER.SPORTS_GROUP,
    OPPORTUNITY_TYPE_FILTER.SOCIAL_SPECIAL_EVENTS,
    OPPORTUNITY_TYPE_FILTER.EDUCATION_SCIENTIFIC,
    OPPORTUNITY_TYPE_FILTER.CORPORATE_MEETINGS_INCENTIVES,
    OPPORTUNITY_TYPE_FILTER.CORPORATE_CELEBRATIONS_PRIVATE_EVENTS,
    OPPORTUNITY_TYPE_FILTER.RELOCATION_EXTENDED_STAY,
  ]);
  if (type && groupTypes.has(type)) {
    return hasFutureDemandEvidence(o) || o.standingOpportunity === true;
  }
  if (isAssociationConferenceEventsOpportunity(o)) {
    return hasFutureDemandEvidence(o);
  }
  const seg = text(o.demandSegmentId || o.researchSegment || "");
  if (
    /association|conference|government|sport|social|incentive|academic|corporate|scaling|relocation/i.test(
      seg,
    )
  ) {
    return hasFutureDemandEvidence(o) || o.standingOpportunity === true;
  }
  return false;
}

function matchesHotelSourcingNotConfirmed(o = {}, subgroup = HOTEL_SOURCING_SUBGROUP.BOTH) {
  const sourcing = normalizeHotelSourcingStatus(o);
  if (sourcing == null) return false;
  const isOpen = sourcing === HOTEL_SOURCING_DISPLAY.HOTEL_SOURCING_OPEN;
  const isUnconfirmed =
    sourcing === HOTEL_SOURCING_DISPLAY.HOTEL_SOURCING_UNCONFIRMED;
  if (!isOpen && !isUnconfirmed) return false;
  if (subgroup === HOTEL_SOURCING_SUBGROUP.OPEN) return isOpen;
  if (subgroup === HOTEL_SOURCING_SUBGROUP.UNCONFIRMED) return isUnconfirmed;
  return isOpen || isUnconfirmed;
}

/**
 * Commercial demand-type keys from persisted revenueStreamClass.
 * Always available for browse filters (including internal Bethesda union rows
 * that are excluded from other sales chips via customerVisible=false).
 */
function commercialDemandTypeFilterKeys(o = {}) {
  const keys = [];
  if (isGuestRoomsDemandOpportunity(o)) {
    keys.push(SALES_OPPORTUNITY_FILTER.GUEST_ROOMS);
  }
  if (
    matchesCommercialDemandTypeFilter(
      o,
      COMMERCIAL_DEMAND_TYPE_FILTER.LOCAL_CATERING,
    )
  ) {
    keys.push(SALES_OPPORTUNITY_FILTER.LOCAL_CATERING);
  }
  if (isBothDemandOpportunity(o)) {
    keys.push(SALES_OPPORTUNITY_FILTER.BOTH);
  }
  return keys;
}

/**
 * All Sales Opportunity keys this opportunity qualifies for (multi-membership OK;
 * counts still do not double-count within a single chip).
 */
export function classifySalesOpportunityKeys(o = {}) {
  const demandTypeKeys = commercialDemandTypeFilterKeys(o);
  if (isExcludedFromSalesFilterInventory(o)) {
    return demandTypeKeys;
  }
  const keys = [...demandTypeKeys];
  if (matchesHotelSourcingNotConfirmed(o, HOTEL_SOURCING_SUBGROUP.BOTH)) {
    keys.push(SALES_OPPORTUNITY_FILTER.HOTEL_SOURCING_NOT_CONFIRMED);
  }
  if (isPursueNowSalesOpportunity(o)) {
    keys.push(SALES_OPPORTUNITY_FILTER.PURSUE_NOW);
  }
  if (isFutureGroupBusiness(o)) {
    keys.push(SALES_OPPORTUNITY_FILTER.FUTURE_GROUP_BUSINESS);
  }
  if (hasCompetitiveVenueEvidence(o)) {
    keys.push(SALES_OPPORTUNITY_FILTER.COMPETITOR_CAPTURE);
  }
  if (isPartnershipOpportunity(o)) {
    keys.push(SALES_OPPORTUNITY_FILTER.PARTNERSHIP_OPPORTUNITIES);
  }
  // Legacy segment/tag catering — only when demand-type key not already present
  if (
    isLocalCateringOpportunity(o) &&
    !keys.includes(SALES_OPPORTUNITY_FILTER.LOCAL_CATERING)
  ) {
    keys.push(SALES_OPPORTUNITY_FILTER.LOCAL_CATERING);
  }
  if (
    o.needDateMatchV1?.confirmedNeedDateMatch === true ||
    o.needDateMatchV1?.timingOverlapStatus === "CONFIRMED_PERIOD_OVERLAP" ||
    o.needDateMatchV1?.timingOverlapStatus === "SUPPORTED_PROBABLE_OVERLAP" ||
    (Array.isArray(o.needDateAssociations) && o.needDateAssociations.length > 0)
  ) {
    keys.push(SALES_OPPORTUNITY_FILTER.MATCHES_NEED_DATES);
  }
  return keys;
}

export function matchesSalesOpportunityFilter(
  o = {},
  filterKey = "",
  sourcingSubgroup = HOTEL_SOURCING_SUBGROUP.BOTH,
) {
  if (!filterKey) return !isExcludedFromSalesFilterInventory(o);
  if (isExcludedFromSalesFilterInventory(o)) return false;
  if (filterKey === SALES_OPPORTUNITY_FILTER.HOTEL_SOURCING_NOT_CONFIRMED) {
    return matchesHotelSourcingNotConfirmed(o, sourcingSubgroup);
  }
  const keys = classifySalesOpportunityKeys(o);
  return keys.includes(filterKey);
}

/**
 * Facet counts for Sales Opportunities given other active filters.
 * Rule: apply all filters EXCEPT salesOpportunity / sourcingSubgroup, then count.
 */
export function countSalesOpportunityFacets(opportunities, matchOtherFilters) {
  const base = (opportunities || []).filter((o) => {
    if (isExcludedFromSalesFilterInventory(o)) return false;
    return typeof matchOtherFilters === "function"
      ? matchOtherFilters(o)
      : true;
  });
  const counts = {
    [SALES_OPPORTUNITY_FILTER.ALL]: base.length,
    [SALES_OPPORTUNITY_FILTER.HOTEL_SOURCING_NOT_CONFIRMED]: 0,
    [SALES_OPPORTUNITY_FILTER.PURSUE_NOW]: 0,
    [SALES_OPPORTUNITY_FILTER.FUTURE_GROUP_BUSINESS]: 0,
    [SALES_OPPORTUNITY_FILTER.COMPETITOR_CAPTURE]: 0,
    [SALES_OPPORTUNITY_FILTER.PARTNERSHIP_OPPORTUNITIES]: 0,
    [SALES_OPPORTUNITY_FILTER.LOCAL_CATERING]: 0,
    [SALES_OPPORTUNITY_FILTER.GUEST_ROOMS]: 0,
    [SALES_OPPORTUNITY_FILTER.BOTH]: 0,
    sourcingOpen: 0,
    sourcingUnconfirmed: 0,
  };
  for (const o of base) {
    const keys = classifySalesOpportunityKeys(o);
    for (const k of keys) {
      if (counts[k] != null) counts[k] += 1;
    }
    const sourcing = normalizeHotelSourcingStatus(o);
    if (sourcing === HOTEL_SOURCING_DISPLAY.HOTEL_SOURCING_OPEN) {
      counts.sourcingOpen += 1;
    }
    if (sourcing === HOTEL_SOURCING_DISPLAY.HOTEL_SOURCING_UNCONFIRMED) {
      counts.sourcingUnconfirmed += 1;
    }
  }
  return counts;
}

export function countCommercialStageFacets(opportunities, matchOtherFilters) {
  const base = (opportunities || []).filter((o) => {
    if (isExcludedFromSalesFilterInventory(o)) return false;
    return typeof matchOtherFilters === "function"
      ? matchOtherFilters(o)
      : true;
  });
  const counts = {
    "": base.length,
    [COMMERCIAL_STAGE_FILTER.PURSUE_NOW]: 0,
    [COMMERCIAL_STAGE_FILTER.FUTURE_CAPTURE]: 0,
    [COMMERCIAL_STAGE_FILTER.RELATIONSHIP_DEVELOPMENT]: 0,
    [COMMERCIAL_STAGE_FILTER.QUALIFICATION_NEEDED]: 0,
    UNCLASSIFIED: 0,
  };
  for (const o of base) {
    const stage = normalizeCommercialStageForFilter(o);
    if (stage && counts[stage] != null) counts[stage] += 1;
    else counts.UNCLASSIFIED += 1;
  }
  return counts;
}

/** Attach structured filter fields onto a list DTO (non-destructive). */
export function attachSalesOpportunityFilterFields(dto = {}) {
  const sourcing = normalizeHotelSourcingStatus(dto);
  const stageFilterKey = normalizeCommercialStageForFilter(dto);
  const salesKeys = classifySalesOpportunityKeys(dto);
  return {
    ...dto,
    hotelSourcingStatusNormalized: sourcing,
    hotelSourcingStatusLabel:
      dto.hotelSourcingStatusLabel || sourcing || dto.hotelSourcingStatusLabel,
    commercialStageFilterKey: stageFilterKey,
    salesOpportunityQuickFilterKeys: salesKeys,
    salesOpportunityQuickFiltersVersion: SALES_OPPORTUNITY_QUICK_FILTERS_V1,
  };
}

/**
 * Default sales-relevant sort comparator (ascending rank = higher first).
 * Does not auto-promote every Unconfirmed above verified actionable rows.
 */
export function salesRelevanceRank(o = {}) {
  const priority = upper(o.priorityFilterKey || o.priority);
  const pRank =
    priority === PRIORITY.HIGH
      ? 0
      : priority === PRIORITY.MEDIUM
        ? 1
        : priority === PRIORITY.WATCHLIST || priority === "WATCH"
          ? 2
          : 3;
  const stage = normalizeCommercialStageForFilter(o);
  const sRank =
    stage === COMMERCIAL_STAGE_FILTER.PURSUE_NOW
      ? 0
      : stage === COMMERCIAL_STAGE_FILTER.FUTURE_CAPTURE
        ? 1
        : stage === COMMERCIAL_STAGE_FILTER.RELATIONSHIP_DEVELOPMENT
          ? 2
          : 3;
  const sourcing = normalizeHotelSourcingStatus(o);
  // Open sourcing is more urgent than Unconfirmed; Confirmed venue less for "compete now"
  const srcRank =
    sourcing === HOTEL_SOURCING_DISPLAY.HOTEL_SOURCING_OPEN
      ? 0
      : sourcing === HOTEL_SOURCING_DISPLAY.HOTEL_SOURCING_UNCONFIRMED
        ? 1
        : sourcing === HOTEL_SOURCING_DISPLAY.HISTORICAL_VENUE_ONLY
          ? 2
          : 3;
  const fit =
    typeof o.hotelFitScore === "number" ? Math.max(0, 100 - o.hotelFitScore) : 50;
  const buyer =
    o.hasVerifiedNamedRelevantContact === true ||
    o.hasCredibleOutreachRoute === true
      ? 0
      : 1;
  return pRank * 10000 + sRank * 1000 + srcRank * 100 + fit + buyer;
}

export function sortBySalesRelevance(rows = [], dir = 1) {
  return [...rows].sort((a, b) => {
    const d = salesRelevanceRank(a) - salesRelevanceRank(b);
    if (d !== 0) return d * dir;
    const ad = text(a.eventStartDate || a.tileEventStartDate);
    const bd = text(b.eventStartDate || b.tileEventStartDate);
    if (ad && bd && ad !== bd) return ad.localeCompare(bd) * dir;
    return text(a.displayTitle || a.title).localeCompare(
      text(b.displayTitle || b.title),
    );
  });
}
