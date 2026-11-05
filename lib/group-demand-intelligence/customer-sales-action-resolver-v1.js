/**
 * GDI Card Pill Standard V1 — customer-facing sales-action + readiness projection.
 *
 * Pill contract: [Commercial Readiness] [Recommended Sales Action] [Timing]
 *
 * Presentation / projection only — does NOT change qualification, commercial state,
 * research state, visibility, or Airtable records.
 *
 * Precedence (sales action):
 *   1. Monitor
 *   2. Pursue Catering
 *   3. Build Venue Partnership
 *   4. Engage Planner
 *   5. Win Room Block
 *   6. Contact Buyer
 *   7. Build Account
 *   8–9. Qualify Opportunity (incomplete + final fallback)
 *
 * Never infers from title text. Never emits Exploratory / classification labels.
 */

import { BOOKING_WINDOW } from "./claim-types.js";
import {
  classifyOpportunityTypeFilter,
  OPPORTUNITY_TYPE_FILTER,
} from "./opportunity-type-filter-v1.js";
import { isPreferredLodgingPartnershipOpportunity } from "./preferred-lodging-partnership/preferred-lodging-partnership-contract-v1.js";
import { isProfessionalEventPlannerOpportunity } from "./professional-event-planners/professional-event-planner-contract-v1.js";
import { HOTEL_SOURCING_DISPLAY } from "./events-opportunity-overview-v1.js";
import {
  HOTEL_REVENUE_TYPE,
  normalizeRevenueTypeTags,
} from "./demand-segments/hotel-revenue-mechanism-v1.js";

export const CUSTOMER_SALES_ACTION_RESOLVER_V1 =
  "gdi-customer-sales-action-resolver-v1";

/** Controlled customer-facing sales-action taxonomy (middle pill). */
export const CUSTOMER_SALES_ACTION = Object.freeze({
  BUILD_ACCOUNT: "BUILD_ACCOUNT",
  CONTACT_BUYER: "CONTACT_BUYER",
  WIN_ROOM_BLOCK: "WIN_ROOM_BLOCK",
  BUILD_VENUE_PARTNERSHIP: "BUILD_VENUE_PARTNERSHIP",
  ENGAGE_PLANNER: "ENGAGE_PLANNER",
  PURSUE_CATERING: "PURSUE_CATERING",
  QUALIFY_OPPORTUNITY: "QUALIFY_OPPORTUNITY",
  MONITOR: "MONITOR",
});

export const CUSTOMER_SALES_ACTION_LABEL = Object.freeze({
  [CUSTOMER_SALES_ACTION.BUILD_ACCOUNT]: "Build Account",
  [CUSTOMER_SALES_ACTION.CONTACT_BUYER]: "Contact Buyer",
  [CUSTOMER_SALES_ACTION.WIN_ROOM_BLOCK]: "Win Room Block",
  [CUSTOMER_SALES_ACTION.BUILD_VENUE_PARTNERSHIP]: "Build Venue Partnership",
  [CUSTOMER_SALES_ACTION.ENGAGE_PLANNER]: "Engage Planner",
  [CUSTOMER_SALES_ACTION.PURSUE_CATERING]: "Pursue Catering",
  [CUSTOMER_SALES_ACTION.QUALIFY_OPPORTUNITY]: "Qualify Opportunity",
  [CUSTOMER_SALES_ACTION.MONITOR]: "Monitor",
});

/** Pill 1 — commercial readiness (customer-facing). */
export const CUSTOMER_READINESS = Object.freeze({
  PURSUE_NOW: "PURSUE_NOW",
  WATCH: "WATCH",
  FUTURE_WATCH: "FUTURE_WATCH",
});

export const CUSTOMER_READINESS_LABEL = Object.freeze({
  [CUSTOMER_READINESS.PURSUE_NOW]: "Pursue Now",
  [CUSTOMER_READINESS.WATCH]: "Watch",
  [CUSTOMER_READINESS.FUTURE_WATCH]: "Future Watch",
});

/** Deterministic precedence order (documented for audit / QA). */
export const CUSTOMER_SALES_ACTION_PRECEDENCE = Object.freeze([
  CUSTOMER_SALES_ACTION.MONITOR,
  CUSTOMER_SALES_ACTION.PURSUE_CATERING,
  CUSTOMER_SALES_ACTION.BUILD_VENUE_PARTNERSHIP,
  CUSTOMER_SALES_ACTION.ENGAGE_PLANNER,
  CUSTOMER_SALES_ACTION.WIN_ROOM_BLOCK,
  CUSTOMER_SALES_ACTION.CONTACT_BUYER,
  CUSTOMER_SALES_ACTION.BUILD_ACCOUNT,
  CUSTOMER_SALES_ACTION.QUALIFY_OPPORTUNITY,
]);

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

function typeFilter(o = {}) {
  return (
    o.opportunityTypeFilterCategory ||
    classifyOpportunityTypeFilter(o) ||
    OPPORTUNITY_TYPE_FILTER.OTHER_UNCLASSIFIED
  );
}

function commercialStageKey(o = {}) {
  return upper(
    o.commercialStageKey ||
      o.tileBadgePlan?.commercialStageKey ||
      o.inventoryLane ||
      "",
  );
}

function hotelSourcingLabel(o = {}) {
  return text(
    o.eventsOpportunityOverview?.hotelSourcing ||
      o.hotelSourcingStatusLabel ||
      o.venueSourcingStatusLabel ||
      o.hotelSourcingStatus ||
      o.venueSourcingStatus ||
      "",
  );
}

function hasUpcomingDate(o = {}) {
  const start = text(
    o.tileEventStartDate ||
      o.futureEditionDate ||
      o.verifiedStartDate ||
      o.eventStartDate ||
      o.eventsOpportunityOverview?.timingStart ||
      "",
  );
  if (!/^\d{4}-\d{2}-\d{2}/.test(start)) return false;
  return start.slice(0, 10) >= new Date().toISOString().slice(0, 10);
}

function isFutureNotYetOpen(o = {}) {
  const stage = commercialStageKey(o);
  const timing = upper(o.actionTimingFilterKey || o.bookingWindowStatus || "");
  const sourcing = hotelSourcingLabel(o);

  if (stage === "FUTURE_CAPTURE" || stage === "FUTURE_OPPORTUNITY") {
    return true;
  }
  if (timing === BOOKING_WINDOW.TOO_EARLY || timing === "TOO_EARLY") {
    return true;
  }
  // Credible future event / cycle with sourcing window not open yet.
  if (
    hasUpcomingDate(o) &&
    (timing === BOOKING_WINDOW.WATCH ||
      timing === "WATCH" ||
      timing === "FUTURE_WATCH" ||
      sourcing === HOTEL_SOURCING_DISPLAY.HOTEL_SOURCING_UNCONFIRMED ||
      /hotel.?tba|sourcing.?not.?open|booking.?window.?not/i.test(
        text(o.decisionState || o.hotelMotion || o.nextActionRecheckReason || ""),
      ))
  ) {
    // Only Monitor when not already actively pursuable.
    if (stage === "PURSUE_NOW" || stage === "SALES_OPPORTUNITY") return false;
    if (sourcing === HOTEL_SOURCING_DISPLAY.HOTEL_SOURCING_OPEN) return false;
    return (
      typeFilter(o) === OPPORTUNITY_TYPE_FILTER.ASSOCIATION_CONFERENCE_GROUP ||
      o.standingOpportunity === true ||
      o.futureDemandEvidence === true ||
      Boolean(o.futureEditionDate)
    );
  }
  return false;
}

function isLocalCateringMotion(o = {}) {
  const cat = typeFilter(o);
  const oppType = upper(o.opportunityType || o.demandSignalType || "");
  // Never treat scaling / corporate-account relationship inventory as catering
  // merely because F&B revenue tags exist on an account-development row.
  if (
    cat === OPPORTUNITY_TYPE_FILTER.SCALING_COMPANY ||
    oppType === "CORPORATE_ACCOUNT_RELATIONSHIP"
  ) {
    if (
      cat !== OPPORTUNITY_TYPE_FILTER.LOCAL_CATERING_DAY_MEETINGS &&
      cat !== OPPORTUNITY_TYPE_FILTER.LOCAL_MEETINGS_CATERING
    ) {
      const seg = text(o.demandSegmentId || "").toLowerCase();
      if (seg !== "local_catering_day_meetings") return false;
    }
  }
  if (
    cat === OPPORTUNITY_TYPE_FILTER.LOCAL_CATERING_DAY_MEETINGS ||
    cat === OPPORTUNITY_TYPE_FILTER.LOCAL_MEETINGS_CATERING
  ) {
    return true;
  }
  const seg = text(o.demandSegmentId || o.researchSegment || "").toLowerCase();
  if (seg === "local_catering_day_meetings") {
    return true;
  }
  const tags = normalizeRevenueTypeTags(
    o.revenueTypes || o.revenueTypeTags || o.revenueMechanisms || [],
  );
  const catering = new Set([
    HOTEL_REVENUE_TYPE.LOCAL_CATERING,
    HOTEL_REVENUE_TYPE.BANQUETS,
    HOTEL_REVENUE_TYPE.PRIVATE_DINING,
    HOTEL_REVENUE_TYPE.MEETING_SPACE_RENTAL,
  ]);
  if (tags.some((t) => catering.has(t))) {
    // Require explicit no-guestroom posture — empty guestroom field is not enough.
    const rooms = upper(o.guestroomDemandStatus || o.lodgingPath || "");
    if (
      rooms === "NONE" ||
      rooms === "NONE_CONFIRMED" ||
      rooms === "NOT_APPLICABLE" ||
      rooms.includes("CATERING_ONLY")
    ) {
      return true;
    }
  }
  return false;
}

function isVenuePartnershipMotion(o = {}) {
  if (isPreferredLodgingPartnershipOpportunity(o)) return true;
  const type = upper(o.opportunityType || o.demandSignalType || "");
  if (type === "VENUE_PARTNERSHIP") return true;
  const cat = typeFilter(o);
  return cat === OPPORTUNITY_TYPE_FILTER.PREFERRED_LODGING_PARTNERSHIP;
}

function isPlannerIntermediaryMotion(o = {}) {
  if (isProfessionalEventPlannerOpportunity(o)) return true;
  const cat = typeFilter(o);
  if (
    cat === OPPORTUNITY_TYPE_FILTER.PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP ||
    cat === OPPORTUNITY_TYPE_FILTER.TRAVEL_TRADE_GROUP_INTERMEDIARIES
  ) {
    return true;
  }
  const subtype = upper(
    o.demandOpportunitySubtype || o.opportunitySubtype || "",
  );
  return (
    subtype.includes("PLANNER") ||
    subtype.includes("INTERMEDIARY") ||
    subtype.includes("TRAVEL_TRADE")
  );
}

function isLodgingRoomBlockMotion(o = {}) {
  const type = upper(o.opportunityType || o.demandSignalType || "");
  if (
    type === "OVERFLOW_HOUSING" ||
    type === "FIXED_VENUE_OPEN_HOUSING" ||
    type === "PRIMARY_PURSUIT" ||
    type.includes("ROOM_BLOCK") ||
    type.includes("OVERFLOW")
  ) {
    return true;
  }
  const lodging = upper(o.lodgingPath || o.hotelMotion || o.demandMechanism || "");
  if (
    /ROOM_BLOCK|OVERFLOW|GUESTROOM|HOUSING|LODGING_BLOCK|PREFERRED_LODGING/.test(
      lodging,
    )
  ) {
    // Preferred lodging venues are partnership, not room-block, unless also event lodging.
    if (isVenuePartnershipMotion(o) && !isAssociationEventLodging(o)) {
      return false;
    }
    return true;
  }
  return isAssociationEventLodging(o);
}

function isAssociationEventLodging(o = {}) {
  const cat = typeFilter(o);
  if (cat !== OPPORTUNITY_TYPE_FILTER.ASSOCIATION_CONFERENCE_GROUP) return false;
  if (isLocalCateringMotion(o)) return false;
  const sourcing = hotelSourcingLabel(o);
  const stage = commercialStageKey(o);
  // Open / unconfirmed hotel sourcing on an association event → room-block motion.
  if (
    sourcing === HOTEL_SOURCING_DISPLAY.HOTEL_SOURCING_OPEN ||
    sourcing === HOTEL_SOURCING_DISPLAY.HOTEL_SOURCING_UNCONFIRMED ||
    stage === "PURSUE_NOW"
  ) {
    return true;
  }
  // Competitive / overflow capture still lodging-led when guestroom demand exists.
  const rooms = upper(o.guestroomDemandStatus || "");
  if (rooms && rooms !== "NONE" && rooms !== "NONE_CONFIRMED") return true;
  if (o.modeledRoomsMin != null || o.peakRooms != null) return true;
  return false;
}

function hasRelevantBuyerPath(o = {}) {
  if (o.hasVerifiedNamedRelevantContact === true) return true;
  if (o.buyerResolved === true) return true;
  if (o.hasCredibleOutreachRoute === true && text(o.contactName)) return true;
  const role = upper(
    o.contactRole || o.buyerRole || o.primaryBuyerFunction || "",
  );
  if (
    role &&
    /PLANNER|BUYER|EVENTS.?DIRECTOR|PROGRAM.?MANAGER|HOUSING|ORGANIZER|TRAVEL/.test(
      role,
    )
  ) {
    return Boolean(text(o.contactName) || o.hasCredibleOutreachRoute === true);
  }
  return false;
}

function isDirectPursuitAppropriate(o = {}) {
  const stage = commercialStageKey(o);
  const timing = upper(o.actionTimingFilterKey || o.bookingWindowStatus || "");
  if (stage === "PURSUE_NOW" || stage === "SALES_OPPORTUNITY") return true;
  if (timing === BOOKING_WINDOW.CONTACT_NOW || timing === "CONTACT_NOW") {
    return true;
  }
  if (o.inventorySalesReady === true) return true;
  if (
    o.commercialReadiness === "READY" ||
    o.commercialReadiness === "ACTIONABLE"
  ) {
    return true;
  }
  return false;
}

function isPublishedAccountDevelopment(o = {}) {
  const cls = upper(o.commercialInventoryClass || "");
  const lane = upper(o.inventoryLane || "");
  return (
    o.accountDevelopmentPublished === true ||
    cls === "ACCOUNT_DEVELOPMENT" ||
    lane === "ACCOUNT_DEVELOPMENT"
  );
}

function isAccountDevelopmentThesis(o = {}) {
  if (isPublishedAccountDevelopment(o)) return true;
  const cat = typeFilter(o);
  // Events "Relationship Development" stage is not corporate Build Account.
  if (cat === OPPORTUNITY_TYPE_FILTER.ASSOCIATION_CONFERENCE_GROUP) {
    return false;
  }
  const stage = commercialStageKey(o);
  // Scaling inventory lane only — never Events RELATIONSHIP_DEVELOPMENT.
  if (stage === "ACCOUNT_DEVELOPMENT") {
    return true;
  }
  if (
    cat === OPPORTUNITY_TYPE_FILTER.SCALING_COMPANY ||
    upper(o.opportunityType || "") === "CORPORATE_ACCOUNT_RELATIONSHIP" ||
    upper(o.demandSignalType || "") === "CORPORATE_ACCOUNT_RELATIONSHIP"
  ) {
    return true;
  }
  // Government / institution standing accounts without a dated lodging event.
  if (
    cat === OPPORTUNITY_TYPE_FILTER.GOVERNMENT_PUBLIC_SECTOR &&
    !hasUpcomingDate(o) &&
    !isLodgingRoomBlockMotion(o)
  ) {
    return true;
  }
  return false;
}

function isCommerciallyIncomplete(o = {}) {
  const stage = commercialStageKey(o);
  if (
    stage === "RESEARCH_NEEDED" ||
    stage === "RESEARCH_WATCHLIST" ||
    stage === "QUALIFICATION_NEEDED"
  ) {
    return true;
  }
  if (o.buyerResolved === false && o.packetComplete !== true) return true;
  if (
    upper(o.decisionState || "") === "UNRESOLVED" ||
    upper(o.lodgingControl || "") === "UNCLEAR"
  ) {
    return true;
  }
  return false;
}

/**
 * Resolve customer-facing sales-action pill (always one of the 8 labels).
 * @returns {{ key: string, label: string, reason: string, sourceFields: string[] }}
 */
export function resolveCustomerSalesAction(opportunity = {}) {
  const sources = [];

  if (isFutureNotYetOpen(opportunity)) {
    sources.push(
      "commercialStageKey|inventoryLane",
      "bookingWindowStatus",
      "hotelSourcingStatusLabel",
      "eventStartDate|futureEditionDate",
    );
    return {
      key: CUSTOMER_SALES_ACTION.MONITOR,
      label: CUSTOMER_SALES_ACTION_LABEL[CUSTOMER_SALES_ACTION.MONITOR],
      reason:
        "Real future opportunity exists but the booking/sourcing window is not yet open for active pursuit",
      sourceFields: sources,
      version: CUSTOMER_SALES_ACTION_RESOLVER_V1,
    };
  }

  if (isLocalCateringMotion(opportunity)) {
    sources.push(
      "opportunityTypeFilterCategory|demandSegmentId",
      "revenueTypes|guestroomDemandStatus",
    );
    return {
      key: CUSTOMER_SALES_ACTION.PURSUE_CATERING,
      label: CUSTOMER_SALES_ACTION_LABEL[CUSTOMER_SALES_ACTION.PURSUE_CATERING],
      reason: "Primary commercial motion is local catering / day meeting (not guestrooms)",
      sourceFields: sources,
      version: CUSTOMER_SALES_ACTION_RESOLVER_V1,
    };
  }

  if (isVenuePartnershipMotion(opportunity)) {
    sources.push(
      "preferredLodgingPartnershipV1|opportunityType|opportunityTypeFilterCategory",
    );
    return {
      key: CUSTOMER_SALES_ACTION.BUILD_VENUE_PARTNERSHIP,
      label:
        CUSTOMER_SALES_ACTION_LABEL[
          CUSTOMER_SALES_ACTION.BUILD_VENUE_PARTNERSHIP
        ],
      reason:
        "Main thesis is an ongoing venue partnership expected to generate recurring room demand",
      sourceFields: sources,
      version: CUSTOMER_SALES_ACTION_RESOLVER_V1,
    };
  }

  // Published Account Development must not resolve as Win Room Block / Contact
  // Buyer — relationship development, not an immediate booking capture.
  if (isPublishedAccountDevelopment(opportunity)) {
    if (isPlannerIntermediaryMotion(opportunity)) {
      sources.push(
        "accountDevelopmentPublished|commercialInventoryClass|inventoryLane",
        "professionalEventPlannerPartnershipV1|opportunityTypeFilterCategory",
      );
      return {
        key: CUSTOMER_SALES_ACTION.ENGAGE_PLANNER,
        label: CUSTOMER_SALES_ACTION_LABEL[CUSTOMER_SALES_ACTION.ENGAGE_PLANNER],
        reason:
          "Published Account Development via planner / intermediary — develop the relationship before treating lodging as bookable demand",
        sourceFields: sources,
        version: CUSTOMER_SALES_ACTION_RESOLVER_V1,
      };
    }
    sources.push(
      "accountDevelopmentPublished|commercialInventoryClass|inventoryLane",
    );
    return {
      key: CUSTOMER_SALES_ACTION.BUILD_ACCOUNT,
      label: CUSTOMER_SALES_ACTION_LABEL[CUSTOMER_SALES_ACTION.BUILD_ACCOUNT],
      reason:
        "Published Account Development — establish or deepen the account relationship; not an immediate room-block pursuit",
      sourceFields: sources,
      version: CUSTOMER_SALES_ACTION_RESOLVER_V1,
    };
  }

  if (isPlannerIntermediaryMotion(opportunity)) {
    sources.push(
      "professionalEventPlannerPartnershipV1|demandOpportunitySubtype|opportunityTypeFilterCategory",
    );
    return {
      key: CUSTOMER_SALES_ACTION.ENGAGE_PLANNER,
      label: CUSTOMER_SALES_ACTION_LABEL[CUSTOMER_SALES_ACTION.ENGAGE_PLANNER],
      reason:
        "Primary commercial path runs through a planner / intermediary relationship",
      sourceFields: sources,
      version: CUSTOMER_SALES_ACTION_RESOLVER_V1,
    };
  }

  if (isLodgingRoomBlockMotion(opportunity)) {
    sources.push(
      "opportunityType|lodgingPath|hotelSourcingStatusLabel|guestroomDemandStatus|commercialStageKey",
    );
    return {
      key: CUSTOMER_SALES_ACTION.WIN_ROOM_BLOCK,
      label: CUSTOMER_SALES_ACTION_LABEL[CUSTOMER_SALES_ACTION.WIN_ROOM_BLOCK],
      reason:
        "Primary sales motion is capturing guestrooms / room block / overflow lodging",
      sourceFields: sources,
      version: CUSTOMER_SALES_ACTION_RESOLVER_V1,
    };
  }

  if (
    hasRelevantBuyerPath(opportunity) &&
    isDirectPursuitAppropriate(opportunity)
  ) {
    sources.push(
      "hasVerifiedNamedRelevantContact|buyerResolved|contactRole",
      "commercialStageKey|inventorySalesReady|bookingWindowStatus",
    );
    return {
      key: CUSTOMER_SALES_ACTION.CONTACT_BUYER,
      label: CUSTOMER_SALES_ACTION_LABEL[CUSTOMER_SALES_ACTION.CONTACT_BUYER],
      reason:
        "Specific commercially relevant buyer/contact path is resolved and direct pursuit is appropriate",
      sourceFields: sources,
      version: CUSTOMER_SALES_ACTION_RESOLVER_V1,
    };
  }

  if (isAccountDevelopmentThesis(opportunity)) {
    sources.push(
      "opportunityType|opportunityTypeFilterCategory|inventoryLane|commercialStageKey",
    );
    return {
      key: CUSTOMER_SALES_ACTION.BUILD_ACCOUNT,
      label: CUSTOMER_SALES_ACTION_LABEL[CUSTOMER_SALES_ACTION.BUILD_ACCOUNT],
      reason:
        "Main commercial motion is establishing or developing an account relationship",
      sourceFields: sources,
      version: CUSTOMER_SALES_ACTION_RESOLVER_V1,
    };
  }

  if (isCommerciallyIncomplete(opportunity)) {
    sources.push(
      "commercialStageKey|inventoryLane|buyerResolved|decisionState|lodgingControl",
    );
    return {
      key: CUSTOMER_SALES_ACTION.QUALIFY_OPPORTUNITY,
      label:
        CUSTOMER_SALES_ACTION_LABEL[CUSTOMER_SALES_ACTION.QUALIFY_OPPORTUNITY],
      reason:
        "Opportunity is credible but important commercial facts remain unresolved — validate before active pursuit",
      sourceFields: sources,
      version: CUSTOMER_SALES_ACTION_RESOLVER_V1,
    };
  }

  sources.push("fallback");
  return {
    key: CUSTOMER_SALES_ACTION.QUALIFY_OPPORTUNITY,
    label:
      CUSTOMER_SALES_ACTION_LABEL[CUSTOMER_SALES_ACTION.QUALIFY_OPPORTUNITY],
    reason:
      "No more specific sales action derived from structured fields — safe qualify fallback",
    sourceFields: sources,
    version: CUSTOMER_SALES_ACTION_RESOLVER_V1,
  };
}

/**
 * Resolve customer-facing readiness pill (Pursue Now / Watch / Future Watch).
 * Uses existing commercial-stage + action-timing signals — does not invent a
 * parallel taxonomy. Never exposes High/Medium priority as readiness.
 */
export function resolveCustomerReadiness(opportunity = {}) {
  const stage = commercialStageKey(opportunity);
  const timing = upper(
    opportunity.actionTimingFilterKey || opportunity.bookingWindowStatus || "",
  );
  const sales = opportunity.salesActionKey
    ? { key: opportunity.salesActionKey }
    : null;

  // Monitor sales action implies Future Watch readiness when stage is not Pursue Now.
  if (
    sales?.key === CUSTOMER_SALES_ACTION.MONITOR &&
    stage !== "PURSUE_NOW" &&
    stage !== "SALES_OPPORTUNITY"
  ) {
    return {
      key: CUSTOMER_READINESS.FUTURE_WATCH,
      label: CUSTOMER_READINESS_LABEL[CUSTOMER_READINESS.FUTURE_WATCH],
      reason: "Future opportunity — monitor until window opens",
      sourceFields: ["salesActionKey", "commercialStageKey"],
      version: CUSTOMER_SALES_ACTION_RESOLVER_V1,
    };
  }

  if (
    stage === "PURSUE_NOW" ||
    stage === "SALES_OPPORTUNITY" ||
    timing === BOOKING_WINDOW.CONTACT_NOW ||
    timing === "CONTACT_NOW" ||
    timing === "PURSUE_NOW"
  ) {
    return {
      key: CUSTOMER_READINESS.PURSUE_NOW,
      label: CUSTOMER_READINESS_LABEL[CUSTOMER_READINESS.PURSUE_NOW],
      reason: "Commercial stage / action timing indicates act-now readiness",
      sourceFields: ["commercialStageKey|inventoryLane", "bookingWindowStatus"],
      version: CUSTOMER_SALES_ACTION_RESOLVER_V1,
    };
  }

  if (
    stage === "FUTURE_CAPTURE" ||
    stage === "FUTURE_OPPORTUNITY" ||
    timing === BOOKING_WINDOW.WATCH ||
    timing === "WATCH" ||
    timing === "FUTURE_WATCH" ||
    timing === BOOKING_WINDOW.TOO_EARLY ||
    timing === "TOO_EARLY"
  ) {
    return {
      key: CUSTOMER_READINESS.FUTURE_WATCH,
      label: CUSTOMER_READINESS_LABEL[CUSTOMER_READINESS.FUTURE_WATCH],
      reason: "Future capture / timing window — not yet open for active pursuit",
      sourceFields: ["commercialStageKey|inventoryLane", "bookingWindowStatus"],
      version: CUSTOMER_SALES_ACTION_RESOLVER_V1,
    };
  }

  return {
    key: CUSTOMER_READINESS.WATCH,
    label: CUSTOMER_READINESS_LABEL[CUSTOMER_READINESS.WATCH],
    reason: "Default customer readiness — develop / watch without act-now urgency",
    sourceFields: ["commercialStageKey|inventoryLane", "bookingWindowStatus"],
    version: CUSTOMER_SALES_ACTION_RESOLVER_V1,
  };
}

/**
 * Attach readiness + sales-action projection fields (derived only).
 */
export function attachCustomerSalesActionProjection(dto = {}) {
  const sales = resolveCustomerSalesAction(dto);
  const withSales = {
    ...dto,
    salesActionKey: sales.key,
    salesActionPillLabel: sales.label,
    salesActionReason: sales.reason,
    salesActionSourceFields: sales.sourceFields,
  };
  const readiness = resolveCustomerReadiness(withSales);
  return {
    ...withSales,
    readinessPillKey: readiness.key,
    readinessPillLabel: readiness.label,
    readinessReason: readiness.reason,
    readinessSourceFields: readiness.sourceFields,
  };
}
