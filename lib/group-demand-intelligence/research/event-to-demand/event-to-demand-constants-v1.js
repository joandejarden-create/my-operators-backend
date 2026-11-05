/**
 * Event-to-Demand constants — cross-cutting discovery pathway.
 * Does NOT introduce new RESEARCH_SEGMENT top-level IDs.
 * Maps event domains → existing segment axes.
 */

import { RESEARCH_SEGMENT } from "../../workflow/hotel-segment-research-plan-v1.js";

export const EVENT_TO_DEMAND_VERSION = "gdi-event-to-demand-v1";

/** Event domain labels for discovery planning (not Airtable segment options). */
export const EVENT_DEMAND_DOMAIN = Object.freeze({
  ASSOCIATION_PROFESSIONAL_CONGRESS: "association_professional_congress",
  EXHIBITION_TRADE_SHOW: "exhibition_trade_show",
  CORPORATE_EVENT_MEETING: "corporate_event_meeting",
  MEDICAL_SCIENTIFIC_ACADEMIC: "medical_scientific_academic",
  ENTERTAINMENT_LUXURY_CULTURAL: "entertainment_luxury_cultural",
  DESTINATION_CITYWIDE: "destination_citywide",
  EVENT_INTERMEDIARY: "event_intermediary",
});

/**
 * Evidence-backed commercial mechanisms from a verified event.
 * SPECULATIVE_MARKET_SIGNAL must not auto-qualify as hotel-facing.
 */
export const EVENT_DEMAND_MECHANISM = Object.freeze({
  HOST_EVENT: "HOST_EVENT",
  EXECUTIVE_DELEGATION: "EXECUTIVE_DELEGATION",
  EXHIBITOR_SPONSOR_LODGING: "EXHIBITOR_SPONSOR_LODGING",
  OFFSITE_MEETING: "OFFSITE_MEETING",
  VIP_HOSPITALITY: "VIP_HOSPITALITY",
  SMALL_GROUP_LODGING: "SMALL_GROUP_LODGING",
  ANCILLARY_EVENT_BUSINESS: "ANCILLARY_EVENT_BUSINESS",
  SPECULATIVE_MARKET_SIGNAL: "SPECULATIVE_MARKET_SIGNAL",
});

/** Hotel-specific classification of how the property relates to the event. */
export const EVENT_HOTEL_FIT_CLASS = Object.freeze({
  ...EVENT_DEMAND_MECHANISM,
});

/** Mechanisms that may become maturation candidates (still need evidence + fit). */
export const ACTIONABLE_EVENT_MECHANISMS = Object.freeze([
  EVENT_DEMAND_MECHANISM.HOST_EVENT,
  EVENT_DEMAND_MECHANISM.EXECUTIVE_DELEGATION,
  EVENT_DEMAND_MECHANISM.EXHIBITOR_SPONSOR_LODGING,
  EVENT_DEMAND_MECHANISM.OFFSITE_MEETING,
  EVENT_DEMAND_MECHANISM.VIP_HOSPITALITY,
  EVENT_DEMAND_MECHANISM.SMALL_GROUP_LODGING,
  EVENT_DEMAND_MECHANISM.ANCILLARY_EVENT_BUSINESS,
]);

/**
 * Domain → primary RESEARCH_SEGMENT (+ optional secondary).
 * Destination-wide stays a signal domain; segment assigned per extracted org.
 */
export const EVENT_DOMAIN_TO_SEGMENTS = Object.freeze({
  [EVENT_DEMAND_DOMAIN.ASSOCIATION_PROFESSIONAL_CONGRESS]: [
    RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
    RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
  ],
  [EVENT_DEMAND_DOMAIN.EXHIBITION_TRADE_SHOW]: [
    RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS,
    RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
  ],
  [EVENT_DEMAND_DOMAIN.CORPORATE_EVENT_MEETING]: [
    RESEARCH_SEGMENT.CORPORATE_MEETINGS_TRAINING,
    RESEARCH_SEGMENT.SCALING_COMPANIES,
  ],
  [EVENT_DEMAND_DOMAIN.MEDICAL_SCIENTIFIC_ACADEMIC]: [
    RESEARCH_SEGMENT.MEDICAL_ACADEMIC,
  ],
  [EVENT_DEMAND_DOMAIN.ENTERTAINMENT_LUXURY_CULTURAL]: [
    RESEARCH_SEGMENT.ENTERTAINMENT_PRODUCTION,
    RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS,
  ],
  [EVENT_DEMAND_DOMAIN.DESTINATION_CITYWIDE]: [
    RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
    RESEARCH_SEGMENT.EXHIBITOR_SPONSOR_TEAMS,
    RESEARCH_SEGMENT.SPORTS,
    RESEARCH_SEGMENT.ENTERTAINMENT_PRODUCTION,
  ],
  [EVENT_DEMAND_DOMAIN.EVENT_INTERMEDIARY]: ["professional_planners_dmc"],
});

export const EVENT_SOURCE_FAMILIES = Object.freeze([
  "official_event",
  "convention_center_calendar",
  "association_calendar",
  "exhibition_directory",
  "venue_calendar",
  "university_institutional",
  "trade_press",
  "city_destination",
  "sponsor_exhibitor_list",
  "event_prospectus",
  "housing_provider",
  "industry_calendar",
]);

export function primarySegmentForDomain(domain) {
  const list = EVENT_DOMAIN_TO_SEGMENTS[domain] || [];
  return list[0] || RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT;
}

export function isActionableMechanism(mechanism) {
  return ACTIONABLE_EVENT_MECHANISMS.includes(String(mechanism || ""));
}
