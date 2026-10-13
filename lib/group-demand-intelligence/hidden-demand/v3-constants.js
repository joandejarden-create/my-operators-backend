/**
 * GDI Hidden Demand V3 — convert qualified entities → lodging demand.
 * No broad source acquisition; deepen the V2 strict queue.
 */

export const HIDDEN_DEMAND_V3 = "gdi_hidden_demand_v3";

export const PARTICIPATION_DEPTH = Object.freeze({
  LIGHT: "LIGHT",
  STANDARD: "STANDARD",
  HEAVY: "HEAVY",
  ANCHOR: "ANCHOR",
});

export const TRAVEL_CLASS = Object.freeze({
  LOCAL: "LOCAL",
  DRIVE_MARKET: "DRIVE_MARKET",
  SHORT_HAUL_AIR: "SHORT_HAUL_AIR",
  LONG_HAUL_AIR: "LONG_HAUL_AIR",
  INTERNATIONAL: "INTERNATIONAL",
  UNKNOWN: "UNKNOWN",
});

export const LODGING_CLASS = Object.freeze({
  LODGING_CONFIRMED: "LODGING_CONFIRMED",
  LODGING_HIGHLY_LIKELY: "LODGING_HIGHLY_LIKELY",
  LODGING_PLAUSIBLE: "LODGING_PLAUSIBLE",
  LODGING_UNLIKELY: "LODGING_UNLIKELY",
  LODGING_UNKNOWN: "LODGING_UNKNOWN",
});

export const ESTIMATE_BASIS = Object.freeze({
  SOURCED: "SOURCED",
  INFERRED_HIGH: "INFERRED_HIGH",
  INFERRED_MEDIUM: "INFERRED_MEDIUM",
  INFERRED_LOW: "INFERRED_LOW",
});

export const HOTEL_FIT_CLASS = Object.freeze({
  HOTEL_FIT_HIGH: "HOTEL_FIT_HIGH",
  HOTEL_FIT_MEDIUM: "HOTEL_FIT_MEDIUM",
  HOTEL_FIT_LOW: "HOTEL_FIT_LOW",
});

export const ACTIONABILITY_V3 = Object.freeze({
  ACTIONABLE_HIGH: "ACTIONABLE_HIGH",
  ACTIONABLE_MEDIUM: "ACTIONABLE_MEDIUM",
  WATCH: "WATCH",
  FILTER: "FILTER",
});

export const BUYER_ROLE = Object.freeze({
  EVENTS_MANAGER: "Events Manager",
  FIELD_MARKETING: "Field Marketing",
  EXPERIENTIAL_MARKETING: "Experiential Marketing",
  TRADE_SHOW_MANAGER: "Trade Show Manager",
  CORPORATE_EVENTS: "Corporate Events",
  MEETINGS_EVENTS: "Meetings & Events",
  MARKETING_OPERATIONS: "Marketing Operations",
  SALES_OPERATIONS: "Sales Operations",
  EXECUTIVE_ASSISTANT: "Executive Assistant",
  OFFICE_MANAGER: "Office Manager",
  TRAVEL_MANAGER: "Travel Manager",
  PROCUREMENT: "Procurement",
  PEOPLE_OPERATIONS: "People Operations",
  REGIONAL_COMMERCIAL: "Regional Commercial Lead",
  UNKNOWN: "Unknown buyer role",
});

export const CONTACT_TIER_V3 = Object.freeze({
  NAMED_DIRECT: "NAMED_DIRECT",
  NAMED_PARTIAL: "NAMED_PARTIAL",
  ORG_PATH: "ORG_PATH",
  NO_CONTACT: "NO_CONTACT",
});
