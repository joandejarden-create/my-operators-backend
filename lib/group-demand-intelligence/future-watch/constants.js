/**
 * GDI Future Watch Trigger Engine V1 — constants & ontology.
 */

export const FUTURE_WATCH_VERSION = "gdi_future_watch_v1";

export const WATCH_STATUS = Object.freeze({
  ACTIVE: "ACTIVE",
  DUE: "DUE",
  RESEARCHING: "RESEARCHING",
  PUBLIC_DATA_CEILING: "PUBLIC_DATA_CEILING",
  STOPPED: "STOPPED",
  PROMOTED: "PROMOTED",
  CLOSED: "CLOSED",
  REJECTED: "REJECTED",
  RESEARCH_BLOCKED_PROVIDER: "RESEARCH_BLOCKED_PROVIDER",
});

export const TRIGGER_TYPE = Object.freeze({
  HOUSING_OPEN: "HOUSING_OPEN",
  REGISTRATION_OPEN: "REGISTRATION_OPEN",
  HOTEL_ANNOUNCED: "HOTEL_ANNOUNCED",
  VENUE_ANNOUNCED: "VENUE_ANNOUNCED",
  RFP_OPEN: "RFP_OPEN",
  FUTURE_CYCLE_PUBLISHED: "FUTURE_CYCLE_PUBLISHED",
  HOST_SELECTED: "HOST_SELECTED",
  TEAM_LIST_PUBLISHED: "TEAM_LIST_PUBLISHED",
  TRAVEL_INFO_PUBLISHED: "TRAVEL_INFO_PUBLISHED",
  ACCOMMODATION_INFO_PUBLISHED: "ACCOMMODATION_INFO_PUBLISHED",
  OVERFLOW_SIGNAL: "OVERFLOW_SIGNAL",
  DATES_CONFIRMED: "DATES_CONFIRMED",
  DESTINATION_CONFIRMED: "DESTINATION_CONFIRMED",
  OTHER: "OTHER",
});

export const STOP_CONDITION = Object.freeze({
  WRONG_MARKET: "WRONG_MARKET",
  CANCELLED: "CANCELLED",
  CURRENT_CYCLE_CLOSED: "CURRENT_CYCLE_CLOSED",
  FULLY_PLACED: "FULLY_PLACED",
  HOTEL_SELECTED_NO_OVERFLOW: "HOTEL_SELECTED_NO_OVERFLOW",
  NO_LODGING_RELATIONSHIP: "NO_LODGING_RELATIONSHIP",
  HISTORICAL_ONLY: "HISTORICAL_ONLY",
  PUBLIC_DATA_CEILING: "PUBLIC_DATA_CEILING",
  NO_FUTURE_CYCLE: "NO_FUTURE_CYCLE",
  ENTITY_MISMATCH: "ENTITY_MISMATCH",
});

export const DATE_PROVENANCE = Object.freeze({
  EVIDENCE_BASED: "EVIDENCE_BASED",
  HEURISTIC: "HEURISTIC",
  MANUAL: "MANUAL",
});

export const MARKET_ARCHETYPE = Object.freeze({
  URBAN_ASSOCIATION: "URBAN_ASSOCIATION",
  URBAN_CORPORATE: "URBAN_CORPORATE",
  RESORT_ISLAND: "RESORT_ISLAND",
  LUXURY_URBAN: "LUXURY_URBAN",
  SPORTS_EVENT: "SPORTS_EVENT",
  MIXED_DESTINATION: "MIXED_DESTINATION",
  OTHER: "OTHER",
});

export const PROVIDER_ERROR_CLASS = Object.freeze({
  RETRYABLE_PROVIDER_ERROR: "RETRYABLE_PROVIDER_ERROR",
  NONRETRYABLE_PROVIDER_ERROR: "NONRETRYABLE_PROVIDER_ERROR",
  PARTIAL_RESEARCH: "PARTIAL_RESEARCH",
  NO_EVIDENCE: "NO_EVIDENCE",
});

export const DUE_PRIORITY = Object.freeze({
  EXPLICIT_TRIGGER: 1,
  EVIDENCE_BASED: 2,
  HEURISTIC: 3,
  MANUAL_OVERRIDE: 4,
});

/** Configurable heuristic windows (days before event). */
export const DEFAULT_HEURISTIC_POLICY = Object.freeze({
  HOUSING_OPEN: { minDaysBefore: 90, maxDaysBefore: 180, fallbackDays: 45 },
  REGISTRATION_OPEN: { minDaysBefore: 120, maxDaysBefore: 240, fallbackDays: 60 },
  HOTEL_ANNOUNCED: { minDaysBefore: 60, maxDaysBefore: 120, fallbackDays: 45 },
  TEAM_LIST_PUBLISHED: { minDaysBefore: 30, maxDaysBefore: 90, fallbackDays: 30 },
  FUTURE_CYCLE_PUBLISHED: { quarterlyDays: 90, fallbackDays: 90 },
  HOST_SELECTED: { quarterlyDays: 90, fallbackDays: 90 },
  PUBLIC_DATA_CEILING: { quarterlyDays: 90, fallbackDays: 90 },
  DEFAULT: { fallbackDays: 60 },
});

export const SCHEDULER_DEFAULTS = Object.freeze({
  maxWatchesPerBatch: 20,
  maxJevActionsPerWatch: 1,
  maxQueriesPerAction: 3,
  maxFetchesPerAction: 5,
  maxProviderErrors: 5,
  maxEstimatedCostUsd: 25,
  maxRetries: 3,
  retryBackoffDays: [1, 3, 7],
  staleClaimMs: 30 * 60 * 1000,
});

export function isValidTriggerType(t) {
  return Object.values(TRIGGER_TYPE).includes(t);
}

export function isValidStopCondition(s) {
  return Object.values(STOP_CONDITION).includes(s);
}

export function isValidMarketArchetype(a) {
  return Object.values(MARKET_ARCHETYPE).includes(a);
}
