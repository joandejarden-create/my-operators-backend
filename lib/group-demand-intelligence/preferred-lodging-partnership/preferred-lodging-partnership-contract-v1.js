/**
 * Preferred Lodging Partnership — Segment 8 subtype contract.
 * Demand segment remains social_special_events; subtype is not a 13th segment.
 */
export const PREFERRED_LODGING_PARTNERSHIP_V1 =
  "gdi-preferred-lodging-partnership-v1";

export const PREFERRED_LODGING_SUBTYPE = "PREFERRED_LODGING_PARTNERSHIP";

export const PREFERRED_LODGING_SEGMENT_ID = "social_special_events";

export const PREFERRED_LODGING_CLASSIFICATION_LINE =
  "Preferred Lodging Partnership · Social Groups & Special Events";

/** Controlled on-site lodging taxonomy — never invent NO_ONSITE from silence. */
export const VENUE_ONSITE_LODGING_STATUS = Object.freeze({
  NO_ONSITE_LODGING: "NO_ONSITE_LODGING",
  LIMITED_ONSITE_LODGING: "LIMITED_ONSITE_LODGING",
  ONSITE_LODGING_AVAILABLE: "ONSITE_LODGING_AVAILABLE",
  UNKNOWN: "UNKNOWN",
});

export const VENUE_ONSITE_LODGING_STATUS_LABEL = Object.freeze({
  [VENUE_ONSITE_LODGING_STATUS.NO_ONSITE_LODGING]: "No on-site lodging (verified)",
  [VENUE_ONSITE_LODGING_STATUS.LIMITED_ONSITE_LODGING]:
    "Limited on-site lodging (overflow possible)",
  [VENUE_ONSITE_LODGING_STATUS.ONSITE_LODGING_AVAILABLE]:
    "On-site lodging available",
  [VENUE_ONSITE_LODGING_STATUS.UNKNOWN]: "On-site lodging status unknown",
});

/** Map legacy private-events enums → controlled taxonomy. */
export function normalizeVenueOnsiteLodgingStatus(raw) {
  const s = String(raw || "").trim().toUpperCase();
  if (!s) return VENUE_ONSITE_LODGING_STATUS.UNKNOWN;
  if (
    s === VENUE_ONSITE_LODGING_STATUS.NO_ONSITE_LODGING ||
    s === "NO_LODGING" ||
    s === "NONE"
  ) {
    return VENUE_ONSITE_LODGING_STATUS.NO_ONSITE_LODGING;
  }
  if (
    s === VENUE_ONSITE_LODGING_STATUS.LIMITED_ONSITE_LODGING ||
    s === "LIMITED" ||
    s === "PARTIAL"
  ) {
    return VENUE_ONSITE_LODGING_STATUS.LIMITED_ONSITE_LODGING;
  }
  if (
    s === VENUE_ONSITE_LODGING_STATUS.ONSITE_LODGING_AVAILABLE ||
    s === "HAS_LODGING" ||
    s === "FULL"
  ) {
    return VENUE_ONSITE_LODGING_STATUS.ONSITE_LODGING_AVAILABLE;
  }
  return VENUE_ONSITE_LODGING_STATUS.UNKNOWN;
}

export const CATCHMENT_CLASS = Object.freeze({
  CORE: "CORE",
  SECONDARY: "SECONDARY",
  EXTENDED: "EXTENDED",
  OUT_OF_CATCHMENT: "OUT_OF_CATCHMENT",
  UNKNOWN: "UNKNOWN",
});

export const CATCHMENT_CLASS_LABEL = Object.freeze({
  [CATCHMENT_CLASS.CORE]: "Core catchment",
  [CATCHMENT_CLASS.SECONDARY]: "Secondary catchment",
  [CATCHMENT_CLASS.EXTENDED]: "Extended catchment",
  [CATCHMENT_CLASS.OUT_OF_CATCHMENT]: "Out of catchment",
  [CATCHMENT_CLASS.UNKNOWN]: "Catchment not yet classified",
});

export const PARTNERSHIP_RELATIONSHIP_STATUS = Object.freeze({
  PARTNERSHIP_DEVELOPMENT: "PARTNERSHIP_DEVELOPMENT",
  ACTIVE_PREFERRED_PARTNER: "ACTIVE_PREFERRED_PARTNER",
  RECEIVING_REFERRALS: "RECEIVING_REFERRALS",
  REVENUE_PRODUCING: "REVENUE_PRODUCING",
  UNKNOWN: "UNKNOWN",
});

export const PARTNERSHIP_RELATIONSHIP_STATUS_LABEL = Object.freeze({
  [PARTNERSHIP_RELATIONSHIP_STATUS.PARTNERSHIP_DEVELOPMENT]:
    "Partnership development",
  [PARTNERSHIP_RELATIONSHIP_STATUS.ACTIVE_PREFERRED_PARTNER]:
    "Active preferred partner",
  [PARTNERSHIP_RELATIONSHIP_STATUS.RECEIVING_REFERRALS]: "Receiving referrals",
  [PARTNERSHIP_RELATIONSHIP_STATUS.REVENUE_PRODUCING]: "Revenue producing",
  [PARTNERSHIP_RELATIONSHIP_STATUS.UNKNOWN]: "Relationship status unknown",
});

export const DISTANCE_METHOD = Object.freeze({
  STRAIGHT_LINE_HAVERSINE: "STRAIGHT_LINE_HAVERSINE",
  ROUTE_PROVIDER: "ROUTE_PROVIDER",
  HISTORICAL_CLAIM_UNVERIFIED: "HISTORICAL_CLAIM_UNVERIFIED",
  UNKNOWN: "UNKNOWN",
});

export const DISTANCE_METHOD_LABEL = Object.freeze({
  [DISTANCE_METHOD.STRAIGHT_LINE_HAVERSINE]: "Straight-line distance (coordinates)",
  [DISTANCE_METHOD.ROUTE_PROVIDER]: "Route-based measurement",
  [DISTANCE_METHOD.HISTORICAL_CLAIM_UNVERIFIED]:
    "Historical research claim — re-verify before sales use",
  [DISTANCE_METHOD.UNKNOWN]: "Distance method unknown",
});

export function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Detect Preferred Lodging Partnership opportunities (legacy + new).
 */
export function isPreferredLodgingPartnershipOpportunity(opp = {}) {
  const subtype = text(
    opp.demandOpportunitySubtype ||
      opp.opportunitySubtype ||
      opp.preferredLodgingSubtype,
  );
  if (subtype === PREFERRED_LODGING_SUBTYPE) return true;
  if (opp.preferredLodgingPartnershipV1 === true) return true;
  if (opp.venueIntelligence?.version) return true;
  const type = text(opp.opportunityType || opp.demandSignalType || opp.detailLayout);
  if (
    type === PREFERRED_LODGING_SUBTYPE ||
    type === "VENUE_PARTNERSHIP" ||
    /preferred.?lodging/i.test(type)
  ) {
    return true;
  }
  if (/Preferred Lodging Partnership/i.test(text(opp.title || opp.opportunityName))) {
    return true;
  }
  return Boolean(opp.peVenueId && opp.standingOpportunity && type === "VENUE_PARTNERSHIP");
}
