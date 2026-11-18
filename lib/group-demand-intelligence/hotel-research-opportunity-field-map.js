/**
 * Canonical field map for GDI Hotel Research Opportunities (Airtable).
 * Logical unique key: (hotelId, opportunityId) — enforced in the adapter via compositeKey.
 */

export const GDI_HOTEL_RESEARCH_OPPORTUNITIES_TABLE_NAME =
  "GDI Hotel Research Opportunities";

export const GDI_HOTEL_RESEARCH_OPPORTUNITY_SCHEMA_VERSION =
  "gdi-hotel-research-opportunity-v1";

/** Primary Airtable field — encodes composite identity. */
export const GDI_HOTEL_RESEARCH_PRIMARY_FIELD = "compositeKey";

export const MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY = Object.freeze({
  compositeKey: "compositeKey",
  opportunityId: "opportunityId",
  hotelId: "hotelId",
  organizationName: "organizationName",
  researchLifecycleStatus: "researchLifecycleStatus",
  commercialEligibilityStatus: "commercialEligibilityStatus",
  demandThesis: "demandThesis",
  priority: "priority",
  title: "title",
  segment: "segment",
  sourceSummary: "sourceSummary",
  firstDiscoveredAt: "firstDiscoveredAt",
  firstDiscoveredRunId: "firstDiscoveredRunId",
  lastResearchedAt: "lastResearchedAt",
  lastMaterialChangeAt: "lastMaterialChangeAt",
  dgGeneratorId: "dgGeneratorId",
  researchPayloadJson: "researchPayloadJson",
  schemaVersion: "schemaVersion",
});

export const VAL_RESEARCH_LIFECYCLE_STATUS = Object.freeze([
  "ACTIVE_RESEARCH",
  "WATCHLIST",
  "EXPLORATORY",
  "CONTINUATION",
  "ARCHIVED_RESEARCH",
]);

export const VAL_COMMERCIAL_ELIGIBILITY_STATUS = Object.freeze([
  "RESEARCH_ONLY",
  "GLOBAL_ORG_ONLY",
  "VENUE_IDENTITY_ONLY",
  "CUSTOMER_ELIGIBLE_CANDIDATE",
  "QUALIFIED_GDO_PRESENT",
]);

export function buildHotelResearchCompositeKey(hotelId, opportunityId) {
  const h = String(hotelId || "").trim();
  const o = String(opportunityId || "").trim();
  if (!h || !o) return null;
  return `${h}::${o}`;
}

export default {
  GDI_HOTEL_RESEARCH_OPPORTUNITIES_TABLE_NAME,
  GDI_HOTEL_RESEARCH_OPPORTUNITY_SCHEMA_VERSION,
  GDI_HOTEL_RESEARCH_PRIMARY_FIELD,
  MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY,
  VAL_RESEARCH_LIFECYCLE_STATUS,
  VAL_COMMERCIAL_ELIGIBILITY_STATUS,
  buildHotelResearchCompositeKey,
};
