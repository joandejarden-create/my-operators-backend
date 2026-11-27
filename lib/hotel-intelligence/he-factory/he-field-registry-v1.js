/**
 * Canonical Hotel Explorer 11-tab field registry (machine-readable).
 * Fail-closed: UI fields must declare Airtable destination or calculated contract.
 */

import { HE_ACTIVE_TAB_IDS } from "../he-canonical-explorer-assets-v1.js";

export const HE_FIELD_REGISTRY_V1 = "he-field-registry-v1";

/** @typedef {'HPC'|'HI_ORGS'|'HI_RELS'|'HI_CONTACTS'|'HI_EVIDENCE'|'DEMAND_ANCHORS'|'TRAVEL_INFRA'|'CALCULATED'|'DISABLED'} DestKind */

/**
 * @type {ReadonlyArray<object>}
 */
export const HE_FIELD_REGISTRY = Object.freeze([
  // Hotel Overview
  {
    fieldId: "hotel.propertyName",
    tabId: "hotel",
    uiComponent: "overview.propertyName",
    label: "Property name",
    destinationKind: "HPC",
    airtableField: "Property Name",
    apiPath: "hotel.name",
    dataType: "string",
    verification: "REQUIRED",
    publication: "CUSTOMER",
    refreshDays: 365,
  },
  {
    fieldId: "hotel.roomsKeys",
    tabId: "hotel",
    uiComponent: "overview.rooms",
    label: "Rooms",
    destinationKind: "HPC",
    airtableField: "Rooms / Keys",
    apiPath: "hotel.rooms",
    dataType: "number",
    verification: "REQUIRED",
    publication: "CUSTOMER",
    refreshDays: 180,
  },
  {
    fieldId: "hotel.yearBuilt",
    tabId: "hotel",
    uiComponent: "overview.yearBuilt",
    label: "Year built",
    destinationKind: "HPC",
    airtableField: "Opening Date",
    apiPath: "hotel.yearBuilt",
    dataType: "year",
    verification: "REQUIRED",
    publication: "CUSTOMER",
    refreshDays: 365,
    mappingNote:
      "HPC has no Year Built column; construction year is Opening Date year with provenance VERIFIED_DERIVED_FROM_OPENING_DATE.",
    calculatedContract: "year_from_opening_date",
  },
  {
    fieldId: "hotel.lastRenovated",
    tabId: "hotel",
    uiComponent: "overview.lastRenovated",
    label: "Last renovated",
    destinationKind: "HPC",
    airtableField: "Renovation / Conversion Date",
    apiPath: "hotel.lastRenovated",
    dataType: "year",
    verification: "REQUIRED",
    publication: "CUSTOMER",
    refreshDays: 180,
    mappingNote:
      "Primary display year from Renovation / Conversion Date; milestones retained in renovationEvents / evidence.",
  },
  {
    fieldId: "hotel.totalMeetingSpace",
    tabId: "hotel",
    uiComponent: "overview.meetingSpace",
    label: "Meeting space",
    destinationKind: "HPC",
    airtableField: "Total Meeting Space (sq ft)",
    apiPath: "hotel.totalMeetingSpace",
    dataType: "number",
    verification: "REQUIRED",
    publication: "CUSTOMER",
    refreshDays: 180,
  },
  {
    fieldId: "hotel.hotelHeadline",
    tabId: "hotel",
    uiComponent: "overview.headline",
    label: "Hotel headline",
    destinationKind: "HPC",
    airtableField: "Hotel Headline",
    apiPath: "hotel.hotelHeadline",
    dataType: "string",
    verification: "OPTIONAL",
    publication: "CUSTOMER",
    refreshDays: 90,
  },
  {
    fieldId: "hotel.hotelDescription",
    tabId: "hotel",
    uiComponent: "overview.description",
    label: "Property description",
    destinationKind: "HPC",
    airtableField: "Hotel Description",
    apiPath: "hotel.hotelDescription",
    dataType: "string",
    verification: "OPTIONAL",
    publication: "CUSTOMER",
    refreshDays: 90,
  },
  // Rooms tab
  {
    fieldId: "rooms.suiteInventory",
    tabId: "rooms",
    uiComponent: "rooms.suites",
    label: "Suite inventory",
    destinationKind: "HPC",
    airtableField: "Guestroom Suites Count",
    apiPath: "hotel.guestRoomsMeetingSpace.guestRooms.suites.value",
    dataType: "number",
    verification: "REQUIRED",
    publication: "CUSTOMER",
    refreshDays: 180,
  },
  {
    fieldId: "rooms.meetingRoomsCount",
    tabId: "rooms",
    uiComponent: "rooms.meetingCount",
    label: "Meeting rooms count",
    destinationKind: "HPC",
    airtableField: "Meeting Rooms Count",
    apiPath: "hotel.guestRoomsMeetingSpace.meetingSpaceSummary.meetingRoomCount.value",
    dataType: "number",
    verification: "REQUIRED",
    publication: "CUSTOMER",
    refreshDays: 180,
  },
  // Ownership / org / relationships / contacts
  {
    fieldId: "ownership.legalOwner",
    tabId: "ownership",
    uiComponent: "ownership.legalOwner",
    label: "Legal property owner",
    destinationKind: "HI_RELS",
    airtableField: "Relationship Type=Legal Owner",
    apiPath: "ownership.report.ownership_and_control.property_company",
    dataType: "org_ref",
    verification: "REQUIRED",
    publication: "CUSTOMER",
    refreshDays: 90,
  },
  {
    fieldId: "ownership.economicOwner",
    tabId: "ownership",
    uiComponent: "ownership.economicOwner",
    label: "Economic sponsor",
    destinationKind: "HI_RELS",
    airtableField: "Relationship Type=Economic Owner",
    apiPath: "ownership.report.ownership_and_control.economic_owner",
    dataType: "org_ref",
    verification: "REQUIRED",
    publication: "CUSTOMER",
    refreshDays: 90,
  },
  {
    fieldId: "contacts.keyPeople",
    tabId: "contacts",
    uiComponent: "contacts.directory",
    label: "Named decision-makers",
    destinationKind: "HI_CONTACTS",
    airtableField: "HI Org Contacts",
    apiPath: "contacts.people",
    dataType: "contact_list",
    verification: "REQUIRED",
    publication: "CUSTOMER",
    refreshDays: 60,
  },
  {
    fieldId: "organization.portfolio",
    tabId: "organization",
    uiComponent: "organization.portfolio",
    label: "Owner portfolio",
    destinationKind: "HI_RELS",
    airtableField: "HI Hotel–Org Relationships portfolio",
    apiPath: "ownership.report.portfolio_preview",
    dataType: "portfolio",
    verification: "OPTIONAL",
    publication: "CUSTOMER",
    refreshDays: 90,
  },
  {
    fieldId: "relationships.ocnEdges",
    tabId: "relationships",
    uiComponent: "relationships.edges",
    label: "OCN relationship edges",
    destinationKind: "HI_RELS",
    airtableField: "HI Hotel–Org Relationships",
    apiPath: "ownership.airtable_graph.relationships",
    dataType: "relationship_list",
    verification: "REQUIRED",
    publication: "CUSTOMER",
    refreshDays: 90,
  },
  // Demand / access / sources
  {
    fieldId: "demand.demandAnchors",
    tabId: "demand",
    uiComponent: "demand.drivers",
    label: "Demand drivers",
    destinationKind: "DEMAND_ANCHORS",
    airtableField: "Demand Anchors (geo query)",
    apiPath: "demand.items",
    dataType: "geo_list",
    verification: "REQUIRED",
    publication: "CUSTOMER",
    refreshDays: 120,
  },
  {
    fieldId: "access.travelInfrastructure",
    tabId: "access",
    uiComponent: "access.connectivity",
    label: "Access & connectivity",
    destinationKind: "TRAVEL_INFRA",
    airtableField: "Travel Infrastructure Data (geo query)",
    apiPath: "access.items",
    dataType: "geo_list",
    verification: "REQUIRED",
    publication: "CUSTOMER",
    refreshDays: 120,
  },
  {
    fieldId: "sources.evidenceRegister",
    tabId: "sources",
    uiComponent: "sources.register",
    label: "Sources & Evidence",
    destinationKind: "HI_EVIDENCE",
    airtableField: "HI Graph Evidence",
    apiPath: "ownership.report.evidence",
    dataType: "evidence_list",
    verification: "REQUIRED",
    publication: "CUSTOMER",
    refreshDays: 90,
  },
  // Calculated / submarket / area hotels
  {
    fieldId: "submarket.snapshot",
    tabId: "submarket",
    uiComponent: "submarket.snapshot",
    label: "Submarket snapshot",
    destinationKind: "CALCULATED",
    airtableField: null,
    apiPath: "submarketSnapshot",
    dataType: "object",
    verification: "DERIVED",
    publication: "CUSTOMER",
    refreshDays: 30,
    calculatedContract: "he_submarket_snapshot_v1",
  },
  {
    fieldId: "area.comparables",
    tabId: "area",
    uiComponent: "area.list",
    label: "Area hotels",
    destinationKind: "CALCULATED",
    airtableField: null,
    apiPath: "areaHotels",
    dataType: "hotel_list",
    verification: "DERIVED",
    publication: "CUSTOMER",
    refreshDays: 30,
    calculatedContract: "he_area_hotels_v1",
  },
  // Disabled sentiment
  {
    fieldId: "sentiment.platformIdentities",
    tabId: "sentiment",
    uiComponent: "sentiment.platforms",
    label: "Guest Sentiment",
    destinationKind: "DISABLED",
    airtableField: null,
    apiPath: "guestSentiment",
    dataType: "object",
    verification: "DISABLED_BY_FOUNDER",
    publication: "DISABLED",
    refreshDays: null,
  },
]);

export function validateHeFieldRegistry(registry = HE_FIELD_REGISTRY) {
  const errors = [];
  const byId = new Set();
  for (const row of registry) {
    if (!row.fieldId) errors.push("missing_fieldId");
    if (byId.has(row.fieldId)) errors.push(`duplicate:${row.fieldId}`);
    byId.add(row.fieldId);
    if (!row.tabId) errors.push(`${row.fieldId}:missing_tabId`);
    if (!row.destinationKind) errors.push(`${row.fieldId}:missing_destinationKind`);
    if (
      row.destinationKind !== "CALCULATED" &&
      row.destinationKind !== "DISABLED" &&
      !row.airtableField
    ) {
      errors.push(`${row.fieldId}:missing_airtable_destination`);
    }
    if (row.destinationKind === "CALCULATED" && !row.calculatedContract) {
      errors.push(`${row.fieldId}:missing_calculated_contract`);
    }
  }
  const activeTabs = new Set(HE_ACTIVE_TAB_IDS);
  // sentiment may appear as DISABLED
  for (const row of registry) {
    if (row.destinationKind === "DISABLED") continue;
    if (!activeTabs.has(row.tabId) && row.tabId !== "sentiment") {
      errors.push(`${row.fieldId}:tab_not_active:${row.tabId}`);
    }
  }
  return {
    ok: errors.length === 0,
    version: HE_FIELD_REGISTRY_V1,
    fieldCount: registry.length,
    errors,
  };
}

export default {
  HE_FIELD_REGISTRY_V1,
  HE_FIELD_REGISTRY,
  validateHeFieldRegistry,
};
