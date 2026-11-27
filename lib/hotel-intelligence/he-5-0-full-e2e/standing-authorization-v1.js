/**
 * HE 5.0 standing founder authorization for the eight-hotel pilot.
 *
 * Expanded 2026-11-20: Demand Anchors, Travel Infrastructure Data,
 * HI Organizations, HI Hotel–Org Relationships, HI Org Contacts,
 * HI Graph Evidence, HPC — within pilot scope only.
 *
 * Does not authorize schema changes, legacy Hotel Census, Surfe persistence,
 * unverified ownership promotion, or out-of-cohort writes.
 */

import {
  HE50_CANONICAL_BASE_ID,
  HE50_HPC_TABLE_ID,
  HE50_HOTEL_CENSUS_TABLE_ID_RETIRED,
  HE50_GLOBAL_USD_CAP,
  HE50_PILOT_RUN_ID,
  HE50_BETHESDA_HPC_ID,
} from "./contract.js";

export const HE50_STANDING_AUTH_VERSION = "he-5-0-standing-authorization-v2-expanded";
export const HE50_STANDING_AUTH_GRANTED_AT = "2026-11-20";
export const HE50_STANDING_AUTH_EXPANDED_AT = "2026-11-20";

/** Live meta-resolved table IDs on canonical base appCCUsuGsE1ifoLk */
export const HE50_DEMAND_ANCHORS_TABLE_ID = "tblJRhi2rmeunzJzQ";
export const HE50_TRAVEL_INFRASTRUCTURE_TABLE_ID = "tblux5Xs9o50MgpWF";
export const HE50_HI_ORGANIZATIONS_TABLE_ID = "tbliXEfUOWk32AN9y";
export const HE50_HI_HOTEL_ORG_RELATIONSHIPS_TABLE_ID = "tblGiwN0foXXDESvZ";
export const HE50_HI_ORG_CONTACTS_TABLE_ID = "tbldJAj0nK1l39JZL";
export const HE50_HI_GRAPH_EVIDENCE_TABLE_ID = "tblc9JhKT1edr4grE";

export const APPROVAL_CLASS = Object.freeze({
  AUTO_APPROVED_RESEARCH: "AUTO_APPROVED_RESEARCH",
  AUTO_APPROVED_CANONICAL_CREATE: "AUTO_APPROVED_CANONICAL_CREATE",
  AUTO_APPROVED_CANONICAL_UPDATE: "AUTO_APPROVED_CANONICAL_UPDATE",
  AUTO_APPROVED_CANONICAL_LINK: "AUTO_APPROVED_CANONICAL_LINK",
  SKIPPED_NO_CHANGE: "SKIPPED_NO_CHANGE",
  QUARANTINED_INSUFFICIENT_EVIDENCE: "QUARANTINED_INSUFFICIENT_EVIDENCE",
  QUARANTINED_PROPOSED_OWNERSHIP: "QUARANTINED_PROPOSED_OWNERSHIP",
  QUARANTINED_RIGHTS_RESTRICTED: "QUARANTINED_RIGHTS_RESTRICTED",
  QUARANTINED_SURFE_SESSION_ONLY: "QUARANTINED_SURFE_SESSION_ONLY",
  BLOCKED_OUT_OF_SCOPE: "BLOCKED_OUT_OF_SCOPE",
  FOUNDER_APPROVAL_REQUIRED: "FOUNDER_APPROVAL_REQUIRED",
});

export const PERSISTENCE_STAGE = Object.freeze({
  RESEARCH_VERIFIED: "RESEARCH_VERIFIED",
  MUTATION_ELIGIBLE: "MUTATION_ELIGIBLE",
  AUTO_APPROVED_IN_SCOPE: "AUTO_APPROVED_IN_SCOPE",
  CANONICAL_PERSISTED: "CANONICAL_PERSISTED",
  AIRTABLE_APPLIED: "AIRTABLE_APPLIED",
  AIRTABLE_READBACK_VALIDATED: "AIRTABLE_READBACK_VALIDATED",
  API_VALIDATED: "API_VALIDATED",
  LIVE_UI_VALIDATED: "LIVE_UI_VALIDATED",
  HOTEL_EXPLORER_UI_VALIDATED: "HOTEL_EXPLORER_UI_VALIDATED",
});

/** Relationship classifications that must remain distinct. */
export const HE50_RELATIONSHIP_CLASSES = Object.freeze({
  VERIFIED_LEGAL_OWNER: "VERIFIED_LEGAL_OWNER",
  VERIFIED_ECONOMIC_SPONSOR: "VERIFIED_ECONOMIC_SPONSOR",
  VERIFIED_OPERATOR: "VERIFIED_OPERATOR",
  VERIFIED_MANAGEMENT_COMPANY: "VERIFIED_MANAGEMENT_COMPANY",
  VERIFIED_BRAND_AFFILIATION: "VERIFIED_BRAND_AFFILIATION",
  VERIFIED_PORTFOLIO_ASSOCIATION: "VERIFIED_PORTFOLIO_ASSOCIATION",
  PROPOSED_RELATIONSHIP: "PROPOSED_RELATIONSHIP",
});

/** Approved pilot hotel keys / HPC ids. */
export const HE50_PILOT_HOTEL_ALLOWLIST = Object.freeze([
  HE50_BETHESDA_HPC_ID,
  "recF7DQBPDW5aUDoC",
  "recYf7HZWmCdWPaSd",
  "recgaBAwa24wsl3xF",
  "recFQ0WvGLbfK5Gpa",
  "recwZJuMcx6kA7f7I",
  "recd7XAM6tLjzQhaI",
  "recq9KWr8UE7z2549",
  "ac_hotel_newtown_square",
  "carton_house_fairmont",
  "hgi_newtown_square_radnor",
  "secrets_papagayo",
  "secrets_st_lucia",
  "jw_marriott_costa_elena",
  "secrets_cap_cana",
  "bethesda_marriott",
]);

export const HE50_ALLOWED_DESTINATION_TABLES = Object.freeze({
  [HE50_HPC_TABLE_ID]: {
    label: "Hotel Property Census",
    tableName: "Hotel Property Census",
    allowedFields: Object.freeze([
      "Property Name",
      "Canonical Property Name",
      "Current Brand",
      "Brand Family",
      "Affiliation Status",
      "Family / Source Family",
      "Country",
      "City",
      "State / Region",
      "Address",
      "Official Property URL",
      "Phone",
      "Rooms / Keys",
      "Rooms Confidence",
      "Rooms Source URL",
      "Radar Status",
      "Property Type",
      "Opening Date",
      "Opening Date Source URL",
      "Renovation / Conversion Status",
      "Renovation / Conversion Date",
      "Renovation / Conversion Source URL",
      "Identity Confidence",
      "Production Use Status",
      "Enrichment Status",
      "Source URL",
      "Source Type",
      "Source Confidence",
      "Market",
      "Submarket",
      "Continent",
      "Latitude",
      "Longitude",
      "Total Meeting Space (sq ft)",
      "Meeting Rooms Count",
      "Largest Meeting Room Name",
      "Largest Meeting Room (sq ft)",
      "Meeting Space Source URL",
      "Guestroom Single Count",
      "Guestroom Double Count",
      "Guestroom Suites Count",
      "Guestroom Mix Confidence",
      "Meeting Space Flag",
      "Amenities",
      "Chain Scale",
      "Operation Type",
      "Operator / Management Company",
      "Location",
      "Hotel Description",
      "Hotel Headline",
    ]),
  },
  [HE50_DEMAND_ANCHORS_TABLE_ID]: {
    label: "Demand Anchors",
    tableName: "Demand Anchors",
    reusableGeographic: true,
    allowedFields: Object.freeze([
      "Demand Anchor Name",
      "Radar Category",
      "Point Type",
      "Point Subtype",
      "City",
      "Country",
      "Region",
      "Submarket",
      "Address / Location",
      "Latitude",
      "Longitude",
      "Demand Relevance",
      "Demand Segment",
      "Demand Pattern",
      "Hotel Demand Rationale",
      "Source",
      "Source URL / Reference",
      "Data Confidence",
      "Include On Radar Map",
      "Map Layer",
      "Visibility",
      "Notes",
      "Last Verified",
    ]),
  },
  [HE50_TRAVEL_INFRASTRUCTURE_TABLE_ID]: {
    label: "Travel Infrastructure Data",
    tableName: "Travel Infrastructure Data",
    reusableGeographic: true,
    allowedFields: Object.freeze([
      "Name",
      "Type",
      "Latitude",
      "Longitude",
      "City",
      "Country",
      "Region",
      "Submarket",
      "IATA Code",
      "ICAO Code",
      "Scale Tier",
      "Infrastructure Role",
      "Airport Type",
      "Source URL",
      "Last Verified",
      "Radar Category",
      "Point Type",
      "Point Subtype",
      "Address / Location",
      "Demand Relevance",
      "Demand Pattern",
      "Hotel Demand Rationale",
      "Source",
      "Source URL / Reference",
      "Data Confidence",
      "Include On Radar Map",
      "Map Layer",
      "Visibility",
      "Notes",
      "Estimated Drive Time",
    ]),
  },
  [HE50_HI_ORGANIZATIONS_TABLE_ID]: {
    label: "HI Organizations",
    tableName: "HI Organizations",
    allowedFields: Object.freeze([
      "Organization ID",
      "Legal Name",
      "Trading Name",
      "Organization Type",
      "Jurisdiction",
      "Registry / Identifier",
      "Official Website",
      "Verification State",
      "Confidence",
      "Dedupe Key",
      "Provenance Summary",
      "Publication State",
      "Evidence Refs",
      "Schema Version",
      "Notes",
      "Parent Organization",
    ]),
  },
  [HE50_HI_HOTEL_ORG_RELATIONSHIPS_TABLE_ID]: {
    label: "HI Hotel–Org Relationships",
    tableName: "HI Hotel–Org Relationships",
    allowedFields: Object.freeze([
      "Relationship ID",
      "HPC Hotel ID",
      "Organization ID",
      "Relationship Type",
      "Current?",
      "Effective From",
      "Effective To",
      "Confidence",
      "Verification State",
      "Conflict Status",
      "Dedupe Key",
      "Evidence Refs",
      "Publication State",
      "Schema Version",
      "Notes",
      "Organization",
      "HPC Hotel",
    ]),
    // PROPOSED ownership must not auto-promote to verified legal/economic owner
    proposedMustRemainProposed: true,
  },
  [HE50_HI_ORG_CONTACTS_TABLE_ID]: {
    label: "HI Org Contacts",
    tableName: "HI Org Contacts",
    allowedFields: Object.freeze([
      "Person ID",
      "Display Name",
      "Organization ID",
      "Title",
      "Function / Role",
      "Hotel / Commercial Responsibility",
      "Affiliation Verified?",
      "Role Currency",
      "Confidence",
      "Verification State",
      "Person Dedupe Key",
      "Affiliation Dedupe Key",
      "Evidence Refs",
      "Publication State",
      "Schema Version",
      "Notes",
      "Organization",
      "HI Contact Channels",
    ]),
    surfePersistenceForbidden: true,
  },
  [HE50_HI_GRAPH_EVIDENCE_TABLE_ID]: {
    label: "HI Graph Evidence",
    tableName: "HI Graph Evidence",
    allowedFields: Object.freeze([
      "Evidence ID",
      "HPC Hotel ID",
      "Entity Type",
      "Entity Record ID",
      "Field Name",
      "Value Observed",
      "Source Name",
      "Source Type",
      "Source URL",
      "Retrieved At",
      "Evidence Snippet",
      "Confidence",
      "Current?",
      "Notes",
      "Schema Version",
      "Hotel Property Census",
    ]),
  },
});

export const HE50_STANDING_APPLY_GATE = Object.freeze({
  standingAuthorization: true,
  authorizationVersion: HE50_STANDING_AUTH_VERSION,
  grantedAt: HE50_STANDING_AUTH_GRANTED_AT,
  expandedAt: HE50_STANDING_AUTH_EXPANDED_AT,
  pilotRunId: HE50_PILOT_RUN_ID,
  baseId: HE50_CANONICAL_BASE_ID,
  globalUsdCap: HE50_GLOBAL_USD_CAP,
  applyAuthorized: true,
  airtableWritesAllowed: true,
  surfeAutomatic: false,
  surfePersistenceAllowed: false,
  productionDeploy: false,
  schemaChangesAllowed: false,
  legacyHotelCensusWritesAllowed: false,
  legacyHotelCensusTableIdBlocked: HE50_HOTEL_CENSUS_TABLE_ID_RETIRED,
  ownershipProposedMustRemainProposed: true,
  guestSentimentUnlicensedPublicationForbidden: true,
  reusableGeographicEntities: true,
  stopMessage:
    "STANDING CONDITIONAL AUTHORIZATION V2 — auto-apply only when all mutation gates pass; quarantine exceptions and continue.",
  reportStatuses: Object.freeze({
    FULL: "AUTO-APPROVED PILOT MUTATIONS APPLIED AND VALIDATED",
    PARTIAL: "PARTIAL AUTO-APPLY — EXCEPTIONS QUARANTINED",
    OUT_OF_SCOPE: "FOUNDER APPROVAL REQUIRED — OUT-OF-SCOPE OPERATION",
  }),
});

export function isLegacyHotelCensusDestination(tableRef) {
  const raw = String(tableRef || "");
  return (
    raw === HE50_HOTEL_CENSUS_TABLE_ID_RETIRED ||
    /Hotel Census/i.test(raw)
  );
}

export function isAllowedDestinationTable(tableRef) {
  const raw = String(tableRef || "").trim();
  if (isLegacyHotelCensusDestination(raw)) return false;
  if (HE50_ALLOWED_DESTINATION_TABLES[raw]) return true;
  return Object.values(HE50_ALLOWED_DESTINATION_TABLES).some(
    (t) => t.tableName === raw || t.label === raw
  );
}

export function resolveDestinationMeta(tableRef) {
  const raw = String(tableRef || "").trim();
  if (HE50_ALLOWED_DESTINATION_TABLES[raw]) {
    return { tableId: raw, ...HE50_ALLOWED_DESTINATION_TABLES[raw] };
  }
  for (const [id, meta] of Object.entries(HE50_ALLOWED_DESTINATION_TABLES)) {
    if (meta.tableName === raw || meta.label === raw) {
      return { tableId: id, ...meta };
    }
  }
  return null;
}

export function isFieldAllowedOnDestination(tableRef, fieldName) {
  const meta = resolveDestinationMeta(tableRef);
  if (!meta?.allowedFields) return false;
  return meta.allowedFields.includes(String(fieldName || "").trim());
}
