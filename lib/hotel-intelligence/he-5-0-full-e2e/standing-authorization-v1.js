/**
 * HE 5.0 standing founder authorization for the eight-hotel pilot.
 *
 * Conditional auto-apply within approved destinations/fields only.
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

export const HE50_STANDING_AUTH_VERSION = "he-5-0-standing-authorization-v1.1-hi-org-contacts";
export const HE50_STANDING_AUTH_GRANTED_AT = "2026-11-20";
/** Founder extension: HI Org Contacts + Channels for eight-hotel pilot only. */
export const HE50_HI_ORG_CONTACTS_AUTH_GRANTED_AT = "2026-11-20";

export const HE50_HI_ORG_CONTACTS_TABLE_ID = "tbldJAj0nK1l39JZL";
export const HE50_HI_CONTACT_CHANNELS_TABLE_ID = "tblqC7C4ypy5Zojyw";
export const HE50_HI_ORGANIZATIONS_TABLE_ID = "tbliXEfUOWk32AN9y";

export const APPROVAL_CLASS = Object.freeze({
  AUTO_APPROVED_RESEARCH: "AUTO_APPROVED_RESEARCH",
  AUTO_APPROVED_CANONICAL_CREATE: "AUTO_APPROVED_CANONICAL_CREATE",
  AUTO_APPROVED_CANONICAL_UPDATE: "AUTO_APPROVED_CANONICAL_UPDATE",
  AUTO_APPROVED_CANONICAL_LINK: "AUTO_APPROVED_CANONICAL_LINK",
  SKIPPED_NO_CHANGE: "SKIPPED_NO_CHANGE",
  QUARANTINED_INSUFFICIENT_EVIDENCE: "QUARANTINED_INSUFFICIENT_EVIDENCE",
  BLOCKED_OUT_OF_SCOPE: "BLOCKED_OUT_OF_SCOPE",
  FOUNDER_APPROVAL_REQUIRED: "FOUNDER_APPROVAL_REQUIRED",
});

export const PERSISTENCE_STAGE = Object.freeze({
  RESEARCH_VERIFIED: "RESEARCH_VERIFIED",
  CANONICAL_PERSISTED: "CANONICAL_PERSISTED",
  AIRTABLE_READBACK_VALIDATED: "AIRTABLE_READBACK_VALIDATED",
  HOTEL_EXPLORER_UI_VALIDATED: "HOTEL_EXPLORER_UI_VALIDATED",
});

/** Approved pilot hotel keys / HPC ids. */
export const HE50_PILOT_HOTEL_ALLOWLIST = Object.freeze([
  HE50_BETHESDA_HPC_ID,
  "recq9KWr8UE7z2549", // JW Marriott Costa Elena
  "recd7XAM6tLjzQhaI", // Secrets Cap Cana
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
      "Identity Confidence",
      "Production Use Status",
      "Enrichment Status",
      "Source URL",
      "Source Type",
      "Source Confidence",
      "Market",
      "Submarket",
      "Continent",
    ]),
  },
  "tblc9JhKT1edr4grE": {
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
  [HE50_HI_ORGANIZATIONS_TABLE_ID]: {
    label: "HI Organizations",
    tableName: "HI Organizations",
    allowedFields: Object.freeze([
      "Organization ID",
      "Legal Name",
      "Trading Name",
      "Organization Type",
      "Jurisdiction",
      "Official Website",
      "Verification State",
      "Confidence",
      "Dedupe Key",
      "Provenance Summary",
      "Publication State",
      "Schema Version",
      "Notes",
    ]),
    notes:
      "Pilot-scoped org upsert for verified contact employers only. Does not authorize Economic/Legal Owner hotel edges.",
  },
  [HE50_HI_ORG_CONTACTS_TABLE_ID]: {
    label: "HI Org Contacts",
    tableName: "HI Org Contacts",
    allowedFields: Object.freeze([
      "Person ID",
      "Display Name",
      "Organization ID",
      "Organization",
      "Title",
      "Function / Role",
      "Hotel / Commercial Responsibility",
      "Affiliation Verified?",
      "Role Currency",
      "Confidence",
      "Verification State",
      "Person Dedupe Key",
      "Affiliation Dedupe Key",
      "Publication State",
      "Schema Version",
      "Notes",
    ]),
    notes:
      "PUBLIC_SOURCE verified named professionals only. Surfe PII forbidden. Property responsibility may remain unconfirmed.",
  },
  [HE50_HI_CONTACT_CHANNELS_TABLE_ID]: {
    label: "HI Contact Channels",
    tableName: "HI Contact Channels",
    allowedFields: Object.freeze([
      "Channel ID",
      "Subject Type",
      "Subject ID",
      "Person",
      "Organization",
      "Channel Kind",
      "Channel Value",
      "Classification",
      "Verification Status",
      "Source Name",
      "Source URL",
      "Observed At",
      "Provider",
      "Usage Provenance",
      "Access / Publication Restriction",
      "Dedupe Key",
      "Publication State",
      "Schema Version",
      "Notes",
    ]),
    notes:
      "Public professional channels only. Never persist Surfe. Never label org switchboard as named direct.",
  },
});

export const HE50_STANDING_APPLY_GATE = Object.freeze({
  standingAuthorization: true,
  authorizationVersion: HE50_STANDING_AUTH_VERSION,
  grantedAt: HE50_STANDING_AUTH_GRANTED_AT,
  hiOrgContactsAuthorizedAt: HE50_HI_ORG_CONTACTS_AUTH_GRANTED_AT,
  hiOrgContactsAuthorized: true,
  hiContactChannelsAuthorized: true,
  hiOrganizationsForContactsAuthorized: true,
  /** Creating Economic Owner / Legal Owner hotel–org edges remains blocked. */
  ownershipRelationshipPromotionAuthorized: false,
  pilotRunId: HE50_PILOT_RUN_ID,
  baseId: HE50_CANONICAL_BASE_ID,
  globalUsdCap: HE50_GLOBAL_USD_CAP,
  applyAuthorized: true,
  airtableWritesAllowed: true,
  surfeAutomatic: false,
  productionDeploy: false,
  schemaChangesAllowed: false,
  legacyHotelCensusWritesAllowed: false,
  legacyHotelCensusTableIdBlocked: HE50_HOTEL_CENSUS_TABLE_ID_RETIRED,
  ownershipProposedMustRemainProposed: true,
  stopMessage:
    "STANDING CONDITIONAL AUTHORIZATION — auto-apply only when all mutation gates pass; quarantine exceptions and continue. HI Org Contacts / Channels authorized for eight-hotel pilot PUBLIC_SOURCE facts only.",
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
