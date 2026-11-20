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

export const HE50_STANDING_AUTH_VERSION = "he-5-0-standing-authorization-v1";
export const HE50_STANDING_AUTH_GRANTED_AT = "2026-11-20";

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
  HE50_BETHESDA_HPC_ID, // Bethesda Marriott
  "recF7DQBPDW5aUDoC", // AC Hotel Newtown Square
  "recYf7HZWmCdWPaSd", // Carton House Fairmont
  "recgaBAwa24wsl3xF", // Hilton Garden Inn Newtown Square Radnor
  "recFQ0WvGLbfK5Gpa", // Secrets Papagayo
  "recwZJuMcx6kA7f7I", // Secrets St. Lucia
  "recd7XAM6tLjzQhaI", // Secrets Cap Cana
  "recq9KWr8UE7z2549", // JW Marriott Costa Elena
  // Pilot keys retained for classification matching
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
});

export const HE50_STANDING_APPLY_GATE = Object.freeze({
  standingAuthorization: true,
  authorizationVersion: HE50_STANDING_AUTH_VERSION,
  grantedAt: HE50_STANDING_AUTH_GRANTED_AT,
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
    "STANDING CONDITIONAL AUTHORIZATION — auto-apply only when all mutation gates pass; quarantine exceptions and continue.",
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
