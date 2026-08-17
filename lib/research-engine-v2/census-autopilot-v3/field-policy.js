/**
 * Field write policy + Golden→Airtable map from live schema.
 */

import { WRITE_CLASS } from "./constants.js";
import {
  AUTOPILOT_ALLOWED_WRITE_FIELDS,
  AUTOPILOT_FORBIDDEN_FIELDS,
} from "../census-autopilot-field-allowlist.js";
import { INSERT_ALLOWED_FIELDS } from "../census-autopilot-source-discovery.js";

/**
 * @param {{ fields: Array<{name:string,type:string}> }} liveSchema
 */
export function buildGoldenToAirtableFieldMap(liveSchema) {
  const byName = new Map((liveSchema.fields || []).map((f) => [f.name, f]));

  const rows = [
    ["Property Identity ID", "Property Identity Key", "singleLineText", WRITE_CLASS.AUTO_WRITE_SAFE, "official_or_deterministic", true],
    ["Hotel Name / Property Name", "Property Name", "singleLineText", WRITE_CLASS.CORROBORATED_WRITE, "official_directory_or_page", true],
    ["Official Hotel Name", "Canonical Property Name", "singleLineText", WRITE_CLASS.CORROBORATED_WRITE, "official_directory_or_page", true],
    ["Brand", "Current Brand", "singleLineText", WRITE_CLASS.CORROBORATED_WRITE, "official_directory", true],
    ["Parent / Brand Family", "Brand Family", "singleLineText", WRITE_CLASS.CORROBORATED_WRITE, "official_directory", true],
    ["Official Property ID", null, null, WRITE_CLASS.STEWARD_REVIEW, "embedded_in_Property_Identity_Key", false],
    ["Official Hotel URL", "Official Property URL", "url", WRITE_CLASS.CORROBORATED_WRITE, "official_directory", true],
    ["City", "City", "singleLineText", WRITE_CLASS.AUTO_WRITE_SAFE, "official_or_dealality", true],
    ["State / Region", "State / Region", "singleLineText", WRITE_CLASS.AUTO_WRITE_SAFE, "official_or_dealality", true],
    ["Country", "Country", "singleLineText", WRITE_CLASS.AUTO_WRITE_SAFE, "dealality_geography", true],
    ["Continent", "Continent", "singleSelect", WRITE_CLASS.AUTO_WRITE_SAFE, "dealality_geography", true],
    ["Sub-Continent", "Sub-Continent", "singleSelect", WRITE_CLASS.AUTO_WRITE_SAFE, "dealality_geography", true],
    ["Market", "Market", "singleLineText", WRITE_CLASS.AUTO_WRITE_SAFE, "dealality_geography", true],
    ["Submarket", "Submarket", "singleLineText", WRITE_CLASS.AUTO_WRITE_SAFE, "dealality_geography", true],
    ["Address", "Address", "singleLineText", WRITE_CLASS.CORROBORATED_WRITE, "official_page_only_this_pilot", true],
    ["Latitude", "Latitude", "number", WRITE_CLASS.CORROBORATED_WRITE, "official_structured_or_blocked_if_serpapi_only", true],
    ["Longitude", "Longitude", "number", WRITE_CLASS.CORROBORATED_WRITE, "official_structured_or_blocked_if_serpapi_only", true],
    ["Phone", "Phone", "singleLineText", WRITE_CLASS.CORROBORATED_WRITE, "official_or_blocked_if_serpapi_only", true],
    ["Website", "Official Property URL", "url", WRITE_CLASS.CORROBORATED_WRITE, "official_directory", true],
    ["Amenities", "Amenities - Structured Tags", "multipleSelects", WRITE_CLASS.BLOCKED_RIGHTS, "serpapi_blocked", true],
    ["Property Type", "Property Type", "singleLineText", WRITE_CLASS.STEWARD_REVIEW, "needs_corroboration", true],
    ["Asset Context", "Asset Context", "singleLineText", WRITE_CLASS.STEWARD_REVIEW, "needs_corroboration", true],
    ["Rooms / Keys", "Rooms / Keys", "number", WRITE_CLASS.FIRST_PARTY_VALIDATION, "native_or_first_party_only", true],
    ["Opening Date", "Opening Date", null, WRITE_CLASS.PROHIBITED, "forbidden_autopilot", false],
    ["Operating Status", null, null, WRITE_CLASS.STEWARD_REVIEW, "schema_gap_or_status_field", false],
    ["Operator / Management Company", "Operator / Management Company", null, WRITE_CLASS.PROHIBITED, "forbidden_autopilot", false],
  ];

  return {
    version: "golden-to-airtable-field-map-v3",
    table: "Hotel Property Census",
    table_id: liveSchema.tableId,
    live_field_count: liveSchema.fieldCount,
    mappings: rows.map(([golden, airtable, type, writeClass, sourceReq, writeSupport]) => {
      const live = airtable ? byName.get(airtable) : null;
      return {
        golden_census_field: golden,
        airtable_field: airtable,
        type: live?.type || type,
        exists_on_live_schema: airtable ? byName.has(airtable) : false,
        write_class: writeClass,
        source_requirement: sourceReq,
        current_write_support: writeSupport && airtable && byName.has(airtable),
        in_autopilot_allowlist: airtable ? AUTOPILOT_ALLOWED_WRITE_FIELDS.includes(airtable) : false,
        in_insert_allowlist: airtable ? INSERT_ALLOWED_FIELDS.includes(airtable) : false,
        forbidden: airtable ? AUTOPILOT_FORBIDDEN_FIELDS.includes(airtable) : false,
      };
    }),
    linked_record_fields_do_not_write: (liveSchema.fields || [])
      .filter((f) => f.type === "multipleRecordLinks")
      .map((f) => f.name),
    formula_lookup_fields_do_not_write: (liveSchema.fields || [])
      .filter((f) => ["formula", "lookup", "rollup", "multipleLookupValues"].includes(f.type))
      .map((f) => f.name),
  };
}

export function buildWritePolicy() {
  return {
    version: "write-policy-v3",
    field_level_only: true,
    rooms_unknown_does_not_block_verified: true,
    prefer_blank_fill_over_overwrite: true,
    temporal_fields_steward_unless_blank: ["Current Brand", "Property Name", "Operator / Management Company"],
    auto_write_safe: [
      "Property Identity Key",
      "Country",
      "Continent",
      "Sub-Continent",
      "City",
      "State / Region",
      "Market",
      "Submarket",
      "Family / Source Family",
      "Source Type",
      "Source Confidence",
      "Identity Confidence",
      "Data Eligible",
      "Production Use Status",
      "Discovery Date",
      "Enrichment Status",
      "Enrichment Priority",
      "Last Reviewed Date",
    ],
    corroborated_write: [
      "Property Name",
      "Canonical Property Name",
      "Current Brand",
      "Brand Family",
      "Official Property URL",
      "Source URL",
      "Address",
      "Address Confidence",
      "Address Source URL",
    ],
    steward_review: ["Property Type", "Asset Context", "Affiliation Status"],
    first_party_validation: ["Rooms / Keys", "Rooms Confidence", "Rooms Source URL", "Rooms Source Type"],
    blocked_rights: [
      // Claim-level: SerpApi-ONLY claims for these fields are BLOCKED_RIGHTS.
      // Official claims for the same fields are CORROBORATED_WRITE via resolveBestEligibleClaim.
      "Amenities - Source Text",
      "Amenities - Structured Tags",
      "Hotel Description - Source Text",
      "Hotel Description - AI Summary",
    ],
    claim_level_sensitive_fields: ["Latitude", "Longitude", "Phone", "Address"],
    prohibited: [...AUTOPILOT_FORBIDDEN_FIELDS],
    serpapi: {
      SERPAPI_RESEARCH_ALLOWED: true,
      SERPAPI_PRODUCTION_PERSISTENCE_APPROVED: false,
      note: "SerpApi-only claims BLOCKED_RIGHTS; official claims for same fields remain writable",
    },
    cvent: { production_evidence_allowed: false },
    legacy: { production_evidence_allowed: false },
  };
}

export function buildSourceRightsWritePolicy() {
  return {
    version: "source-rights-write-policy-v3",
    official_brand_directory: {
      research: true,
      production_persist: "ALLOWED_WITH_CONSTRAINTS",
      fields: ["identity", "brand", "url", "city", "country", "geography_dealality"],
    },
    official_property_page: {
      research: true,
      production_persist: "ALLOWED_WITH_CONSTRAINTS",
      fields: ["address", "phone_if_official", "coords_if_official_structured"],
    },
    serpapi_google_hotels: {
      research: true,
      production_persist: "NOT_APPROVED — DOWNSTREAM_USE_REVIEW / persistence clarification pending",
      fields_blocked: ["address", "coords", "phone", "amenities", "description"],
    },
    cvent: { research_challenge_only: true, production_persist: false },
    legacy_census: { research_comparison_only: true, production_persist: false },
    images: { reuse: false },
  };
}
