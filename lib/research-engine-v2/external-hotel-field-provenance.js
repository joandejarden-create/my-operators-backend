/**
 * External hotel field provenance helpers + schema gap report.
 */

import { resolveExternalSource } from "./external-hotel-source-registry.js";
import { mapEvidenceTierCodeToSelect } from "./production-census-rooms-evidence-tier-schema.js";

export const EXTERNAL_HOTEL_PROVENANCE_VERSION =
  "external-hotel-field-provenance-v1";

/** Recommended Airtable provenance fields (schema gap until created). */
export const RECOMMENDED_EXTERNAL_PROVENANCE_FIELDS = Object.freeze([
  {
    name: "External Source Name",
    type: "singleLineText",
    purpose: "Vendor/source label for last external enrichment",
  },
  {
    name: "External Source ID",
    type: "singleLineText",
    purpose: "Vendor property ID (GIATA, Expedia, Place ID, etc.)",
  },
  {
    name: "External Source URL",
    type: "url",
    purpose: "Deep link or dataset URL for the evidence",
  },
  {
    name: "External Source License Tier",
    type: "singleSelect",
    purpose: "Tier A–E Dealality license classification",
    options: [
      "Tier_A_licensed_hotel_master_data",
      "Tier_B_licensed_travel_content_api",
      "Tier_C_official_tourism_registry",
      "Tier_D_place_geo_verification_api",
      "Tier_E_public_web_verification",
    ],
  },
  {
    name: "External Match Confidence",
    type: "singleSelect",
    purpose: "High/Medium/Low match to Census record",
    options: ["High", "Medium", "Low"],
  },
  {
    name: "External Match Date",
    type: "date",
    purpose: "When external match was evaluated/written",
  },
  {
    name: "Address Source Type",
    type: "singleSelect",
    purpose: "Provenance type for Address",
  },
  {
    name: "Phone Source Type",
    type: "singleSelect",
    purpose: "Provenance type for Phone",
  },
  {
    name: "Hotel URL Source Type",
    type: "singleSelect",
    purpose: "Provenance type for Official Property URL",
  },
  {
    name: "Data License Notes",
    type: "multilineText",
    purpose: "License/storage notes for steward",
  },
]);

/** Existing Census fields that already carry provenance for some domains. */
export const EXISTING_PROVENANCE_FIELDS = Object.freeze([
  "Rooms Source URL",
  "Rooms Source Type",
  "Rooms Evidence Tier",
  "Rooms Confidence",
  "Rooms Reviewed Date",
  "Rooms Notes",
  "Address Source URL",
  "Address Confidence",
  "Coordinate Source Type",
  "Coordinate Confidence",
  "Geocode Provider",
  "Source URL",
  "Source Type",
  "Source Confidence",
]);

/**
 * Build provenance notes fallback when dedicated External* fields missing.
 * @param {{
 *   source_id: string,
 *   external_id?: string,
 *   external_url?: string,
 *   match_confidence?: string,
 *   license_tier?: string,
 *   fields?: string[],
 * }} meta
 */
export function buildExternalProvenanceNotes(meta) {
  const src = resolveExternalSource(meta.source_id);
  const parts = [
    `external_source=${src?.id || meta.source_id}`,
    `license_tier=${meta.license_tier || src?.tier || "unknown"}`,
  ];
  if (meta.external_id) parts.push(`external_id=${meta.external_id}`);
  if (meta.external_url) parts.push(`external_url=${String(meta.external_url).slice(0, 120)}`);
  if (meta.match_confidence) parts.push(`match=${meta.match_confidence}`);
  if (meta.fields?.length) parts.push(`fields=${meta.fields.join(",")}`);
  return parts.join(" | ");
}

/**
 * Attach rooms provenance when proposing Rooms / Keys from an external source.
 * @param {object} proposalFields
 * @param {{ source_id: string, source_url?: string, evidence_code?: string, confidence?: string, today?: string }} meta
 */
export function attachRoomsProvenance(proposalFields, meta) {
  const out = { ...proposalFields };
  if (out["Rooms / Keys"] == null) return out;
  const today = meta.today || new Date().toISOString().slice(0, 10);
  const src = resolveExternalSource(meta.source_id);
  out["Rooms Source URL"] = meta.source_url || out["Rooms Source URL"];
  out["Rooms Source Type"] =
    src?.tier?.includes("Tier_C") || src?.id === "tourism_registry"
      ? "trusted_secondary_source"
      : src?.tier?.includes("Tier_A")
        ? "trusted_secondary_source"
        : "trusted_secondary_source";
  out["Rooms Confidence"] = meta.confidence || "Medium";
  out["Rooms Reviewed Date"] = today;
  const evidence =
    meta.evidence_code ||
    (src?.id === "tourism_registry"
      ? "secondary_tourism_board_destination_authority"
      : "secondary_licensed_hospitality_dataset");
  const select = mapEvidenceTierCodeToSelect(evidence);
  if (select) out["Rooms Evidence Tier"] = select;
  out["Rooms Notes"] = buildExternalProvenanceNotes({
    source_id: meta.source_id,
    external_url: meta.source_url,
    license_tier: src?.tier,
    fields: ["Rooms / Keys"],
  });
  // Never Official High for non-official web
  if (src?.id !== "hotel_brand_websites" && out["Rooms Confidence"] === "High") {
    out["Rooms Confidence"] = "Medium";
  }
  return out;
}

/**
 * @param {Set<string>|string[]} existingFieldNames
 */
export function reportExternalProvenanceSchemaGaps(existingFieldNames = []) {
  const set = existingFieldNames instanceof Set
    ? existingFieldNames
    : new Set(existingFieldNames);
  const missing = RECOMMENDED_EXTERNAL_PROVENANCE_FIELDS.filter(
    (f) => !set.has(f.name)
  );
  const present = RECOMMENDED_EXTERNAL_PROVENANCE_FIELDS.filter((f) =>
    set.has(f.name)
  );
  const existingUseful = EXISTING_PROVENANCE_FIELDS.filter((n) => set.has(n));
  return {
    version: EXTERNAL_HOTEL_PROVENANCE_VERSION,
    recommended_fields: RECOMMENDED_EXTERNAL_PROVENANCE_FIELDS,
    present: present.map((f) => f.name),
    missing: missing.map((f) => f.name),
    existing_useful_provenance: existingUseful,
    fallback:
      "Use Rooms Notes / Address Source URL / Source URL + structured external_source= notes until External* fields are created.",
    create_allowed_note:
      "Do not auto-create External* fields in this mission unless a dedicated schema task is approved.",
  };
}
