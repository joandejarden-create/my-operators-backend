/**
 * Production eligibility gates for Verified Independent Census (data vs images).
 */

import { CORE_MATERIAL_FIELDS } from "./provenance.js";

/**
 * @param {object} record
 */
export function assessProductionEligibility(record) {
  const claims = record.claims || record.field_claims || [];
  const filled = new Map(
    claims.filter((c) => c.value != null && c.value !== "").map((c) => [c.field, c])
  );

  const failures = [];
  const warnings = [];

  if (record.legacy_used_as_source === true) {
    failures.push("legacy_source_dependence");
  }
  if (!record.discovery_source) failures.push("missing_discovery_provenance");
  if (!record.first_independently_discovered_at) failures.push("missing_discovery_timestamp");

  for (const f of CORE_MATERIAL_FIELDS) {
    if (!filled.has(f)) failures.push(`missing_core_field:${f}`);
  }

  const unknownMaterial = claims.filter((c) => c.claim_status === "Unknown").map((c) => c.field);
  // Unknown is OK if classified honestly — warn only
  if (unknownMaterial.length) {
    warnings.push(`unresolved_material_fields:${unknownMaterial.length}`);
  }

  const identityOk =
    Boolean(record.fields?.name || record.canonical_hotel_name) &&
    Boolean(record.fields?.Website || record.official_property_url) &&
    Boolean(record.fields?.["Property ID"] || (record.official_property_ids || []).length);

  if (!identityOk) failures.push("identity_incomplete");

  // Exact/High identity confidence: require Property ID + Website + name
  const identityConfidence = identityOk ? "High" : "Low";
  if (identityConfidence !== "High" && identityConfidence !== "Exact") {
    failures.push("identity_confidence_below_high");
  }

  if (record.reconstruction_status === "Hold — Evidence Conflict" || record.reconstruction_state === "Hold — Evidence Conflict") {
    failures.push("unresolved_critical_contradiction");
  }

  const dataEligible = failures.length === 0;
  const imageStatus = record.image_rights_status || "Unknown Rights";
  const imageEligible = ["First-Party Supplied", "First-Party Approved", "Licensed", "Dealality-Owned"].includes(
    imageStatus
  );

  return {
    production_eligibility_data: dataEligible ? "ELIGIBLE" : "NOT_ELIGIBLE",
    production_eligibility_images: imageEligible ? "ELIGIBLE" : "NOT_ELIGIBLE",
    identity_confidence: identityConfidence,
    failures,
    warnings,
    unknown_material_fields: unknownMaterial,
    image_rights_status: imageStatus,
    note: "Data-complete hotels may remain image-incomplete. No Airtable write in VIC v1.",
  };
}

/**
 * @param {object[]} records
 */
export function batchAssessProductionEligibility(records) {
  return (records || []).map((r) => ({
    independent_record_id: r.independent_record_id,
    name: r.fields?.name || r.canonical_hotel_name,
    ...assessProductionEligibility(r),
  }));
}
