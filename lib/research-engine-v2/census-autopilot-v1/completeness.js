/**
 * Completeness + research readiness scores (not max filled cells).
 */

import { FIELD_RESOLUTION_STATUS } from "./constants.js";

const CORE_FIELDS = new Set([
  "Property Name",
  "Current Brand",
  "Brand Family",
  "Affiliation Status",
  "Country",
  "City",
  "Official Property URL",
  "Property Identity Key",
  "Family / Source Family",
  "Source URL",
]);

const MATERIAL_FIELDS = new Set([
  ...CORE_FIELDS,
  "Rooms / Keys",
  "Address",
  "State / Region",
  "Latitude",
  "Longitude",
  "Phone",
  "Opening Date",
  "Operator / Management Company",
  "Owner Name",
  "Amenities - Source Text",
  "Property Type",
  "Market / Submarket",
]);

/**
 * @param {object} fieldResult resolveAllResearchableFields output
 * @param {object} record
 */
export function assessCompleteness(fieldResult, record) {
  const fields = fieldResult.fields || [];
  const byName = new Map(fields.map((f) => [f.field, f]));

  const pct = (set) => {
    const list = [...set].filter((n) => byName.has(n));
    if (!list.length) return 0;
    const ok = list.filter((n) => isResolved(byName.get(n))).length;
    return Math.round((100 * ok) / list.length);
  };

  const evidenceOk = fields.filter((f) => f.evidence?.url || f.evidence?.discovery_source).length;
  const evidencePct = fields.length ? Math.round((100 * evidenceOk) / fields.length) : 0;

  const provenanceOk = fields.filter(
    (f) => f.legacy_used_as_source === false && f.cvent_used_as_source === false
  ).length;
  const provenancePct = fields.length ? Math.round((100 * provenanceOk) / fields.length) : 0;

  const imageCompleteness = record.image_integrity_score != null ? record.image_integrity_score : 0;
  const freshnessCompleteness =
    record.last_verified != null ? 70 : record.discovery_source ? 40 : 20;

  const core = pct(CORE_FIELDS);
  // Prefer richer of contract-field resolution vs prior VIC material_pct (freeze may
  // have resolved fields not present on the compact index row).
  const materialFromFields = pct(MATERIAL_FIELDS);
  const materialFromVic = Number(record.material_pct);
  const material = Number.isFinite(materialFromVic)
    ? Math.max(materialFromFields, materialFromVic)
    : materialFromFields;

  // Hard gates override percentages
  const hard_gate_failures = [];
  if (!record.name) hard_gate_failures.push("missing_name");
  if (!record.website && !record.discovery_source) hard_gate_failures.push("missing_discovery_url");
  if (record.legacy_used_as_source === true) hard_gate_failures.push("legacy_source_dependence");
  if (record.cvent_used_as_source === true) hard_gate_failures.push("cvent_source_dependence");
  if (record.reconstruction_status === "Hold — Evidence Conflict") {
    hard_gate_failures.push("evidence_conflict");
  }

  let overall =
    0.25 * core +
    0.25 * material +
    0.15 * evidencePct +
    0.15 * provenancePct +
    0.1 * freshnessCompleteness +
    0.1 * imageCompleteness;

  if (hard_gate_failures.length) overall = Math.min(overall, 35);

  return {
    core_completeness: core,
    material_completeness: material,
    evidence_completeness: evidencePct,
    freshness_completeness: freshnessCompleteness,
    image_completeness: imageCompleteness,
    provenance_completeness: provenancePct,
    overall_research_readiness: Math.round(overall),
    hard_gate_failures,
    note: "100% filled cells is NOT the objective; hard gates override percentages.",
  };
}

function isResolved(f) {
  if (!f) return false;
  return [
    FIELD_RESOLUTION_STATUS.VERIFIED,
    FIELD_RESOLUTION_STATUS.CONFIRMED_EXISTING,
    FIELD_RESOLUTION_STATUS.MISSING_FOUND,
    FIELD_RESOLUTION_STATUS.DERIVED,
  ].includes(f.resolution_status);
}
