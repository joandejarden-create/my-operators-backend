/**
 * Per-field resolution for Autopilot V1 (dry / staging).
 */

import { FIELD_RESOLUTION_STATUS, SOURCE_LANE } from "./constants.js";

/** Map VIC index fields → contract field names where known. */
const INDEX_VALUE_MAP = Object.freeze({
  "Property Name": (r) => r.name || null,
  "Canonical Property Name": (r) => r.name || null,
  "Current Brand": (r) => r.brand || null,
  "Brand Family": (r) => r.family || null,
  Country: (r) => r.country || null,
  City: (r) => r.city || null,
  "Affiliation Status": (r) => r.status || null,
  "Official Property URL": (r) => r.website || null,
  "Family / Source Family": (r) => r.family || null,
  "Source URL": (r) => r.discovery_source || r.website || null,
  "Property Identity Key": (r) =>
    (r.property_ids && r.property_ids[0]) || r.property_id || r.independent_record_id || null,
  "Discovery Date": () => null,
  Phone: (r) => r.phone || null,
  Address: (r) => r.address || null,
  "State / Region": (r) => r.state || r.region || null,
  Latitude: (r) => r.latitude ?? r.lat ?? null,
  Longitude: (r) => r.longitude ?? r.lng ?? null,
  "Rooms / Keys": (r) => r.rooms ?? null,
  "Opening Date": (r) => r.open_date || r.opening_date || null,
  "Owner Name": (r) => r.owner || null,
  "Operator / Management Company": (r) => r.operator || r.management_company || null,
  "Hotel Description - Source Text": (r) => r.description || null,
  "Amenities - Source Text": (r) => r.amenities || null,
});

/**
 * @param {object} record
 * @param {object} fieldRoute researchable registry row
 * @param {object} [opts]
 */
export function resolveFieldForRecord(record, fieldRoute, opts = {}) {
  const field = fieldRoute.field;
  const getter = INDEX_VALUE_MAP[field];
  const currentValue = getter ? getter(record) : null;
  const hasValue = currentValue != null && currentValue !== "";

  let resolution_status;
  let confidence = null;
  let proposed_action = "none";
  let escalation_required = false;
  let source_type = null;
  let temporal_status = "current";

  if (hasValue) {
    resolution_status = FIELD_RESOLUTION_STATUS.VERIFIED;
    confidence = record.page_source_state === "Available" ? "High" : "Medium";
    source_type = fieldRoute.route?.preferred_lane || SOURCE_LANE.A_STRUCTURED_OFFICIAL;
    proposed_action = "retain_independent_claim";
  } else if (fieldRoute.route?.escalate_opaque) {
    resolution_status = FIELD_RESOLUTION_STATUS.DEEP_RESEARCH_REQUIRED;
    confidence = "Unknown";
    escalation_required = true;
    proposed_action = "escalate_lane_c";
    source_type = SOURCE_LANE.C_DEEP_ESCALATION;
  } else if (/Flag$/i.test(field) || /Over-modeled/i.test(fieldRoute.notes || "")) {
    resolution_status = FIELD_RESOLUTION_STATUS.UNKNOWN_NO_EVIDENCE;
    confidence = "Unknown";
    proposed_action = "research_official_amenities_page";
  } else if (opts.sourceBlocked) {
    resolution_status = FIELD_RESOLUTION_STATUS.SOURCE_BLOCKED;
    escalation_required = true;
    proposed_action = "retry_or_escalate";
  } else {
    resolution_status = FIELD_RESOLUTION_STATUS.UNKNOWN_NO_EVIDENCE;
    confidence = "Unknown";
    proposed_action = "attempt_lane_a_then_b";
  }

  // Stopping rule hint: if verified with High confidence, stop research for this field
  const stop_research =
    resolution_status === FIELD_RESOLUTION_STATUS.VERIFIED &&
    (confidence === "High" || confidence === "Exact");

  return {
    field,
    group: fieldRoute.group,
    current_value: hasValue ? currentValue : null,
    independently_researched_value: hasValue ? currentValue : null,
    resolution_status,
    confidence,
    evidence: hasValue
      ? {
          url: record.website || record.discovery_source || null,
          discovery_source: record.discovery_source || null,
          page_source_state: record.page_source_state || null,
        }
      : null,
    source_type,
    evidence_date: null,
    temporal_status,
    proposed_action,
    escalation_requirement: escalation_required,
    stop_research,
    preferred_lane: fieldRoute.route?.preferred_lane || null,
    plan_key: fieldRoute.route?.plan_key || null,
    legacy_used_as_source: false,
    cvent_used_as_source: false,
  };
}

/**
 * @param {object} record
 * @param {object[]} researchableFields
 * @param {object} [opts]
 */
export function resolveAllResearchableFields(record, researchableFields, opts = {}) {
  const sourceBlocked = record.page_source_state === "Blocked";
  const fields = researchableFields.map((f) =>
    resolveFieldForRecord(record, f, { ...opts, sourceBlocked })
  );

  const resolved = fields.filter((f) =>
    [
      FIELD_RESOLUTION_STATUS.VERIFIED,
      FIELD_RESOLUTION_STATUS.CONFIRMED_EXISTING,
      FIELD_RESOLUTION_STATUS.MISSING_FOUND,
      FIELD_RESOLUTION_STATUS.DERIVED,
      FIELD_RESOLUTION_STATUS.NOT_APPLICABLE,
    ].includes(f.resolution_status)
  ).length;

  const unresolved = fields.length - resolved;
  const escalate = fields.filter((f) => f.escalation_requirement);

  return {
    independent_record_id: record.independent_record_id,
    name: record.name,
    family: record.family,
    brand: record.brand,
    country: record.country,
    fields_researched: fields.length,
    fields_resolved: resolved,
    fields_unresolved: unresolved,
    fields,
    escalations: escalate.map((f) => ({ field: f.field, status: f.resolution_status })),
  };
}
