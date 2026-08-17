/**
 * Brand aggregation + activation candidates + operator staging + image integrity (dry).
 */

import { OUTPUT_CLASS } from "./constants.js";

/**
 * Aggregate verified Census intelligence upward for Brand Explorer staging.
 * @param {object[]} processedHotels
 */
export function aggregateBrands(processedHotels) {
  /** @type {Map<string, object>} */
  const map = new Map();

  for (const h of processedHotels || []) {
    const brand = h.brand || "Unknown";
    const family = h.family || "Unknown";
    const key = `${family}::${brand}`;
    if (!map.has(key)) {
      map.set(key, {
        brand,
        family,
        hotel_count_independent: 0,
        mexico_presence: false,
        cala_presence: false,
        countries: new Set(),
        pipeline_signals: 0,
        openings_signals: 0,
        reflag_signals: 0,
        operator_relationship_samples: 0,
        owner_examples: 0,
        image_issues: 0,
        output_classes: {},
        review_flags: [],
        census_hotel_ids: [],
      });
    }
    const b = map.get(key);
    b.hotel_count_independent += 1;
    b.census_hotel_ids.push(h.independent_record_id);
    if (h.country === "Mexico") b.mexico_presence = true;
    if (h.country) b.countries.add(h.country);
    b.cala_presence = b.mexico_presence || [...b.countries].some((c) => c !== "United States");
    if (/pipeline|future|under construction/i.test(String(h.status || ""))) b.pipeline_signals += 1;
    if (/open/i.test(String(h.status || ""))) b.openings_signals += 0; // status Open ≠ opening event
    const oc = h.output_class || "unknown";
    b.output_classes[oc] = (b.output_classes[oc] || 0) + 1;
    if (h.image_integrity?.issues?.length) b.image_issues += 1;
    if (h.operator_staging) b.operator_relationship_samples += 1;
  }

  const brands = [...map.values()].map((b) => ({
    ...b,
    countries: [...b.countries],
    brand_explorer_conflict_flag:
      "Brand Explorer must not claim hotel counts that conflict with Verified Census without review",
    staging_only: true,
    airtable_writes: false,
    activation: "NONE",
  }));

  brands.sort((a, b) => b.hotel_count_independent - a.hotel_count_independent);
  return {
    version: "census-autopilot-v1-brand-aggregation",
    brand_count: brands.length,
    brands,
  };
}

/**
 * Brands that need completion packs — do NOT activate.
 * @param {object} brandAggregation
 * @param {object} [readiness] prior BE readiness artifact
 */
export function buildActivationCandidates(brandAggregation, readiness = null) {
  const candidates = [];
  for (const b of brandAggregation.brands || []) {
    const prior = (readiness?.suitable_pilot_candidates || []).find(
      (p) => p.brand === b.brand && p.family === b.family
    );
    const incomplete =
      !prior ||
      prior.completion_readiness !== "completion_ready" ||
      (b.output_classes[OUTPUT_CLASS.DEEP_RESEARCH_REQUIRED] || 0) > 0;

    // Every independently discovered brand is a completion candidate for review packing
    candidates.push({
      type: "BRAND COMPLETION CANDIDATE",
      brand: b.brand,
      family: b.family,
      independent_hotel_count: b.hotel_count_independent,
      mexico_presence: b.mexico_presence,
      prior_completion_readiness: prior?.completion_readiness || "unknown",
      run_activation_research_mode: true,
      activate: false,
      brand_status_change: false,
      airtable_writes: false,
      remediation_pack: {
        census_hotel_ids: b.census_hotel_ids.slice(0, 20),
        image_issues: b.image_issues,
        notes: incomplete
          ? "Prepare full remediation pack; do not activate"
          : "Strong VIC cohort — completion pack still staging-only",
      },
    });
  }

  return {
    version: "census-autopilot-v1-activation-candidates",
    do_not_activate: true,
    candidate_count: candidates.length,
    candidates,
  };
}

/**
 * Preserve operator relationship claims discovered during Census research.
 * @param {object} hotel processed hotel
 */
export function stageOperatorRelationship(hotel) {
  const opField = (hotel.field_result?.fields || []).find(
    (f) => f.field === "Operator / Management Company"
  );
  const value = opField?.independently_researched_value || null;
  return {
    property_id: hotel.independent_record_id,
    property_name: hotel.name,
    operator: value,
    valid_from: null,
    valid_to: null,
    source: opField?.evidence?.url || hotel.website || null,
    confidence: opField?.confidence || "Unknown",
    resolution_status: opField?.resolution_status || "Unknown — No Reliable Evidence",
    staging_for: "future_operator_explorer",
    legacy_used_as_source: false,
    cvent_used_as_source: false,
  };
}

/**
 * Image integrity assessment — no download/rehost.
 * @param {object} record
 */
export function assessImageIntegrity(record) {
  const issues = [];
  // VIC freeze does not include production image rights confirmation
  issues.push("rights_status_unknown_review_required");
  if (!record.website) issues.push("no_official_property_url_for_entity_check");

  return {
    current_image_exists: "Unknown",
    source: null,
    rights_status: "Unknown — Review Required",
    correct_property: "Unknown",
    correct_brand: "Unknown",
    stale: "Unknown",
    rendering_only: false,
    official_newer_imagery_exists: "Unknown",
    duplicate: "Unknown",
    issues,
    auto_download: false,
    auto_replace: false,
    production_display_allowed: false,
  };
}
