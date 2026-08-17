/**
 * Phase A — full-universe classification, gap maps, SerpApi demand forecast (no paid mass crawl).
 */

import {
  GOLDEN_FIELD_REGISTRY,
  priorityFields,
  GOLDEN_SCHEMA_VERSION,
} from "../census-autopilot-v1/golden/golden-schema.js";
import { CLASSIFICATION, PRIORITY, OPERATING_STATES, SERPAPI_ALLOWED_FIELDS, SERPAPI_PROHIBITED_FIELDS } from "./constants.js";
import { assignDealalityGeoLite } from "./geography-brand.js";

/**
 * Baseline priority completeness for VIC hotels using V1.3 overlay when present.
 * Cvent-only candidates start near 0% (name+country only) — not invented.
 */
export function scoreBaselineCompleteness(classifiedRows, v13Map) {
  const pf = priorityFields();
  const hotels = [];

  for (const r of classifiedRows) {
    if (r.classification !== CLASSIFICATION.EXISTING_VERIFIED_PROPERTY && r.candidate_origin !== "VERIFIED_INDEPENDENT") {
      // New challenges: only count name+country as supported among applicable G1
      const supported = ["Property Name", "Country"].filter(Boolean);
      const applicable = pf.filter((e) => e.applicability === "REQUIRED").length;
      const pct = Math.round((100 * 2) / Math.max(1, applicable));
      hotels.push({
        property_identity_id: r.property_identity_id,
        candidate_id: r.candidate_id,
        name: r.origin_name,
        country: r.origin_country,
        classification: r.classification,
        priority_completeness_pct: Math.min(pct, 15),
        source: "challenge_seed_minimal",
        operating_state: OPERATING_STATES.PARTIAL_RESEARCH_CONTINUES,
      });
      continue;
    }

    const v13 = v13Map.get(r.match_vic_id || r.origin_source_record_id);
    const pct13 =
      v13?.raw_priority_completeness_pct ??
      v13?.priority_completeness_pct ??
      v13?.material_weighted_completeness_pct ??
      v13?.completeness_pct;
    if (v13 && typeof pct13 === "number") {
      hotels.push({
        property_identity_id: r.property_identity_id,
        candidate_id: r.candidate_id,
        name: r.origin_name,
        country: r.origin_country,
        classification: r.classification,
        priority_completeness_pct: pct13,
        unknown_fields: v13.unknown_fields || [],
        source: "v1_3_golden_gap_overlay",
        operating_state:
          pct13 >= 95
            ? OPERATING_STATES.VERIFIED_95_PRODUCTION_CANDIDATE
            : OPERATING_STATES.VERIFIED_LT95_REMEDIATION,
      });
    } else {
      // VIC seed without overlay — identity fields known from directory harvest
      const known = [
        "Property Name",
        "Current Brand",
        "Brand Family",
        "Official Property URL",
        "City",
        "Country",
        "Continent",
        "Sub-Continent",
      ];
      const applicable = pf.filter((e) => e.applicability === "REQUIRED").length;
      const pct = Math.round((100 * known.length) / Math.max(1, applicable));
      hotels.push({
        property_identity_id: r.property_identity_id,
        candidate_id: r.candidate_id,
        name: r.origin_name,
        country: r.origin_country,
        classification: r.classification,
        priority_completeness_pct: pct,
        unknown_fields: ["Address", "Latitude", "Longitude", "Rooms / Keys", "Phone"],
        source: "vic_seed_estimate",
        operating_state: OPERATING_STATES.VERIFIED_LT95_REMEDIATION,
      });
    }
  }

  const avg =
    hotels.length === 0
      ? 0
      : hotels.reduce((s, h) => s + (h.priority_completeness_pct || 0), 0) / hotels.length;
  const ge95 = hotels.filter((h) => (h.priority_completeness_pct || 0) >= 95).length;

  return {
    schema_version: GOLDEN_SCHEMA_VERSION,
    priority_field_count: pf.length,
    hotel_scores: hotels,
    baseline_avg_priority_completeness_pct: Math.round(avg * 10) / 10,
    hotels_ge_95: ge95,
  };
}

export function buildFieldGapMap(completeness) {
  /** @type {Record<string, number>} */
  const gaps = {};
  for (const h of completeness.hotel_scores || []) {
    for (const f of h.unknown_fields || []) {
      gaps[f] = (gaps[f] || 0) + 1;
    }
  }
  // For challenge seeds without unknown_fields list, attribute common gaps
  for (const h of completeness.hotel_scores || []) {
    if (h.source === "challenge_seed_minimal") {
      for (const f of [
        "Address",
        "Latitude",
        "Longitude",
        "Phone",
        "Rooms / Keys",
        "Official Property URL",
        "Current Brand",
        "Market",
        "Submarket",
      ]) {
        gaps[f] = (gaps[f] || 0) + 1;
      }
    }
  }
  const ranked = Object.entries(gaps)
    .map(([field, missing_count]) => ({ field, missing_count }))
    .sort((a, b) => b.missing_count - a.missing_count);
  return { ranked, top_5: ranked.slice(0, 5) };
}

export function buildRoomsGapMap(classifiedRows, completeness) {
  const byFamily = {};
  let totalMissing = 0;
  for (const h of completeness.hotel_scores || []) {
    const missing =
      !h.unknown_fields ||
      h.unknown_fields.includes("Rooms / Keys") ||
      h.source === "challenge_seed_minimal";
    if (!missing && h.source === "v1_3_golden_gap_overlay") {
      // if unknown_fields present and Rooms not listed, treat as known
      if (h.unknown_fields && !h.unknown_fields.includes("Rooms / Keys")) continue;
    }
    totalMissing += 1;
    const row = classifiedRows.find((r) => r.candidate_id === h.candidate_id);
    const fam = row?.brand_family_inferred || "Unknown";
    if (!byFamily[fam]) {
      byFamily[fam] = {
        family: fam,
        missing: 0,
        native_resolvable: 0,
        first_party: 0,
        public_research: 0,
        webhound_candidate: 0,
        unknown: 0,
      };
    }
    byFamily[fam].missing += 1;
    if (["IHG", "Hilton", "Choice"].includes(fam)) byFamily[fam].native_resolvable += 1;
    else if (["Marriott", "Accor", "Hyatt", "Wyndham"].includes(fam)) byFamily[fam].first_party += 1;
    else if (fam === "Independent") byFamily[fam].public_research += 1;
    else byFamily[fam].webhound_candidate += 1;
  }

  return {
    total_rooms_missing_estimate: totalMissing,
    by_family: Object.values(byFamily).sort((a, b) => b.missing - a.missing),
    note: "Rooms never inferred from bedrooms/occupancy/room-types. SerpApi NOT_SUPPORTED for Rooms.",
  };
}

export function buildSerpApiDemandForecast(classifiedRows) {
  const newCandidates = classifiedRows.filter(
    (r) => r.classification === CLASSIFICATION.NEW_PROPERTY_CANDIDATE && r.serpapi_needed
  );
  const enrichGaps = classifiedRows.filter(
    (r) =>
      r.classification === CLASSIFICATION.EXISTING_VERIFIED_PROPERTY &&
      r.candidate_origin === "VERIFIED_INDEPENDENT"
  );

  // Estimate: 1 search for direct-hit confirmation; ~20% need +1 details call already embedded
  const expectedCallsNew = Math.ceil(newCandidates.length * 1.15);
  // Only call SerpApi for existing when address/coords/phone gaps — assume ~40% of VIC seeds
  const existingNeed = Math.ceil(enrichGaps.length * 0.4);
  const expectedCallsExisting = Math.ceil(existingNeed * 1.1);

  return {
    serpapi_allowed_fields: SERPAPI_ALLOWED_FIELDS,
    serpapi_prohibited_fields: SERPAPI_PROHIBITED_FIELDS,
    match_gate: "EXACT_OR_HIGH_ONLY",
    production_rights: "PENDING — technically eligible ≠ rights eligible",
    new_candidates_needing_confirmation: newCandidates.length,
    existing_estimated_gap_calls: existingNeed,
    estimated_serpapi_calls_full_universe: expectedCallsNew + expectedCallsExisting,
    estimated_breakdown: {
      new_independent_confirmation: expectedCallsNew,
      existing_gap_enrichment: expectedCallsExisting,
    },
    do_not_call_when: [
      "already verified + SerpApi-allowed fields complete",
      "insufficient identity",
      "identity conflict pending review",
      "rights blocked for persistence (research may still run in shadow)",
    ],
  };
}

export function buildPriorityQueue(classifiedRows, completeness) {
  const scoreByCand = new Map(
    (completeness.hotel_scores || []).map((h) => [h.candidate_id, h])
  );
  const queue = [];

  for (const r of classifiedRows) {
    const sc = scoreByCand.get(r.candidate_id);
    let p = PRIORITY.P4;
    if (r.classification === CLASSIFICATION.IDENTITY_CONFLICT) p = PRIORITY.P0;
    else if (
      r.classification === CLASSIFICATION.EXISTING_VERIFIED_PROPERTY &&
      sc &&
      (sc.priority_completeness_pct || 0) < 95
    )
      p = PRIORITY.P1;
    else if (["IHG", "Hilton", "Choice", "Marriott"].includes(r.brand_family_inferred) && r.classification === CLASSIFICATION.NEW_PROPERTY_CANDIDATE)
      p = PRIORITY.P2;
    else if (r.classification === CLASSIFICATION.NEW_PROPERTY_CANDIDATE && r.serpapi_needed)
      p = PRIORITY.P3;
    else if (r.brand_family_inferred === "Independent") p = PRIORITY.P4;
    if (r.classification === CLASSIFICATION.INSUFFICIENT_IDENTITY) p = PRIORITY.P5;

    queue.push({
      priority: p,
      candidate_id: r.candidate_id,
      property_identity_id: r.property_identity_id,
      name: r.origin_name,
      country: r.origin_country,
      classification: r.classification,
      brand_family_inferred: r.brand_family_inferred,
      completeness_pct: sc?.priority_completeness_pct ?? null,
      expected_completeness_gain: p === PRIORITY.P3 ? "high_low_cost" : p === PRIORITY.P1 ? "medium" : "variable",
    });
  }

  const order = { P0: 0, P1: 1, P2: 2, P3: 3, P4: 4, P5: 5 };
  queue.sort((a, b) => (order[a.priority] ?? 9) - (order[b.priority] ?? 9));

  return {
    total: queue.length,
    by_priority: Object.fromEntries(
      Object.keys(order).map((k) => [k, queue.filter((q) => q.priority === k).length])
    ),
    // Persist head only in summary artifacts; full queue written separately as capped
    queue_head: queue.slice(0, 500),
    queue_full_count: queue.length,
  };
}

export function buildAuthoritativeSchemaArtifact() {
  return {
    version: GOLDEN_SCHEMA_VERSION,
    reused_from: "lib/research-engine-v2/census-autopilot-v1/golden/golden-schema.js",
    redefined: false,
    priority_fields: priorityFields().map((f) => ({
      field: f.field,
      group: f.group,
      applicability: f.applicability,
      weight: f.weight,
      weight_band: f.weight_band,
    })),
    separate_tracks: ["LIFECYCLE", "OWNERSHIP_OPERATION", "IMAGE", "GOVERNANCE"],
    hotel_identity_must_include: [
      "Property Identity",
      "Hotel Name / Property Name",
      "Brand",
      "Parent Company / Brand Family",
      "Property Type",
      "Address",
      "City",
      "State / Region",
      "Country",
      "Continent",
      "Sub-Continent",
      "Market",
      "Submarket",
      "Latitude",
      "Longitude",
      "Website",
      "Telephone",
    ],
    registry_size: GOLDEN_FIELD_REGISTRY.length,
  };
}

export function attachGeography(classifiedRows) {
  return classifiedRows.map((r) => ({
    candidate_id: r.candidate_id,
    origin_country: r.origin_country,
    geography: assignDealalityGeoLite(r.origin_country),
  }));
}
