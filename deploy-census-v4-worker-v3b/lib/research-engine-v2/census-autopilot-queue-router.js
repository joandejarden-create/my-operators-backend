/**
 * Autopilot queue router — ordered A→I queues with field targets + skip rules.
 */

export const QUEUE_ORDER = Object.freeze([
  {
    id: "source_discovery",
    letter: "A",
    label: "Source discovery / record matching",
    target_fields: ["Source URL", "Official Website", "Official Property URL"],
    writes: "propose_inserts_only",
    auto_create: false,
    existing_module: "census-autopilot-source-discovery",
    note: "Find official directories; match Hotel Property Census; propose High inserts only",
  },
  {
    id: "brand_normalization",
    letter: "A0brand",
    label: "Brand normalization / Brand Source-of-Truth",
    target_fields: [
      "Current Brand",
      "Brand Family",
      "Data Confidence Tier",
      "Human Review Required",
      "Enrichment Status",
      "Enrichment Priority",
      "Last Reviewed Date",
    ],
    writes: "high_canonical_brand_only",
    existing_module: "census-brand-normalization",
    note: "Normalize Brand to Active/Live canonical names; steward source conflicts; gates Clean Core",
  },
  {
    id: "parent_company_normalization",
    letter: "A0parent",
    label: "Brand Family / Parent Company normalization",
    target_fields: [
      "Brand Family",
      "Data Confidence Tier",
      "Human Review Required",
      "Enrichment Status",
      "Enrichment Priority",
      "Public Display Review Status",
      "Radar Display Status",
      "Radar Display Reason",
      "Last Reviewed Date",
    ],
    writes: "high_canonical_parent_only",
    existing_module: "census-parent-company-normalization",
    note: "Normalize Brand Family to canonical parent companies; steward brand/source conflicts; gates Clean Core",
  },
  {
    id: "core_identity_quality",
    letter: "A0",
    label: "Core identity quality gate",
    target_fields: [
      "Property Name",
      "Canonical Property Name",
      "Current Brand",
      "City",
      "State / Region",
      "Country",
    ],
    writes: "high_normalize_only",
    existing_module: "census-data-quality-gate",
    note: "Normalize City/State/Canonical; block dirty identity from coords/public; steward Unknown/descriptor cities",
  },
  {
    id: "city_state_normalization",
    letter: "A0b",
    label: "City / State normalization",
    target_fields: ["City", "State / Region"],
    writes: "high_normalize_only",
    existing_module: "census-city-state-normalizer",
    note: "Alias path — executed via core_identity_quality gate",
  },
  {
    id: "core_identity_source_lookup",
    letter: "A0c",
    label: "Core identity source lookup",
    target_fields: ["Source URL", "Official Property URL", "City", "Canonical Property Name"],
    writes: "none_routing_only",
    existing_module: "census-map-contact-size-readiness",
    note: "Route dirty-identity rows with official URLs to source lookup; no Airtable writes",
  },
  {
    id: "clean_core_classification",
    letter: "A0d",
    label: "Clean Core classification",
    target_fields: [
      "Property Name",
      "Canonical Property Name",
      "Current Brand",
      "City",
      "Country",
      "Source URL",
      "Family / Source Family",
    ],
    writes: "none_classification_only",
    existing_module: "census-map-contact-size-readiness",
    note: "Level 1 Clean Core vs Level 2 Map/Contact/Size; does not block on lat/long/phone/rooms",
  },
  {
    id: "canonical_property_name_completion",
    letter: "A0e",
    label: "Canonical Property Name completion",
    target_fields: ["Canonical Property Name"],
    writes: "high_autofill_cleanup_only",
    existing_module: "census-canonical-property-name",
    note: "Alias — executed via key_field_completion / core_identity_quality during identity repair",
  },
  {
    id: "market_geography_completion",
    letter: "A0f",
    label: "Market geography completion",
    target_fields: ["Continent", "Sub-Continent", "Market", "Submarket"],
    writes: "high_country_city_map_only",
    existing_module: "census-market-submarket-classifier",
    note: "Continent/Sub-Continent from Country map; Market from clean City; Submarket High-only; no Mapbox/Google",
  },
  {
    id: "key_field_completion",
    letter: "A1",
    label: "Key field completion",
    target_fields: [
      "Property Name",
      "Canonical Property Name",
      "Current Brand",
      "City",
      "State / Region",
      "Country",
      "Source URL",
      "Family / Source Family",
      "Source Confidence",
      "Production Use Status",
      "Address",
      "Latitude",
      "Longitude",
    ],
    writes: "high_autofill_only",
    existing_module: "census-autopilot-key-field-completion",
    note: "Matrix + High autofill for foundational gaps including Canonical Property Name; geocode soft-blocked without provider",
  },
  {
    id: "property_name_cleanup",
    letter: "A2",
    label: "Property Name cleanup",
    target_fields: ["Property Name"],
    writes: "high_official_clean_name_only",
    existing_module: "production-census-property-name-cleanup-queue",
    note: "Replace marketing/tagline Property Name with official clean hotel name; High only",
  },
  {
    id: "address_confirmation",
    letter: "B",
    label: "Address confirmation",
    target_fields: ["Address", "Street Address", "City", "State / Province", "Postal Code", "Country"],
    writes: "high_official_only",
    existing_module: "production-census-address-geocode-resolver",
    note: "Official hotel address only when High confidence",
  },
  {
    id: "coordinate_completion",
    letter: "C",
    label: "Coordinate completion (Mapbox Permanent)",
    target_fields: [
      "Latitude",
      "Longitude",
      "Coordinate Source Type",
      "Coordinate Confidence",
      "Geocode Provider",
      "Geocode Method",
      "Geocode Reviewed Date",
    ],
    writes: "high_mapbox_permanent_official_address_only",
    existing_module: "census-coordinate-completion",
    note: "Mapbox Permanent Geocoding for High official addresses only; no temporary/Nominatim/centroids/0,0",
    blocked_until: "provider_storage_decision",
  },
  {
    id: "phone_number_enrichment",
    letter: "C1",
    label: "Phone number enrichment",
    target_fields: ["Phone"],
    writes: "high_official_only",
    existing_module: "census-phone-number-enrichment",
    note: "Official property/directory/JSON-LD phone only after Clean Core; never OTA/Google/Mapbox",
  },
  {
    id: "coordinate_resolution",
    letter: "C2",
    label: "Coordinate resolution (legacy soft-defer)",
    target_fields: [
      "Latitude",
      "Longitude",
      "Coordinate Provenance",
      "Coordinate Confidence",
      "Coordinate Source URL",
    ],
    writes: "high_official_or_approved_geocode",
    existing_module: "production-census-coordinate-resolver",
    note: "Legacy alias — prefer coordinate_completion for Mapbox Permanent Autopilot writes",
    blocked_until: "provider_storage_decision",
  },
  {
    id: "radar_public_readiness",
    letter: "D",
    label: "Radar / public readiness",
    target_fields: [
      "Radar Display Status",
      "Radar Geography Status",
      "Public Census Eligibility",
      "Public Display Confidence",
      "Public Display Review Status",
    ],
    writes: "high_identity_clear",
    existing_module: "production-census-population-lane-2",
    note: "No public eligibility for held / brand-unconfirmed",
  },
  {
    id: "description_extraction",
    letter: "E",
    label: "Description extraction",
    target_fields: ["Hotel Description - Source Text", "Hotel Description - AI Summary"],
    writes: "high_grounded_only",
    existing_module: "production-census-description-extraction",
    note: "Grounded official/public source only",
  },
  {
    id: "amenities_extraction",
    letter: "F",
    label: "Amenities extraction",
    target_fields: ["Amenities - Source Text", "Amenities - Structured Tags"],
    writes: "high_explicit_only",
    existing_module: "production-census-population-lane-2",
    note: "Strategic flags only; explicit support required",
  },
  {
    id: "property_type_asset_context",
    letter: "G",
    label: "Property type / asset context",
    target_fields: ["Property Type", "Asset Context", "Market", "Submarket"],
    writes: "high_only",
    existing_module: "production-census-population-lane-2",
    note: "Dealality Market/Submarket; Property Type / Asset Context",
  },
  {
    id: "rooms_keys",
    letter: "H",
    label: "Rooms / Keys",
    target_fields: ["Rooms / Keys", "Rooms Confidence", "Rooms Source URL"],
    writes: "high_official_hotel_rooms_only",
    existing_module: "production-census-rooms-keys-queue",
    queue_engine_id: "rooms_keys_missing",
    note: "Early queue; Medium/Low/Hold → steward; needs v1.1.4 provenance fields",
    needs_schema: "v1.1.4",
  },
  {
    id: "steward_webhound_hard_cases",
    letter: "I",
    label: "Steward / Webhound hard cases",
    target_fields: [],
    writes: "never_direct_airtable",
    existing_module: null,
    note: "Unresolved hard cases only; Webhound sample 10–25; never writes Airtable",
    webhound_max: 25,
  },
]);

export const BLOCKED_PRODUCTION_WRITE_LANES = Object.freeze([
  "Owner Name",
  "Developer",
  "Operator / Management Company",
  "Opening Date",
  "Renovation Date",
  "Affiliation Start Date",
  "Recent Momentum",
  "Brand Explorer fields",
  "Company Validated",
  "Brand Verified",
]);

/**
 * Whether a record should skip a queue (target fields already filled).
 * @param {Record<string, unknown>} fields
 * @param {string[]} targetFields
 */
export function recordNeedsQueue(fields = {}, targetFields = []) {
  if (!targetFields.length) return true;
  const missing = targetFields.filter((f) => {
    const v = fields[f];
    if (v == null || v === "") return true;
    if (Array.isArray(v) && v.length === 0) return true;
    return false;
  });
  // Need queue if any primary target missing (use first as primary when multi)
  return missing.length > 0;
}

/**
 * Route queues for a parent company / region run.
 * @param {{
 *   parentCompany?: string|null,
 *   region?: string,
 *   country?: string|null,
 *   mode?: string,
 *   includeCompleted?: boolean,
 *   geocodeProviderReady?: boolean,
 *   schemaV114Ready?: boolean,
 * }} opts
 */
export function routeAutopilotQueues(opts = {}) {
  const geocodeReady = Boolean(opts.geocodeProviderReady);
  const schemaReady = Boolean(opts.schemaV114Ready);

  const queues = QUEUE_ORDER.map((q) => {
    let status = "scheduled";
    const blockers = [];

    if (q.blocked_until === "provider_storage_decision" && !geocodeReady) {
      status = "blocked_provider";
      blockers.push("geocode_provider_or_storage_terms_missing");
    }
    if (q.needs_schema === "v1.1.4" && !schemaReady) {
      // Rooms still runnable for High writes to existing 3 fields; provenance incomplete
      status = "runnable_needs_schema";
      blockers.push("v1.1.4_rooms_provenance_fields_missing");
    }
    if (q.id === "steward_webhound_hard_cases") {
      status = "learning_only";
    }
    if (q.writes === "propose_inserts_only" && opts.mode === "apply") {
      status = "propose_only_no_auto_create";
      blockers.push("inserts_require_separate_approval");
    }

    return {
      ...q,
      status,
      blockers,
      run_in_mode: {
        plan: true,
        "dry-run":
          status !== "blocked_provider" ||
          q.id === "coordinate_resolution" ||
          q.id === "coordinate_completion",
        controlled: true,
        apply: status !== "learning_only" && status !== "propose_only_no_auto_create",
      },
    };
  });

  return {
    parent_company: opts.parentCompany || null,
    region: opts.region || "CALA",
    country: opts.country || null,
    order: queues.map((q) => q.id),
    queues,
    blocked_production_lanes: BLOCKED_PRODUCTION_WRITE_LANES,
    recommended_first_executable: queues.find(
      (q) => q.status === "scheduled" || q.status === "runnable_needs_schema"
    )?.id || null,
  };
}

/**
 * Pick queues that should execute for a given mode.
 * @param {ReturnType<typeof routeAutopilotQueues>} routed
 * @param {string} mode
 */
export function selectQueuesForMode(routed, mode) {
  return (routed.queues || []).filter((q) => {
    if (mode === "plan") return true;
    if (mode === "apply") return q.run_in_mode?.apply && q.id !== "steward_webhound_hard_cases";
    // dry-run / controlled: include blocked queues as dry-run diagnostics
    return q.id !== "source_discovery" || mode === "plan" || mode === "controlled";
  });
}

/**
 * Build Webhound hard-case candidates (capped).
 * @param {object[]} hardCases
 * @param {{ max?: number }} [opts]
 */
export function routeWebhoundCandidates(hardCases = [], opts = {}) {
  const max = Math.min(Math.max(Number(opts.max) || 25, 1), 25);
  const sliced = hardCases.slice(0, max);
  return {
    candidates: sliced.map((c) => ({
      ...c,
      webhound_direct_write: false,
      role: "hard_case_learning_only",
    })),
    capped_at: max,
    total_input: hardCases.length,
    truncated: hardCases.length > max,
  };
}
