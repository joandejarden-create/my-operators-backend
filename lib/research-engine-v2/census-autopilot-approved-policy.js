/**
 * Census Autopilot Approved Policy — single source of truth for field/source gates.
 * Founder-approved 2026-08-08. No per-field approval prompts once encoded.
 *
 * Write target: Hotel Property Census only.
 */

export const CENSUS_AUTOPILOT_APPROVED_POLICY_VERSION =
  "census-autopilot-approved-policy-v2-confidence-tiered-internal";

export const POLICY_FIELD = Object.freeze({
  ADDRESS: "Address",
  WEBSITE: "Official Property URL",
  COORDINATES: "Latitude/Longitude",
  ROOMS: "Rooms / Keys",
  MARKET: "Market",
  SUBMARKET: "Submarket",
  PHONE: "Phone",
  INSERTS: "new_hotel_inserts",
});

/** Approved write posture per field. */
export const APPROVED_FIELD_POLICY = Object.freeze({
  Address: {
    status: "approved",
    confidence_default_dataforseo_local: "Medium",
    confidence_official: "High",
    sources_approved: [
      "official_parent_brand",
      "official_hotel_website",
      "official_tourism_registry",
      "dataforseo_local_match_high",
      "approved_secondary_exact_match",
    ],
    require_street_level: true,
    require_clean_identity: true,
    reject_duplicate_risk: true,
    reject_existing_conflict: true,
  },
  "Official Property URL": {
    status: "approved",
    sources_approved: [
      "brand_official",
      "parent_official_property_url",
      "hotel_official_strict",
    ],
    preserve_stronger_brand_url: true,
    reject_ota_affiliate_mirror_directory: true,
    hotel_website_field_exists: false,
  },
  Coordinates: {
    status: "approved_via_mapbox_or_official_only",
    allow_dataforseo_local_direct: false,
    allow_google_local_direct: false,
    mapbox_after_validated_address: true,
    mapbox_after_medium_match_high_address: true,
    require_street_level_address: true,
    require_address_provenance: true,
    require_address_source_url: true,
    reject_city_centroid: true,
    reject_zero_zero: true,
    reject_ota_affiliate_address_source: true,
    medium_address_coordinate_confidence: "Medium",
    high_only_if_official_or_steward: true,
  },
  "Rooms / Keys": {
    status: "approved_secondary",
    sources_approved: [
      "official_parent_brand",
      "official_hotel_website",
      "official_factsheet_pdf",
      "official_tourism_registry",
      "owner_developer",
      "convention_bureau_destination_authority",
      "reputable_hospitality_trade",
      "steward_verified",
    ],
    require_exact_property_match: true,
    require_source_url: true,
    require_source_type: true,
    require_confidence: true,
    forbid_meeting_rooms: true,
    forbid_ai_estimates: true,
    forbid_sitewide_defaults: true,
  },
  Market: {
    status: "approved",
    sources_approved: [
      "dealality_commercial_market_map",
      "clean_city_country_mapping",
      "steward_approved_market_map",
    ],
  },
  Submarket: {
    status: "approved_conditional",
    sources_approved: [
      "approved_submarket_map",
      "address_source_tokens",
      "explicit_district_resort_area",
    ],
    hold_without_mapping: true,
  },
  Phone: {
    status: "approved_medium_internal",
    reason: null,
    confidence_default_dataforseo_local: "Medium",
    allow_dataforseo_local: true,
    allow_google_local: false,
    allow_central_reservation: false,
    require_match_high: true,
    require_exact_property_match: true,
    reject_duplicate_risk: true,
    require_provenance_note: true,
    phone_confidence_field_exists: false,
    public_exposure: false,
    official_source_only: false,
  },
  new_hotel_inserts: {
    status: "approved_internal_census_only_when_flags",
    auto_insert: true,
    require_flags: [
      "ENABLE_DATAFORSEO_LOCAL_INSERTS",
      "ENABLE_HIGH_CONFIDENCE_INSERTS",
    ],
    insert_requirements: [
      "approve_insert_high_or_new_hotel_candidate_high",
      "hotel_category_confirmed",
      "no_duplicate_risk",
      "address_or_coordinates_or_place_id_present",
      "source_provenance_present",
      "Production Use Status=Census Only / Not Owner-Facing",
      "Public Display Review Status=Hold",
      "Radar Display Status=Hold",
      "Human Review Required=true",
    ],
  },
});

/** Fields that must never be written by Autopilot. */
export const NEVER_WRITE_FIELDS = Object.freeze([
  "Owner",
  "Owner Name",
  "Operator",
  "Operator Name",
  "Developer",
  "Opening Date",
  "Renovation Date",
  "Affiliation Start Date",
  "Recent Momentum",
  "Company Validated",
  "Company Validation Date",
  "Brand Verified",
  "Brand Status",
  "External Display Status",
  "Release Status",
]);

export const POLICY_CONTROLLER_PASSES = Object.freeze([
  "gap_audit",
  "existing_record_enrichment",
  "mapbox_coordinates",
  "rooms_completion",
  "market_submarket",
  "new_hotel_discovery",
  "reaudit",
]);

/**
 * Resolve runtime gates from env for the policy controller.
 * @param {NodeJS.ProcessEnv} [env]
 */
export function resolveCensusAutopilotPolicyGates(env = process.env) {
  const controller =
    String(env.ENABLE_CENSUS_POLICY_CONTROLLER || "0").trim() === "1";
  const address =
    String(env.ENABLE_DATAFORSEO_LOCAL_ADDRESS_WRITES || "0").trim() === "1";
  const website =
    String(env.ENABLE_DATAFORSEO_LOCAL_WEBSITE_WRITES || "0").trim() === "1";
  const phone =
    String(env.ENABLE_DATAFORSEO_LOCAL_PHONE_WRITES || "0").trim() === "1";
  const dfsCoords =
    String(env.ENABLE_DATAFORSEO_LOCAL_COORDINATE_WRITES || "0").trim() === "1";
  const mapboxAfter =
    String(env.ENABLE_MAPBOX_AFTER_VALIDATED_ADDRESS || "0").trim() === "1";
  const mapboxMedium =
    String(env.ENABLE_MAPBOX_AFTER_MEDIUM_MATCH_HIGH_ADDRESS || "0").trim() ===
    "1";
  const secondaryHotel =
    String(env.ENABLE_SECONDARY_HOTEL_DATA_SOURCES || "0").trim() === "1";
  const secondaryRooms =
    String(env.ENABLE_SECONDARY_ROOMS_SOURCES || "0").trim() === "1";
  const secondaryPhone =
    String(env.ENABLE_SECONDARY_PHONE_SOURCES || "0").trim() === "1";
  const localInserts =
    String(env.ENABLE_DATAFORSEO_LOCAL_INSERTS || "0").trim() === "1";
  const highInserts =
    String(env.ENABLE_HIGH_CONFIDENCE_INSERTS || "0").trim() === "1";
  const candidatesOnly =
    String(env.DATAFORSEO_WRITE_CANDIDATES_ONLY || "0").trim() === "1";
  const dataforseo =
    String(env.DATAFORSEO_ENABLED || "0").trim() === "1";
  const validated =
    String(env.ENABLE_DATAFORSEO_VALIDATED_WRITES || "0").trim() === "1";
  const internalMedium =
    String(env.ENABLE_CENSUS_INTERNAL_MEDIUM_COMPLETION || "0").trim() === "1";

  const blockers = [];
  if (!controller) blockers.push("ENABLE_CENSUS_POLICY_CONTROLLER_must_be_1");
  if (phone && !internalMedium) {
    blockers.push(
      "ENABLE_DATAFORSEO_LOCAL_PHONE_WRITES_requires_ENABLE_CENSUS_INTERNAL_MEDIUM_COMPLETION"
    );
  }
  if (dfsCoords) {
    blockers.push("ENABLE_DATAFORSEO_LOCAL_COORDINATE_WRITES_must_be_0");
  }
  if (secondaryPhone) {
    blockers.push("ENABLE_SECONDARY_PHONE_SOURCES_must_be_0");
  }
  if (mapboxMedium && !mapboxAfter) {
    blockers.push(
      "ENABLE_MAPBOX_AFTER_MEDIUM_MATCH_HIGH_ADDRESS_requires_ENABLE_MAPBOX_AFTER_VALIDATED_ADDRESS"
    );
  }

  return {
    ok: blockers.length === 0,
    blockers,
    controller_enabled: controller,
    dataforseo_enabled: dataforseo,
    validated_writes: validated,
    candidates_only: candidatesOnly,
    internal_medium_completion: internalMedium,
    address_writes: address,
    website_writes: website,
    phone_writes: Boolean(phone && internalMedium),
    dataforseo_coordinate_writes: false,
    mapbox_after_validated_address: mapboxAfter,
    mapbox_after_medium_match_high_address: mapboxMedium,
    secondary_hotel_data: secondaryHotel,
    secondary_rooms: secondaryRooms,
    secondary_phone: false,
    local_inserts: localInserts,
    high_confidence_inserts: highInserts,
    inserts_enabled: localInserts && highInserts,
    founder_gate_between_passes: false,
    never_write_fields: NEVER_WRITE_FIELDS,
    approved_field_policy: APPROVED_FIELD_POLICY,
    policy_version: CENSUS_AUTOPILOT_APPROVED_POLICY_VERSION,
  };
}

/**
 * Whether high-confidence inserts are allowed under current mode + flags.
 */
export function assertHighConfidenceInsertPolicy(opts = {}) {
  const censusMode = opts.censusMode;
  const gates = opts.gates || resolveCensusAutopilotPolicyGates(opts.env);
  if (censusMode === "candidate-only") {
    return {
      ok: false,
      allowed: false,
      reason: "inserts_forbidden_in_candidate_only_mode",
      census_mode: censusMode,
    };
  }
  // field-completion-only may insert Census Only / Hold rows when
  // confidence-tiered internal completion + explicit insert flags are on.
  if (
    censusMode === "field-completion-only" &&
    !gates.internal_medium_completion
  ) {
    return {
      ok: false,
      allowed: false,
      reason: "inserts_forbidden_in_field_completion_without_internal_medium",
      census_mode: censusMode,
    };
  }
  if (!gates.inserts_enabled) {
    return {
      ok: false,
      allowed: false,
      reason: "high_confidence_inserts_require_explicit_flags",
      require: [
        "ENABLE_DATAFORSEO_LOCAL_INSERTS=1",
        "ENABLE_HIGH_CONFIDENCE_INSERTS=1",
      ],
    };
  }
  return { ok: true, allowed: true, reason: null };
}

/**
 * Classify phone under approved confidence-tiered internal policy.
 * DataForSEO local match_high Medium writes are allowed when flags are on.
 */
export function classifyPhoneUnderAutopilotPolicy(candidate = {}, opts = {}) {
  const gates = opts.gates || resolveCensusAutopilotPolicyGates(opts.env);
  if (!gates.phone_writes) {
    return {
      write: false,
      reason: "phone_writes_flag_off_or_internal_medium_disabled",
      held: true,
      confidence: null,
      exposure: "internal_only",
    };
  }
  if (candidate?.match_class && candidate.match_class !== "match_high") {
    return {
      write: false,
      reason: "not_match_high",
      held: true,
      confidence: "Medium",
      exposure: "internal_only",
    };
  }
  return {
    write: true,
    reason: null,
    held: false,
    confidence: "Medium",
    exposure: "internal_only",
  };
}

/**
 * Classify direct DataForSEO / Google local coordinates (always held).
 */
export function classifyDirectLocalCoordinatesUnderPolicy() {
  return {
    write: false,
    reason: "coordinate_local_direct_not_approved_use_mapbox_after_address",
    held: true,
  };
}
