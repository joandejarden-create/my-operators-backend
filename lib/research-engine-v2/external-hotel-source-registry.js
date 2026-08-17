/**
 * External hotel content source registry — candidate vendors + Dealality tiers.
 * Evaluation only until license + field approval + ENABLE_EXTERNAL_HOTEL_CONTENT_WRITES.
 */

export const EXTERNAL_HOTEL_SOURCE_REGISTRY_VERSION =
  "external-hotel-source-registry-v1";

/** Dealality evidence / license tiers for external content. */
export const EXTERNAL_SOURCE_TIER = Object.freeze({
  A_LICENSED_MASTER: "Tier_A_licensed_hotel_master_data",
  B_LICENSED_CONTENT_API: "Tier_B_licensed_travel_content_api",
  C_TOURISM_REGISTRY: "Tier_C_official_tourism_registry",
  D_PLACE_GEO: "Tier_D_place_geo_verification_api",
  E_PUBLIC_WEB: "Tier_E_public_web_verification",
});

/**
 * @typedef {'unknown'|'yes'|'partial'|'no'|'license_required'|'policy_required'} Capability
 */

/**
 * Canonical source IDs used by CLI --source=
 */
export const EXTERNAL_SOURCE_IDS = Object.freeze([
  "giata",
  "northstar",
  "expedia",
  "booking",
  "hotelbeds",
  "amadeus",
  "google_places",
  "openstreetmap",
  "tourism_registry",
  "cvent",
  "costar",
  "str",
  "hotel_brand_websites",
  "ota_consumer_sites",
]);

/**
 * Static evaluation matrix (no secrets). Update when commercial access changes.
 * @type {Record<string, object>}
 */
export const EXTERNAL_HOTEL_SOURCES = Object.freeze({
  giata: {
    id: "giata",
    name: "GIATA MultiCodes / Hotel Guide",
    tier: EXTERNAL_SOURCE_TIER.A_LICENSED_MASTER,
    pilot_order: 1,
    capabilities: {
      hotel_name: "yes",
      address: "yes",
      lat_long: "yes",
      phone: "partial",
      hotel_website: "partial",
      rooms: "partial",
      brand_chain: "yes",
      descriptions: "partial",
      api: "yes",
      bulk_export: "yes",
    },
    licensing_storage: "license_required",
    cost_friction: "high_commercial_negotiation",
    cala_coverage: "strong_global_incl_latam_chains",
    field_reliability: "high_for_identity_address_geo_chain",
    expected_match_quality: "high_with_giata_id",
    best_use_fields: [
      "Canonical Property Name",
      "Address",
      "Latitude",
      "Longitude",
      "Current Brand",
      "Brand Family",
      "Rooms / Keys",
      "Official Property URL",
    ],
    blocked_fields_until_license: ["*"],
    legal_notes:
      "Commercial master-data license required before any Census storage. Do not scrape GIATA web UI.",
    recommended_use:
      "Pilot 1 master-data fill for name/address/geo/chain/rooms when licensed field exists.",
    go_no_go: "conditional_go_after_license",
    adapter_status: "interface_stub",
  },
  northstar: {
    id: "northstar",
    name: "Northstar Travel Group / Travel Weekly Hotel Search",
    tier: EXTERNAL_SOURCE_TIER.A_LICENSED_MASTER,
    pilot_order: 1,
    capabilities: {
      hotel_name: "yes",
      address: "yes",
      lat_long: "partial",
      phone: "partial",
      hotel_website: "partial",
      rooms: "partial",
      brand_chain: "yes",
      descriptions: "partial",
      api: "unknown",
      bulk_export: "partial",
    },
    licensing_storage: "license_required",
    cost_friction: "high_commercial_negotiation",
    cala_coverage: "good_for_branded_and_meetings_hotels",
    field_reliability: "medium_high_editorial_plus_directory",
    expected_match_quality: "medium_high",
    best_use_fields: [
      "Canonical Property Name",
      "Address",
      "Current Brand",
      "Rooms / Keys",
      "Phone",
    ],
    blocked_fields_until_license: ["*"],
    legal_notes:
      "Directory/search products are not a free scrape target. Require Northstar data license / feed.",
    recommended_use: "Pilot alongside GIATA for master identity + rooms where licensed.",
    go_no_go: "conditional_go_after_license",
    adapter_status: "interface_stub",
  },
  expedia: {
    id: "expedia",
    name: "Expedia Rapid Content API",
    tier: EXTERNAL_SOURCE_TIER.B_LICENSED_CONTENT_API,
    pilot_order: 2,
    capabilities: {
      hotel_name: "yes",
      address: "yes",
      lat_long: "yes",
      phone: "yes",
      hotel_website: "partial",
      rooms: "partial",
      brand_chain: "yes",
      descriptions: "yes",
      api: "yes",
      bulk_export: "partial",
    },
    licensing_storage: "license_required",
    cost_friction: "medium_api_partner_access",
    cala_coverage: "strong_ota_inventory",
    field_reliability: "high_for_address_geo_content_medium_for_rooms_semantics",
    expected_match_quality: "high_with_expedia_property_id",
    best_use_fields: [
      "Address",
      "Latitude",
      "Longitude",
      "Phone",
      "Hotel Description - AI Summary",
      "Canonical Property Name",
    ],
    blocked_fields_until_license: ["*"],
    legal_notes:
      "Rapid Content ToS governs storage/display. Descriptions need license check. Rooms field meaning must be validated (hotel keys vs unit inventory).",
    recommended_use:
      "Pilot 2 content/address/geo/phone; rooms only if field meaning approved and labeled.",
    go_no_go: "conditional_go_after_api_credentials_and_tos_review",
    adapter_status: "interface_stub",
  },
  booking: {
    id: "booking",
    name: "Booking.com Demand API",
    tier: EXTERNAL_SOURCE_TIER.B_LICENSED_CONTENT_API,
    pilot_order: 2,
    capabilities: {
      hotel_name: "yes",
      address: "yes",
      lat_long: "yes",
      phone: "partial",
      hotel_website: "partial",
      rooms: "partial",
      brand_chain: "partial",
      descriptions: "yes",
      api: "yes",
      bulk_export: "partial",
    },
    licensing_storage: "license_required",
    cost_friction: "medium_high_partner_program",
    cala_coverage: "strong_ota_inventory",
    field_reliability: "high_for_address_geo_content",
    expected_match_quality: "high_with_booking_hotel_id",
    best_use_fields: ["Address", "Latitude", "Longitude", "Canonical Property Name"],
    blocked_fields_until_license: ["*"],
    legal_notes:
      "Demand API partnership required. Consumer site scrape is forbidden (see ota_consumer_sites).",
    recommended_use: "Licensed content/geo fill; never consumer scrape.",
    go_no_go: "conditional_go_after_partnership",
    adapter_status: "interface_stub",
  },
  hotelbeds: {
    id: "hotelbeds",
    name: "Hotelbeds / HBX Content API",
    tier: EXTERNAL_SOURCE_TIER.B_LICENSED_CONTENT_API,
    pilot_order: 2,
    capabilities: {
      hotel_name: "yes",
      address: "yes",
      lat_long: "yes",
      phone: "partial",
      hotel_website: "partial",
      rooms: "partial",
      brand_chain: "partial",
      descriptions: "yes",
      api: "yes",
      bulk_export: "yes",
    },
    licensing_storage: "license_required",
    cost_friction: "medium_api_partner_access",
    cala_coverage: "strong_latam_wholesale",
    field_reliability: "high_for_identity_address_geo",
    expected_match_quality: "high_with_hotelbeds_code",
    best_use_fields: ["Address", "Latitude", "Longitude", "Canonical Property Name", "Phone"],
    blocked_fields_until_license: ["*"],
    legal_notes: "HBX content license + credential required before Census storage.",
    recommended_use: "Strong LATAM content/geo pilot candidate after credentials.",
    go_no_go: "conditional_go_after_api_credentials",
    adapter_status: "interface_stub",
  },
  amadeus: {
    id: "amadeus",
    name: "Amadeus Hotel API",
    tier: EXTERNAL_SOURCE_TIER.B_LICENSED_CONTENT_API,
    pilot_order: 2,
    capabilities: {
      hotel_name: "yes",
      address: "yes",
      lat_long: "yes",
      phone: "partial",
      hotel_website: "partial",
      rooms: "partial",
      brand_chain: "yes",
      descriptions: "partial",
      api: "yes",
      bulk_export: "partial",
    },
    licensing_storage: "license_required",
    cost_friction: "medium_self_service_plus_enterprise",
    cala_coverage: "good_global_gds_biased",
    field_reliability: "medium_high",
    expected_match_quality: "medium_high",
    best_use_fields: ["Address", "Latitude", "Longitude", "Current Brand"],
    blocked_fields_until_license: ["*"],
    legal_notes: "Amadeus developer / enterprise terms apply to stored content.",
    recommended_use: "Secondary licensed content/geo when Expedia/Hotelbeds unavailable.",
    go_no_go: "conditional_go_after_tos_review",
    adapter_status: "interface_stub",
  },
  costar: {
    id: "costar",
    name: "STR / CoStar Census",
    tier: EXTERNAL_SOURCE_TIER.A_LICENSED_MASTER,
    pilot_order: 5,
    capabilities: {
      hotel_name: "yes",
      address: "yes",
      lat_long: "yes",
      phone: "partial",
      hotel_website: "partial",
      rooms: "yes",
      brand_chain: "yes",
      descriptions: "no",
      api: "partial",
      bulk_export: "yes",
    },
    licensing_storage: "license_required",
    cost_friction: "very_high_enterprise",
    cala_coverage: "strong_where_licensed",
    field_reliability: "very_high_for_census_rooms_identity",
    expected_match_quality: "high",
    best_use_fields: ["Rooms / Keys", "Address", "Canonical Property Name", "Current Brand"],
    blocked_fields_until_license: ["*"],
    legal_notes:
      "CoStar/STR data must NEVER be product-facing without license. Never expose CoStar in Brand Explorer / public UI. GTM use remains separate.",
    recommended_use:
      "Pilot 5 only after commercial license; Census internal enrichment only if contract allows.",
    go_no_go: "no_go_until_commercial_license_and_product_policy",
    adapter_status: "blocked_pending_license",
  },
  str: {
    id: "str",
    name: "STR (CoStar)",
    tier: EXTERNAL_SOURCE_TIER.A_LICENSED_MASTER,
    pilot_order: 5,
    alias_of: "costar",
    capabilities: {},
    licensing_storage: "license_required",
    recommended_use: "See costar entry.",
    go_no_go: "no_go_until_commercial_license_and_product_policy",
    adapter_status: "alias",
  },
  cvent: {
    id: "cvent",
    name: "Cvent Supplier Network",
    tier: EXTERNAL_SOURCE_TIER.A_LICENSED_MASTER,
    pilot_order: 5,
    capabilities: {
      hotel_name: "yes",
      address: "yes",
      lat_long: "partial",
      phone: "partial",
      hotel_website: "partial",
      rooms: "yes",
      brand_chain: "yes",
      descriptions: "partial",
      api: "partial",
      bulk_export: "partial",
    },
    licensing_storage: "license_required",
    cost_friction: "high_enterprise",
    cala_coverage: "medium_meetings_focused",
    field_reliability: "high_for_meeting_hotels_rooms",
    expected_match_quality: "medium_high",
    best_use_fields: ["Rooms / Keys", "Address", "Phone", "Canonical Property Name"],
    blocked_fields_until_license: ["*"],
    legal_notes: "Cvent supplier data requires commercial access; meeting-room counts must not be written as hotel keys.",
    recommended_use: "Rooms only when guest-room inventory is explicit; never meeting rooms as Rooms / Keys.",
    go_no_go: "conditional_go_after_license",
    adapter_status: "interface_stub",
  },
  tourism_registry: {
    id: "tourism_registry",
    name: "Official tourism registries (country adapters)",
    tier: EXTERNAL_SOURCE_TIER.C_TOURISM_REGISTRY,
    pilot_order: 4,
    capabilities: {
      hotel_name: "yes",
      address: "partial",
      lat_long: "no",
      phone: "partial",
      hotel_website: "partial",
      rooms: "yes",
      brand_chain: "partial",
      descriptions: "no",
      api: "partial",
      bulk_export: "partial",
    },
    licensing_storage: "often_open_government_cite_source",
    cost_friction: "low_adapter_engineering",
    cala_coverage: "country_specific_colombia_rnt_live_others_discovery",
    field_reliability: "medium_for_rooms_identity_varies_by_country",
    expected_match_quality: "medium_requires_strict_name_city_match",
    best_use_fields: ["Rooms / Keys", "Address", "Phone"],
    blocked_fields_until_license: [],
    legal_notes:
      "Colombia RNT already adapted. Mexico SECTUR/DATATUR aggregate ≠ property keys. DR MITUR listing lacks habitaciones columns in public table.",
    recommended_use: "Pilot 4 country-by-country rooms/address; continue Colombia; build Mexico property-level adapter.",
    go_no_go: "go_for_existing_adapters_expand_country_by_country",
    adapter_status: "partial_live_colombia_rnt",
  },
  google_places: {
    id: "google_places",
    name: "Google Places API",
    tier: EXTERNAL_SOURCE_TIER.D_PLACE_GEO,
    pilot_order: 3,
    capabilities: {
      hotel_name: "yes",
      address: "yes",
      lat_long: "yes",
      phone: "yes",
      hotel_website: "yes",
      rooms: "no",
      brand_chain: "no",
      descriptions: "partial",
      api: "yes",
      bulk_export: "no",
    },
    licensing_storage: "policy_required_places_tos_storage",
    cost_friction: "medium_per_request",
    cala_coverage: "very_high",
    field_reliability: "high_for_address_phone_website_geo",
    expected_match_quality: "high_with_place_id",
    best_use_fields: ["Address", "Latitude", "Longitude", "Phone", "Official Property URL"],
    blocked_fields_until_license: ["Address", "Latitude", "Longitude", "Phone", "Official Property URL"],
    legal_notes:
      "Do not store Places content without Places API ToS / storage review (GOOGLE_GEOCODE_STORAGE_TERMS_REVIEWED pattern). Never use for Rooms / Keys.",
    recommended_use: "Pilot 3 validation only after storage policy approval. Rooms forever blocked.",
    go_no_go: "no_go_until_storage_policy_approved",
    adapter_status: "blocked_pending_policy",
  },
  openstreetmap: {
    id: "openstreetmap",
    name: "OpenStreetMap",
    tier: EXTERNAL_SOURCE_TIER.D_PLACE_GEO,
    pilot_order: 3,
    capabilities: {
      hotel_name: "yes",
      address: "partial",
      lat_long: "yes",
      phone: "partial",
      hotel_website: "partial",
      rooms: "no",
      brand_chain: "no",
      descriptions: "no",
      api: "yes",
      bulk_export: "yes",
    },
    licensing_storage: "policy_required_odbl_compliance",
    cost_friction: "low",
    cala_coverage: "variable_urban_bias",
    field_reliability: "medium",
    expected_match_quality: "medium",
    best_use_fields: ["Latitude", "Longitude", "Address"],
    blocked_fields_until_license: ["Address", "Latitude", "Longitude", "Phone"],
    legal_notes: "ODbL share-alike / attribution obligations must be approved before Census writes.",
    recommended_use: "Geo verification only after ODbL policy. Never rooms.",
    go_no_go: "no_go_until_odbl_policy_approved",
    adapter_status: "blocked_pending_policy",
  },
  hotel_brand_websites: {
    id: "hotel_brand_websites",
    name: "Hotel / brand websites (official)",
    tier: EXTERNAL_SOURCE_TIER.E_PUBLIC_WEB,
    pilot_order: 0,
    capabilities: {
      hotel_name: "yes",
      address: "yes",
      lat_long: "partial",
      phone: "yes",
      hotel_website: "yes",
      rooms: "yes",
      brand_chain: "yes",
      descriptions: "yes",
      api: "partial",
      bulk_export: "no",
    },
    licensing_storage: "public_pages_respect_tos_no_bulk_abuse",
    cost_friction: "low_engineering_bot_blocks_high",
    cala_coverage: "strong_for_chained_brands",
    field_reliability: "highest_when_fetch_succeeds",
    expected_match_quality: "high_via_official_url",
    best_use_fields: [
      "Rooms / Keys",
      "Phone",
      "Address",
      "Official Property URL",
      "Hotel Description - AI Summary",
    ],
    blocked_fields_until_license: [],
    legal_notes:
      "Primary Autopilot path. Runtime often 403/Akamai. No owner/operator/date extraction writes.",
    recommended_use: "Continue as Tier E primary; pair with licensed master data for scale.",
    go_no_go: "go_already_in_production_paths",
    adapter_status: "live_partial_bot_blocked",
  },
  ota_consumer_sites: {
    id: "ota_consumer_sites",
    name: "Booking / Expedia consumer sites (scrape)",
    tier: EXTERNAL_SOURCE_TIER.E_PUBLIC_WEB,
    pilot_order: 99,
    capabilities: {
      hotel_name: "yes",
      address: "yes",
      lat_long: "partial",
      phone: "partial",
      hotel_website: "no",
      rooms: "partial",
      brand_chain: "partial",
      descriptions: "yes",
      api: "no",
      bulk_export: "no",
    },
    licensing_storage: "forbidden_scrape",
    cost_friction: "n_a",
    cala_coverage: "high_but_forbidden",
    field_reliability: "n_a",
    expected_match_quality: "n_a",
    best_use_fields: [],
    blocked_fields_until_license: ["*"],
    legal_notes: "Consumer OTA scrape is blocked. Use licensed APIs (expedia/booking) instead.",
    recommended_use: "Do not use. Always no-go.",
    go_no_go: "no_go_hard_block",
    adapter_status: "hard_blocked",
  },
});

/**
 * @param {string} sourceId
 */
export function resolveExternalSource(sourceId) {
  const id = String(sourceId || "")
    .trim()
    .toLowerCase()
    .replace(/-/g, "_");
  const aliases = {
    northstar_travel: "northstar",
    travel_weekly: "northstar",
    expedia_rapid: "expedia",
    booking_com: "booking",
    booking_demand: "booking",
    hbx: "hotelbeds",
    google: "google_places",
    places: "google_places",
    osm: "openstreetmap",
    tourism: "tourism_registry",
    colombia_rnt: "tourism_registry",
    str_costar: "costar",
    official_websites: "hotel_brand_websites",
    brand_websites: "hotel_brand_websites",
  };
  const resolved = aliases[id] || id;
  const src = EXTERNAL_HOTEL_SOURCES[resolved];
  if (!src) return null;
  if (src.alias_of) {
    return { ...EXTERNAL_HOTEL_SOURCES[src.alias_of], id: resolved, requested_id: id };
  }
  return { ...src, requested_id: id };
}

/**
 * Pilot order list (unique, excludes hard-blocked consumer scrape as pilot).
 */
export function listPilotSources() {
  return Object.values(EXTERNAL_HOTEL_SOURCES)
    .filter((s) => !s.alias_of && s.id !== "ota_consumer_sites")
    .sort((a, b) => (a.pilot_order ?? 99) - (b.pilot_order ?? 99));
}

/**
 * Adapter interface contract — every source adapter should implement these methods.
 */
export const EXTERNAL_SOURCE_ADAPTER_INTERFACE = Object.freeze([
  "searchByNameCityCountry",
  "searchByCoordinates",
  "lookupByExternalId",
  "lookupByOfficialUrl",
  "normalizeProperty",
  "proposeFieldUpdates",
  "validateMatch",
  "classifyLicense",
  "classifyEvidenceTier",
]);

/**
 * Stub adapter factory — returns not_configured until credentials/license land.
 * @param {string} sourceId
 */
export function createExternalSourceAdapterStub(sourceId) {
  const source = resolveExternalSource(sourceId);
  const base = {
    source_id: source?.id || sourceId,
    source,
    adapter_status: source?.adapter_status || "unknown",
  };
  const notConfigured = async (method) => ({
    ok: false,
    reason: "adapter_not_configured",
    method,
    source_id: base.source_id,
    adapter_status: base.adapter_status,
    results: [],
  });

  return {
    ...base,
    searchByNameCityCountry: () => notConfigured("searchByNameCityCountry"),
    searchByCoordinates: () => notConfigured("searchByCoordinates"),
    lookupByExternalId: () => notConfigured("lookupByExternalId"),
    lookupByOfficialUrl: () => notConfigured("lookupByOfficialUrl"),
    normalizeProperty: (raw) => ({
      ok: false,
      reason: "adapter_not_configured",
      normalized: raw || null,
    }),
    proposeFieldUpdates: () => ({
      ok: false,
      reason: "adapter_not_configured",
      proposals: [],
    }),
    validateMatch: () => ({ ok: false, reason: "adapter_not_configured" }),
    classifyLicense: () => source?.licensing_storage || "unknown",
    classifyEvidenceTier: () => source?.tier || null,
  };
}
