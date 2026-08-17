/**
 * Country rooms secondary-source discovery (Wave 2).
 * Documents governed sources per priority country — does not invent room counts.
 */

export const ROOMS_COUNTRY_DISCOVERY_VERSION =
  "census-rooms-country-source-discovery-v1";

export const ROOMS_COUNTRY_PRIORITY = Object.freeze([
  "Mexico",
  "Dominican Republic",
  "Panama",
  "Costa Rica",
  "Colombia",
  "Peru",
  "Chile",
  "Argentina",
  "Brazil",
  "Caribbean",
]);

/**
 * Static discovery matrix (repeatable adapters vs portal-only / aggregate-only).
 * Update when a deterministic property-level rooms adapter is built.
 */
export const COUNTRY_ROOMS_SOURCE_MATRIX = Object.freeze({
  Mexico: {
    priority: 1,
    sources: [
      {
        name: "SECTUR Registro Nacional de Turismo (consulta portal)",
        url: "https://rnt-consulta.sectur.gob.mx/",
        category: "tourism_board_convention_bureau_destination_authority",
        property_level_rooms: "unknown_portal_consulta_not_bulk_api",
        adapter_status: "discovery_only",
        notes:
          "Official consulta portal; no stable bulk JSON/CSV with habitaciones for Autopilot yet. Do not scrape Google mirrors (rntsecturgob.com deprecated).",
      },
      {
        name: "DATATUR hotel monitoring / open stats",
        url: "https://www.datatur.sectur.gob.mx/SitePages/hoteleria.aspx",
        category: "tourism_board_convention_bureau_destination_authority",
        property_level_rooms: false,
        adapter_status: "blocked_aggregate_only",
        notes: "Occupancy / cuartos at destination aggregate — not property keys.",
      },
      {
        name: "Official parent property pages (Marriott/IHG/Hilton/Accor/Choice)",
        category: "official_parent_brand_source",
        property_level_rooms: true,
        adapter_status: "bot_blocked_from_runtime",
        notes: "403/Akamai common from Autopilot runtime; keep as Tier 1 when fetch succeeds.",
      },
    ],
  },
  "Dominican Republic": {
    priority: 2,
    sources: [
      {
        name: "MITUR Registro Nacional Turístico",
        url: "https://rnt.mitur.gob.do/",
        category: "tourism_board_convention_bureau_destination_authority",
        property_level_rooms: false,
        adapter_status: "discovery_only",
        notes:
          "Listing shows identity/status/phone/email columns; habitaciones not exposed in public table columns reviewed.",
      },
      {
        name: "SITUR hotel industry dashboards / Excel",
        url: "https://situr.mitur.gob.do/estadisticas/",
        category: "tourism_board_convention_bureau_destination_authority",
        property_level_rooms: false,
        adapter_status: "blocked_aggregate_only",
        notes: "Zone/destination occupancy — not property-level keys.",
      },
      {
        name: "ASONAHORES / association directories",
        category: "reputable_hospitality_trade_publication",
        property_level_rooms: "unknown",
        adapter_status: "discovery_only",
        notes: "Needs steward review before treating as Tier 6 SoT.",
      },
    ],
  },
  Panama: {
    priority: 3,
    sources: [
      {
        name: "Visit Panama / ATP destination directories",
        url: "https://www.visitpanama.com/",
        category: "tourism_board_convention_bureau_destination_authority",
        property_level_rooms: false,
        adapter_status: "discovery_only",
        notes: "Destination marketing pages; property keys not systematically published.",
      },
      {
        name: "Official parent property pages",
        category: "official_parent_brand_source",
        property_level_rooms: true,
        adapter_status: "bot_blocked_from_runtime",
      },
    ],
  },
  "Costa Rica": {
    priority: 4,
    sources: [
      {
        name: "ICT tourism / lodging registry (investigation)",
        category: "tourism_board_convention_bureau_destination_authority",
        property_level_rooms: "unknown",
        adapter_status: "discovery_only",
        notes: "Confirm ICT open dataset with habitaciones before adapter; do not invent.",
      },
      {
        name: "Official parent property pages",
        category: "official_parent_brand_source",
        property_level_rooms: true,
        adapter_status: "bot_blocked_from_runtime",
      },
    ],
  },
  Colombia: {
    priority: 5,
    sources: [
      {
        name: "MinCIT RNT open data (datos.gov.co thwd-ivmp)",
        url: "https://www.datos.gov.co/Comercio-Industria-y-Turismo/Registro-Nacional-de-Turismo-RNT/thwd-ivmp",
        category: "tourism_board_convention_bureau_destination_authority",
        property_level_rooms: true,
        adapter_status: "adapter_live",
        notes: "Wave 1 wrote 58; Wave 2 fuzzy steward for remaining ambiguous matches.",
      },
    ],
  },
  Peru: {
    priority: 6,
    sources: [
      {
        name: "MINCETUR establecimientos hospedaje calificados CSV",
        url: "https://www.mincetur.gob.pe/Datos_abiertos/DGPDT/Establecimientos_hospedajes_calificados.csv",
        category: "tourism_board_convention_bureau_destination_authority",
        property_level_rooms: true,
        adapter_status: "adapter_ready_enrich_existing_only",
        notes: "Field-completion only when Peru rows exist in Hotel Property Census.",
      },
    ],
  },
  Chile: { priority: 7, sources: [], notes: "No CALA census blank rooms in current 1,224 slice." },
  Argentina: { priority: 8, sources: [], notes: "No CALA census blank rooms in current 1,224 slice." },
  Brazil: { priority: 9, sources: [], notes: "No CALA census blank rooms in current 1,224 slice." },
  Caribbean: {
    priority: 10,
    sources: [],
    notes: "Covered via Dominican Republic / other island markets when present.",
  },
});

/**
 * @param {string} country
 * @param {{ missing_rooms?: number, with_official_url?: number, bot_blocked_samples?: number }} [stats]
 */
export function buildCountryDiscoveryEntry(country, stats = {}) {
  const matrix = COUNTRY_ROOMS_SOURCE_MATRIX[country] || {
    priority: 99,
    sources: [],
    notes: "unlisted",
  };
  const sources = matrix.sources || [];
  const live = sources.filter((s) => s.adapter_status === "adapter_live").length;
  const blocked = sources.filter((s) =>
    String(s.adapter_status || "").includes("blocked")
  ).length;
  const discovery = sources.filter((s) => s.adapter_status === "discovery_only").length;
  return {
    country,
    priority: matrix.priority,
    missing_rooms: stats.missing_rooms ?? null,
    with_official_url: stats.with_official_url ?? null,
    bot_blocked_samples: stats.bot_blocked_samples ?? null,
    sources,
    summary: {
      adapter_live: live,
      discovery_only: discovery,
      blocked_or_aggregate: blocked,
      notes: matrix.notes || null,
    },
    next_action:
      live > 0
        ? "continue_adapter_enrichment_with_strict_match"
        : discovery > 0
          ? "build_deterministic_property_level_adapter_or_steward_pack"
          : "official_parent_fetch_when_unblocked",
  };
}

/**
 * @param {Record<string, { missing_rooms?: number, with_official_url?: number, bot_blocked_samples?: number }>} [byCountry]
 */
export function buildRoomsCountryDiscoveryReport(byCountry = {}) {
  const countries = ROOMS_COUNTRY_PRIORITY.map((c) =>
    buildCountryDiscoveryEntry(c, byCountry[c] || {})
  );
  return {
    version: ROOMS_COUNTRY_DISCOVERY_VERSION,
    generated_at: new Date().toISOString(),
    priority_order: ROOMS_COUNTRY_PRIORITY.slice(),
    countries,
  };
}
