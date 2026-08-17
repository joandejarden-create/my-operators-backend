/**
 * CALA Official Lodging / Rooms Source Registry v1
 *
 * Durable 52-geography matrix of official tourism/lodging registries for
 * Property Fundamentals Rooms / Keys expansion.
 *
 * Tiers:
 *   TIER_A_BULK_ROOMS_HIGH — property-level rooms, bulk/open, HIGH-eligible
 *   TIER_B_USEFUL_FUNDAMENTALS — identity + some capacity/contact, not clean bulk rooms
 *   TIER_C_IDENTITY_ONLY — identity/contact useful; no usable property rooms
 *   TIER_D_AGGREGATE_ONLY — rooms only at destination/region aggregate
 *   UNUSABLE — no usable public property inventory
 *   USAGE_REVIEW — exists but access/terms/quality need steward review
 */
import { listDealalityCalaGeographies } from "./dealality-cala-geography-registry-v1.js";

export const OFFICIAL_ROOMS_SOURCE_REGISTRY_VERSION =
  "cala-official-rooms-source-registry-v1";

export const SOURCE_TIER = Object.freeze({
  TIER_A_BULK_ROOMS_HIGH: "TIER_A_BULK_ROOMS_HIGH",
  TIER_B_USEFUL_FUNDAMENTALS: "TIER_B_USEFUL_FUNDAMENTALS",
  TIER_C_IDENTITY_ONLY: "TIER_C_IDENTITY_ONLY",
  TIER_D_AGGREGATE_ONLY: "TIER_D_AGGREGATE_ONLY",
  UNUSABLE: "UNUSABLE",
  USAGE_REVIEW: "USAGE_REVIEW",
});

/**
 * Room-field semantic codes used by adapters.
 */
export const ROOM_FIELD_SEMANTICS = Object.freeze({
  HABITACIONES: "habitaciones_hotel_rooms",
  UNIDADE_HABITACIONAIS: "unidade_habitacionais_uh_rooms",
  NUMBER_OF_BEDROOMS: "number_of_bedrooms_directory",
  HABI: "habi_habitaciones",
  LEITOS_BEDS: "leitos_beds_NOT_rooms",
  CAMAS_BEDS: "camas_beds_NOT_rooms",
  PLAZAS_CAPACITY: "plazas_capacity_NOT_rooms",
  AGGREGATE_ONLY: "aggregate_destination_inventory",
  UNKNOWN: "unknown",
  ABSENT: "absent",
});

/**
 * Canonical source entries keyed by source_id.
 * One geography may have multiple sources.
 * @type {Record<string, object>}
 */
export const OFFICIAL_ROOMS_SOURCES = Object.freeze({
  colombia_rnt: {
    source_id: "colombia_rnt",
    geography: "Colombia",
    geography_id: "colombia",
    source_name: "Registro Nacional de Turismo (RNT)",
    source_authority: "MinCIT / datos.gov.co",
    official_domain: "datos.gov.co",
    source_type: "national_tourism_registry_open_data",
    publicly_accessible: true,
    requires_login: false,
    api_available: true,
    bulk_download_available: true,
    searchable_directory: true,
    property_name: true,
    address: false,
    postal_code: false,
    city: true,
    state_region: true,
    phone: false,
    website: false,
    room_count_available: true,
    room_field_name: "habitaciones",
    room_field_definition: ROOM_FIELD_SEMANTICS.HABITACIONES,
    property_type_available: true,
    registry_id_available: true,
    last_updated: "2026-live",
    usage_terms_located: true,
    storage_reuse_status: "approved_open_data",
    technical_accessibility: "adapter_live",
    expected_match_yield: "high",
    recommended_action: "continue_high_match_null_fill; hold medium candidates",
    tier: SOURCE_TIER.TIER_A_BULK_ROOMS_HIGH,
    adapter_module: "colombia-rnt-open-data-adapter.js",
    urls: {
      dataset:
        "https://www.datos.gov.co/Comercio-Industria-y-Turismo/Registro-Nacional-de-Turismo-RNT/thwd-ivmp",
      api: "https://www.datos.gov.co/resource/thwd-ivmp.json",
    },
  },
  peru_mincetur_hospedaje: {
    source_id: "peru_mincetur_hospedaje",
    geography: "Peru",
    geography_id: "peru",
    source_name: "Establecimientos de Hospedaje Calificados",
    source_authority: "MINCETUR",
    official_domain: "mincetur.gob.pe",
    source_type: "national_lodging_classification_open_csv",
    publicly_accessible: true,
    requires_login: false,
    api_available: false,
    bulk_download_available: true,
    searchable_directory: false,
    property_name: true,
    address: true,
    postal_code: false,
    city: true,
    state_region: true,
    phone: true,
    website: true,
    room_count_available: true,
    room_field_name: "HABI",
    room_field_definition: ROOM_FIELD_SEMANTICS.HABI,
    property_type_available: true,
    registry_id_available: true,
    last_updated: "2026-08-15",
    usage_terms_located: true,
    storage_reuse_status: "approved_open_data",
    technical_accessibility: "adapter_ready",
    expected_match_yield: "high",
    recommended_action: "integrate_enrich_existing_census_null_fill",
    tier: SOURCE_TIER.TIER_A_BULK_ROOMS_HIGH,
    adapter_module: "peru-mincetur-open-data-adapter.js",
    urls: {
      csv: "https://www.mincetur.gob.pe/Datos_abiertos/DGPDT/Establecimientos_hospedajes_calificados.csv",
      catalog:
        "https://www.datosabiertos.gob.pe/dataset/directorio-nacional-de-prestadores-de-servicios-turisticos-calificados",
    },
    notes: "CAMA = beds — never write as Rooms / Keys.",
  },
  brazil_cadastur_meios: {
    source_id: "brazil_cadastur_meios",
    geography: "Brazil",
    geography_id: "brazil",
    source_name: "CADASTUR Meios de Hospedagem (open data XLSX)",
    source_authority: "Ministério do Turismo / dados.turismo.gov.br",
    official_domain: "dados.turismo.gov.br",
    source_type: "national_tourism_provider_open_data",
    publicly_accessible: true,
    requires_login: false,
    api_available: "partial_conecta_auth_required",
    bulk_download_available: true,
    searchable_directory: true,
    property_name: true,
    address: true,
    postal_code: true,
    city: true,
    state_region: true,
    phone: true,
    website: true,
    room_count_available: true,
    room_field_name: "Unidade Habitacionais",
    room_field_definition: ROOM_FIELD_SEMANTICS.UNIDADE_HABITACIONAIS,
    property_type_available: true,
    registry_id_available: true,
    last_updated: "2026-Q1",
    usage_terms_located: true,
    storage_reuse_status: "approved_open_data",
    technical_accessibility: "adapter_new",
    expected_match_yield: "very_high",
    recommended_action: "integrate_immediately_uh_only_not_leitos",
    tier: SOURCE_TIER.TIER_A_BULK_ROOMS_HIGH,
    adapter_module: "brazil-cadastur-open-data-adapter.js",
    urls: {
      dataset: "https://dados.turismo.gov.br/dataset/meios-de-hospedagem",
      xlsx_q1_2026:
        "https://dados.turismo.gov.br/dataset/d2333d1b-db1e-438b-955a-028db80a031e/resource/938cb620-7252-4cd0-9def-443dd2fe3f3b/download/meio-de-hospedagem-1-trimestre-2026.xlsx",
      portal: "https://cadastur.turismo.gov.br/",
    },
    notes: "Leitos = beds — NEVER write as Rooms / Keys. Prefer UH for Hotel/Resort/Flat.",
  },
  barbados_btpa_directory: {
    source_id: "barbados_btpa_directory",
    geography: "Barbados",
    geography_id: "barbados",
    source_name: "BTPA Registered Tourist Accommodation Directory",
    source_authority: "Barbados Tourism Product Authority",
    official_domain: "barbadostouristaccommodation.com",
    source_type: "official_licensed_accommodation_directory",
    publicly_accessible: true,
    requires_login: false,
    api_available: false,
    bulk_download_available: false,
    searchable_directory: true,
    property_name: true,
    address: false,
    postal_code: false,
    city: false,
    state_region: false,
    phone: false,
    website: false,
    room_count_available: true,
    room_field_name: "Number of Bedrooms",
    room_field_definition: ROOM_FIELD_SEMANTICS.NUMBER_OF_BEDROOMS,
    property_type_available: true,
    registry_id_available: false,
    last_updated: "2026-01",
    usage_terms_located: true,
    storage_reuse_status: "public_directory_ok",
    technical_accessibility: "html_directory_parse",
    expected_match_yield: "medium_high",
    recommended_action: "integrate_hotels_category_only",
    tier: SOURCE_TIER.TIER_A_BULK_ROOMS_HIGH,
    adapter_module: "barbados-btpa-directory-adapter.js",
    urls: {
      directory: "https://www.barbadostouristaccommodation.com/directory",
      portal: "https://www.barbadostouristaccommodation.com/",
    },
  },
  chile_sernatur_buscador: {
    source_id: "chile_sernatur_buscador",
    geography: "Chile",
    geography_id: "chile",
    source_name: "SERNATUR Registro / Buscador Prestadores",
    source_authority: "SERNATUR",
    official_domain: "sernatur.cl",
    source_type: "national_tourism_provider_directory",
    publicly_accessible: true,
    requires_login: false,
    api_available: false,
    bulk_download_available: "stale_legacy_csv_only",
    searchable_directory: true,
    property_name: true,
    address: true,
    postal_code: false,
    city: true,
    state_region: true,
    phone: true,
    website: true,
    room_count_available: false,
    room_field_name: null,
    room_field_definition: ROOM_FIELD_SEMANTICS.ABSENT,
    property_type_available: true,
    registry_id_available: true,
    last_updated: "unknown_current",
    usage_terms_located: true,
    storage_reuse_status: "public_ok_identity",
    technical_accessibility: "portal_search",
    expected_match_yield: "low_for_rooms",
    recommended_action: "identity_fundamentals_only_until_rooms_export",
    tier: SOURCE_TIER.TIER_C_IDENTITY_ONLY,
    urls: {
      buscador: "https://serviciosturisticos.sernatur.cl/",
      legacy_csv:
        "http://datos.gob.cl/uploads/recursos/RegistroAlojamientosTuristicos.csv",
    },
  },
  chile_siet_stats: {
    source_id: "chile_siet_stats",
    geography: "Chile",
    geography_id: "chile",
    source_name: "SERNATUR SIET / SIIT lodging statistics",
    source_authority: "SERNATUR",
    official_domain: "datosturismo.sernatur.cl",
    source_type: "statistical_lodging_directory_aggregate",
    publicly_accessible: true,
    requires_login: false,
    api_available: false,
    bulk_download_available: true,
    searchable_directory: false,
    property_name: false,
    address: false,
    postal_code: false,
    city: false,
    state_region: true,
    phone: false,
    website: false,
    room_count_available: false,
    room_field_name: "habitaciones/plazas/camas (aggregate)",
    room_field_definition: ROOM_FIELD_SEMANTICS.AGGREGATE_ONLY,
    property_type_available: true,
    registry_id_available: false,
    last_updated: "rolling",
    usage_terms_located: true,
    storage_reuse_status: "stats_only",
    technical_accessibility: "reports",
    expected_match_yield: "none_property",
    recommended_action: "do_not_use_for_rooms_keys",
    tier: SOURCE_TIER.TIER_D_AGGREGATE_ONLY,
    urls: {
      siet: "https://datosturismo.sernatur.cl/siet/reporteDinamicoEMAT",
    },
  },
  dominican_mitur_rnt: {
    source_id: "dominican_mitur_rnt",
    geography: "Dominican Republic",
    geography_id: "dominican_republic",
    source_name: "MITUR Registro Nacional Turístico",
    source_authority: "MITUR",
    official_domain: "mitur.gob.do",
    source_type: "national_tourism_registry_consulta",
    publicly_accessible: true,
    requires_login: false,
    api_available: false,
    bulk_download_available: false,
    searchable_directory: true,
    property_name: true,
    address: false,
    postal_code: false,
    city: false,
    state_region: true,
    phone: true,
    website: false,
    room_count_available: false,
    room_field_name: null,
    room_field_definition: ROOM_FIELD_SEMANTICS.ABSENT,
    property_type_available: true,
    registry_id_available: true,
    last_updated: "live_portal",
    usage_terms_located: true,
    storage_reuse_status: "public_consulta",
    technical_accessibility: "interactive_only",
    expected_match_yield: "identity_only",
    recommended_action: "identity_phone_lane_if_needed; no_rooms",
    tier: SOURCE_TIER.TIER_C_IDENTITY_ONLY,
    urls: { consulta: "https://rnt.mitur.gob.do/" },
  },
  mexico_sectur_rnt: {
    source_id: "mexico_sectur_rnt",
    geography: "Mexico",
    geography_id: "mexico",
    source_name: "SECTUR Registro Nacional de Turismo (consulta)",
    source_authority: "SECTUR",
    official_domain: "sectur.gob.mx",
    source_type: "national_tourism_registry_consulta",
    publicly_accessible: true,
    requires_login: false,
    api_available: false,
    bulk_download_available: false,
    searchable_directory: true,
    property_name: true,
    address: true,
    postal_code: false,
    city: true,
    state_region: true,
    phone: false,
    website: true,
    room_count_available: false,
    room_field_name: null,
    room_field_definition: ROOM_FIELD_SEMANTICS.UNKNOWN,
    property_type_available: true,
    registry_id_available: true,
    last_updated: "live_portal",
    usage_terms_located: true,
    storage_reuse_status: "public_consulta",
    technical_accessibility: "portal_not_bulk",
    expected_match_yield: "identity_only_until_bulk",
    recommended_action: "monitor_for_bulk_habitaciones_export",
    tier: SOURCE_TIER.TIER_C_IDENTITY_ONLY,
    urls: {
      rnt: "https://rnt.sectur.gob.mx/",
      consulta: "https://rnt-consulta.sectur.gob.mx/",
    },
  },
  mexico_datatur: {
    source_id: "mexico_datatur",
    geography: "Mexico",
    geography_id: "mexico",
    source_name: "DataTur hotel monitoring / occupancy",
    source_authority: "SECTUR DataTur",
    official_domain: "datatur.sectur.gob.mx",
    source_type: "statistical_lodging_aggregate",
    publicly_accessible: true,
    requires_login: false,
    api_available: false,
    bulk_download_available: true,
    searchable_directory: false,
    property_name: false,
    address: false,
    postal_code: false,
    city: false,
    state_region: true,
    phone: false,
    website: false,
    room_count_available: false,
    room_field_name: "cuartos disponibles (destino)",
    room_field_definition: ROOM_FIELD_SEMANTICS.AGGREGATE_ONLY,
    property_type_available: false,
    registry_id_available: false,
    last_updated: "rolling",
    usage_terms_located: true,
    storage_reuse_status: "stats_only",
    technical_accessibility: "reports",
    expected_match_yield: "none_property",
    recommended_action: "never_write_destination_cuartos_as_property_rooms",
    tier: SOURCE_TIER.TIER_D_AGGREGATE_ONLY,
    urls: {
      hoteleria: "https://www.datatur.sectur.gob.mx/SitePages/hoteleria.aspx",
    },
  },
  costa_rica_ict_oferta: {
    source_id: "costa_rica_ict_oferta",
    geography: "Costa Rica",
    geography_id: "costa_rica",
    source_name: "ICT oferta de hospedaje / stats",
    source_authority: "Instituto Costarricense de Turismo",
    official_domain: "ict.go.cr",
    source_type: "statistical_lodging_aggregate",
    publicly_accessible: true,
    requires_login: false,
    api_available: false,
    bulk_download_available: "pdf_series",
    searchable_directory: false,
    property_name: false,
    address: false,
    postal_code: false,
    city: false,
    state_region: false,
    phone: false,
    website: false,
    room_count_available: false,
    room_field_name: "national supply series",
    room_field_definition: ROOM_FIELD_SEMANTICS.AGGREGATE_ONLY,
    property_type_available: true,
    registry_id_available: false,
    last_updated: "periodic",
    usage_terms_located: true,
    storage_reuse_status: "stats_only",
    technical_accessibility: "pdf",
    expected_match_yield: "none",
    recommended_action: "fallback_to_property_page_research",
    tier: SOURCE_TIER.TIER_D_AGGREGATE_ONLY,
    urls: {
      oferta:
        "https://www.ict.go.cr/es/administrar-estadistica/cifras-turisticas/oferta-de-hospedaje.html",
    },
  },
  panama_atp_rnt: {
    source_id: "panama_atp_rnt",
    geography: "Panama",
    geography_id: "panama",
    source_name: "ATP Registro Nacional de Turismo / Hospedaje",
    source_authority: "Autoridad de Turismo de Panamá",
    official_domain: "atp.gob.pa",
    source_type: "licensing_registry",
    publicly_accessible: false,
    requires_login: true,
    api_available: false,
    bulk_download_available: false,
    searchable_directory: false,
    property_name: false,
    address: false,
    postal_code: false,
    city: false,
    state_region: false,
    phone: false,
    website: false,
    room_count_available: false,
    room_field_name: "relación de habitaciones (licensing internal)",
    room_field_definition: ROOM_FIELD_SEMANTICS.UNKNOWN,
    property_type_available: false,
    registry_id_available: false,
    last_updated: "unknown",
    usage_terms_located: false,
    storage_reuse_status: "not_public_bulk",
    technical_accessibility: "operator_filing_only",
    expected_match_yield: "none",
    recommended_action: "no_public_rooms_source",
    tier: SOURCE_TIER.UNUSABLE,
    urls: {
      rnt: "https://www.atp.gob.pa/asistencia-y-licencias/registro-nacional-de-turismo/",
    },
  },
  argentina_puna: {
    source_id: "argentina_puna",
    geography: "Argentina",
    geography_id: "argentina",
    source_name: "PUNA — Padrón Único Nacional de Alojamiento",
    source_authority: "Ministerio de Turismo y Deportes / YVERA",
    official_domain: "datos.yvera.gob.ar",
    source_type: "statistical_lodging_aggregate",
    publicly_accessible: true,
    requires_login: false,
    api_available: true,
    bulk_download_available: true,
    searchable_directory: false,
    property_name: false,
    address: false,
    postal_code: false,
    city: true,
    state_region: true,
    phone: false,
    website: false,
    room_count_available: false,
    room_field_name: "habitaciones/plazas by locality/category",
    room_field_definition: ROOM_FIELD_SEMANTICS.AGGREGATE_ONLY,
    property_type_available: true,
    registry_id_available: false,
    last_updated: "periodic",
    usage_terms_located: true,
    storage_reuse_status: "open_stats",
    technical_accessibility: "csv_aggregate",
    expected_match_yield: "none_property",
    recommended_action: "do_not_use_for_property_rooms",
    tier: SOURCE_TIER.TIER_D_AGGREGATE_ONLY,
    urls: {
      dataset: "https://datos.yvera.gob.ar/dataset/padron-unico-nacional-alojamiento",
    },
  },
  uruguay_mintur_alojamientos: {
    source_id: "uruguay_mintur_alojamientos",
    geography: "Uruguay",
    geography_id: "uruguay",
    source_name: "Registro de operadores turísticos — alojamientos",
    source_authority: "Ministerio de Turismo",
    official_domain: "catalogodatos.gub.uy",
    source_type: "national_tourism_operator_registry",
    publicly_accessible: true,
    requires_login: false,
    api_available: true,
    bulk_download_available: true,
    searchable_directory: false,
    property_name: true,
    address: true,
    postal_code: false,
    city: true,
    state_region: true,
    phone: true,
    website: true,
    room_count_available: false,
    room_field_name: "Habitaciones (schema empty in 2020 extract)",
    room_field_definition: ROOM_FIELD_SEMANTICS.ABSENT,
    property_type_available: true,
    registry_id_available: true,
    last_updated: "2020_stale_capacity",
    usage_terms_located: true,
    storage_reuse_status: "open_data_identity",
    technical_accessibility: "csv",
    expected_match_yield: "identity_fundamentals",
    recommended_action: "tier_b_if_refreshed_with_habitaciones; currently identity",
    tier: SOURCE_TIER.TIER_C_IDENTITY_ONLY,
    urls: {
      dataset:
        "https://catalogodatos.gub.uy/dataset/ministerio-de-turismo-registro-de-operadores-turisticos-para-alojamientos",
    },
  },
  ecuador_mintur_catastro: {
    source_id: "ecuador_mintur_catastro",
    geography: "Ecuador",
    geography_id: "ecuador",
    source_name: "MINTUR Catastro Turístico consolidado",
    source_authority: "Ministerio de Turismo",
    official_domain: "turismo.gob.ec",
    source_type: "national_tourism_catastro",
    publicly_accessible: true,
    requires_login: false,
    api_available: false,
    bulk_download_available: true,
    searchable_directory: false,
    property_name: true,
    address: true,
    postal_code: false,
    city: true,
    state_region: true,
    phone: true,
    website: false,
    room_count_available: false,
    room_field_name: null,
    room_field_definition: ROOM_FIELD_SEMANTICS.ABSENT,
    property_type_available: true,
    registry_id_available: false,
    last_updated: "2026-08",
    usage_terms_located: true,
    storage_reuse_status: "public_xlsx",
    technical_accessibility: "xlsx_identity",
    expected_match_yield: "identity_fundamentals",
    recommended_action: "tier_b_fundamentals_if_needed; capacity dashboard is aggregate",
    tier: SOURCE_TIER.TIER_C_IDENTITY_ONLY,
    urls: {
      page: "https://servicios.turismo.gob.ec/catastro-turistico/",
      xlsx:
        "https://servicios.turismo.gob.ec/wp-content/uploads/2026/08/Consolidado-Nacional-2026-publico-7-web.xlsx",
    },
  },
  puerto_rico_roomtax: {
    source_id: "puerto_rico_roomtax",
    geography: "Puerto Rico",
    geography_id: "puerto_rico",
    source_name: "PRTC Room Tax / Hoteliers Registry",
    source_authority: "Puerto Rico Tourism Company",
    official_domain: "tourism.pr.gov",
    source_type: "room_tax_registry",
    publicly_accessible: false,
    requires_login: true,
    api_available: false,
    bulk_download_available: false,
    searchable_directory: false,
    property_name: true,
    address: true,
    postal_code: true,
    city: true,
    state_region: true,
    phone: false,
    website: false,
    room_count_available: "likely_internal",
    room_field_name: "Rooms (tax registry)",
    room_field_definition: ROOM_FIELD_SEMANTICS.UNKNOWN,
    property_type_available: true,
    registry_id_available: true,
    last_updated: "unknown",
    usage_terms_located: false,
    storage_reuse_status: "USAGE_REVIEW",
    technical_accessibility: "login_tax_portal",
    expected_match_yield: "unknown",
    recommended_action: "founder_usage_review_before_any_write",
    tier: SOURCE_TIER.USAGE_REVIEW,
    urls: {
      portal: "https://roomtax.tourism.pr.gov/",
    },
  },
});

/**
 * Lightweight per-geography assessments for remaining CALA geographies
 * (primary source summary when no dedicated entry above).
 */
const GEO_PRIMARY_ASSESSMENT = Object.freeze({
  Belize: {
    tier: SOURCE_TIER.UNUSABLE,
    note: "No verified open property-level lodging rooms dump",
    action: "property_page_fallback",
  },
  "El Salvador": {
    tier: SOURCE_TIER.UNUSABLE,
    note: "CORSATUR licensing not confirmed as open property rooms",
    action: "property_page_fallback",
  },
  Guatemala: {
    tier: SOURCE_TIER.UNUSABLE,
    note: "INGUAT registry not confirmed as open property rooms bulk",
    action: "property_page_fallback",
  },
  Honduras: {
    tier: SOURCE_TIER.UNUSABLE,
    note: "IHT lodging registry not confirmed as open bulk rooms",
    action: "property_page_fallback",
  },
  Nicaragua: {
    tier: SOURCE_TIER.UNUSABLE,
    note: "INTUR registry not confirmed as open property rooms",
    action: "property_page_fallback",
  },
  Bolivia: {
    tier: SOURCE_TIER.UNUSABLE,
    note: "Viceministerio Turismo registry not confirmed open rooms",
    action: "property_page_fallback",
  },
  "French Guiana": {
    tier: SOURCE_TIER.UNUSABLE,
    note: "FR/CT Guyane tourism listings — no CALA bulk rooms SoT",
    action: "property_page_fallback",
  },
  Guyana: {
    tier: SOURCE_TIER.UNUSABLE,
    note: "No verified open lodging rooms registry",
    action: "property_page_fallback",
  },
  Paraguay: {
    tier: SOURCE_TIER.UNUSABLE,
    note: "SENATUR registry not confirmed open property rooms",
    action: "property_page_fallback",
  },
  Suriname: {
    tier: SOURCE_TIER.UNUSABLE,
    note: "No verified open lodging rooms registry",
    action: "property_page_fallback",
  },
  Venezuela: {
    tier: SOURCE_TIER.USAGE_REVIEW,
    note: "MINTUR listings historically unstable; verify before use",
    action: "usage_review",
  },
  Jamaica: {
    tier: SOURCE_TIER.UNUSABLE,
    note: "TPDCo licensing operational; no open rooms inventory",
    action: "property_page_fallback",
  },
  Bahamas: {
    tier: SOURCE_TIER.TIER_D_AGGREGATE_ONLY,
    note: "Island-level hotel rooms PDF aggregates only",
    action: "do_not_use_aggregate_as_property",
  },
  Cuba: {
    tier: SOURCE_TIER.TIER_B_USEFUL_FUNDAMENTALS,
    note: "Official operator pages (Cubanacan etc.) better than national dump",
    action: "prefer_operator_official_pages",
  },
  Aruba: {
    tier: SOURCE_TIER.UNUSABLE,
    note: "ATA listings not confirmed as open rooms bulk",
    action: "property_page_fallback",
  },
  "Curaçao": {
    tier: SOURCE_TIER.UNUSABLE,
    note: "CTB listings not confirmed as open rooms bulk",
    action: "property_page_fallback",
  },
  "Trinidad and Tobago": {
    tier: SOURCE_TIER.USAGE_REVIEW,
    note: "TTTIC certified listing — rooms field not verified bulk",
    action: "usage_review",
  },
  Bermuda: {
    tier: SOURCE_TIER.UNUSABLE,
    note: "BTA listings — no open rooms inventory verified",
    action: "property_page_fallback",
  },
});

const CARIBBEAN_DEFAULT = Object.freeze({
  tier: SOURCE_TIER.UNUSABLE,
  note: "Small-island tourism authority — no verified open property rooms dump",
  action: "property_page_fallback_or_licensed_directory_hunt",
});

/**
 * @param {object} source
 */
export function sourceToMatrixRow(source) {
  return {
    Geography: source.geography,
    "Source Name": source.source_name,
    "Source Authority": source.source_authority,
    "Official Domain": source.official_domain,
    "Source Type": source.source_type,
    "Publicly Accessible": source.publicly_accessible,
    "Requires Login": source.requires_login,
    "API Available": source.api_available,
    "Bulk Download Available": source.bulk_download_available,
    "Searchable Directory": source.searchable_directory,
    "Property Name Available": source.property_name,
    "Address Available": source.address,
    "Postal Code Available": source.postal_code,
    "City Available": source.city,
    "State/Region Available": source.state_region,
    "Phone Available": source.phone,
    "Website Available": source.website,
    "ROOM COUNT AVAILABLE": source.room_count_available,
    "Room Field Name": source.room_field_name,
    "Room Field Definition": source.room_field_definition,
    "Property Type Available": source.property_type_available,
    "Registry ID Available": source.registry_id_available,
    "Last Updated if determinable": source.last_updated,
    "Usage / Access Terms Located": source.usage_terms_located,
    "Storage / Reuse Status": source.storage_reuse_status,
    "Technical Accessibility": source.technical_accessibility,
    "Expected Match Yield": source.expected_match_yield,
    "Recommended Action": source.recommended_action,
    Tier: source.tier,
    source_id: source.source_id,
  };
}

/**
 * Build full 52-geography assessment matrix.
 */
export function buildOfficialRoomsSourceMatrix() {
  const geos = listDealalityCalaGeographies({ includeScopeReview: false });
  const sources = Object.values(OFFICIAL_ROOMS_SOURCES);
  const byGeo = new Map();
  for (const g of geos) {
    byGeo.set(g.name, {
      geography: g.name,
      geography_id: g.geography_id,
      region: g.region,
      tourism_priority: g.tourism_priority,
      sources: [],
      primary_tier: null,
      primary_note: null,
      recommended_action: null,
    });
  }

  for (const s of sources) {
    const row = byGeo.get(s.geography);
    if (!row) continue;
    row.sources.push(sourceToMatrixRow(s));
  }

  for (const row of byGeo.values()) {
    if (row.sources.length) {
      const tiers = row.sources.map((s) => s.Tier);
      if (tiers.includes(SOURCE_TIER.TIER_A_BULK_ROOMS_HIGH)) {
        row.primary_tier = SOURCE_TIER.TIER_A_BULK_ROOMS_HIGH;
      } else if (tiers.includes(SOURCE_TIER.TIER_B_USEFUL_FUNDAMENTALS)) {
        row.primary_tier = SOURCE_TIER.TIER_B_USEFUL_FUNDAMENTALS;
      } else if (tiers.includes(SOURCE_TIER.USAGE_REVIEW)) {
        row.primary_tier = SOURCE_TIER.USAGE_REVIEW;
      } else if (tiers.includes(SOURCE_TIER.TIER_C_IDENTITY_ONLY)) {
        row.primary_tier = SOURCE_TIER.TIER_C_IDENTITY_ONLY;
      } else if (tiers.includes(SOURCE_TIER.TIER_D_AGGREGATE_ONLY)) {
        row.primary_tier = SOURCE_TIER.TIER_D_AGGREGATE_ONLY;
      } else {
        row.primary_tier = SOURCE_TIER.UNUSABLE;
      }
      row.recommended_action = row.sources[0]["Recommended Action"];
      row.primary_note = row.sources.map((s) => s["Source Name"]).join("; ");
    } else {
      const fallback =
        GEO_PRIMARY_ASSESSMENT[row.geography] || CARIBBEAN_DEFAULT;
      row.primary_tier = fallback.tier;
      row.primary_note = fallback.note;
      row.recommended_action = fallback.action;
      row.sources.push({
        Geography: row.geography,
        "Source Name": "(no verified official rooms source)",
        Tier: fallback.tier,
        "Recommended Action": fallback.action,
        Notes: fallback.note,
        "ROOM COUNT AVAILABLE": false,
      });
    }
  }

  const matrix = [...byGeo.values()].sort((a, b) =>
    a.geography.localeCompare(b.geography)
  );
  const flat_source_rows = matrix.flatMap((g) => g.sources);
  const tierA = sources.filter(
    (s) => s.tier === SOURCE_TIER.TIER_A_BULK_ROOMS_HIGH
  );
  const tierB = sources.filter(
    (s) => s.tier === SOURCE_TIER.TIER_B_USEFUL_FUNDAMENTALS
  );
  const usageReview = [
    ...sources.filter((s) => s.tier === SOURCE_TIER.USAGE_REVIEW),
    ...matrix
      .filter((g) => g.primary_tier === SOURCE_TIER.USAGE_REVIEW)
      .map((g) => ({ geography: g.geography, note: g.primary_note })),
  ];
  const noScalable = matrix
    .filter(
      (g) =>
        g.primary_tier === SOURCE_TIER.UNUSABLE ||
        g.primary_tier === SOURCE_TIER.TIER_D_AGGREGATE_ONLY ||
        g.primary_tier === SOURCE_TIER.TIER_C_IDENTITY_ONLY
    )
    .map((g) => g.geography);

  return {
    version: OFFICIAL_ROOMS_SOURCE_REGISTRY_VERSION,
    generated_at: new Date().toISOString(),
    GEOGRAPHIES_ASSESSED: matrix.length,
    GEOGRAPHIES_ASSESSED_LABEL: `${matrix.length} / 52`,
    TIER_A_SOURCES_FOUND: tierA.map((s) => s.source_id),
    TIER_B_SOURCES_FOUND: tierB.map((s) => s.source_id),
    USAGE_REVIEW_SOURCES: usageReview,
    GEOGRAPHIES_WITH_NO_SCALABLE_ROOM_SOURCE: noScalable,
    geographies: matrix,
    flat_source_rows,
    sources: OFFICIAL_ROOMS_SOURCES,
  };
}

export function listTierASources() {
  return Object.values(OFFICIAL_ROOMS_SOURCES).filter(
    (s) => s.tier === SOURCE_TIER.TIER_A_BULK_ROOMS_HIGH
  );
}
