/**
 * Contact Intelligence V1.1 — development live discovery cohort (10 real hotels).
 * Held-out reserved. Portuguese case unavailable in current HI fixtures.
 * owner_entity_id here is a DEVELOPMENT COHORT SEED fallback when ownership surface lacks the hotel.
 */

export const LIVE_SLICE_TEN_VERSION = "contact-live-slice-ten-v1.1";

export const LIVE_SLICE_TEN_HOTELS = Object.freeze([
  {
    hotel_id: "recUNycnMwOVFX0hc",
    hotel_name: "Krystal Grand Puerto Vallarta",
    city: "Puerto Vallarta",
    country: "Mexico",
    language: "es",
    owner_group: "gsf",
    owner_entity_id: "dle_06G6AB1VK0BCCD94DNN7W8DRWZ",
    owner_display_name: "Grupo Hotelero Santa Fe",
    case_type: "multi_hotel_owner_spanish",
    seedHints: {
      hotel_website: "https://www.krystal-hotels.com/krystal-grand-puerto-vallarta/",
      owner_website: "https://www.gsfhotels.com/",
      known_people: [
        {
          display_name: "Francisco Ancira Elizondo",
          title: "Board President",
          role_observed_at: "2025-12-01T00:00:00.000Z",
          why_relevant:
            "Board leadership at GSF, economic owner of KGPV — candidate for ownership-level decisions; not auto-selected as sole decision maker.",
        },
      ],
    },
  },
  {
    hotel_id: "recId5nDFUgVbJnzH",
    hotel_name: "Krystal Grand Residences Villas San Miguel de Allende",
    city: "San Miguel de Allende",
    country: "Mexico",
    language: "es",
    owner_group: "gsf",
    owner_entity_id: "dle_06G6AB1VK0BCCD94DNN7W8DRWZ",
    owner_display_name: "Grupo Hotelero Santa Fe",
    case_type: "portfolio_reuse_operated_not_owned",
    seedHints: {
      hotel_website: "https://www.krystal-hotels.com/",
      owner_website: "https://www.gsfhotels.com/",
    },
  },
  {
    hotel_id: "recIwaP1etgx2g9nA",
    hotel_name: "Cambridge Beaches Resort & Spa",
    city: "Sandys Parish",
    country: "Bermuda",
    language: "en",
    owner_group: "dovetail",
    owner_entity_id: "ent_dovetail-hospitality",
    owner_display_name: "Dovetail Hospitality",
    case_type: "private_single_property_english",
    seedHints: {
      hotel_website: "https://www.cambridgebeaches.com/",
      owner_website: "https://www.dovetailandco.com/",
    },
  },
  {
    hotel_id: "recsYJb2R1jarPpK3",
    hotel_name: "Sheraton Guadalajara Expo",
    city: "Guadalajara",
    country: "Mexico",
    language: "es",
    owner_group: "hnf",
    owner_entity_id: "ent_inmobiliaria-hnf",
    owner_display_name: "Inmobiliaria HNF",
    case_type: "difficult_ownership_structure_spanish",
    seedHints: {
      hotel_website:
        "https://www.marriott.com/es/hotels/gdlse-sheraton-guadalajara-expo/overview/",
    },
  },
  {
    hotel_id: "recTYaiA4S6fR6ixx",
    hotel_name: "Real Inn Cancún / voco Cancún",
    city: "Cancún",
    country: "Mexico",
    language: "es",
    owner_group: "alliance",
    owner_entity_id: "ent_alliance-hotel-management",
    owner_display_name: "Alliance Hotel Management",
    case_type: "alliance_portfolio_anchor",
    seedHints: {
      hotel_website: "https://www.ihg.com/voco/hotels/us/en/cancun/cunzh/hoteldetail",
    },
  },
  {
    hotel_id: "recGZZCek9vDQGG1L",
    hotel_name: "voco Guadalajara Expo",
    city: "Guadalajara",
    country: "Mexico",
    language: "es",
    owner_group: "alliance",
    owner_entity_id: "ent_alliance-hotel-management",
    owner_display_name: "Alliance Hotel Management",
    case_type: "alliance_reuse",
    seedHints: {},
  },
  {
    hotel_id: "rec79Xs4mZkuiWnuN",
    hotel_name: "Real Inn Ciudad Juárez",
    city: "Ciudad Juárez",
    country: "Mexico",
    language: "es",
    owner_group: "alliance",
    owner_entity_id: "ent_alliance-hotel-management",
    owner_display_name: "Alliance Hotel Management",
    case_type: "alliance_spanish",
    seedHints: {},
  },
  {
    hotel_id: "recFspIiglYxJp1N1",
    hotel_name: "Real Inn San Luis Potosí",
    city: "San Luis Potosí",
    country: "Mexico",
    language: "es",
    owner_group: "alliance",
    owner_entity_id: "ent_alliance-hotel-management",
    owner_display_name: "Alliance Hotel Management",
    case_type: "alliance_spanish",
    seedHints: {},
  },
  {
    hotel_id: "recogJrXdZHRV06Bl",
    hotel_name: "Real Inn Nuevo Laredo",
    city: "Nuevo Laredo",
    country: "Mexico",
    language: "es",
    owner_group: "alliance",
    owner_entity_id: "ent_alliance-hotel-management",
    owner_display_name: "Alliance Hotel Management",
    case_type: "alliance_spanish",
    seedHints: {},
  },
  {
    hotel_id: "recZxCHVNG0bDQhfG",
    hotel_name: "Real Inn Torreón",
    city: "Torreón",
    country: "Mexico",
    language: "es",
    owner_group: "alliance",
    owner_entity_id: "ent_alliance-hotel-management",
    owner_display_name: "Alliance Hotel Management",
    case_type: "alliance_spanish",
    seedHints: {},
  },
]);

export const LIVE_SLICE_LIMITATIONS = Object.freeze({
  portuguese_language_case: "UNAVAILABLE",
  portuguese_detail:
    "No Brazil/Portugal hotels in Hotel Intelligence development fixtures / owner portfolios. Cannot substitute fabricated PT case.",
  held_out_reserved: true,
  paid_enrichment: false,
  thousand_hotel_stage: "BLOCKED",
  population_wide: "BLOCKED",
});
