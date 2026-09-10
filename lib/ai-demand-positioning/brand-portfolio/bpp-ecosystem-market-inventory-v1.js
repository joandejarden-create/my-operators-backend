/**
 * ADP_BPP_ECOSYSTEM_MARKET_INVENTORY_COMPLETENESS
 *
 * Canonical same-loyalty-ecosystem inventories for governed ADP markets.
 * Suppression is invalid if this inventory is incomplete.
 *
 * METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 * Inventory enrichment is DATA_QUALITY / DISCOVERY — not a methodology change.
 */

export const ADP_BPP_ECOSYSTEM_MARKET_INVENTORY_COMPLETENESS =
  "ADP_BPP_ECOSYSTEM_MARKET_INVENTORY_COMPLETENESS";
export const ADP_BPP_CANONICAL_ECOSYSTEM_DISCOVERY =
  "ADP_BPP_CANONICAL_ECOSYSTEM_DISCOVERY";
export const BPP_SUPPRESSION_IS_INVALID_IF_CANONICAL_ECOSYSTEM_INVENTORY_IS_INCOMPLETE =
  "BPP_SUPPRESSION_IS_INVALID_IF_CANONICAL_ECOSYSTEM_INVENTORY_IS_INCOMPLETE";
export const FULL_LOYALTY_ECOSYSTEM_MARKET_DISCOVERY_PRECEDES_SUPPRESSION =
  "FULL_LOYALTY_ECOSYSTEM_MARKET_DISCOVERY_PRECEDES_SUPPRESSION";

export const INVENTORY_VERSION = "ADP_BPP_ECOSYSTEM_MARKET_INVENTORY_V1_20260910";
export const INVENTORY_REFRESH_DATE = "2026-09-10";

/** Descriptive peer-role labels (context under current V1; formalized in pending MCR V2). */
export const BPP_PEER_ROLE_DESCRIPTIVE = Object.freeze({
  LIKE_FOR_LIKE_PORTFOLIO_PEER: "LIKE_FOR_LIKE_PORTFOLIO_PEER",
  BROADER_LOYALTY_ALTERNATIVE: "BROADER_LOYALTY_ALTERNATIVE",
  SUBJECT: "SUBJECT",
  EXCLUDED: "EXCLUDED",
});

function row(partial) {
  return Object.freeze({
    censusId: null,
    activeStatus: "OPEN_VERIFIED",
    bppEligible: true,
    ...partial,
  });
}

/**
 * Governed Punta Cana traveler choice set:
 * Cap Cana · Puntacana / Punta Cana core · Bávaro · Uvero Alto.
 * Miches is OUTSIDE (separate destination).
 */
export const PUNTA_CANA_MARKET_GEOGRAPHY = Object.freeze({
  canonicalMarket: "Punta Cana",
  includedSubmarkets: Object.freeze([
    "cap_cana",
    "punta_cana",
    "puntacana",
    "bavaro",
    "uvero_alto",
  ]),
  excludedSubmarkets: Object.freeze([
    {
      id: "miches",
      reason: "Separate east-coast destination — not part of governed Punta Cana traveler set for Cap Cana ADP",
    },
  ]),
  aliasNote:
    "Punta Cana / Puntacana / Cap Cana / Bávaro / Uvero Alto are submarkets of one governed destination market, not unrelated markets.",
  sources: Object.freeze([
    "marriott.com Dominican Republic / Punta Cana destination inventory",
    "cap_cana_entity_registry_v1 (enriched)",
  ]),
});

/** Current Marriott Bonvoy open hotels in governed Punta Cana market. */
export const PUNTA_CANA_BONVOY_INVENTORY_V1 = Object.freeze([
  row({
    canonicalHotelId: "st_regis_cap_cana",
    displayName: "The St. Regis Cap Cana Resort",
    brand: "St. Regis",
    parent: "Marriott International",
    loyaltyEcosystem: "marriott_bonvoy",
    physicalLocation: "Cap Cana / Punta Espada",
    canonicalMarket: "Punta Cana",
    submarket: "cap_cana",
    hotelType: "luxury_beach_resort",
    chainScale: "Luxury",
    subjectPropertyId: "adp_st_regis_cap_cana",
    evidenceSource: "marriott.com/pujxr",
  }),
  row({
    canonicalHotelId: "sanctuary_cap_cana",
    displayName:
      "Sanctuary Cap Cana, a Luxury Collection Resort, Dominican Republic, Adult All-Inclusive",
    brand: "Luxury Collection",
    parent: "Marriott International",
    loyaltyEcosystem: "marriott_bonvoy",
    physicalLocation: "Cap Cana",
    canonicalMarket: "Punta Cana",
    submarket: "cap_cana",
    hotelType: "all_inclusive_resort",
    chainScale: "Luxury",
    evidenceSource: "marriott.com Luxury Collection / Bonvoy destination list",
    affiliationCorrection:
      "Prior exhaustion ledger incorrectly treated Sanctuary as Hilton Honors — property is Luxury Collection / Bonvoy",
  }),
  row({
    canonicalHotelId: "westin_punta_cana",
    displayName: "The Westin Puntacana Resort & Club",
    brand: "Westin",
    parent: "Marriott International",
    loyaltyEcosystem: "marriott_bonvoy",
    physicalLocation: "Playa Blanca / PUNTACANA Resort & Club",
    canonicalMarket: "Punta Cana",
    submarket: "punta_cana",
    hotelType: "beach_resort",
    chainScale: "Upper Upscale",
    evidenceSource: "marriott.com/pujwi",
  }),
  row({
    canonicalHotelId: "w_punta_cana",
    displayName: "W Punta Cana, Adult All-Inclusive",
    brand: "W Hotels",
    parent: "Marriott International",
    loyaltyEcosystem: "marriott_bonvoy",
    physicalLocation: "Uvero Alto / Higüey",
    canonicalMarket: "Punta Cana",
    submarket: "uvero_alto",
    hotelType: "lifestyle_all_inclusive_resort",
    chainScale: "Luxury",
    evidenceSource: "marriott.com/pujwh",
  }),
  row({
    canonicalHotelId: "four_points_sheraton_puntacana_village",
    displayName: "Four Points by Sheraton Puntacana Village",
    brand: "Four Points by Sheraton",
    parent: "Marriott International",
    loyaltyEcosystem: "marriott_bonvoy",
    physicalLocation: "Puntacana Village",
    canonicalMarket: "Punta Cana",
    submarket: "punta_cana",
    hotelType: "select_service_hotel",
    chainScale: "Upscale",
    evidenceSource: "marriott.com DR inventory + demand-anchors DR fixtures",
  }),
  row({
    canonicalHotelId: "ac_hotel_punta_cana",
    displayName: "AC Hotel Punta Cana",
    brand: "AC Hotels by Marriott",
    parent: "Marriott International",
    loyaltyEcosystem: "marriott_bonvoy",
    physicalLocation: "Downtown Punta Cana",
    canonicalMarket: "Punta Cana",
    submarket: "punta_cana",
    hotelType: "select_service_hotel",
    chainScale: "Upscale",
    evidenceSource: "marriott.com/pujac + fixtures/marriott-overview-pujac-sample.html",
  }),
  row({
    canonicalHotelId: "royalton_bavaro_autograph",
    displayName: "Royalton Bavaro, An Autograph Collection All-Inclusive Resort & Casino",
    brand: "Autograph Collection",
    parent: "Marriott International",
    loyaltyEcosystem: "marriott_bonvoy",
    physicalLocation: "Bávaro",
    canonicalMarket: "Punta Cana",
    submarket: "bavaro",
    hotelType: "all_inclusive_resort",
    chainScale: "Upper Upscale",
    evidenceSource: "marriott.com Punta Cana resort destination list",
  }),
  row({
    canonicalHotelId: "royalton_splash_punta_cana",
    displayName:
      "Royalton Splash Punta Cana, An Autograph Collection All-Inclusive Resort & Casino",
    brand: "Autograph Collection",
    parent: "Marriott International",
    loyaltyEcosystem: "marriott_bonvoy",
    physicalLocation: "Bávaro / Punta Cana",
    canonicalMarket: "Punta Cana",
    submarket: "bavaro",
    hotelType: "all_inclusive_resort",
    chainScale: "Upper Upscale",
    evidenceSource: "marriott.com Punta Cana resort destination list",
  }),
  row({
    canonicalHotelId: "royalton_punta_cana",
    displayName:
      "Royalton Punta Cana, An Autograph Collection All-Inclusive Resort & Casino",
    brand: "Autograph Collection",
    parent: "Marriott International",
    loyaltyEcosystem: "marriott_bonvoy",
    physicalLocation: "Punta Cana",
    canonicalMarket: "Punta Cana",
    submarket: "bavaro",
    hotelType: "all_inclusive_resort",
    chainScale: "Upper Upscale",
    evidenceSource: "marriott.com Punta Cana resort destination list",
  }),
  row({
    canonicalHotelId: "royalton_hideaway_punta_cana",
    displayName:
      "Royalton Hideaway Punta Cana, An Autograph Collection All-Inclusive Resort & Casino – Adults Only",
    brand: "Autograph Collection",
    parent: "Marriott International",
    loyaltyEcosystem: "marriott_bonvoy",
    physicalLocation: "Punta Cana",
    canonicalMarket: "Punta Cana",
    submarket: "bavaro",
    hotelType: "all_inclusive_resort",
    chainScale: "Upper Upscale",
    evidenceSource: "marriott.com Punta Cana resort destination list",
  }),
  row({
    canonicalHotelId: "royalton_chic_punta_cana",
    displayName:
      "Royalton CHIC Punta Cana, An Autograph Collection All-Inclusive Resort & Casino - Adults Only",
    brand: "Autograph Collection",
    parent: "Marriott International",
    loyaltyEcosystem: "marriott_bonvoy",
    physicalLocation: "Punta Cana",
    canonicalMarket: "Punta Cana",
    submarket: "bavaro",
    hotelType: "all_inclusive_resort",
    chainScale: "Upper Upscale",
    evidenceSource: "marriott.com Punta Cana resort destination list",
  }),
  row({
    canonicalHotelId: "st_regis_cap_cana_residences",
    displayName: "The Residences at The St. Regis Cap Cana Resort",
    brand: "St. Regis Residences",
    parent: "Marriott International",
    loyaltyEcosystem: "marriott_bonvoy",
    physicalLocation: "Cap Cana",
    canonicalMarket: "Punta Cana",
    submarket: "cap_cana",
    hotelType: "branded_residence",
    chainScale: "Luxury",
    bppEligible: false,
    exclusionReason: "Branded residences — not a substitutable hotel peer for BPP",
    evidenceSource: "marriott.com/pujrx",
  }),
]);

/** Monterrey Valle L1 + broader Monterrey metro L2 Bonvoy inventory. */
export const MONTERREY_BONVOY_INVENTORY_V1 = Object.freeze([
  row({
    canonicalHotelId: "jw_marriott_monterrey_valle",
    displayName: "JW Marriott Hotel Monterrey Valle",
    brand: "JW Marriott",
    parent: "Marriott International",
    loyaltyEcosystem: "marriott_bonvoy",
    physicalLocation: "San Pedro Garza García / Valle",
    canonicalMarket: "Monterrey",
    submarket: "monterrey_valle",
    hotelType: "luxury_full_service_hotel",
    chainScale: "Luxury",
    subjectPropertyId: "adp_jw_marriott_monterrey_valle",
    expansionLevel: 1,
    evidenceSource: "marriott.com/mtyjw",
  }),
  row({
    canonicalHotelId: "westin_monterrey_valle",
    displayName: "The Westin Monterrey Valle",
    brand: "Westin",
    parent: "Marriott International",
    loyaltyEcosystem: "marriott_bonvoy",
    physicalLocation: "San Pedro Garza García / Punto Valle",
    canonicalMarket: "Monterrey",
    submarket: "monterrey_valle",
    hotelType: "full_service_hotel",
    chainScale: "Upper Upscale",
    subjectPropertyId: "adp_westin_monterrey_valle",
    expansionLevel: 1,
    evidenceSource: "marriott.com/mtywi",
  }),
  row({
    canonicalHotelId: "ac_hotel_monterrey_valle",
    displayName: "AC Hotel by Marriott Monterrey Valle",
    brand: "AC Hotels by Marriott",
    parent: "Marriott International",
    loyaltyEcosystem: "marriott_bonvoy",
    physicalLocation: "San Pedro Garza García / Valle",
    canonicalMarket: "Monterrey",
    submarket: "monterrey_valle",
    hotelType: "select_service_hotel",
    chainScale: "Upscale",
    expansionLevel: 1,
    evidenceSource: "marriott.com Monterrey destination + valle registry",
  }),
  row({
    canonicalHotelId: "courtyard_monterrey_san_jeronimo_valle",
    displayName: "Courtyard by Marriott Monterrey San Jeronimo/Valle",
    brand: "Courtyard",
    parent: "Marriott International",
    loyaltyEcosystem: "marriott_bonvoy",
    physicalLocation: "San Jerónimo / Valle corridor",
    canonicalMarket: "Monterrey",
    submarket: "san_jeronimo_valle",
    hotelType: "select_service_hotel",
    chainScale: "Upscale",
    expansionLevel: 2,
    evidenceSource: "marriott.com Monterrey destination inventory",
  }),
  row({
    canonicalHotelId: "four_points_galerias_monterrey",
    displayName: "Four Points by Sheraton Galerias Monterrey",
    brand: "Four Points by Sheraton",
    parent: "Marriott International",
    loyaltyEcosystem: "marriott_bonvoy",
    physicalLocation: "Galerías Monterrey",
    canonicalMarket: "Monterrey",
    submarket: "galerias",
    hotelType: "select_service_hotel",
    chainScale: "Upscale",
    expansionLevel: 2,
    evidenceSource: "marriott.com Monterrey destination inventory",
  }),
  row({
    canonicalHotelId: "four_points_monterrey_linda_vista",
    displayName: "Four Points by Sheraton Monterrey Linda Vista",
    brand: "Four Points by Sheraton",
    parent: "Marriott International",
    loyaltyEcosystem: "marriott_bonvoy",
    physicalLocation: "Linda Vista",
    canonicalMarket: "Monterrey",
    submarket: "linda_vista",
    hotelType: "select_service_hotel",
    chainScale: "Upscale",
    expansionLevel: 2,
    evidenceSource: "marriott.com Monterrey destination inventory",
  }),
  row({
    canonicalHotelId: "courtyard_monterrey_airport",
    displayName: "Courtyard by Marriott Monterrey Airport",
    brand: "Courtyard",
    parent: "Marriott International",
    loyaltyEcosystem: "marriott_bonvoy",
    physicalLocation: "Monterrey Airport / Apodaca",
    canonicalMarket: "Monterrey",
    submarket: "airport",
    hotelType: "select_service_hotel",
    chainScale: "Upscale",
    expansionLevel: 2,
    evidenceSource: "marriott.com Monterrey destination inventory",
  }),
  row({
    canonicalHotelId: "four_points_monterrey_airport",
    displayName: "Four Points by Sheraton Monterrey Airport",
    brand: "Four Points by Sheraton",
    parent: "Marriott International",
    loyaltyEcosystem: "marriott_bonvoy",
    physicalLocation: "Monterrey Airport",
    canonicalMarket: "Monterrey",
    submarket: "airport",
    hotelType: "select_service_hotel",
    chainScale: "Upscale",
    expansionLevel: 2,
    evidenceSource: "marriott.com Monterrey destination inventory",
  }),
  row({
    canonicalHotelId: "fairfield_monterrey_airport",
    displayName: "Fairfield by Marriott Monterrey Aeropuerto",
    brand: "Fairfield",
    parent: "Marriott International",
    loyaltyEcosystem: "marriott_bonvoy",
    physicalLocation: "Monterrey Airport",
    canonicalMarket: "Monterrey",
    submarket: "airport",
    hotelType: "select_service_hotel",
    chainScale: "Upper Midscale",
    expansionLevel: 2,
    evidenceSource: "marriott.com / regional Marriott listings",
  }),
]);

export const BOGOTA_CHOICE_INVENTORY_V1 = Object.freeze([
  row({
    canonicalHotelId: "faranda_collection_bogota",
    displayName: "Faranda Collection Bogotá",
    brand: "Radisson Individuals / Faranda Collection",
    parent: "Choice Hotels",
    loyaltyEcosystem: "choice_privileges",
    physicalLocation: "Bogotá Norte",
    canonicalMarket: "Bogotá",
    submarket: "bogota_norte",
    hotelType: "full_service_hotel",
    chainScale: "Upscale",
    subjectPropertyId: "adp_faranda_collection_bogota",
    choicePropertyCode: "CB012",
    evidenceSource: "choicehotels.com / Choice CALA press",
  }),
  row({
    canonicalHotelId: "radisson_bogota_metrotel",
    displayName: "Radisson Bogotá Metrotel",
    brand: "Radisson",
    parent: "Choice Hotels",
    loyaltyEcosystem: "choice_privileges",
    physicalLocation: "Calle 74 / financial corridor",
    canonicalMarket: "Bogotá",
    submarket: "calle_74_zona_rosa",
    hotelType: "full_service_hotel",
    chainScale: "Upper Upscale",
    choicePropertyCode: "CB008",
    evidenceSource: "choicehotels.com/cb008 + radissonbogotametrotel.com",
  }),
]);

export const SANTO_DOMINGO_CHOICE_INVENTORY_V1 = Object.freeze([
  row({
    canonicalHotelId: "radisson_santo_domingo",
    displayName: "Radisson Hotel Santo Domingo",
    brand: "Radisson",
    parent: "Choice Hotels",
    loyaltyEcosystem: "choice_privileges",
    physicalLocation: "Naco",
    canonicalMarket: "Santo Domingo",
    submarket: "naco",
    hotelType: "full_service_hotel",
    chainScale: "Upper Upscale",
    subjectPropertyId: "adp_radisson_santo_domingo",
    evidenceSource: "choicehotels.com / Radisson Americas inventory",
  }),
]);

/** IHG One Rewards open inventory — Greater Santo Domingo (first-party IHG destination list + census). */
export const SANTO_DOMINGO_IHG_INVENTORY_V1 = Object.freeze([
  row({
    canonicalHotelId: "casas_del_xvi",
    displayName: "Casas del XVI",
    brand: "Vignette Collection",
    parent: "IHG Hotels & Resorts",
    loyaltyEcosystem: "ihg_one_rewards",
    physicalLocation: "Zona Colonial",
    canonicalMarket: "Santo Domingo",
    submarket: "zona_colonial",
    hotelType: "boutique_hotel",
    chainScale: "Luxury",
    subjectPropertyId: "adp_casas_del_xvi",
    evidenceSource: "ihg.com/vignettecollection/.../sdqcd + census recjDsNzu93CFfe87",
  }),
  row({
    canonicalHotelId: "kimpton_las_mercedes_santo_domingo",
    displayName: "Kimpton Las Mercedes",
    brand: "Kimpton",
    parent: "IHG Hotels & Resorts",
    loyaltyEcosystem: "ihg_one_rewards",
    physicalLocation: "Zona Colonial",
    canonicalMarket: "Santo Domingo",
    submarket: "zona_colonial",
    hotelType: "boutique_hotel",
    chainScale: "Luxury",
    evidenceSource: "ihg.com/kimptonhotels/.../sdqlm + census recc4Q3BZldMeMtlA",
  }),
  row({
    canonicalHotelId: "intercontinental_real_santo_domingo",
    displayName: "InterContinental Real Santo Domingo",
    brand: "InterContinental",
    parent: "IHG Hotels & Resorts",
    loyaltyEcosystem: "ihg_one_rewards",
    physicalLocation: "Piantini / Winston Churchill",
    canonicalMarket: "Santo Domingo",
    submarket: "piantini",
    hotelType: "full_service_hotel",
    chainScale: "Upper Upscale",
    evidenceSource: "ihg.com/intercontinental/.../sdqic + census recyP6DQ1R6mhBse5",
  }),
  row({
    canonicalHotelId: "holiday_inn_santo_domingo",
    displayName: "Holiday Inn Santo Domingo",
    brand: "Holiday Inn",
    parent: "IHG Hotels & Resorts",
    loyaltyEcosystem: "ihg_one_rewards",
    physicalLocation: "Piantini / Abraham Lincoln",
    canonicalMarket: "Santo Domingo",
    submarket: "piantini",
    hotelType: "full_service_hotel",
    chainScale: "Upscale",
    evidenceSource: "ihg.com/holidayinn/.../sdqex + census rec6QsU0IyLwBGJdy",
  }),
  row({
    canonicalHotelId: "crowne_plaza_santo_domingo",
    displayName: "Crowne Plaza Santo Domingo",
    brand: "Crowne Plaza",
    parent: "IHG Hotels & Resorts",
    loyaltyEcosystem: "ihg_one_rewards",
    physicalLocation: "Malecón",
    canonicalMarket: "Santo Domingo",
    submarket: "malecon",
    hotelType: "full_service_hotel",
    chainScale: "Upscale",
    evidenceSource: "ihg.com/crowneplaza/.../sdqha + census recUoUcP9lEuNkl0F",
  }),
]);

export const CARTAGENA_CHOICE_INVENTORY_V1 = Object.freeze([
  row({
    canonicalHotelId: "hotel_caribe_faranda_grand",
    displayName: "Hotel Caribe by Faranda Grand, a member of Radisson Individuals",
    brand: "Radisson Individuals / Faranda Grand",
    parent: "Choice Hotels",
    loyaltyEcosystem: "choice_privileges",
    physicalLocation: "Bocagrande",
    canonicalMarket: "Cartagena",
    submarket: "bocagrande",
    hotelType: "beachfront_full_service_hotel",
    chainScale: "Upper Upscale",
    subjectPropertyId: "adp_hotel_caribe_faranda_grand",
    evidenceSource: "Choice / Radisson Individuals CALA portfolio",
  }),
  row({
    canonicalHotelId: "radisson_cartagena_ocean_pavillion",
    displayName: "Radisson Cartagena Ocean Pavillion Hotel",
    brand: "Radisson",
    parent: "Choice Hotels",
    loyaltyEcosystem: "choice_privileges",
    physicalLocation: "Cartagena beachfront",
    canonicalMarket: "Cartagena",
    submarket: "bocagrande_el_laguito",
    hotelType: "full_service_hotel",
    chainScale: "Upper Upscale",
    evidenceSource: "Choice Privileges / Radisson Americas + ADP period competitorsMentioned",
  }),
  row({
    canonicalHotelId: "hotel_casa_la_factoria_faranda_boutique",
    displayName: "Hotel Casa La Factoría by Faranda Boutique, a member of Radisson Individuals",
    brand: "Radisson Individuals / Faranda Boutique",
    parent: "Choice Hotels",
    loyaltyEcosystem: "choice_privileges",
    physicalLocation: "Centro Histórico / Cartagena",
    canonicalMarket: "Cartagena",
    submarket: "centro_historico",
    hotelType: "boutique_hotel",
    chainScale: "Upscale",
    evidenceSource: "Choice Privileges suite redemption lists + Choice CALA press",
  }),
  row({
    canonicalHotelId: "hotel_casa_don_luis_faranda_boutique",
    displayName: "Hotel Casa Don Luis by Faranda Boutique, a member of Radisson Individuals",
    brand: "Radisson Individuals / Faranda Boutique",
    parent: "Choice Hotels",
    loyaltyEcosystem: "choice_privileges",
    physicalLocation: "Cartagena",
    canonicalMarket: "Cartagena",
    submarket: "centro_historico",
    hotelType: "boutique_hotel",
    chainScale: "Upscale",
    evidenceSource: "Choice Privileges PR (Casa Don Luis Cartagena) + Choice CALA press",
  }),
]);

export const ECOSYSTEM_INVENTORIES_BY_PROPERTY = Object.freeze({
  adp_st_regis_cap_cana: Object.freeze({
    propertyId: "adp_st_regis_cap_cana",
    ecosystemId: "marriott_bonvoy",
    inventoryKey: "punta_cana_bonvoy",
    inventory: PUNTA_CANA_BONVOY_INVENTORY_V1,
    geography: PUNTA_CANA_MARKET_GEOGRAPHY,
    subjectEntityId: "st_regis_cap_cana",
  }),
  adp_jw_marriott_monterrey_valle: Object.freeze({
    propertyId: "adp_jw_marriott_monterrey_valle",
    ecosystemId: "marriott_bonvoy",
    inventoryKey: "monterrey_bonvoy",
    inventory: MONTERREY_BONVOY_INVENTORY_V1,
    geography: Object.freeze({
      canonicalMarket: "Monterrey",
      includedSubmarkets: Object.freeze(["monterrey_valle", "san_jeronimo_valle", "galerias", "linda_vista", "airport"]),
      excludedSubmarkets: Object.freeze([]),
      note: "L1 = Valle; L2 = broader Monterrey metro under PEER_EXPANSION_HIERARCHY_V1",
    }),
    subjectEntityId: "jw_marriott_monterrey_valle",
  }),
  adp_westin_monterrey_valle: Object.freeze({
    propertyId: "adp_westin_monterrey_valle",
    ecosystemId: "marriott_bonvoy",
    inventoryKey: "monterrey_bonvoy",
    inventory: MONTERREY_BONVOY_INVENTORY_V1,
    geography: Object.freeze({
      canonicalMarket: "Monterrey",
      includedSubmarkets: Object.freeze(["monterrey_valle", "san_jeronimo_valle", "galerias", "linda_vista", "airport"]),
      excludedSubmarkets: Object.freeze([]),
      note: "L1 = Valle; L2 = broader Monterrey metro under PEER_EXPANSION_HIERARCHY_V1",
    }),
    subjectEntityId: "westin_monterrey_valle",
  }),
  adp_faranda_collection_bogota: Object.freeze({
    propertyId: "adp_faranda_collection_bogota",
    ecosystemId: "choice_privileges",
    inventoryKey: "bogota_choice",
    inventory: BOGOTA_CHOICE_INVENTORY_V1,
    geography: Object.freeze({
      canonicalMarket: "Bogotá",
      includedSubmarkets: Object.freeze(["bogota_norte", "calle_74_zona_rosa"]),
      excludedSubmarkets: Object.freeze([]),
    }),
    subjectEntityId: "faranda_collection_bogota",
  }),
  adp_radisson_santo_domingo: Object.freeze({
    propertyId: "adp_radisson_santo_domingo",
    ecosystemId: "choice_privileges",
    inventoryKey: "santo_domingo_choice",
    inventory: SANTO_DOMINGO_CHOICE_INVENTORY_V1,
    geography: Object.freeze({
      canonicalMarket: "Santo Domingo",
      includedSubmarkets: Object.freeze(["naco", "piantini", "malecon", "gazcue", "zona_colonial"]),
      excludedSubmarkets: Object.freeze([]),
      note: "Full Choice Privileges search across Greater Santo Domingo — only Radisson SD confirmed active",
    }),
    subjectEntityId: "radisson_santo_domingo",
  }),
  adp_casas_del_xvi: Object.freeze({
    propertyId: "adp_casas_del_xvi",
    ecosystemId: "ihg_one_rewards",
    inventoryKey: "santo_domingo_ihg",
    inventory: SANTO_DOMINGO_IHG_INVENTORY_V1,
    geography: Object.freeze({
      canonicalMarket: "Santo Domingo",
      includedSubmarkets: Object.freeze(["zona_colonial", "piantini", "naco", "malecon", "gazcue"]),
      excludedSubmarkets: Object.freeze([]),
      note: "Full IHG One Rewards search across Greater Santo Domingo — chain scale is context, not exclusion",
    }),
    subjectEntityId: "casas_del_xvi",
  }),
  adp_hotel_caribe_faranda_grand: Object.freeze({
    propertyId: "adp_hotel_caribe_faranda_grand",
    ecosystemId: "choice_privileges",
    inventoryKey: "cartagena_choice",
    inventory: CARTAGENA_CHOICE_INVENTORY_V1,
    geography: Object.freeze({
      canonicalMarket: "Cartagena",
      includedSubmarkets: Object.freeze(["bocagrande", "bocagrande_el_laguito", "centro_historico"]),
      excludedSubmarkets: Object.freeze([]),
    }),
    subjectEntityId: "hotel_caribe_faranda_grand",
  }),
});

/**
 * Classify peer role from chain-scale + property type vs subject (descriptive only).
 */
export function classifyPeerRoleDescriptive(subject, candidate) {
  if (!subject || !candidate) return BPP_PEER_ROLE_DESCRIPTIVE.BROADER_LOYALTY_ALTERNATIVE;
  const subScale = String(subject.chainScale || "").toLowerCase();
  const candScale = String(candidate.chainScale || "").toLowerCase();
  const subType = String(subject.hotelType || subject.propertyType || "").toLowerCase();
  const candType = String(candidate.hotelType || candidate.propertyType || "").toLowerCase();

  const luxuryBand = /luxury|upper upscale/;
  const selectBand = /select|midscale|upper midscale|limited/;
  const resortBand = /resort|all_inclusive/;

  const bothLuxuryBand = luxuryBand.test(subScale) && luxuryBand.test(candScale);
  const bothResort = resortBand.test(subType) && resortBand.test(candType);
  const candidateSelect = selectBand.test(candScale) || /select_service|limited_service/.test(candType);

  if (candidateSelect && !selectBand.test(subScale)) {
    return BPP_PEER_ROLE_DESCRIPTIVE.BROADER_LOYALTY_ALTERNATIVE;
  }
  if (bothLuxuryBand || bothResort) {
    return BPP_PEER_ROLE_DESCRIPTIVE.LIKE_FOR_LIKE_PORTFOLIO_PEER;
  }
  if (subScale === candScale) {
    return BPP_PEER_ROLE_DESCRIPTIVE.LIKE_FOR_LIKE_PORTFOLIO_PEER;
  }
  return BPP_PEER_ROLE_DESCRIPTIVE.BROADER_LOYALTY_ALTERNATIVE;
}

export function getEcosystemInventoryForProperty(propertyId) {
  return ECOSYSTEM_INVENTORIES_BY_PROPERTY[propertyId] || null;
}

export function listEligibleEcosystemPeers(propertyId) {
  const pack = getEcosystemInventoryForProperty(propertyId);
  if (!pack) return [];
  return pack.inventory.filter(
    (h) =>
      h.bppEligible !== false &&
      h.loyaltyEcosystem === pack.ecosystemId &&
      h.canonicalHotelId !== pack.subjectEntityId
  );
}
