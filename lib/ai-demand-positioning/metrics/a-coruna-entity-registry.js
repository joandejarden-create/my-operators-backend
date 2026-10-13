/**
 * A Coruña entity registry — hotel-instance stewardship for adp_ac_hotel_a_coruna.
 * Subject + certified peers. Not a Spain/Galicia production switch.
 */

import { createEntityRegistry } from "./adp-entity-registry-factory.js";

export const A_CORUNA_ENTITY_VERSION = "adp_a_coruna_entity_registry_v1";

export const A_CORUNA_CANONICAL_HOTELS = Object.freeze([
  {
    entityId: "ac_hotel_a_coruna",
    canonical: "AC Hotel A Coruña",
    market: "A Coruña",
    geography: "matogrande_business_corridor",
    chainScale: "Upper Upscale",
    propertyType: "urban_full_service_hotel",
    identityConfidence: "HIGH",
    meetings: true,
    spa: false,
    subject: true,
    brand: "AC Hotels by Marriott",
    parentCompany: "Marriott International",
    loyaltyEcosystem: "marriott_bonvoy",
    aliases: [
      "ac hotel a coruña",
      "ac hotel a coruna",
      "ac hotels a coruña",
      "ac by marriott a coruña",
      "lcgco",
    ],
  },
  {
    entityId: "peer_nh_collection_finisterre_coruna",
    canonical: "NH Collection A Coruña Finisterre",
    market: "A Coruña",
    geography: "centro_historic_port",
    chainScale: "Upscale",
    propertyType: "urban_meetings_hotel",
    identityConfidence: "HIGH",
    meetings: true,
    brand: "NH Collection",
    parentCompany: "Minor Hotels / NH",
    aliases: [
      "nh collection a coruña finisterre",
      "nh collection finisterre",
      "hotel finisterre a coruña",
      "nh finisterre",
    ],
  },
  {
    entityId: "peer_melia_maria_pita",
    canonical: "Meliá María Pita",
    market: "A Coruña",
    geography: "orzan_waterfront",
    chainScale: "Upper Upscale",
    propertyType: "urban_full_service_hotel",
    identityConfidence: "HIGH",
    meetings: true,
    brand: "Meliá",
    parentCompany: "Meliá Hotels International",
    aliases: ["meliá maría pita", "melia maria pita", "hotel melia maria pita"],
  },
  {
    entityId: "peer_eurostars_atlantico",
    canonical: "Eurostars Atlántico",
    market: "A Coruña",
    geography: "city_center",
    chainScale: "Upper Upscale",
    propertyType: "urban_business_hotel",
    identityConfidence: "HIGH",
    meetings: true,
    brand: "Eurostars",
    parentCompany: "Hotusa",
    aliases: ["eurostars atlántico", "eurostars atlantico", "hotel eurostars atlantico"],
  },
  {
    entityId: "peer_hesperia_coruna_centro",
    canonical: "Hesperia A Coruña Centro",
    market: "A Coruña",
    geography: "city_center",
    chainScale: "Upper Upscale",
    propertyType: "urban_business_hotel",
    identityConfidence: "HIGH",
    meetings: true,
    brand: "Hesperia",
    parentCompany: "Hesperia / Barceló",
    aliases: [
      "hesperia a coruña centro",
      "hesperia a coruna centro",
      "hesperia coruña",
    ],
  },
  {
    entityId: "peer_attica21_coruna",
    canonical: "Attica21 Coruña",
    market: "A Coruña",
    geography: "city_waterfront_business",
    chainScale: "Upper Midscale",
    propertyType: "urban_business_hotel",
    identityConfidence: "HIGH",
    meetings: true,
    brand: "Attica21",
    parentCompany: "Grupo Hotusa / Attica",
    aliases: ["attica21 coruña", "attica21 coruna", "attica 21 a coruña", "hotel attica21 coruña"],
  },
]);

export const A_CORUNA_REGISTRY = createEntityRegistry({
  version: A_CORUNA_ENTITY_VERSION,
  market: "A Coruña",
  hotels: A_CORUNA_CANONICAL_HOTELS,
  ambiguous: ["ac hotel santiago", "parador a coruña", "hotel coruña"],
});
