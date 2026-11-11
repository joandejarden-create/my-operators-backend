/**
 * Cancún All-Inclusive / Hotel Zone entity registry for
 * Fiesta Americana Condesa Cancún All Inclusive.
 * Evidence-backed from Core observation competitor extraction (252 responses).
 */

import { createEntityRegistry } from "./adp-entity-registry-factory.js";

export const CANCUN_AI_RESORT_ENTITY_VERSION = "adp_cancun_ai_resort_entity_v1";

function hotel(partial) {
  return Object.freeze({
    meetings: false,
    spa: true,
    identityConfidence: "HIGH",
    ...partial,
    aliases: Object.freeze(partial.aliases || []),
  });
}

export const CANCUN_AI_RESORT_CANONICAL_HOTELS = Object.freeze([
  hotel({
    entityId: "fiesta_americana_condesa_cancun_ai",
    canonical: "Fiesta Americana Condesa Cancún All Inclusive",
    market: "Cancún",
    geography: "hotel_zone",
    chainScale: "Upper Upscale",
    propertyType: "all_inclusive_beach_resort",
    subject: true,
    aliases: [
      "fiesta americana condesa cancun all inclusive",
      "fiesta americana condesa cancún all inclusive",
      "fiesta americana condesa cancun",
      "fiesta americana condesa cancún",
      "condesa cancun all inclusive",
      "condesa cancún",
    ],
  }),
  hotel({
    entityId: "le_blanc_spa_resort_cancun",
    canonical: "Le Blanc Spa Resort Cancún",
    market: "Cancún",
    geography: "hotel_zone",
    chainScale: "Luxury",
    propertyType: "all_inclusive_beach_resort",
    aliases: [
      "le blanc spa resort cancún",
      "le blanc spa resort cancun",
      "le blanc spa resort",
      "le blanc cancun",
      "le blanc cancún",
    ],
  }),
  hotel({
    entityId: "live_aqua_beach_resort_cancun",
    canonical: "Live Aqua Beach Resort Cancún",
    market: "Cancún",
    geography: "hotel_zone",
    chainScale: "Luxury",
    propertyType: "all_inclusive_beach_resort",
    aliases: [
      "live aqua beach resort cancún",
      "live aqua beach resort cancun",
      "live aqua beach resort",
      "live aqua cancun",
      "live aqua cancún",
    ],
  }),
  hotel({
    entityId: "nizuc_resort_spa_cancun",
    canonical: "NIZUC Resort & Spa",
    market: "Cancún",
    geography: "punta_nizuc",
    chainScale: "Luxury",
    propertyType: "luxury_beach_resort",
    aliases: [
      "nizuc resort & spa",
      "nizuc resort and spa",
      "nizuc resort",
      "nizuc cancun",
      "nizuc cancún",
    ],
  }),
  hotel({
    entityId: "jw_marriott_cancun_resort_spa",
    canonical: "JW Marriott Cancun Resort & Spa",
    market: "Cancún",
    geography: "hotel_zone",
    chainScale: "Luxury",
    propertyType: "luxury_beach_resort",
    aliases: [
      "jw marriott cancun resort & spa",
      "jw marriott cancun resort and spa",
      "jw marriott cancun resort",
      "jw marriott cancún resort",
      "jw marriott cancun",
    ],
  }),
  hotel({
    entityId: "grand_fiesta_americana_coral_beach_cancun",
    canonical: "Grand Fiesta Americana Coral Beach Cancún",
    market: "Cancún",
    geography: "hotel_zone",
    chainScale: "Luxury",
    propertyType: "all_inclusive_beach_resort",
    aliases: [
      "grand fiesta americana coral beach cancún",
      "grand fiesta americana coral beach cancun",
      "grand fiesta americana coral beach",
      "coral beach cancun",
      "coral beach cancún",
    ],
  }),
  hotel({
    entityId: "kempinski_hotel_cancun",
    canonical: "Kempinski Hotel Cancún",
    market: "Cancún",
    geography: "hotel_zone",
    chainScale: "Luxury",
    propertyType: "luxury_beach_resort",
    identityConfidence: "MEDIUM",
    aliases: [
      "kempinski hotel cancún",
      "kempinski hotel cancun",
      "kempinski cancun",
      "kempinski cancún",
      "kempinski hotel",
    ],
  }),
  hotel({
    entityId: "secrets_the_vine_cancun",
    canonical: "Secrets The Vine Cancun",
    market: "Cancún",
    geography: "hotel_zone",
    chainScale: "Upper Upscale",
    propertyType: "all_inclusive_beach_resort",
    identityConfidence: "MEDIUM",
    aliases: [
      "secrets the vine cancun",
      "secrets the vine cancún",
      "secrets vine cancun",
    ],
  }),
  hotel({
    entityId: "hyatt_zilara_cancun",
    canonical: "Hyatt Zilara Cancun",
    market: "Cancún",
    geography: "hotel_zone",
    chainScale: "Luxury",
    propertyType: "all_inclusive_beach_resort",
    identityConfidence: "MEDIUM",
    aliases: ["hyatt zilara cancun", "hyatt zilara cancún", "zilara cancun"],
  }),
  hotel({
    entityId: "marriott_cancun_resort",
    canonical: "Marriott Cancun Resort",
    market: "Cancún",
    geography: "hotel_zone",
    chainScale: "Upper Upscale",
    propertyType: "full_service_beach_resort",
    identityConfidence: "MEDIUM",
    aliases: [
      "marriott cancun resort",
      "marriott cancún resort",
      "cancun marriott resort",
    ],
  }),
]);

export const CANCUN_AI_RESORT_REGISTRY = createEntityRegistry({
  version: CANCUN_AI_RESORT_ENTITY_VERSION,
  hotels: CANCUN_AI_RESORT_CANONICAL_HOTELS,
  genericExact: [
    "beach resort",
    "all inclusive resort",
    "all-inclusive resort",
    "luxury resort",
    "this resort",
    "this hotel",
    "cancun hotel",
    "cancún hotel",
    "n resort",
  ],
  venueOrClub: ["hotel zone", "zona hotelera", "playa delfines", "punta nizuc"],
  brandOnly: [
    "le blanc",
    "live aqua",
    "nizuc",
    "jw marriott cancun",
    "kempinski",
    "secrets",
    "hyatt zilara",
  ],
  ambiguous: ["cancun resort", "cancún resort", "beach hotel cancun"],
});
