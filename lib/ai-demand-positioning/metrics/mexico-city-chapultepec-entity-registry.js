/**
 * Mexico City / Reforma–Chapultepec entity registry for Grand Fiesta Americana Chapultepec.
 * Evidence-backed from Core observation competitor extraction (252 responses).
 * Reuses Mexico City Reforma canonical hotels with Chapultepec as subject.
 */

import { createEntityRegistry } from "./adp-entity-registry-factory.js";
import { MEXICO_CITY_REFORMA_CANONICAL_HOTELS } from "./cala-six-entity-registries.js";

export const MEXICO_CITY_CHAPULTEPEC_ENTITY_VERSION =
  "adp_mexico_city_chapultepec_entity_v1";

function hotel(partial) {
  return Object.freeze({
    meetings: true,
    spa: false,
    identityConfidence: "HIGH",
    ...partial,
    aliases: Object.freeze(partial.aliases || []),
  });
}

/** Strip St. Regis subject flag — Chapultepec is the ADP subject for this registry. */
const MARKET_PEERS = MEXICO_CITY_REFORMA_CANONICAL_HOTELS.map((h) =>
  hotel({
    ...h,
    aliases: [...(h.aliases || [])],
    subject: false,
  })
);

export const MEXICO_CITY_CHAPULTEPEC_CANONICAL_HOTELS = Object.freeze([
  hotel({
    entityId: "grand_fiesta_americana_chapultepec",
    canonical: "Grand Fiesta Americana Chapultepec",
    market: "Mexico City",
    geography: "chapultepec_reforma",
    chainScale: "Luxury",
    propertyType: "luxury_full_service_hotel",
    spa: true,
    subject: true,
    aliases: [
      "grand fiesta americana chapultepec",
      "fiesta americana chapultepec",
      "grand fiesta americana chapultepec mexico city",
      "gfa chapultepec",
      "chapultepec grand fiesta americana",
    ],
  }),
  ...MARKET_PEERS,
  // Evidence-backed additions from Chapultepec Core archives (high mention frequency).
  hotel({
    entityId: "hyatt_regency_mexico_city",
    canonical: "Hyatt Regency Mexico City",
    market: "Mexico City",
    geography: "polanco",
    chainScale: "Upper Upscale",
    propertyType: "full_service_hotel",
    aliases: [
      "hyatt regency mexico city",
      "hyatt regency cdmx",
      "hyatt regency polanco",
    ],
  }),
  hotel({
    entityId: "camino_real_polanco_mexico_city",
    canonical: "Camino Real Polanco Mexico City",
    market: "Mexico City",
    geography: "polanco",
    chainScale: "Upper Upscale",
    propertyType: "full_service_hotel",
    aliases: [
      "camino real polanco mexico city",
      "camino real polanco",
      "camino real mexico city polanco",
      "hotel camino real polanco",
    ],
  }),
  hotel({
    entityId: "hilton_mexico_city_reforma",
    canonical: "Hilton Mexico City Reforma",
    market: "Mexico City",
    geography: "paseo_de_la_reforma",
    chainScale: "Upper Upscale",
    propertyType: "full_service_hotel",
    aliases: [
      "hilton mexico city reforma",
      "hilton reforma mexico city",
      "hilton reforma",
    ],
  }),
]);

export const MEXICO_CITY_CHAPULTEPEC_REGISTRY = createEntityRegistry({
  version: MEXICO_CITY_CHAPULTEPEC_ENTITY_VERSION,
  hotels: MEXICO_CITY_CHAPULTEPEC_CANONICAL_HOTELS,
  genericExact: [
    "boutique hotel",
    "luxury hotel",
    "business hotel",
    "mexico city hotel",
    "this hotel",
    "rooms/suites:",
    "rooms/suites",
    "spacious suites",
    "executive suites",
    "boutique suites",
  ],
  // Do NOT put bare "chapultepec" here — it would fight subject hotel aliases.
  venueOrClub: [
    "angel de la independencia",
    "bosque de chapultepec",
    "polanco",
    "zona rosa",
    "king cole bar",
    "diana restaurant",
  ],
  brandOnly: [
    "st. regis mexico",
    "four seasons mexico",
    "ritz-carlton mexico",
    "jw marriott mexico",
    "hyatt regency mexico",
    "hilton mexico",
    "camino real",
  ],
  ambiguous: ["reforma hotel", "hotel reforma", "the st. regis", "jw marriott hotel"],
});
