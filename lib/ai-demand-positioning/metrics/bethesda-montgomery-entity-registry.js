/**
 * Bethesda / Montgomery County entity registry — subject WASBT + peers.
 * Bethesda North (WASBN) is a DISTINCT_ENTITY peer, never a subject alias.
 */

import { createEntityRegistry } from "./adp-entity-registry-factory.js";
import {
  BETHESDA_ENTITY_ID,
  BETHESDA_NORTH_ENTITY_ID,
} from "../execution/bethesda-marriott-foundation-v1.js";

export const BETHESDA_MONTGOMERY_ENTITY_VERSION = "adp_bethesda_montgomery_entity_registry_v1";

export const BETHESDA_MONTGOMERY_CANONICAL_HOTELS = Object.freeze([
  {
    entityId: BETHESDA_ENTITY_ID,
    canonical: "Bethesda Marriott",
    market: "Bethesda / Montgomery County",
    geography: "pooks_hill",
    chainScale: "Upper Upscale",
    propertyType: "full_service_hotel",
    identityConfidence: "HIGH",
    meetings: true,
    spa: false,
    subject: true,
    aliases: [
      "bethesda marriott",
      "bethesda marriott hotel",
      // Intentionally omit short "marriott bethesda" — collides with Downtown / Residence Inn.
      "bethesda marriott at pooks hill",
      "bethesda marriott pooks hill",
      "wasbt",
    ],
  },
  {
    entityId: "hyatt_regency_bethesda",
    canonical: "Hyatt Regency Bethesda",
    market: "Bethesda / Montgomery County",
    geography: "downtown_bethesda",
    chainScale: "Upper Upscale",
    propertyType: "full_service_hotel",
    identityConfidence: "HIGH",
    meetings: true,
    spa: false,
    aliases: ["hyatt regency bethesda", "hyatt bethesda", "bethesda hyatt regency"],
  },
  {
    entityId: "the_bethesdan_hotel",
    canonical: "The Bethesdan Hotel, Tapestry Collection by Hilton",
    market: "Bethesda / Montgomery County",
    geography: "downtown_bethesda",
    chainScale: "Upper Upscale",
    propertyType: "lifestyle_boutique_hotel",
    identityConfidence: "HIGH",
    meetings: true,
    spa: false,
    aliases: [
      "the bethesdan hotel",
      "the bethesdan",
      "bethesdan hotel",
      "bethesdan tapestry",
      "the bethesdan hotel tapestry collection",
      "the bethesdan hotel tapestry collection by hilton",
      "the bethesdan hotel (tapestry collection by hilton)",
    ],
  },
  {
    entityId: "marriott_bethesda_downtown",
    canonical: "Marriott Bethesda Downtown at Marriott HQ",
    market: "Bethesda / Montgomery County",
    geography: "downtown_bethesda",
    chainScale: "Upper Upscale",
    propertyType: "full_service_hotel",
    identityConfidence: "HIGH",
    meetings: true,
    spa: false,
    aliases: [
      "marriott bethesda downtown",
      "marriott bethesda downtown at marriott hq",
      "the hotel marriott bethesda downtown at marriott hq",
      "marriott hq bethesda",
      "wasbd",
    ],
  },
  {
    entityId: BETHESDA_NORTH_ENTITY_ID,
    canonical: "Bethesda North Marriott Hotel & Conference Center",
    market: "Bethesda / Montgomery County",
    geography: "north_bethesda",
    chainScale: "Upper Upscale",
    propertyType: "conference_center_hotel",
    identityConfidence: "HIGH",
    meetings: true,
    spa: false,
    distinctFromSubject: true,
    aliases: [
      "bethesda north marriott hotel & conference center",
      "the bethesda north marriott hotel & conference center",
      "bethesda north marriott",
      "bethesda north marriott hotel and conference center",
      "marriott bethesda north",
      "wasbn",
    ],
  },
  {
    entityId: "ac_hotel_bethesda_downtown",
    canonical: "AC Hotel Bethesda Downtown",
    market: "Bethesda / Montgomery County",
    geography: "downtown_bethesda",
    chainScale: "Upscale",
    propertyType: "select_service_lifestyle",
    identityConfidence: "HIGH",
    meetings: false,
    spa: false,
    aliases: ["ac hotel bethesda downtown", "ac hotel bethesda", "ac bethesda"],
  },
  {
    entityId: "hilton_garden_inn_bethesda",
    canonical: "Hilton Garden Inn Bethesda Downtown",
    market: "Bethesda / Montgomery County",
    geography: "downtown_bethesda",
    chainScale: "Upscale",
    propertyType: "select_service_hotel",
    identityConfidence: "HIGH",
    meetings: true,
    spa: false,
    aliases: [
      "hilton garden inn bethesda downtown",
      "hilton garden inn bethesda",
      "hilton garden inn",
      "hgi bethesda",
    ],
  },
  {
    entityId: "residence_inn_bethesda_downtown",
    canonical: "Residence Inn Bethesda Downtown",
    market: "Bethesda / Montgomery County",
    geography: "downtown_bethesda",
    chainScale: "Upscale",
    propertyType: "extended_stay",
    identityConfidence: "HIGH",
    meetings: false,
    spa: false,
    aliases: [
      "residence inn bethesda downtown",
      "residence inn by marriott bethesda downtown",
      "residence inn bethesda",
      "residence inn by marriott bethesda",
    ],
  },
]);

export const BETHESDA_MONTGOMERY_REGISTRY = createEntityRegistry({
  version: BETHESDA_MONTGOMERY_ENTITY_VERSION,
  hotels: BETHESDA_MONTGOMERY_CANONICAL_HOTELS,
  genericExact: [
    "boutique hotel",
    "full-service hotel",
    "upscale hotel",
    "bethesda hotel",
    "montgomery county hotel",
    "this hotel",
    "marriott hotels",
    "marriott bonvoy",
  ],
  venueOrClub: ["nih", "walter reed", "downtown bethesda", "rockville pike"],
  brandOnly: [
    "hilton bethesda",
    "hyatt bethesda",
    "marriott montgomery county",
  ],
  ambiguous: [],
});

/**
 * Regression: Bethesda North must never canonicalize to subject WASBT.
 * @returns {{ pass: boolean, subjectId: string|null, northId: string|null, rule: string }}
 */
export function assertBethesdaMarriottVsNorthDistinctEntity() {
  const rule = "BETHESDA_MARRIOTT_VS_BETHESDA_NORTH_DISTINCT_ENTITY";
  const subjectId = BETHESDA_MONTGOMERY_REGISTRY.canonicalizeToEntityId("Bethesda Marriott");
  const northId = BETHESDA_MONTGOMERY_REGISTRY.canonicalizeToEntityId(
    "Bethesda North Marriott Hotel & Conference Center"
  );
  const northAlt = BETHESDA_MONTGOMERY_REGISTRY.canonicalizeToEntityId("Bethesda North Marriott");
  const pass =
    subjectId === BETHESDA_ENTITY_ID &&
    northId === BETHESDA_NORTH_ENTITY_ID &&
    northAlt === BETHESDA_NORTH_ENTITY_ID &&
    subjectId !== northId;

  return {
    pass,
    rule,
    subjectId,
    northId,
    northAlt,
    distinctEntity: pass ? "DISTINCT_ENTITY" : "MERGE_RISK",
  };
}
