/**
 * Frozen Waterstone CORE benchmark set — Presence Index V2 + future ACI.
 * RESEARCH ONLY. Explicit lists; no defaultRole leakage from the full registry.
 */

import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";
import { SOUTH_FLORIDA_CANONICAL_HOTELS } from "./south-florida-entity-registry.js";
import { ENTITY_RESOLUTION_VERSION } from "./south-florida-entity-registry.js";
import { ELIGIBILITY_VERSION } from "./aci-research-engine.js";
import {
  isGovernedNonWaterstoneProperty,
  stabilizedCoreIdsForProperty,
} from "./property-core-governance-data.js";
import { registryHotelById } from "./adp-property-entity-registries.js";

export const PRESENCE_BENCHMARK_VERSION = "adp_presence_benchmark_v1";
export const PRESENCE_INDEX_V2_METRIC_VERSION = "adp_presence_index_v2";
export const MIN_CORE_PEERS_RESEARCH = 3;
export const MIN_CORE_PEERS_PRODUCTION = 4;
export const MIN_PROVIDER_OBSERVATIONS = 5;

export const STABILIZED_CORE_IDS = Object.freeze({
  [TRAVELER_INTENTS.BUSINESS]: [
    "renaissance_boca_raton",
    "wyndham_boca_raton",
    "marriott_boca_raton",
    "hilton_boca_raton_suites",
    "the_boca_raton",
    "embassy_suites_boca",
  ],
  [TRAVELER_INTENTS.LEISURE]: [
    "the_boca_raton",
    "boca_beach_club",
    "seagate_hotel_delray",
    "opal_grand_delray",
    "delray_sands_resort",
    "the_ray_hotel",
  ],
  [TRAVELER_INTENTS.COUPLES]: [
    "the_boca_raton",
    "boca_beach_club",
    "seagate_hotel_delray",
    "opal_grand_delray",
    "delray_sands_resort",
    "the_ray_hotel",
    "colony_hotel_delray",
  ],
  [TRAVELER_INTENTS.FAMILY]: [
    "hilton_boca_raton_suites",
    "the_boca_raton",
    "opal_grand_delray",
    "delray_sands_resort",
    "embassy_suites_boca",
    "boca_beach_club",
  ],
  [TRAVELER_INTENTS.GROUP_MEETING]: [
    "renaissance_boca_raton",
    "wyndham_boca_raton",
    "marriott_boca_raton",
    "the_boca_raton",
  ],
  [TRAVELER_INTENTS.WELLNESS]: [
    "the_boca_raton",
    "boca_beach_club",
    "seagate_hotel_delray",
    "opal_grand_delray",
  ],
  [TRAVELER_INTENTS.ADVENTURE]: [
    "the_boca_raton",
    "boca_beach_club",
    "delray_sands_resort",
  ],
  [TRAVELER_INTENTS.CELEBRATION]: [
    "the_boca_raton",
    "boca_beach_club",
    "seagate_hotel_delray",
    "opal_grand_delray",
  ],
});

/** Audit-v2 CORE counts before explicit freeze (leaky registry scan). */
export const OLD_CORE_COUNTS = Object.freeze({
  business: 7,
  leisure: 7,
  couples: 7,
  family: 6,
  group_meeting: 4,
  wellness: 4,
  adventure: 3,
  celebration: 4,
});

export const CORE_REMEDIATION = Object.freeze({
  business: {
    removed: [],
    downgraded: ["seagate_hotel_delray"],
    added: [],
    rationale: "Seagate is Delray boutique; Boca business-trip substitutability is weak vs in-city upper-upscale hotels.",
  },
  leisure: {
    removed: [],
    downgraded: ["colony_hotel_delray"],
    added: [],
    rationale: "Colony is a historic inn, not a resort; keep for Couples, not Resort Leisure CORE.",
  },
  couples: { removed: [], downgraded: [], added: [], rationale: "Colony retained for boutique romantic stays." },
  family: { removed: [], downgraded: [], added: [], rationale: "No change. Suites + beach/resort CORE retained." },
  group_meeting: {
    removed: [],
    downgraded: [],
    added: [],
    rationale: "Beach Club stays CONDITIONAL (not the meetings venue). Do not pad with Delray spas.",
  },
  wellness: { removed: [], downgraded: [], added: [], rationale: "Spa-capable Palm Beach County set only. Eau stays CONDITIONAL (luxury / Manalapan)." },
  adventure: {
    removed: [],
    downgraded: [],
    added: [],
    rationale: "Do not force a fourth peer. Pelican Grand / Harbor Beach are Fort Lauderdale, not Boca adventure substitutes.",
  },
  celebration: { removed: [], downgraded: [], added: [], rationale: "Event-capable Boca + nearby spa/beach resorts only." },
});

export function hotelById(entityId, propertyProfile = null) {
  const propertyId = propertyProfile?.propertyId;
  if (propertyId && isGovernedNonWaterstoneProperty(propertyId)) {
    return registryHotelById(propertyId, entityId);
  }
  return SOUTH_FLORIDA_CANONICAL_HOTELS.find((h) => h.entityId === entityId) || null;
}

export function coreIdsForIntent(intent, propertyProfile = null) {
  const propertyId = propertyProfile?.propertyId;
  if (propertyId && isGovernedNonWaterstoneProperty(propertyId)) {
    return stabilizedCoreIdsForProperty(propertyId, intent);
  }
  return [...(STABILIZED_CORE_IDS[intent] || [])];
}

export function assertCoreSetIntegrity(coreIds, propertyProfile = null) {
  const ids = coreIds || [];
  const unique = new Set(ids);
  const hotels = ids.map((id) => hotelById(id, propertyProfile));
  return {
    DUPLICATE_CORE_ENTITIES: ids.length - unique.size,
    GENERIC_CORE_ENTITIES: hotels.filter((h) => !h).length,
    BRAND_ONLY_CORE_ENTITIES: 0,
    allCanonical: hotels.every((h) => h && h.identityConfidence !== "LOW"),
  };
}

export function benchmarkVersions() {
  return {
    PRESENCE_BENCHMARK_VERSION,
    ENTITY_RESOLUTION_VERSION,
    ELIGIBILITY_VERSION,
    PRESENCE_INDEX_V2_METRIC_VERSION,
  };
}

export function coreRelationshipAudit() {
  const rows = [];
  function row(intent, entityId, declared, decision, rationale, flags) {
    const h = hotelById(entityId);
    rows.push({
      intent,
      CANDIDATE: h?.canonical || entityId,
      entityId,
      DECLARED_OR_OBSERVED: declared,
      IDENTITY_VALID: Boolean(h && h.identityConfidence !== "LOW"),
      GEOGRAPHIC_SUBSTITUTABILITY: flags.geo,
      POSITIONING_RELEVANCE: flags.pos,
      PROPERTY_TYPE_RELEVANCE: flags.type,
      TERRITORY_RELEVANCE: flags.terr,
      RECURRENCE: "NOT_USED_FOR_CORE_DECISION",
      CORE_DECISION: decision,
      RATIONALE: rationale,
    });
  }

  row("business", "renaissance_boca_raton", "DECLARED", "KEEP", "In-city upper-upscale meetings hotel.", { geo: "HIGH", pos: "HIGH", type: "HIGH", terr: "HIGH" });
  row("business", "marriott_boca_raton", "DECLARED", "KEEP", "In-city upper-upscale meetings hotel.", { geo: "HIGH", pos: "HIGH", type: "HIGH", terr: "HIGH" });
  row("business", "wyndham_boca_raton", "DECLARED", "KEEP", "In-city upscale business hotel; weaker positioning than subject but substitutable.", { geo: "HIGH", pos: "MEDIUM", type: "HIGH", terr: "HIGH" });
  row("business", "hilton_boca_raton_suites", "DECLARED", "KEEP", "In-city all-suites; business/extended-stay substitute.", { geo: "HIGH", pos: "MEDIUM", type: "MEDIUM", terr: "HIGH" });
  row("business", "the_boca_raton", "DECLARED", "KEEP", "Primary Boca alternative; luxury/scale mismatch but same destination meetings set.", { geo: "HIGH", pos: "MEDIUM", type: "MEDIUM", terr: "HIGH" });
  row("business", "embassy_suites_boca", "OBSERVED", "KEEP", "In-city all-suites; identity MEDIUM but canonical Boca property.", { geo: "HIGH", pos: "MEDIUM", type: "MEDIUM", terr: "HIGH" });
  row("business", "seagate_hotel_delray", "OBSERVED", "DOWNGRADE", "Delray boutique; not a practical Boca business-trip substitute.", { geo: "MEDIUM", pos: "MEDIUM", type: "MEDIUM", terr: "LOW" });

  row("leisure", "the_boca_raton", "DECLARED", "KEEP", "Destination resort in same city.", { geo: "HIGH", pos: "MEDIUM", type: "HIGH", terr: "HIGH" });
  row("leisure", "boca_beach_club", "DECLARED", "KEEP", "Oceanfront Boca leisure substitute; subject is Intracoastal not beach.", { geo: "HIGH", pos: "HIGH", type: "HIGH", terr: "HIGH" });
  row("leisure", "seagate_hotel_delray", "OBSERVED", "KEEP", "Nearby upper-upscale boutique leisure.", { geo: "HIGH", pos: "HIGH", type: "HIGH", terr: "HIGH" });
  row("leisure", "opal_grand_delray", "OBSERVED", "KEEP", "Nearby oceanfront resort.", { geo: "HIGH", pos: "HIGH", type: "HIGH", terr: "HIGH" });
  row("leisure", "delray_sands_resort", "OBSERVED", "KEEP", "Nearby beach resort.", { geo: "HIGH", pos: "MEDIUM", type: "HIGH", terr: "HIGH" });
  row("leisure", "the_ray_hotel", "OBSERVED", "KEEP", "Nearby boutique leisure hotel.", { geo: "HIGH", pos: "HIGH", type: "MEDIUM", terr: "HIGH" });
  row("leisure", "colony_hotel_delray", "OBSERVED", "DOWNGRADE", "Historic inn, not resort positioning for Resort Leisure CORE.", { geo: "HIGH", pos: "MEDIUM", type: "LOW", terr: "MEDIUM" });

  row("couples", "colony_hotel_delray", "OBSERVED", "KEEP", "Boutique inn is relevant for romantic stays.", { geo: "HIGH", pos: "HIGH", type: "HIGH", terr: "HIGH" });
  row("family", "hilton_boca_raton_suites", "DECLARED", "KEEP", "Suites fit family trips.", { geo: "HIGH", pos: "MEDIUM", type: "HIGH", terr: "HIGH" });
  row("group_meeting", "boca_beach_club", "DECLARED", "DOWNGRADE", "Already CONDITIONAL — beach club is not the meetings plant.", { geo: "HIGH", pos: "LOW", type: "LOW", terr: "LOW" });
  row("adventure", "pelican_grand", "OBSERVED", "REMOVE", "Fort Lauderdale; do not pad Adventure to 4.", { geo: "LOW", pos: "MEDIUM", type: "HIGH", terr: "MEDIUM" });
  row("wellness", "eau_palm_beach", "OBSERVED", "DOWNGRADE", "Luxury Manalapan; CONDITIONAL not CORE.", { geo: "MEDIUM", pos: "LOW", type: "HIGH", terr: "MEDIUM" });

  return rows;
}
