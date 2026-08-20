/**
 * Territory-specific CORE competitive universes — RESEARCH ONLY.
 * Declared competitors are evidence, not automatic CORE.
 * AI mention frequency is never sufficient for CORE.
 */

import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";
import { territoryLabelForIntent } from "./intent-territory-labels.js";
import {
  SOUTH_FLORIDA_CANONICAL_HOTELS,
  canonicalizeToEntityId,
  classifyObservedEntity,
} from "./south-florida-entity-registry.js";
import { isGovernedNonWaterstoneProperty } from "./property-core-governance-data.js";
import { buildPropertyTerritoryBenchmarkSets } from "./property-specific-core-benchmark-governance-v1.js";

export const BENCHMARK_SET_VERSION = "adp_aci_benchmark_set_v2";
export const MIN_CORE_COMPETITORS = 3;

export const COMPETITIVE_CLASSES = Object.freeze({
  CORE_COMPETITOR: "CORE_COMPETITOR",
  SECONDARY_ALTERNATIVE: "SECONDARY_ALTERNATIVE",
  CONDITIONAL: "CONDITIONAL",
  NON_COMPARABLE: "NON_COMPARABLE",
  OBSERVED_ONLY_UNVALIDATED: "OBSERVED_ONLY_UNVALIDATED",
});

const ALL = Object.values(TRAVELER_INTENTS);

/**
 * Governed role overrides: entityId -> intent -> class.
 * Default fallback uses geography + property type + chain scale vs subject.
 */
const WATERSTONE_ROLE_OVERRIDES = Object.freeze({
  the_boca_raton: {
    [TRAVELER_INTENTS.BUSINESS]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.LEISURE]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.COUPLES]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.FAMILY]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.GROUP_MEETING]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.WELLNESS]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.ADVENTURE]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.CELEBRATION]: "CORE_COMPETITOR",
  },
  boca_beach_club: {
    [TRAVELER_INTENTS.BUSINESS]: "CONDITIONAL",
    [TRAVELER_INTENTS.LEISURE]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.COUPLES]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.FAMILY]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.GROUP_MEETING]: "CONDITIONAL",
    [TRAVELER_INTENTS.WELLNESS]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.ADVENTURE]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.CELEBRATION]: "CORE_COMPETITOR",
  },
  renaissance_boca_raton: {
    [TRAVELER_INTENTS.BUSINESS]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.LEISURE]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.COUPLES]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.FAMILY]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.GROUP_MEETING]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.WELLNESS]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.ADVENTURE]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.CELEBRATION]: "SECONDARY_ALTERNATIVE",
  },
  marriott_boca_raton: {
    [TRAVELER_INTENTS.BUSINESS]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.LEISURE]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.COUPLES]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.FAMILY]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.GROUP_MEETING]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.WELLNESS]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.ADVENTURE]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.CELEBRATION]: "SECONDARY_ALTERNATIVE",
  },
  wyndham_boca_raton: {
    [TRAVELER_INTENTS.BUSINESS]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.LEISURE]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.COUPLES]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.FAMILY]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.GROUP_MEETING]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.WELLNESS]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.ADVENTURE]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.CELEBRATION]: "NON_COMPARABLE",
  },
  hilton_boca_raton_suites: {
    [TRAVELER_INTENTS.BUSINESS]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.LEISURE]: "CONDITIONAL",
    [TRAVELER_INTENTS.COUPLES]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.FAMILY]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.GROUP_MEETING]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.WELLNESS]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.ADVENTURE]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.CELEBRATION]: "NON_COMPARABLE",
  },
  embassy_suites_boca: {
    [TRAVELER_INTENTS.BUSINESS]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.LEISURE]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.COUPLES]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.FAMILY]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.GROUP_MEETING]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.WELLNESS]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.ADVENTURE]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.CELEBRATION]: "NON_COMPARABLE",
  },
  seagate_hotel_delray: {
    [TRAVELER_INTENTS.BUSINESS]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.LEISURE]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.COUPLES]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.FAMILY]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.GROUP_MEETING]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.WELLNESS]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.ADVENTURE]: "CONDITIONAL",
    [TRAVELER_INTENTS.CELEBRATION]: "CORE_COMPETITOR",
  },
  delray_sands_resort: {
    [TRAVELER_INTENTS.BUSINESS]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.LEISURE]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.COUPLES]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.FAMILY]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.GROUP_MEETING]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.WELLNESS]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.ADVENTURE]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.CELEBRATION]: "SECONDARY_ALTERNATIVE",
  },
  colony_hotel_delray: {
    [TRAVELER_INTENTS.BUSINESS]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.LEISURE]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.COUPLES]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.FAMILY]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.GROUP_MEETING]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.WELLNESS]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.ADVENTURE]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.CELEBRATION]: "SECONDARY_ALTERNATIVE",
  },
  the_ray_hotel: {
    [TRAVELER_INTENTS.BUSINESS]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.LEISURE]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.COUPLES]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.FAMILY]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.GROUP_MEETING]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.WELLNESS]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.ADVENTURE]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.CELEBRATION]: "CONDITIONAL",
  },
  opal_grand_delray: {
    [TRAVELER_INTENTS.BUSINESS]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.LEISURE]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.COUPLES]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.FAMILY]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.GROUP_MEETING]: "CONDITIONAL",
    [TRAVELER_INTENTS.WELLNESS]: "CORE_COMPETITOR",
    [TRAVELER_INTENTS.ADVENTURE]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.CELEBRATION]: "CORE_COMPETITOR",
  },
  eau_palm_beach: {
    [TRAVELER_INTENTS.BUSINESS]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.LEISURE]: "CONDITIONAL",
    [TRAVELER_INTENTS.COUPLES]: "CONDITIONAL",
    [TRAVELER_INTENTS.FAMILY]: "CONDITIONAL",
    [TRAVELER_INTENTS.GROUP_MEETING]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.WELLNESS]: "CONDITIONAL",
    [TRAVELER_INTENTS.ADVENTURE]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.CELEBRATION]: "CONDITIONAL",
  },
  four_seasons_palm_beach: Object.fromEntries(ALL.map((i) => [i, "NON_COMPARABLE"])),
  acqualina: Object.fromEntries(ALL.map((i) => [i, "NON_COMPARABLE"])),
  hawks_cay: Object.fromEntries(ALL.map((i) => [i, "NON_COMPARABLE"])),
  cheeca_lodge: Object.fromEntries(ALL.map((i) => [i, "NON_COMPARABLE"])),
  loews_miami_beach: Object.fromEntries(ALL.map((i) => [i, "NON_COMPARABLE"])),
  faena_miami: Object.fromEntries(ALL.map((i) => [i, "NON_COMPARABLE"])),
  carillon_miami: {
    [TRAVELER_INTENTS.WELLNESS]: "CONDITIONAL",
    [TRAVELER_INTENTS.LEISURE]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.BUSINESS]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.COUPLES]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.FAMILY]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.GROUP_MEETING]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.ADVENTURE]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.CELEBRATION]: "NON_COMPARABLE",
  },
  diplomat_beach_resort: {
    [TRAVELER_INTENTS.BUSINESS]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.LEISURE]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.COUPLES]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.FAMILY]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.GROUP_MEETING]: "CONDITIONAL",
    [TRAVELER_INTENTS.WELLNESS]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.ADVENTURE]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.CELEBRATION]: "SECONDARY_ALTERNATIVE",
  },
  pelican_grand: {
    [TRAVELER_INTENTS.LEISURE]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.FAMILY]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.COUPLES]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.ADVENTURE]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.BUSINESS]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.GROUP_MEETING]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.WELLNESS]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.CELEBRATION]: "NON_COMPARABLE",
  },
  harbor_beach_marriott: {
    [TRAVELER_INTENTS.LEISURE]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.GROUP_MEETING]: "CONDITIONAL",
    [TRAVELER_INTENTS.FAMILY]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.BUSINESS]: "NON_COMPARABLE",
    [TRAVELER_INTENTS.COUPLES]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.WELLNESS]: "CONDITIONAL",
    [TRAVELER_INTENTS.ADVENTURE]: "SECONDARY_ALTERNATIVE",
    [TRAVELER_INTENTS.CELEBRATION]: "CONDITIONAL",
  },
});

function defaultRole(hotel, intent) {
  if (!hotel) return COMPETITIVE_CLASSES.OBSERVED_ONLY_UNVALIDATED;
  if (hotel.geography === "distant_florida") return COMPETITIVE_CLASSES.NON_COMPARABLE;
  if (hotel.chainScale === "Luxury" && hotel.geography !== "boca_local") {
    return COMPETITIVE_CLASSES.CONDITIONAL;
  }
  if (hotel.geography === "south_florida_coast") return COMPETITIVE_CLASSES.SECONDARY_ALTERNATIVE;
  if (hotel.geography === "boca_local" || hotel.geography === "palm_beach_county") {
    if (intent === TRAVELER_INTENTS.BUSINESS || intent === TRAVELER_INTENTS.GROUP_MEETING) {
      return hotel.meetings ? COMPETITIVE_CLASSES.CORE_COMPETITOR : COMPETITIVE_CLASSES.SECONDARY_ALTERNATIVE;
    }
    return COMPETITIVE_CLASSES.SECONDARY_ALTERNATIVE;
  }
  return COMPETITIVE_CLASSES.OBSERVED_ONLY_UNVALIDATED;
}

export function classifyCandidateForTerritory(entityId, intent) {
  const hotel = SOUTH_FLORIDA_CANONICAL_HOTELS.find((h) => h.entityId === entityId);
  const override = WATERSTONE_ROLE_OVERRIDES[entityId]?.[intent];
  const role = override || defaultRole(hotel, intent);
  return { entityId, intent, role, hotel: hotel || null };
}

export function isDeclaredName(name, propertyProfile) {
  const declared = propertyProfile?.declaredCompSet || [];
  const id = canonicalizeToEntityId(name);
  if (id && declared.some((d) => canonicalizeToEntityId(d) === id)) return true;
  const low = String(name || "").toLowerCase();
  return declared.some((d) => d.toLowerCase() === low || d.toLowerCase().includes(low) || low.includes(d.toLowerCase()));
}

export function buildTerritoryBenchmarkSets(propertyProfile, observedNames) {
  if (isGovernedNonWaterstoneProperty(propertyProfile?.propertyId)) {
    return buildPropertyTerritoryBenchmarkSets(propertyProfile, observedNames);
  }

  const intents = [...new Set(Object.values(TRAVELER_INTENTS))];
  const declaredIds = (propertyProfile.declaredCompSet || [])
    .map((d) => canonicalizeToEntityId(d))
    .filter(Boolean);
  const observedIds = [...new Set((observedNames || []).map((n) => canonicalizeToEntityId(n)).filter(Boolean))];
  const candidateIds = [...new Set([...declaredIds, ...observedIds, ...SOUTH_FLORIDA_CANONICAL_HOTELS.map((h) => h.entityId)])];

  const byIntent = {};
  for (const intent of intents) {
    const classified = candidateIds.map((id) => classifyCandidateForTerritory(id, intent));
    const pick = (role) => classified.filter((c) => c.role === role);
    const core = pick(COMPETITIVE_CLASSES.CORE_COMPETITOR);
    const secondary = pick(COMPETITIVE_CLASSES.SECONDARY_ALTERNATIVE);
    const conditional = pick(COMPETITIVE_CLASSES.CONDITIONAL);
    const nonComparable = pick(COMPETITIVE_CLASSES.NON_COMPARABLE);

    const declaredCore = declaredIds.filter((id) => core.some((c) => c.entityId === id));
    const declaredSecondary = declaredIds.filter((id) => secondary.some((c) => c.entityId === id));
    const declaredNon = declaredIds.filter((id) => nonComparable.some((c) => c.entityId === id) || conditional.some((c) => c.entityId === id && !core.some((x) => x.entityId === id) && !secondary.some((x) => x.entityId === id)));
    const observedCore = observedIds.filter((id) => core.some((c) => c.entityId === id) && !declaredIds.includes(id));
    const observedSecondary = observedIds.filter((id) => secondary.some((c) => c.entityId === id) && !declaredIds.includes(id));

    byIntent[intent] = {
      territory: territoryLabelForIntent(intent),
      intent,
      coreCompetitors: core.map((c) => c.hotel?.canonical || c.entityId),
      coreIds: core.map((c) => c.entityId),
      coreCount: core.length,
      secondaryAlternatives: secondary.map((c) => c.hotel?.canonical || c.entityId),
      secondaryCount: secondary.length,
      conditional: conditional.map((c) => c.hotel?.canonical || c.entityId),
      conditionalCount: conditional.length,
      nonComparable: nonComparable.map((c) => c.hotel?.canonical || c.entityId),
      observedOnlyUnvalidated: (observedNames || []).filter((n) => !canonicalizeToEntityId(n)).length,
      declaredCore: declaredCore.length,
      declaredSecondary: declaredSecondary.length,
      declaredNonComparable: declaredNon.length,
      observedCore: observedCore.length,
      observedSecondary: observedSecondary.length,
      observedOnly: (observedNames || []).filter((n) => classifyObservedEntity(n).class !== "CANONICAL_HOTEL" && classifyObservedEntity(n).class !== "DUPLICATE_ALIAS").length,
      certifiableUniverse: core.length >= MIN_CORE_COMPETITORS,
      minCoreResearch: {
        at3: core.length >= 3,
        at4: core.length >= 4,
        at5: core.length >= 5,
        productionEnough: core.length >= 4,
      },
    };
  }

  return {
    version: BENCHMARK_SET_VERSION,
    propertyId: propertyProfile.propertyId,
    minCoreCompetitors: MIN_CORE_COMPETITORS,
    byIntent,
  };
}

export { WATERSTONE_ROLE_OVERRIDES };
