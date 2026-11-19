/**
 * P3A six-hotel evaluation cohort — exact HPC IDs from frozen pilot manifest.
 */

import { loadPilotManifest, resolvePilotHotel } from "../pilot-manifest.js";
import { BENCHMARK_CONTRACT_ID, BENCHMARK_CONTRACT_VERSION } from "./contract.js";

/** Founder-ordered first six hotels for P3A evaluation. */
export const P3A_SIX_HOTEL_SPECS = Object.freeze([
  { order: 1, hpcId: "recLuxvwwxID7U2B8", expectedName: "Bethesda Marriott" },
  { order: 2, hpcId: "recnPB3Q02NO8Ma1o", expectedName: "Wyndham San Diego Bayside" },
  {
    order: 3,
    hpcId: "recG66DQJKP2c0UNh",
    expectedName: "Renaissance New York Times Square Hotel",
  },
  {
    order: 4,
    hpcId: "recvVZ5unh4d3Krzi",
    expectedName: "The Ritz-Carlton, Mexico City",
  },
  {
    order: 5,
    hpcId: "recBJZwMvgbdG7y4F",
    expectedName: "Hyatt Centric Playa del Carmen",
    dualHouse: true,
  },
  { order: 6, hpcId: "recrTWMTHXQpA5geC", expectedName: "Bruma Wine Resort" },
]);

export const P3A_SIX_HPC_IDS = Object.freeze(
  P3A_SIX_HOTEL_SPECS.map((h) => h.hpcId)
);

export function loadP3ASixHotelCohort(opts = {}) {
  const manifest = opts.manifest || loadPilotManifest(opts);
  const hotels = [];
  const errors = [];

  for (const spec of P3A_SIX_HOTEL_SPECS) {
    const resolved = resolvePilotHotel(spec.hpcId, manifest);
    if (!resolved.ok) {
      errors.push({ hpcId: spec.hpcId, reason: resolved.reason });
      continue;
    }
    const primary = resolved.primary;
    hotels.push({
      order: spec.order,
      hpcId: spec.hpcId,
      canonicalHpcId: resolved.canonicalHpcId,
      name: primary.name,
      expectedName: spec.expectedName,
      cohort: primary.cohort || null,
      dualHouse: Boolean(spec.dualHouse || resolved.dualHouse),
      founderExperienceCount: resolved.founderExperienceCount,
      manifestEntries: resolved.entries,
    });
  }

  const distinct = new Set(hotels.map((h) => h.hpcId));
  return {
    ok: errors.length === 0 && hotels.length === 6 && distinct.size === 6,
    contract: BENCHMARK_CONTRACT_ID,
    contract_version: BENCHMARK_CONTRACT_VERSION,
    cohortId: "HE-INTEL-001-P3A-SIX-HOTEL-V1",
    fullPilotDistinctHotels: Number(manifest.hpcDistinctRecordCount || 15),
    fullPilotFounderExperiences: Number(manifest.founderPropertyCount || 16),
    hotels,
    hpcIds: hotels.map((h) => h.hpcId),
    errors,
    manifestId: manifest.manifestId,
  };
}

/**
 * Mode A seed — limited hotel identity only (no owner/operator ground truth).
 */
export function buildIndependentDiscoverySeed(hotel) {
  const primary = hotel.manifestEntries?.[0] || hotel;
  return {
    mode: "INDEPENDENT_DISCOVERY",
    hpcId: hotel.hpcId,
    propertyName: primary.name || hotel.name,
    brand: primary.brand || null,
    city: primary.city || null,
    stateRegion: primary.stateRegion || primary.state || null,
    country: primary.country || null,
    market: primary.market || null,
    address: primary.address || null,
    officialPropertyUrl: primary.officialPropertyUrl || primary.sources?.[0] || null,
    lat: primary.lat ?? primary.coords?.lat ?? null,
    lng: primary.lng ?? primary.coords?.lng ?? null,
    excludedFromSeed: [
      "owner",
      "operator",
      "contacts",
      "golden_demo",
      "prior_research_packages",
    ],
  };
}

/**
 * Mode B seed — frozen verified prior snapshot (fixture/staging only).
 * Gaps remain research targets; known priors tagged KNOWN_PRIOR.
 */
export function buildIncrementalProductionSeed(hotel, priorSnapshot = null) {
  const base = buildIndependentDiscoverySeed(hotel);
  const priors = Array.isArray(priorSnapshot?.verifiedFacts)
    ? priorSnapshot.verifiedFacts
    : [];
  return {
    ...base,
    mode: "INCREMENTAL_PRODUCTION",
    known_prior_facts: priors.map((f) => ({
      ...f,
      provenance_tag: "KNOWN_PRIOR",
    })),
    unresolved_gaps: Array.isArray(priorSnapshot?.unresolvedGaps)
      ? priorSnapshot.unresolvedGaps
      : defaultUnresolvedGaps(),
    cost_attribution: "DISCOVERY_SPEND_ONLY",
  };
}

export function defaultUnresolvedGaps() {
  return [
    "ULTIMATE_OWNER",
    "CONTROLLING_INVESTOR",
    "LEGAL_TITLEHOLDER",
    "OWNERSHIP_STRUCTURE_EDGE",
    "OPERATOR_ENTITY",
    "OWNER_VS_OPERATOR",
    "ORG_RELATIONSHIP",
    "OWNERSHIP_DECISION_MAKER",
    "PUBLIC_EMAIL",
    "PUBLIC_PHONE",
    "ALTERNATIVE_CONTACT_PATHWAY",
  ];
}
