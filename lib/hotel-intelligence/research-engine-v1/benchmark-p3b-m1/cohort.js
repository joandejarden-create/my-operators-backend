/**
 * P3B-M1 three-hotel mini-pilot + retain six-hotel subsequent cohort.
 */

import {
  loadP3ASixHotelCohort,
  P3A_SIX_HPC_IDS,
  buildIndependentDiscoverySeed,
  buildIncrementalProductionSeed,
} from "../benchmark-p3a/cohort.js";
import { loadPilotManifest, resolvePilotHotel } from "../pilot-manifest.js";

export const P3B_M1_HOTEL_SPECS = Object.freeze([
  { order: 1, hpcId: "recLuxvwwxID7U2B8", expectedName: "Bethesda Marriott" },
  { order: 2, hpcId: "recnPB3Q02NO8Ma1o", expectedName: "Wyndham San Diego Bayside" },
  {
    order: 3,
    hpcId: "recvVZ5unh4d3Krzi",
    expectedName: "The Ritz-Carlton, Mexico City",
  },
]);

export const P3B_M1_HPC_IDS = Object.freeze(P3B_M1_HOTEL_SPECS.map((h) => h.hpcId));

export function loadP3BM1PilotCohort(opts = {}) {
  const manifest = opts.manifest || loadPilotManifest(opts);
  const hotels = [];
  const errors = [];
  for (const spec of P3B_M1_HOTEL_SPECS) {
    const resolved = resolvePilotHotel(spec.hpcId, manifest);
    if (!resolved.ok) {
      errors.push({ hpcId: spec.hpcId, reason: resolved.reason });
      continue;
    }
    hotels.push({
      order: spec.order,
      hpcId: spec.hpcId,
      name: resolved.primary.name,
      expectedName: spec.expectedName,
      cohort: resolved.primary.cohort || null,
      manifestEntries: resolved.entries,
    });
  }
  const six = loadP3ASixHotelCohort({ manifest });
  return {
    ok: errors.length === 0 && hotels.length === 3,
    cohortId: "HE-INTEL-001-P3B-M1-THREE-HOTEL-V1",
    hotels,
    hpcIds: hotels.map((h) => h.hpcId),
    subsequentSixHotelCohort: {
      cohortId: six.cohortId,
      hpcIds: [...P3A_SIX_HPC_IDS],
      retained: true,
    },
    errors,
    buildSeedA: (h) => buildIndependentDiscoverySeed(h),
    buildSeedB: (h, prior) => buildIncrementalProductionSeed(h, prior),
  };
}
