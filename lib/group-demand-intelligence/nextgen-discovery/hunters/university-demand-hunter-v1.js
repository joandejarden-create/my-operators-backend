/**
 * University Demand specialist hunter.
 * Zero-paid default: historical public campus events evidence (Georgetown).
 */

import { SPECIALIST_HUNTER } from "../specialist-hunter-contract-v1.js";
import { createCacheBackedHunter } from "./create-cache-hunter-v1.js";
import { runPublicSourceAdapter } from "../adapters/public-source-adapters-v1.js";

const cacheHunter = createCacheBackedHunter(SPECIALIST_HUNTER.UNIVERSITY_DEMAND);

export async function discoverUniversityDemand(ctx = {}) {
  if (ctx.forceSyntheticCache === true) {
    return cacheHunter.discoverFn(ctx);
  }
  return runPublicSourceAdapter(SPECIALIST_HUNTER.UNIVERSITY_DEMAND, ctx);
}

export const universityDemandHunter = {
  hunterId: SPECIALIST_HUNTER.UNIVERSITY_DEMAND,
  discoverFn: discoverUniversityDemand,
  discoverFnCache: cacheHunter.discoverFn,
  run: async (options = {}) => {
    const { runSpecialistHunter } = await import("../hunter-base-v1.js");
    return runSpecialistHunter({
      hunterId: SPECIALIST_HUNTER.UNIVERSITY_DEMAND,
      discoverFn: discoverUniversityDemand,
      hotelId: options.hotelId,
      hotelName: options.hotelName,
      territory: options.territory,
      commercialPriorities: options.commercialPriorities || null,
      dryRun: options.dryRun !== false,
      cacheFirst: options.cacheFirst !== false,
      budgetUsd: options.budgetUsd || 0,
      horizonMonths: options.horizonMonths,
      existingKeys: options.existingKeys || [],
      reportDir: options.reportDir || "",
      rootDir: options.rootDir || process.cwd(),
    });
  },
  runFromCli: cacheHunter.runFromCli,
};

export const runUniversityDemandHunter = universityDemandHunter.run;
