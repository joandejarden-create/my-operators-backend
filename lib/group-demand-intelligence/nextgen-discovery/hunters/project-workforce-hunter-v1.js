/**
 * Project Workforce specialist hunter.
 * Zero-paid default: independent public discovery (CPE segment config).
 * Falls back to synthetic cache only when forceSyntheticCache=true.
 */

import { SPECIALIST_HUNTER } from "../specialist-hunter-contract-v1.js";
import { createCacheBackedHunter } from "./create-cache-hunter-v1.js";
import { runPublicSourceAdapter } from "../adapters/public-source-adapters-v1.js";

const cacheHunter = createCacheBackedHunter(SPECIALIST_HUNTER.PROJECT_WORKFORCE);

export async function discoverProjectWorkforce(ctx = {}) {
  if (ctx.forceSyntheticCache === true) {
    return cacheHunter.discoverFn(ctx);
  }
  return runPublicSourceAdapter(SPECIALIST_HUNTER.PROJECT_WORKFORCE, {
    ...ctx,
    // Prefer offline-safe historical/public skip unless explicitly enabling fetches.
    skipDirectPublicFetches: ctx.skipDirectPublicFetches !== false,
    preferHistoricalOnly: ctx.preferHistoricalOnly === true,
  });
}

export const projectWorkforceHunter = {
  hunterId: SPECIALIST_HUNTER.PROJECT_WORKFORCE,
  discoverFn: discoverProjectWorkforce,
  discoverFnCache: cacheHunter.discoverFn,
  run: async (options = {}) => {
    const { runSpecialistHunter } = await import("../hunter-base-v1.js");
    return runSpecialistHunter({
      hunterId: SPECIALIST_HUNTER.PROJECT_WORKFORCE,
      discoverFn: discoverProjectWorkforce,
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

export const runProjectWorkforceHunter = projectWorkforceHunter.run;
