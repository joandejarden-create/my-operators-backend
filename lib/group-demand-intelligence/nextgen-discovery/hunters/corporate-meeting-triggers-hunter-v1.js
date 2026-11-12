/**
 * Corporate Meeting Triggers specialist hunter.
 * Zero-paid default: independent public discovery (CMT segment config).
 * Falls back to synthetic cache only when forceSyntheticCache=true.
 */

import { SPECIALIST_HUNTER } from "../specialist-hunter-contract-v1.js";
import { createCacheBackedHunter } from "./create-cache-hunter-v1.js";
import { runPublicSourceAdapter } from "../adapters/public-source-adapters-v1.js";

const cacheHunter = createCacheBackedHunter(
  SPECIALIST_HUNTER.CORPORATE_MEETING_TRIGGERS
);

export async function discoverCorporateMeetingTriggers(ctx = {}) {
  if (ctx.forceSyntheticCache === true) {
    return cacheHunter.discoverFn(ctx);
  }
  return runPublicSourceAdapter(SPECIALIST_HUNTER.CORPORATE_MEETING_TRIGGERS, {
    ...ctx,
    skipDirectPublicFetches: ctx.skipDirectPublicFetches !== false,
    preferHistoricalOnly: ctx.preferHistoricalOnly === true,
  });
}

export const corporateMeetingTriggersHunter = {
  hunterId: SPECIALIST_HUNTER.CORPORATE_MEETING_TRIGGERS,
  discoverFn: discoverCorporateMeetingTriggers,
  discoverFnCache: cacheHunter.discoverFn,
  run: async (options = {}) => {
    const { runSpecialistHunter } = await import("../hunter-base-v1.js");
    return runSpecialistHunter({
      hunterId: SPECIALIST_HUNTER.CORPORATE_MEETING_TRIGGERS,
      discoverFn: discoverCorporateMeetingTriggers,
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

export const runCorporateMeetingTriggersHunter =
  corporateMeetingTriggersHunter.run;
