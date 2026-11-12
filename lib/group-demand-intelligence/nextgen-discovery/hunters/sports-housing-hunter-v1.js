/**
 * Sports Housing specialist hunter.
 * Zero-paid default: historical public program evidence (Maryland SoccerPlex).
 * Apify remains primary in provider plan but fail-closed without founder auth.
 */

import { SPECIALIST_HUNTER } from "../specialist-hunter-contract-v1.js";
import { createCacheBackedHunter } from "./create-cache-hunter-v1.js";
import { runPublicSourceAdapter } from "../adapters/public-source-adapters-v1.js";
import { loadFirstWaveCache } from "./load-first-wave-cache-v1.js";
import {
  DISCOVERY_EVIDENCE_CLASS,
  stampDiscoveryEvidenceClass,
} from "../shared/discovery-evidence-class-v1.js";

const cacheHunter = createCacheBackedHunter(SPECIALIST_HUNTER.SPORTS_HOUSING);

export async function discoverSportsHousing(ctx = {}) {
  if (ctx.forceSyntheticCache === true) {
    return cacheHunter.discoverFn(ctx);
  }
  const out = await runPublicSourceAdapter(SPECIALIST_HUNTER.SPORTS_HOUSING, ctx);
  // Always include leisure rejection fixtures so Party Bus gate stays measurable
  // without affecting the other nine hunters.
  const cache = loadFirstWaveCache(ctx.rootDir || process.cwd());
  const rejects = (cache.rejectionFixtures || []).map((row) =>
    stampDiscoveryEvidenceClass(
      { ...row },
      DISCOVERY_EVIDENCE_CLASS.SYNTHETIC_TEST_FIXTURE
    )
  );
  return {
    discoveries: [...(out.discoveries || []), ...rejects],
    meta: {
      ...(out.meta || {}),
      leisureGateFixturesAttached: rejects.length,
    },
  };
}

export const sportsHousingHunter = {
  hunterId: SPECIALIST_HUNTER.SPORTS_HOUSING,
  discoverFn: discoverSportsHousing,
  discoverFnCache: cacheHunter.discoverFn,
  run: async (options = {}) => {
    const { runSpecialistHunter } = await import("../hunter-base-v1.js");
    return runSpecialistHunter({
      hunterId: SPECIALIST_HUNTER.SPORTS_HOUSING,
      discoverFn: discoverSportsHousing,
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

export const runSportsHousingHunter = sportsHousingHunter.run;
