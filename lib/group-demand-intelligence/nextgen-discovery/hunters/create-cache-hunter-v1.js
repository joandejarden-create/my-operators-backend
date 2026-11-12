import { SPECIALIST_HUNTER } from "../specialist-hunter-contract-v1.js";
import { runSpecialistHunter, parseHunterCliArgs } from "../hunter-base-v1.js";
import {
  loadFirstWaveCache,
  loadHunterCacheDiscoveries,
} from "./load-first-wave-cache-v1.js";

export function createCacheBackedHunter(hunterId) {
  async function discoverFn(ctx = {}) {
    const loaded = loadHunterCacheDiscoveries({
      hunterId,
      hotelId: ctx.hotelId,
      rootDir: ctx.rootDir || process.cwd(),
    });
    const discoveries = [...loaded.discoveries];

    // Prove Party Bus / leisure gate still rejects on sports housing path
    if (hunterId === SPECIALIST_HUNTER.SPORTS_HOUSING) {
      const cache = loadFirstWaveCache(ctx.rootDir || process.cwd());
      for (const row of cache.rejectionFixtures || []) {
        discoveries.push({ ...row });
      }
    }

    return {
      discoveries,
      meta: {
        mode: "CACHE_FIRST_ZERO_PAID",
        source: loaded.source,
        cacheSchema: loaded.cacheSchema,
        commercialPrioritiesKeys: ctx.commercialPriorities
          ? Object.keys(ctx.commercialPriorities)
          : [],
        horizonMonths: ctx.horizonMonths,
        providerCategories: (ctx.providerPlan?.providers || []).map(
          (p) => p.category
        ),
      },
    };
  }

  async function run(options = {}) {
    return runSpecialistHunter({
      hunterId,
      discoverFn,
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
  }

  async function runFromCli(argv = process.argv.slice(2)) {
    const args = parseHunterCliArgs(argv);
    return run(args);
  }

  return { hunterId, discoverFn, run, runFromCli };
}
