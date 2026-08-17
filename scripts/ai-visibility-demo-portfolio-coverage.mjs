#!/usr/bin/env node
/**
 * Read-only coverage report for showcase Brand portfolios.
 * No provider calls. No public crawl. No Airtable writes.
 */
import {
  listDemoBrandPortfolioOptions,
  resolveDemoBrandPortfolio,
} from "../lib/dealality/demo-brand-portfolio-context.js";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import { findMatchingSummaries, parseGeographyQuery } from "../lib/ai-visibility/brand-read-service.js";
import { entityInMonitoredUniverse } from "../lib/ai-visibility/brand-read-service.js";

const GEOS = [
  { key: "Global", geography: "Global" },
  { key: "CALA", geography: "CALA" },
  { key: "Europe", geography: "Europe" },
  { key: "North America", geography: "North America" },
];

async function monitoredInGeo(store, brandId, geography) {
  const geo = parseGeographyQuery({ geography });
  try {
    const summaries = await findMatchingSummaries(store, geo, "openai", { language: "en" });
    if (!summaries.length) return false;
    return entityInMonitoredUniverse(summaries[0], brandId, []);
  } catch {
    return false;
  }
}

async function hasSnapshot(store, brandId) {
  if (typeof store.listMetricSnapshots !== "function") return false;
  const snaps = await store.listMetricSnapshots({ entityId: brandId, provider: "openai" });
  return Array.isArray(snaps) && snaps.length > 0;
}

async function main() {
  const store = createBrandAiVisibilityReadStore({});
  const options = listDemoBrandPortfolioOptions();
  const report = [];

  for (const opt of options) {
    const resolved = resolveDemoBrandPortfolio(opt.companyKey);
    const brands = [];
    for (const b of resolved.brands || []) {
      const row = {
        BRAND: b.brandName,
        BRAND_ID: b.brandId,
        COMPANY: resolved.canonicalCompanyName,
        ACTIVE: "ASSUMED_ACTIVE_FROM_SHOWCASE_CONFIG",
        MONITORED_GLOBAL: await monitoredInGeo(store, b.brandId, "Global"),
        MONITORED_CALA: await monitoredInGeo(store, b.brandId, "CALA"),
        MONITORED_EUROPE: await monitoredInGeo(store, b.brandId, "Europe"),
        MONITORED_NORTH_AMERICA: await monitoredInGeo(store, b.brandId, "North America"),
        LATEST_SNAPSHOT_AVAILABLE: await hasSnapshot(store, b.brandId),
        DISCOVERABILITY_PUBLIC_CHECK_AVAILABLE: false,
      };
      brands.push(row);
    }
    report.push({
      PORTFOLIO: opt.companyKey,
      CANONICAL_COMPANY_NAME: resolved.canonicalCompanyName,
      SHOWCASE_PORTFOLIO_ID: resolved.showcasePortfolioId,
      BRAND_COUNT: resolved.brandIds?.length || 0,
      CANONICAL_BRAND_IDS: resolved.brandIds,
      CANONICAL_VALID: resolved.ok,
      BRANDS: brands,
    });
  }

  console.log(JSON.stringify({ AIRTABLE_WRITES: 0, LIVE_AI_PROVIDER_CALLS: 0, portfolios: report }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
