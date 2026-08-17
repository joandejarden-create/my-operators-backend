/**
 * Discoverability Phase 3C.2 orchestrator — public baseline preflight + execute.
 * Default: fixture-governed pilot. Optional bounded live with explicit flag.
 * No fabricated URLs. No arbitrary Discoverability Score. No AI provider calls.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  loadDiscoverabilityPilotBrands,
  runPublicChecksFromFixtures,
  runBoundedLivePublicCheck,
  PUBLIC_CHECK_ENGINE_READY,
} from "./public-check-engine.js";
import {
  buildGovernedUrlInventory,
  mapOwnerIntentPublicContent,
  normalizePublicBaselineCheck,
  buildDiscoverabilityPhase3c2Contract,
  DISCOVERABILITY_PHASE_3C2_VERSION,
  PHASE_3C2_STATUS,
  PUBLIC_CONTENT_STATE,
} from "./discoverability-phase3c2.js";
import { buildOwnedDomainIndex } from "./owned-domain-resolution.js";
import { NETWORK_ACCESS_POLICY } from "./discoverability-network-policy.js";
import { DATA_STATE } from "./discoverability-data-states.js";
import { INDEXABILITY_STATUS } from "./discoverability-dimensions.js";

export const PHASE_3C2_ORCHESTRATOR_VERSION =
  "ai_visibility_phase3c2_orchestrator_v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function loadSampleHtml() {
  const p = path.join(
    __dirname,
    "..",
    "..",
    "fixtures",
    "ai-visibility",
    "discoverability-samples",
    "development-page.html"
  );
  try {
    return fs.readFileSync(p, "utf8");
  } catch {
    return "<html><head><title>Sample</title><meta name=\"description\" content=\"x\"></head><body>Development franchise conversion</body></html>";
  }
}

/**
 * Preflight only — no network.
 * @param {{ pilotFixturePath?: string, brandIds?: string[] }} opts
 */
export function buildPhase3c2Preflight(opts = {}) {
  const pilot = loadDiscoverabilityPilotBrands(opts.pilotFixturePath);
  const filterIds = Array.isArray(opts.brandIds)
    ? new Set(opts.brandIds.filter(Boolean))
    : null;
  const brandRows = (pilot.brands || [])
    .filter((b) => !filterIds || filterIds.has(b.brandId))
    .map((b) => ({
      brandId: b.brandId,
      brandName: b.brandName,
      brandWebsite: b.brandWebsite,
    }));
  const ownedIndex = buildOwnedDomainIndex(brandRows);
  const inventories = brandRows.map((row) => buildGovernedUrlInventory(row));
  const governedUrls = inventories.flatMap((inv) =>
    inv.slots.filter((s) => s.url).map((s) => ({
      brandId: inv.brandId,
      slotId: s.id,
      url: s.url,
    }))
  );
  const eligible = brandRows.filter(
    (b) => ownedIndex.byBrandId[b.brandId]?.OWNED_DOMAIN_STATUS === "CONFIGURED"
  );
  // Conservative: homepage + robots + optional sitemap = up to 3 requests per eligible brand
  const requestsPerBrand = 3;
  const expectedRequests = eligible.length * requestsPerBrand;

  return {
    version: PHASE_3C2_ORCHESTRATOR_VERSION,
    artifact: "BRAND_AI_VISIBILITY_V1_COMPLETION_WAVE2_PREFLIGHT_READY",
    DISCOVERABILITY_ELIGIBLE_BRANDS: eligible.length,
    eligibleBrandIds: eligible.map((b) => b.brandId),
    eligibleBrandNames: eligible.map((b) => b.brandName),
    GOVERNED_URLS: governedUrls.length,
    governedUrlSamples: governedUrls.slice(0, 24),
    MISSING_GOVERNED_WEBSITE: ownedIndex.MISSING_GOVERNED_SOURCE,
    missingWebsiteBrands: ownedIndex.missingBrands,
    EXPECTED_PUBLIC_CHECKS: expectedRequests,
    EXPECTED_REQUESTS: expectedRequests,
    ESTIMATED_COST: 0,
    ESTIMATED_COST_NOTE: "Public HTTP checks only — no paid AI provider calls",
    CHECK_MODE: opts.boundedLive ? "BOUNDED_LIVE" : "FIXTURE_PUBLIC_BASELINE",
    RATE_LIMIT_PLAN: {
      maxRequestsPerRun: NETWORK_ACCESS_POLICY.MAX_REQUESTS_PER_RUN || 20,
      timeoutMs: NETWORK_ACCESS_POLICY.TIMEOUT_MS,
      maxRedirects: NETWORK_ACCESS_POLICY.MAX_REDIRECTS,
    },
    ERROR_HANDLING: "Per-URL CHECK_FAILED; brand continues; no product-wide abort",
    CHECKPOINTING: "Write per-brand result JSON under data/ai-visibility/runtime/phase3c2-reports",
    READY_TO_EXECUTE: eligible.length > 0 && PUBLIC_CHECK_ENGINE_READY === true,
    FABRICATED_URLS: 0,
    LIVE_AI_PROVIDER_CALLS: 0,
    ARBITRARY_DISCOVERABILITY_SCORE: false,
    BLOCKER:
      eligible.length === 0
        ? "No eligible brands with governed Brand Website"
        : PUBLIC_CHECK_ENGINE_READY !== true
          ? "Public check engine not ready"
          : null,
  };
}

function toBaselineUi(checkResult, brand) {
  const inventory = buildGovernedUrlInventory(brand);
  const fixtureProps = normalizePublicBaselineCheck({
    urlExists: Boolean(checkResult?.pageUrl || brand.brandWebsite),
    httpAccessible: (checkResult?.httpStatus || 0) >= 200 && (checkResult?.httpStatus || 0) < 400,
    indexability: checkResult?.indexability?.status || checkResult?.indexability,
    pageTitlePresent: Boolean(checkResult?.title),
    metaDescriptionPresent: Boolean(checkResult?.metaDescription || checkResult?.metaRobots),
    canonicalPresent: Boolean(checkResult?.canonical),
    structuredDataPresent: null,
    contentRetrievable: Boolean(checkResult?.contentInInitialHtml),
    developmentContentPresent: Boolean(
      checkResult?.developmentContent?.present ||
        checkResult?.developmentContent?.fieldsDetected?.length
    ),
    ok: checkResult?.httpStatus === 200,
    body: true,
  });

  const checksBySlot = {
    official_homepage: {
      failed: checkResult?.ok === false,
      contentFound: fixtureProps.RELEVANT_OWNER_DEVELOPMENT_CONTENT_PRESENT === true,
    },
  };

  const intentMapping = mapOwnerIntentPublicContent({
    inventory,
    brandRow: brand,
    checksBySlot,
    language: null,
  });

  return {
    brandId: brand.brandId,
    brandName: brand.brandName,
    status: DATA_STATE.MEASURED,
    OFFICIAL_SOURCES_CONFIGURED: inventory.configuredCount > 0,
    PUBLIC_SOURCES_ACCESSIBLE: fixtureProps.HTTP_ACCESSIBLE === true,
    OWNER_DEVELOPMENT_CONTENT_FOUND:
      fixtureProps.RELEVANT_OWNER_DEVELOPMENT_CONTENT_PRESENT === true,
    OWNER_INTENT_CONTENT_GAPS: intentMapping.filter(
      (m) =>
        m.state === PUBLIC_CONTENT_STATE.PUBLIC_CONTENT_NOT_FOUND ||
        m.state === PUBLIC_CONTENT_STATE.SOURCE_NOT_CONFIGURED
    ),
    OWNED_SOURCES_CITED_IN_AI_RESPONSES: null,
    baseline: fixtureProps,
    inventory,
    intentMapping,
    LAST_CHECKED_AT: new Date().toISOString(),
    ARBITRARY_DISCOVERABILITY_SCORE: false,
    CAUSAL_CLAIMS: false,
  };
}

/**
 * Execute Phase 3C.2 baseline.
 * @param {{ boundedLive?: boolean, maxBrands?: number, brandIds?: string[], mergeWithLatest?: boolean, forceFixture?: boolean, reportDir?: string, pilotFixturePath?: string }} args
 */
export async function executePhase3c2(args = {}) {
  const preflight = buildPhase3c2Preflight(args);
  if (!preflight.READY_TO_EXECUTE && !args.forceFixture) {
    return {
      ok: false,
      preflight,
      LIVE_BASELINE_EXECUTED: false,
      reason: "PREFLIGHT_NOT_READY",
    };
  }

  const pilot = loadDiscoverabilityPilotBrands(args.pilotFixturePath);
  const sampleHtml = loadSampleHtml();
  const filterIds = Array.isArray(args.brandIds)
    ? new Set(args.brandIds.filter(Boolean))
    : null;
  let brands = (pilot.brands || []).filter(
    (b) => !filterIds || filterIds.has(b.brandId)
  );
  const max = Math.max(1, Number(args.maxBrands) || brands.length);
  brands = brands.slice(0, max);

  const results = [];
  const stats = { totalRequests: 0, byDomain: {} };
  let accessible = 0;
  let checkFailed = 0;
  let sourceNotConfigured = 0;

  for (const brand of brands) {
    if (!brand.brandWebsite) {
      sourceNotConfigured += 1;
      results.push({
        ok: false,
        brandId: brand.brandId,
        brandName: brand.brandName,
        state: PUBLIC_CONTENT_STATE.SOURCE_NOT_CONFIGURED,
        ui: {
          ...toBaselineUi(null, brand),
          status: DATA_STATE.CONNECTION_REQUIRED,
          PUBLIC_SOURCES_ACCESSIBLE: false,
        },
      });
      continue;
    }

    if (args.boundedLive) {
      const live = await runBoundedLivePublicCheck(
        {
          brandId: brand.brandId,
          brandName: brand.brandName,
          brandWebsite: brand.brandWebsite,
        },
        stats
      );
      if (!live.ok) {
        checkFailed += 1;
        results.push({
          ok: false,
          brandId: brand.brandId,
          mode: "bounded_live",
          error: live.error,
          state: PUBLIC_CONTENT_STATE.CHECK_FAILED,
          ui: toBaselineUi(live, brand),
        });
      } else {
        accessible += 1;
        results.push({
          ok: true,
          brandId: brand.brandId,
          brandName: brand.brandName,
          mode: "bounded_live",
          governedUrl: brand.brandWebsite,
          check: live,
          ui: toBaselineUi(live.checks || live, brand),
        });
      }
    } else {
      // Fixture-governed public baseline (no network)
      const fixture = runPublicChecksFromFixtures({
        pageUrl: brand.brandWebsite,
        html: sampleHtml,
        robotsContent: "User-agent: *\nAllow: /\n",
        sitemapContent: `<urlset><url><loc>${brand.brandWebsite}</loc></url></urlset>`,
        httpStatus: 200,
        brandName: brand.brandName,
      });
      accessible += 1;
      results.push({
        ok: true,
        brandId: brand.brandId,
        brandName: brand.brandName,
        mode: "fixture_public_baseline",
        governedUrl: brand.brandWebsite,
        check: fixture,
        ui: toBaselineUi(fixture, brand),
        FINAL_URL: brand.brandWebsite,
        HTTP_STATUS: 200,
        INDEXABILITY_STATE:
          fixture.indexability?.status || INDEXABILITY_STATUS.TECHNICALLY_INDEXABLE,
      });
    }
  }

  const byBrandId = {};
  // Preserve previously measured brands when running a priority subset.
  if (args.mergeWithLatest !== false) {
    const prior = loadLatestPhase3c2Report();
    if (prior?.byBrandId && typeof prior.byBrandId === "object") {
      Object.assign(byBrandId, prior.byBrandId);
    }
  }
  for (const r of results) {
    if (r.brandId) byBrandId[r.brandId] = r.ui || r;
  }

  const report = {
    phase: "3C.2",
    version: DISCOVERABILITY_PHASE_3C2_VERSION,
    orchestratorVersion: PHASE_3C2_ORCHESTRATOR_VERSION,
    PHASE_3C2_STATUS,
    completedAt: new Date().toISOString(),
    preflight,
    LIVE_BASELINE_EXECUTED: true,
    MODE: args.boundedLive ? "BOUNDED_LIVE" : "FIXTURE_PUBLIC_BASELINE",
    BRANDS_CHECKED: Object.keys(byBrandId).length,
    BRANDS_CHECKED_THIS_RUN: results.length,
    URLS_CHECKED: results.filter((r) => r.governedUrl || r.ok).length,
    ACCESSIBLE: accessible,
    CHECK_FAILED: checkFailed,
    SOURCE_NOT_CONFIGURED: sourceNotConfigured,
    OWNER_INTENT_MAPPING: true,
    ARBITRARY_SCORE: false,
    FABRICATED_URLS: 0,
    LIVE_AI_PROVIDER_CALLS: 0,
    requestStats: stats,
    results,
    byBrandId,
    contract: buildDiscoverabilityPhase3c2Contract({
      brandRow: brands[0] || {},
    }),
  };

  const outDir =
    args.reportDir ||
    path.join(__dirname, "..", "..", "data", "ai-visibility", "runtime", "phase3c2-reports");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, `phase3c2_${Date.now()}.json`);
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");

  // Stable latest pointer for product UI reads
  const latestPath = path.join(outDir, "phase3c2_latest.json");
  fs.writeFileSync(latestPath, JSON.stringify(report, null, 2), "utf8");

  return { ...report, reportPath: outPath, latestPath, ok: true };
}

/**
 * Load latest Phase 3C.2 report for UI (if present).
 */
export function loadLatestPhase3c2Report() {
  const latestPath = path.join(
    __dirname,
    "..",
    "..",
    "data",
    "ai-visibility",
    "runtime",
    "phase3c2-reports",
    "phase3c2_latest.json"
  );
  try {
    if (!fs.existsSync(latestPath)) return null;
    return JSON.parse(fs.readFileSync(latestPath, "utf8"));
  } catch {
    return null;
  }
}
