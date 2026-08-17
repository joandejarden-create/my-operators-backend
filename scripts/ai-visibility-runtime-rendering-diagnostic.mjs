#!/usr/bin/env node
/**
 * Brand AI Visibility runtime/rendering diagnostic (read-only).
 * No AI provider calls. No deploys.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import { getBrandExecutiveSummaryPayload } from "../lib/ai-visibility/brand-executive-summary.js";
import {
  getBrandPortfolioPayload,
  resolveBrandGeographyMonitoringState,
} from "../lib/ai-visibility/brand-read-service.js";
import { buildFixtureEntitlementGraph } from "../lib/ai-visibility/entitlements.js";
import { resolvePeerSetMembership, PEER_SET_ID_V2 } from "../lib/ai-visibility/peer-sets.js";
import { loadLatestPhase3c2Report } from "../lib/ai-visibility/phase3c2-orchestrator.js";
import { listClientMetricDefinitions } from "../lib/ai-visibility/client-metric-definitions.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(__dirname, "..");
const SHOWCASE = JSON.parse(
  fs.readFileSync(
    path.join(REPO, "fixtures/ai-visibility/brand-ai-showcase-companies-v1.json"),
    "utf8"
  )
);

const companyKey = process.argv[2] || "marriott";
const company = SHOWCASE.companies.find((c) => c.companyKey === companyKey);
if (!company) {
  console.error("Unknown company", companyKey);
  process.exit(1);
}

const membership = resolvePeerSetMembership({
  peerSetId: PEER_SET_ID_V2,
  commercialRegion: "CALA",
});
const brandNamesById = Object.fromEntries(
  (company.brands || []).map((b) => [b.brandId, b.brandName])
);
const entitlementGraph = buildFixtureEntitlementGraph({
  entitledBrandIds: company.brandIds,
  peerBrandIds: membership.entityIds || [],
  source: `runtime-diagnostic:${companyKey}`,
});

const store = createBrandAiVisibilityReadStore({});
const dealalityUser = { id: "runtime-diagnostic", demoBrandPortfolioKey: companyKey };

const args = {
  dealalityUser,
  entitlementGraph,
  brandNamesById,
  store,
  provider: "openai",
  geography: "CALA",
  language: "en",
};

const exec = await getBrandExecutiveSummaryPayload(args);
const portfolio = await getBrandPortfolioPayload(args);

const brandRows = [];
for (const id of company.brandIds || []) {
  const mon = await resolveBrandGeographyMonitoringState({
    store,
    brandId: id,
    geoFilter: { geographyScope: "Region", commercialRegion: "CALA", key: "CALA" },
    provider: "openai",
    language: "en",
  });
  brandRows.push({
    BRAND: brandNamesById[id] || id,
    brandId: id,
    ENTITLED: true,
    HAS_CALA_OPENAI_ENGLISH_RUN: !!mon.monitored,
    RUN_STATUS: mon.code,
    LATEST_RUN_ID: mon.batchId || mon.latestSummary?.batchId || null,
    OBSERVATIONS_N: mon.promptDenominator ?? null,
    PRESENCE_DENOMINATOR: mon.promptDenominator ?? null,
    PRESENCE_VAL: mon.presenceVal ?? null,
    EXCLUSION_REASON: mon.monitored ? null : mon.message || mon.code,
  });
}

const phase3c2 = loadLatestPhase3c2Report();
const defs = listClientMetricDefinitions();
const cp = exec.currentPosition || {};
const snap = exec.portfolioSnapshot || {};

const out = {
  artifact: "BRAND_AI_VISIBILITY_RUNTIME_RENDERING_DIAGNOSTIC_COMPLETE",
  completedAt: new Date().toISOString(),
  companyKey,
  linkedBrands: company.brandIds?.length,
  ExecutiveData: {
    PORTFOLIO_AI_PRESENCE:
      cp.portfolioAiPresence?.display ||
      snap.PORTFOLIO_AI_PRESENCE?.display ||
      (cp.portfolioAiPresence?.value != null
        ? cp.portfolioAiPresence.value
        : null),
    BRANDS_MONITORED: cp.brandsMonitored?.display || cp.brandsMonitored || null,
    STRONGEST_BRAND:
      cp.topBrandByAiPresence?.brandName ||
      snap.STRONGEST_BRAND_BY_PRESENCE?.brandName ||
      null,
    BEST_COMPETITIVE_POSITION:
      cp.bestCompetitivePosition?.display ||
      cp.topBrandByCompetitivePosition?.display ||
      null,
    QUESTIONS_MISSING: cp.questionsMissing?.display || null,
    availability: exec.availability || null,
    ok: exec.ok,
  },
  AllProviders: {
    READY: exec.allProvidersPanel?.READY ?? null,
    PROVIDERS_MONITORED: exec.allProvidersPanel?.PROVIDERS_MONITORED ?? null,
    NOT_COMPARABLE: exec.allProvidersPanel?.NOT_COMPARABLE ?? null,
    message: exec.allProvidersPanel?.message || null,
    crossProviderPresence: Boolean(exec.crossProviderPresence),
    availableProviders: portfolio.availableProviders || exec.availableProviders || null,
  },
  PriorityReview: {
    status: exec.priorityReviewItems?.status,
    itemCount: (exec.priorityReviewItems?.items || []).length,
    sample: (exec.priorityReviewItems?.items || []).slice(0, 3),
  },
  PortfolioBrands: {
    count: (exec.portfolioOverview?.brands || portfolio.brands || []).length,
    sample: (exec.portfolioOverview?.brands || portfolio.brands || [])
      .slice(0, 5)
      .map((b) => ({
        brandName: b.brandName,
        availability: b.aiPresence?.availability || b.availability,
        display: b.aiPresence?.display,
        message: b.aiPresence?.message || b.availabilityMessage,
      })),
  },
  Discoverability: {
    publicDiscoverability: exec.publicDiscoverability
      ? {
          DISCOVERABILITY: exec.publicDiscoverability.DISCOVERABILITY,
          LIVE_BASELINE: exec.publicDiscoverability.LIVE_BASELINE,
          OFFICIAL_SOURCES_CONFIGURED:
            exec.publicDiscoverability.OFFICIAL_SOURCES_CONFIGURED,
          message: exec.publicDiscoverability.message,
        }
      : null,
    discoverabilityBusinessImpactStatus:
      exec.discoverabilityBusinessImpact?.status || null,
    phase3c2Latest: phase3c2
      ? {
          MODE: phase3c2.MODE,
          LIVE_BASELINE_EXECUTED: phase3c2.LIVE_BASELINE_EXECUTED,
          DISCOVERABILITY_3C2_EXECUTION_STATUS:
            phase3c2.DISCOVERABILITY_3C2_EXECUTION_STATUS,
          BRANDS_CHECKED: phase3c2.BRANDS_CHECKED,
        }
      : null,
  },
  Citation: {
    READY: exec.sourceExecutivePanel?.READY ?? null,
    CITATION_RATE: exec.sourceExecutivePanel?.CITATION_RATE || null,
    OWNED_SOURCE_CITATION_RATE:
      exec.sourceExecutivePanel?.OWNED_SOURCE_CITATION_RATE || null,
    RESPONSES_WITH_CITATIONS:
      exec.sourceExecutivePanel?.RESPONSES_WITH_CITATIONS ?? null,
  },
  MetricDefinitions: {
    API_FIELD_PRESENT: Array.isArray(exec.clientMetricDefinitions),
    COUNT: (exec.clientMetricDefinitions || []).length,
    LOCAL_DEFS_COUNT: defs.length,
  },
  BrandMonitoring: brandRows,
  monitoredCount: brandRows.filter((b) => b.HAS_CALA_OPENAI_ENGLISH_RUN).length,
  COMPLETION_WAVE: exec.brandV1?.COMPLETION_WAVE ?? null,
  brandExecutiveSummaryVersion: exec.brandExecutiveSummaryVersion || null,
};

const outPath = path.join(
  REPO,
  "data/ai-visibility/validation/brand-ai-visibility-runtime-rendering-diagnostic.json"
);
fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, JSON.stringify(out, null, 2), "utf8");
console.log(JSON.stringify(out, null, 2));
console.log("\nWrote", outPath);
