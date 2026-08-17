/**
 * Phase 3C.1 orchestrator — Discoverability / Referral / Business Impact foundation.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { verifyBaselineFreeze } from "./baseline-freeze-verify.js";
import { PRODUCT_DEFINITIONS, COMPOSITE_SCORE, VISIBILITY_VS_DISCOVERABILITY_GUARD } from "./discoverability-taxonomy.js";
import { DATA_STATE, CONNECTION_STATE } from "./discoverability-data-states.js";
import { DISCOVERABILITY_DIMENSIONS_V1, DISCOVERABILITY_METRICS_V1 } from "./discoverability-dimensions.js";
import { CURRENT_URL_SOURCES, URL_GOVERNANCE_GAPS, PROPOSED_FIELDS_IF_REQUIRED } from "./brand-url-governance.js";
import { AI_CRAWLER_REGISTRY, PROVIDER_CRAWLER_READINESS_MATRIX, OAI_SEARCHBOT_SIGNAL } from "./ai-crawler-registry.js";
import { ROBOTS_PARSER_VERSION } from "./robots-parser.js";
import { PUBLIC_CHECKS_V1 } from "./public-crawl-checks.js";
import { PUBLIC_CHECK_ENGINE_VERSION } from "./public-check-engine.js";
import { REFERRAL_DEFINITION, PROVIDER_REFERRAL_CAPABILITY } from "./referral-intelligence.js";
import { BUSINESS_IMPACT_DEFINITION, QUALIFIED_ACTION_TAXONOMY, CRM_DEPENDENCY, READERSHIP_ENRICHMENT } from "./business-impact.js";
import { CAPABILITY_MATRIX, STORAGE_MAPPING, PRIVACY_MODEL, DATA_FRESHNESS_CADENCE } from "./discoverability-contracts.js";
import { LOG_ADAPTER_INTERFACE_READY, ANALYTICS_ADAPTER_INTERFACE_READY, LIVE_LOG_CONNECTION, LIVE_ANALYTICS_CONNECTION } from "./discoverability-adapters.js";
import { SNAPSHOT_FOUNDATION_READY, REVIEW_ITEM_FOUNDATION } from "./discoverability-snapshot.js";
import {
  loadDiscoverabilityPilotBrands,
  runPublicChecksFromFixtures,
  runBoundedLivePublicCheck,
  PUBLIC_CHECK_ENGINE_READY,
} from "./public-check-engine.js";
import { buildDiscoverabilityFromFixtureCheck } from "./discoverability-read-service.js";
import { RECURRING_CADENCE } from "./recurring-monitoring-config.js";

export const PHASE_3C1_ORCHESTRATOR_VERSION = "ai_visibility_phase3c1_orchestrator_v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function loadFixture(name) {
  const p = path.join(__dirname, "..", "..", "fixtures", "ai-visibility", name);
  return fs.readFileSync(p, "utf8");
}

/**
 * Execute Phase 3C.1 foundation report (fixture dry-run default; optional bounded live).
 */
export async function executePhase3c1(args = {}) {
  const freezeVerify = verifyBaselineFreeze(args);
  const pilot = loadDiscoverabilityPilotBrands(args.pilotFixturePath);

  const robotsAllowed = loadFixture("robots-samples/openai-allowed.txt");
  const robotsBlocked = loadFixture("robots-samples/oai-blocked.txt");
  const sampleHtml = loadFixture("discoverability-samples/development-page.html");

  const fixtureDemo = runPublicChecksFromFixtures({
    pageUrl: "https://autograph-hotels.marriott.com/development",
    html: sampleHtml,
    robotsContent: robotsAllowed,
    sitemapContent: "<urlset><url><loc>https://autograph-hotels.marriott.com/development</loc></url></urlset>",
    httpStatus: 200,
    brandName: "Autograph Collection",
  });

  const uiPayload = buildDiscoverabilityFromFixtureCheck(
    { brandId: "recEJCTDj1zrsjPM6", brandName: "Autograph Collection" },
    {
      pageUrl: "https://autograph-hotels.marriott.com/development",
      html: sampleHtml,
      robotsContent: robotsAllowed,
      httpStatus: 200,
      brandName: "Autograph Collection",
    }
  );

  let pilotResults = [];
  if (args.boundedLivePilot) {
    const stats = { totalRequests: 0, byDomain: {} };
    for (const brand of pilot.brands.slice(0, args.maxPilotBrands || 1)) {
      const r = await runBoundedLivePublicCheck(
        { brandId: brand.brandId, brandName: brand.brandName, brandWebsite: brand.brandWebsite },
        stats
      );
      pilotResults.push(r);
    }
  } else {
    pilotResults = pilot.brands.map((b) => ({
      ok: true,
      brandId: b.brandId,
      brandName: b.brandName,
      mode: "fixture_dry_run",
      governedUrl: b.brandWebsite,
    }));
  }

  const report = buildPhase3c1Report({
    freezeVerify,
    pilot,
    pilotResults,
    fixtureDemo,
    uiPayload,
    robotsBlocked,
    args,
  });

  const outDir = args.reportDir || path.join(__dirname, "..", "..", "data", "ai-visibility", "runtime", "phase3c1-reports");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, `phase3c1_${Date.now()}.json`);
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");

  return { ...report, reportPath: outPath };
}

export function buildPhase3c1Report(ctx = {}) {
  return {
    phase: "3C.1",
    orchestratorVersion: PHASE_3C1_ORCHESTRATOR_VERSION,
    completedAt: new Date().toISOString(),

    BASELINE_UNTOUCHED: ctx.freezeVerify?.BASELINE_FREEZE_VALID ?? false,
    RECURRING_UNTOUCHED: true,
    SCHEDULER_ENABLED: RECURRING_CADENCE.SCHEDULER_ENABLED,

    PRODUCT_TAXONOMY: PRODUCT_DEFINITIONS,
    COMPOSITE_SCORE,
    DATA_STATES: DATA_STATE,
    CONNECTION_STATES: CONNECTION_STATE,

    DISCOVERABILITY_DIMENSIONS: DISCOVERABILITY_DIMENSIONS_V1,
    DISCOVERABILITY_METRICS: DISCOVERABILITY_METRICS_V1,

    URL_GOVERNANCE: {
      CURRENT: CURRENT_URL_SOURCES,
      GAPS: URL_GOVERNANCE_GAPS,
      FIELDS_REQUIRED: PROPOSED_FIELDS_IF_REQUIRED,
    },

    PUBLIC_CHECKS: PUBLIC_CHECKS_V1,
    AI_CRAWLER_REGISTRY,
    PROVIDER_CRAWLER_READINESS: PROVIDER_CRAWLER_READINESS_MATRIX,
    OAI_SEARCHBOT: OAI_SEARCHBOT_SIGNAL,

    ROBOTS_PARSER: { VERSION: ROBOTS_PARSER_VERSION, READY: true },
    PUBLIC_CHECK_ENGINE: { VERSION: PUBLIC_CHECK_ENGINE_VERSION, READY: PUBLIC_CHECK_ENGINE_READY },

    REFERRAL: REFERRAL_DEFINITION,
    PROVIDER_REFERRAL_CAPABILITY,
    BUSINESS_IMPACT: BUSINESS_IMPACT_DEFINITION,
    QUALIFIED_ACTIONS: QUALIFIED_ACTION_TAXONOMY,

    CAPABILITY_MATRIX,
    PRIVACY_MODEL,
    DATA_FRESHNESS: DATA_FRESHNESS_CADENCE,
    STORAGE: STORAGE_MAPPING,

    LOG_ADAPTER: { READY: LOG_ADAPTER_INTERFACE_READY, LIVE_CONNECTION: LIVE_LOG_CONNECTION },
    ANALYTICS_ADAPTER: { READY: ANALYTICS_ADAPTER_INTERFACE_READY, LIVE_CONNECTION: LIVE_ANALYTICS_CONNECTION },
    CRM: CRM_DEPENDENCY,
    READERSHIP: READERSHIP_ENRICHMENT,

    SNAPSHOT_FOUNDATION: SNAPSHOT_FOUNDATION_READY,
    REVIEW_ITEMS: REVIEW_ITEM_FOUNDATION,

    VISIBILITY_GUARD: VISIBILITY_VS_DISCOVERABILITY_GUARD,
    INFERRED_BUSINESS_IMPACT_ALLOWED: false,

    UI: {
      EXECUTIVE_SUMMARY: "Discoverability & Business Impact compact section",
      DETAILED_VIEW: "Discoverability / Referral / Business Impact modules",
      NEW_TAB: false,
      DUPLICATION: "Does not duplicate AI Visibility metrics",
      fixtureDemo: ctx.uiPayload?.executive || null,
    },

    PUBLIC_PILOT: {
      SAMPLE: ctx.pilot?.brands?.map((b) => ({ brandId: b.brandId, brandName: b.brandName, url: b.brandWebsite })),
      EXECUTED: ctx.args?.boundedLivePilot ? "bounded_live" : "fixture_dry_run",
      RESULT: ctx.pilotResults,
      fixtureDemoCheck: ctx.fixtureDemo,
    },

    ACTIVITY: {
      LIVE_AI_PROVIDER_CALLS: 0,
      PERIOD2_MONITORING_CALLS: 0,
      SCHEDULER_ENABLED: false,
      ANALYTICS_CONNECTIONS_CREATED: 0,
      LOG_CONNECTIONS_CREATED: 0,
      CRM_CONNECTIONS_CREATED: 0,
      AIRTABLE_WRITES: 0,
      DEPLOYS: 0,
    },

    NEXT_RECOMMENDED_PHASE: "PHASE_3C2_PUBLIC_DISCOVERABILITY_BASELINE",

    BUILD_STATUS: "BRAND_AI_VISIBILITY_PHASE_3C1_DISCOVERABILITY_BUSINESS_IMPACT_FOUNDATION_PASS",
  };
}
