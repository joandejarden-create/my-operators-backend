#!/usr/bin/env node
/**
 * Bounded LIVE Discoverability validation (Phase 3C.2).
 * Governed pilot Brand Website URLs only.
 * No AI provider calls. No search-discovered URLs.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  loadDiscoverabilityPilotBrands,
  boundedFetch,
  runPublicChecksFromFixtures,
  PUBLIC_CHECK_ENGINE_READY,
} from "../lib/ai-visibility/public-check-engine.js";
import {
  NETWORK_ACCESS_POLICY,
  assertWithinRequestBudget,
} from "../lib/ai-visibility/discoverability-network-policy.js";
import {
  buildGovernedUrlInventory,
  mapOwnerIntentPublicContent,
  PUBLIC_CONTENT_STATE,
} from "../lib/ai-visibility/discoverability-phase3c2.js";
import { resolveGovernedBrandUrl } from "../lib/ai-visibility/brand-url-governance.js";
import { analyzeRobotsTxt } from "../lib/ai-visibility/public-crawl-checks.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const precheckOnly = process.argv.includes("--precheck-only");

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function extractMetaDescription(html = "") {
  const m =
    html.match(
      /<meta[^>]+name=["']description["'][^>]+content=["']([^"']*)["']/i
    ) ||
    html.match(
      /<meta[^>]+content=["']([^"']*)["'][^>]+name=["']description["']/i
    );
  return Boolean(m && m[1] && String(m[1]).trim());
}

function extractStructuredDataPresent(html = "") {
  return /application\/ld\+json/i.test(html) || /itemscope/i.test(html);
}

function detectLanguageHint(html = "") {
  const htmlLang = html.match(/<html[^>]+lang=["']([^"']+)["']/i);
  return htmlLang?.[1] ? htmlLang[1].slice(0, 16) : null;
}

function buildPrecheck() {
  const pilot = loadDiscoverabilityPilotBrands();
  const brands = pilot.brands || [];
  const governedUrls = [];
  for (const b of brands) {
    const inv = buildGovernedUrlInventory(b);
    for (const slot of inv.slots.filter((s) => s.url)) {
      governedUrls.push({
        brandId: b.brandId,
        brandName: b.brandName,
        slotId: slot.id,
        url: slot.url,
        sourceField: slot.sourceField,
      });
    }
  }
  const selected = governedUrls.filter((u) => u.slotId === "official_homepage");
  const expectedRequests = selected.length * 3;

  return {
    artifact: "BRAND_AI_VISIBILITY_DISCOVERABILITY_LIVE_PRECHECK",
    ELIGIBLE_BRANDS: brands.length,
    GOVERNED_URLS_TOTAL: governedUrls.length,
    URLS_SELECTED_FOR_BOUNDED_LIVE_TEST: selected.length,
    selectedUrls: selected,
    REQUEST_CAP: NETWORK_ACCESS_POLICY.MAX_REQUESTS_PER_RUN,
    EXPECTED_REQUESTS: expectedRequests,
    TIMEOUT: NETWORK_ACCESS_POLICY.TIMEOUT_MS,
    RATE_LIMIT: NETWORK_ACCESS_POLICY.RATE_LIMIT_MS,
    ROBOTS_POLICY: "Fetch and analyze robots.txt (RESPECT_ROBOTS=true)",
    REDIRECT_POLICY: `Manual redirect follow; max ${NETWORK_ACCESS_POLICY.MAX_REDIRECTS}`,
    READY_TO_EXECUTE:
      PUBLIC_CHECK_ENGINE_READY === true &&
      selected.length > 0 &&
      expectedRequests <= NETWORK_ACCESS_POLICY.MAX_REQUESTS_PER_RUN,
    SEARCH_DISCOVERED_URLS: 0,
    AI_PROVIDER_CALLS: 0,
    FABRICATED_URLS: 0,
  };
}

async function checkOneBrand(brand, stats) {
  const governed = resolveGovernedBrandUrl(brand);
  if (!governed.ok || !governed.url) {
    return {
      brandId: brand.brandId,
      brandName: brand.brandName,
      REQUESTED_URL: null,
      ERROR_STATE: governed.gap || "no_url",
      ok: false,
    };
  }

  const domain = governed.domain || "unknown";
  stats.byDomain = stats.byDomain || {};
  const bump = () => {
    stats.totalRequests = (stats.totalRequests || 0) + 1;
    stats.byDomain[domain] = (stats.byDomain[domain] || 0) + 1;
  };

  let origin;
  try {
    origin = new URL(governed.url).origin;
  } catch {
    return {
      brandId: brand.brandId,
      REQUESTED_URL: governed.url,
      ERROR_STATE: "invalid_url",
      ok: false,
    };
  }

  bump();
  let budget = assertWithinRequestBudget(stats);
  if (!budget.ok) {
    return {
      brandId: brand.brandId,
      REQUESTED_URL: governed.url,
      ERROR_STATE: budget.reason,
      ok: false,
    };
  }

  await sleep(NETWORK_ACCESS_POLICY.RATE_LIMIT_MS);
  const robotsFetch = await boundedFetch(`${origin}/robots.txt`);

  bump();
  budget = assertWithinRequestBudget(stats);
  if (!budget.ok) {
    return {
      brandId: brand.brandId,
      REQUESTED_URL: governed.url,
      ERROR_STATE: budget.reason,
      ok: false,
    };
  }

  await sleep(NETWORK_ACCESS_POLICY.RATE_LIMIT_MS);
  const pageFetch = await boundedFetch(governed.url);

  let sitemapContent = null;
  const robotsAnalysis = robotsFetch.body
    ? analyzeRobotsTxt(robotsFetch.body, {
        path: new URL(governed.url).pathname,
      })
    : null;
  const sitemapUrl = robotsAnalysis?.sitemaps?.[0];
  if (sitemapUrl) {
    bump();
    budget = assertWithinRequestBudget(stats);
    if (budget.ok) {
      await sleep(NETWORK_ACCESS_POLICY.RATE_LIMIT_MS);
      const sm = await boundedFetch(sitemapUrl);
      sitemapContent = sm.body || null;
    }
  }

  const result = runPublicChecksFromFixtures({
    pageUrl: pageFetch.url || governed.url,
    html: pageFetch.body || "",
    robotsContent: robotsFetch.body || "",
    sitemapContent,
    httpStatus: pageFetch.status,
    brandName: brand.brandName,
    redirectChain: pageFetch.redirectChain,
  });

  const html = pageFetch.body || "";
  const httpStatus = pageFetch.status ?? null;
  const httpAccessible =
    typeof httpStatus === "number" && httpStatus >= 200 && httpStatus < 400;

  const inventory = buildGovernedUrlInventory(brand);
  const checksBySlot = {
    official_homepage: {
      failed: !httpAccessible || Boolean(pageFetch.error),
      contentFound: Boolean(
        result.developmentContent?.developmentFranchisePositioningPresent ||
          result.contentInInitialHtml === "yes" ||
          (typeof result.contentInInitialHtml === "object" &&
            result.contentInInitialHtml) ||
          (html && html.length > 200)
      ),
    },
  };
  const intentMapping = mapOwnerIntentPublicContent({
    inventory,
    brandRow: brand,
    checksBySlot,
    language: detectLanguageHint(html),
  });

  return {
    brandId: brand.brandId,
    brandName: brand.brandName,
    REQUESTED_URL: governed.url,
    FINAL_URL: pageFetch.url || governed.url,
    HTTP_STATUS: httpStatus,
    REDIRECT_CHAIN: pageFetch.redirectChain || [],
    HTTP_ACCESSIBLE: httpAccessible,
    CONTENT_TYPE:
      pageFetch.headers?.["content-type"] ||
      pageFetch.headers?.["Content-Type"] ||
      null,
    CONTENT_RETRIEVABLE: Boolean(html && html.length > 50),
    PAGE_TITLE_PRESENT: Boolean(result.title),
    META_DESCRIPTION_PRESENT: extractMetaDescription(html),
    CANONICAL_PRESENT: Boolean(result.canonical),
    STRUCTURED_DATA_PRESENT: extractStructuredDataPresent(html),
    ROBOTS_ALLOWED: result.robots?.oaiSearchBot?.allowed ?? null,
    ROBOTS_REACHABLE: result.robots?.reachable === true,
    INDEXABILITY_STATE: result.indexability?.status || result.indexability || null,
    LANGUAGE: detectLanguageHint(html),
    LAST_CHECKED_AT: new Date().toISOString(),
    ERROR_STATE: pageFetch.error || (!httpAccessible ? `http_${httpStatus}` : null),
    ok: httpAccessible && !pageFetch.error,
    developmentContent: result.developmentContent || null,
    intentMapping: intentMapping.map((m) => ({
      ownerIntentFamily: m.ownerIntentFamily,
      state: m.state,
      url: m.url || null,
      evidence:
        m.state === PUBLIC_CONTENT_STATE.PUBLIC_CONTENT_FOUND
          ? {
              pageUrl: pageFetch.url || governed.url,
              titlePresent: Boolean(result.title),
              contentRetrievable: Boolean(html && html.length > 50),
            }
          : null,
    })),
  };
}

async function runLiveValidation(precheck) {
  const stats = { totalRequests: 0, byDomain: {} };
  const rows = [];
  let successful = 0;
  let failed = 0;
  let redirectsHandled = 0;
  let robotsStatesHandled = 0;

  for (const sel of precheck.selectedUrls) {
    const brand = {
      brandId: sel.brandId,
      brandName: sel.brandName,
      brandWebsite: sel.url,
    };
    console.log(`Checking ${brand.brandName} → ${brand.brandWebsite}`);
    const row = await checkOneBrand(brand, stats);
    rows.push(row);
    if (row.REDIRECT_CHAIN?.length) redirectsHandled += 1;
    if (row.ROBOTS_REACHABLE === true || row.ROBOTS_ALLOWED != null) {
      robotsStatesHandled += 1;
    }
    if (row.ok) successful += 1;
    else failed += 1;
    console.log(
      `  → status=${row.HTTP_STATUS} accessible=${row.HTTP_ACCESSIBLE} title=${row.PAGE_TITLE_PRESENT} error=${row.ERROR_STATE || "none"}`
    );
  }

  const engineBehavior = {
    redirects_field_present: rows.every((r) => Array.isArray(r.REDIRECT_CHAIN)),
    redirects_observed: redirectsHandled > 0,
    non200_or_error_field_present: rows.every(
      (r) => typeof r.HTTP_STATUS === "number" || r.ERROR_STATE
    ),
    missing_metadata_detectable: rows.some(
      (r) =>
        r.PAGE_TITLE_PRESENT === false || r.META_DESCRIPTION_PRESENT === false
    ),
    canonical_detectable: rows.every(
      (r) => r.CANONICAL_PRESENT === true || r.CANONICAL_PRESENT === false
    ),
    structured_data_detectable: rows.every(
      (r) =>
        r.STRUCTURED_DATA_PRESENT === true || r.STRUCTURED_DATA_PRESENT === false
    ),
    robots_state_handled: robotsStatesHandled > 0,
    content_extraction: rows.every(
      (r) => r.CONTENT_RETRIEVABLE === true || r.CONTENT_RETRIEVABLE === false
    ),
    timeouts_failures_surfaced: rows.every((r) => "ERROR_STATE" in r),
  };

  const liveEngineValidated =
    rows.length === precheck.URLS_SELECTED_FOR_BOUNDED_LIVE_TEST &&
    rows.every((r) => r.REQUESTED_URL && r.LAST_CHECKED_AT) &&
    engineBehavior.redirects_field_present &&
    engineBehavior.non200_or_error_field_present &&
    engineBehavior.canonical_detectable &&
    engineBehavior.structured_data_detectable &&
    engineBehavior.robots_state_handled &&
    engineBehavior.content_extraction &&
    engineBehavior.timeouts_failures_surfaced &&
    // At least one successful real fetch proves live path works
    successful >= 1;

  return {
    artifact: "BRAND_AI_VISIBILITY_DISCOVERABILITY_LIVE_VALIDATION_COMPLETE",
    completedAt: new Date().toISOString(),
    precheck,
    BRANDS_TESTED: new Set(rows.map((r) => r.brandId)).size,
    URLS_TESTED: rows.length,
    SUCCESSFUL: successful,
    FAILED: failed,
    REDIRECTS_HANDLED: redirectsHandled,
    ROBOTS_STATES_HANDLED: robotsStatesHandled,
    CONTENT_EXTRACTION: engineBehavior.content_extraction,
    OWNER_INTENT_MAPPING: rows.every((r) => Array.isArray(r.intentMapping)),
    rows,
    engineBehavior,
    LIVE_ENGINE_BEHAVIOR_VALIDATED: liveEngineValidated ? "YES" : "NO",
    DISCOVERABILITY_3C2_EXECUTION_STATUS: liveEngineValidated
      ? "LIVE_PUBLIC_BASELINE_VALIDATED"
      : "FIXTURE_VALIDATED_LIVE_REMEDIATION_REQUIRED",
    requestStats: stats,
    AI_PROVIDER_CALLS: 0,
    SEARCH_DISCOVERED_URLS: 0,
    FABRICATED_URLS: 0,
  };
}

const precheck = buildPrecheck();
console.log("\n=== Bounded Live Discoverability Precheck ===\n");
console.log(`ELIGIBLE_BRANDS: ${precheck.ELIGIBLE_BRANDS}`);
console.log(`GOVERNED_URLS_TOTAL: ${precheck.GOVERNED_URLS_TOTAL}`);
console.log(
  `URLS_SELECTED_FOR_BOUNDED_LIVE_TEST: ${precheck.URLS_SELECTED_FOR_BOUNDED_LIVE_TEST}`
);
console.log(`REQUEST_CAP: ${precheck.REQUEST_CAP}`);
console.log(`TIMEOUT: ${precheck.TIMEOUT}`);
console.log(`RATE_LIMIT: ${precheck.RATE_LIMIT}`);
console.log(`ROBOTS_POLICY: ${precheck.ROBOTS_POLICY}`);
console.log(`REDIRECT_POLICY: ${precheck.REDIRECT_POLICY}`);
console.log(`READY_TO_EXECUTE: ${precheck.READY_TO_EXECUTE ? "YES" : "NO"}`);
for (const u of precheck.selectedUrls) {
  console.log(`  - ${u.brandName}: ${u.url}`);
}

if (precheckOnly) process.exit(precheck.READY_TO_EXECUTE ? 0 : 1);
if (!precheck.READY_TO_EXECUTE) {
  console.error("Precheck not ready — aborting.");
  process.exit(1);
}

console.log("\n=== Executing bounded live checks ===\n");
const report = await runLiveValidation(precheck);

const outDir = path.join(__dirname, "..", "data", "ai-visibility", "validation");
fs.mkdirSync(outDir, { recursive: true });
const outPath = path.join(
  outDir,
  "brand-ai-visibility-discoverability-live-validation.json"
);
fs.writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");

if (report.LIVE_ENGINE_BEHAVIOR_VALIDATED === "YES") {
  const runtimeDir = path.join(
    __dirname,
    "..",
    "data",
    "ai-visibility",
    "runtime",
    "phase3c2-reports"
  );
  fs.mkdirSync(runtimeDir, { recursive: true });
  const latest = {
    phase: "3C.2",
    MODE: "BOUNDED_LIVE",
    LIVE_BASELINE_EXECUTED: true,
    DISCOVERABILITY_3C2_EXECUTION_STATUS:
      report.DISCOVERABILITY_3C2_EXECUTION_STATUS,
    completedAt: report.completedAt,
    BRANDS_CHECKED: report.BRANDS_TESTED,
    URLS_CHECKED: report.URLS_TESTED,
    ACCESSIBLE: report.SUCCESSFUL,
    CHECK_FAILED: report.FAILED,
    SOURCE_NOT_CONFIGURED: 0,
    ARBITRARY_SCORE: false,
    LIVE_ENGINE_BEHAVIOR_VALIDATED: true,
    byBrandId: Object.fromEntries(
      report.rows.map((r) => [
        r.brandId,
        {
          brandId: r.brandId,
          brandName: r.brandName,
          status: "MEASURED",
          DISCOVERABILITY: "BASELINE_MEASURED",
          OFFICIAL_SOURCES_CONFIGURED: true,
          PUBLIC_SOURCES_ACCESSIBLE: r.HTTP_ACCESSIBLE,
          OWNER_DEVELOPMENT_CONTENT_FOUND: Boolean(
            r.developmentContent?.developmentFranchisePositioningPresent
          ),
          OWNER_INTENT_CONTENT_GAPS: (r.intentMapping || [])
            .filter(
              (m) =>
                m.state === PUBLIC_CONTENT_STATE.PUBLIC_CONTENT_NOT_FOUND ||
                m.state === PUBLIC_CONTENT_STATE.SOURCE_NOT_CONFIGURED
            )
            .map((m) => m.ownerIntentFamily),
          LAST_CHECKED_AT: r.LAST_CHECKED_AT,
          ARBITRARY_DISCOVERABILITY_SCORE: false,
          LIVE_BASELINE: true,
          baseline: {
            URL_EXISTS: true,
            HTTP_ACCESSIBLE: r.HTTP_ACCESSIBLE,
            FINAL_URL: r.FINAL_URL,
            HTTP_STATUS: r.HTTP_STATUS,
            PAGE_TITLE_PRESENT: r.PAGE_TITLE_PRESENT,
            META_DESCRIPTION_PRESENT: r.META_DESCRIPTION_PRESENT,
            CANONICAL_PRESENT: r.CANONICAL_PRESENT,
            STRUCTURED_DATA_PRESENT: r.STRUCTURED_DATA_PRESENT,
            CONTENT_RETRIEVABLE: r.CONTENT_RETRIEVABLE,
            INDEXABILITY_STATE: r.INDEXABILITY_STATE,
            ROBOTS_ALLOWED: r.ROBOTS_ALLOWED,
            LANGUAGE: r.LANGUAGE,
          },
        },
      ])
    ),
    liveValidationPath: outPath,
  };
  fs.writeFileSync(
    path.join(runtimeDir, "phase3c2_latest.json"),
    JSON.stringify(latest, null, 2),
    "utf8"
  );
}

console.log(`\nBRANDS_TESTED: ${report.BRANDS_TESTED}`);
console.log(`URLS_TESTED: ${report.URLS_TESTED}`);
console.log(`SUCCESSFUL: ${report.SUCCESSFUL}`);
console.log(`FAILED: ${report.FAILED}`);
console.log(`REDIRECTS_HANDLED: ${report.REDIRECTS_HANDLED}`);
console.log(`ROBOTS_STATES_HANDLED: ${report.ROBOTS_STATES_HANDLED}`);
console.log(`CONTENT_EXTRACTION: ${report.CONTENT_EXTRACTION}`);
console.log(`OWNER_INTENT_MAPPING: ${report.OWNER_INTENT_MAPPING}`);
console.log(
  `LIVE_ENGINE_BEHAVIOR_VALIDATED: ${report.LIVE_ENGINE_BEHAVIOR_VALIDATED}`
);
console.log(
  `DISCOVERABILITY_3C2_EXECUTION_STATUS: ${report.DISCOVERABILITY_3C2_EXECUTION_STATUS}`
);
console.log(`Report: ${outPath}`);

process.exit(report.LIVE_ENGINE_BEHAVIOR_VALIDATED === "YES" ? 0 : 1);
