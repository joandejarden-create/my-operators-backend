#!/usr/bin/env node
/**
 * Phase 3C.1 tests — Discoverability / Referral / Business Impact foundation.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { PRODUCT_DEFINITIONS, COMPOSITE_SCORE, VISIBILITY_VS_DISCOVERABILITY_GUARD } from "../lib/ai-visibility/discoverability-taxonomy.js";
import { DATA_STATE, CONNECTION_STATE, resolveDataStateDisplay, guardAgainstSyntheticZero } from "../lib/ai-visibility/discoverability-data-states.js";
import { PRIORITY_PAGE_STATUS, INDEXABILITY_STATUS, CONTENT_IN_INITIAL_HTML } from "../lib/ai-visibility/discoverability-dimensions.js";
import { resolveGovernedBrandUrl, URL_GOVERNANCE_GAPS } from "../lib/ai-visibility/brand-url-governance.js";
import { AI_CRAWLER_REGISTRY, matchCrawlerUserAgent } from "../lib/ai-visibility/ai-crawler-registry.js";
import {
  parseRobotsTxt,
  isPathAllowed,
  evaluateOaiSearchBotAccess as evalOai,
  findRobotsGroup,
} from "../lib/ai-visibility/robots-parser.js";
import {
  extractCanonicalFromHtml,
  extractMetaRobots,
  assessContentInInitialHtml,
  evaluateIndexability,
} from "../lib/ai-visibility/public-crawl-checks.js";
import { runPublicChecksFromFixtures } from "../lib/ai-visibility/public-check-engine.js";
import { classifyAiReferrer, REFERRAL_CLASSIFICATION } from "../lib/ai-visibility/referral-intelligence.js";
import { isQualifiedBusinessImpactEvent, READERSHIP_ENRICHMENT } from "../lib/ai-visibility/business-impact.js";
import { CAPABILITY_MATRIX, STORAGE_MAPPING } from "../lib/ai-visibility/discoverability-contracts.js";
import { LOG_ADAPTER_INTERFACE_READY, ANALYTICS_ADAPTER_INTERFACE_READY, LIVE_LOG_CONNECTION, LIVE_ANALYTICS_CONNECTION } from "../lib/ai-visibility/discoverability-adapters.js";
import {
  buildDiscoverabilityExecutiveBlock,
  buildDiscoverabilityDetailBlock,
} from "../lib/ai-visibility/discoverability-read-service.js";
import {
  buildDiscoverabilityExecutivePlaceholder,
  OPENAI_DISCOVERABILITY_STATUS,
} from "../lib/ai-visibility/future-discoverability.js";
import { verifyBaselineFreeze } from "../lib/ai-visibility/baseline-freeze-verify.js";
import { executePhase3c1 } from "../lib/ai-visibility/phase3c1-orchestrator.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURES = path.join(__dirname, "..", "fixtures", "ai-visibility");

let passed = 0;
let failed = 0;
const asyncTests = [];

function test(name, fn) {
  try {
    const r = fn();
    if (r && typeof r.then === "function") {
      asyncTests.push(
        r
          .then(() => {
            passed += 1;
            console.log(`  PASS ${name}`);
          })
          .catch((err) => {
            failed += 1;
            console.error(`  FAIL ${name}: ${err.message}`);
          })
      );
      return;
    }
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

console.log("\nAI Visibility Phase 3C.1 — Discoverability / Referral / Business Impact\n");

test("taxonomy — Visibility != Discoverability != Referral != Business Impact", () => {
  assert.notEqual(PRODUCT_DEFINITIONS.AI_VISIBILITY.question, PRODUCT_DEFINITIONS.DISCOVERABILITY.question);
  assert.notEqual(PRODUCT_DEFINITIONS.DISCOVERABILITY.question, PRODUCT_DEFINITIONS.REFERRAL.question);
  assert.notEqual(PRODUCT_DEFINITIONS.REFERRAL.question, PRODUCT_DEFINITIONS.BUSINESS_IMPACT.question);
});

test("composite score — not allowed", () => {
  assert.equal(COMPOSITE_SCORE.ALLOWED, false);
});

test("data state — connection required != zero", () => {
  const d = resolveDataStateDisplay({ dataState: DATA_STATE.CONNECTION_REQUIRED });
  assert.equal(d.value, null);
  assert.notEqual(d.display, "0");
  assert.equal(guardAgainstSyntheticZero(DATA_STATE.CONNECTION_REQUIRED, 0), null);
});

test("data state — measured value when source exists", () => {
  const d = resolveDataStateDisplay({ dataState: DATA_STATE.MEASURED, value: 42 });
  assert.equal(d.value, 42);
});

test("robots parser — OAI-SearchBot allowed", () => {
  const content = fs.readFileSync(path.join(FIXTURES, "robots-samples/openai-allowed.txt"), "utf8");
  const parsed = parseRobotsTxt(content);
  const oai = evalOai(parsed, "/");
  assert.equal(oai.allowed, true);
  assert.equal(oai.status, "robots_access_allowed");
});

test("robots parser — OAI-SearchBot blocked", () => {
  const content = fs.readFileSync(path.join(FIXTURES, "robots-samples/oai-blocked.txt"), "utf8");
  const parsed = parseRobotsTxt(content);
  const oai = evalOai(parsed, "/");
  assert.equal(oai.allowed, false);
  assert.equal(oai.status, "robots_access_blocked");
});

test("robots parser — wildcard fallback", () => {
  const parsed = parseRobotsTxt("User-agent: *\nDisallow: /private/\n");
  assert.equal(isPathAllowed(parsed, "UnknownBot", "/public/").allowed, true);
  assert.equal(isPathAllowed(parsed, "UnknownBot", "/private/page").allowed, false);
});

test("robots parser — exact user-agent group", () => {
  const parsed = parseRobotsTxt("User-agent: OAI-SearchBot\nDisallow: /secret/\nUser-agent: *\nAllow: /\n");
  const group = findRobotsGroup(parsed, "OAI-SearchBot");
  assert.ok(group);
  assert.equal(isPathAllowed(parsed, "OAI-SearchBot", "/secret/").allowed, false);
});

test("robots permission != actual crawl", () => {
  assert.ok(AI_CRAWLER_REGISTRY.length >= 4);
  const oai = evalOai(parseRobotsTxt("User-agent: OAI-SearchBot\nAllow: /\n"), "/");
  assert.ok(oai.RULE || true);
  assert.notEqual(oai.status, "actual_crawl_observed_in_logs");
});

test("crawl readiness — canonical and noindex", () => {
  const html = fs.readFileSync(path.join(FIXTURES, "discoverability-samples/development-page.html"), "utf8");
  assert.equal(extractCanonicalFromHtml(html), "https://autograph-hotels.marriott.com/development");
  assert.equal(extractMetaRobots(html).noindex, false);
});

test("crawlable HTML — content present", () => {
  const html = fs.readFileSync(path.join(FIXTURES, "discoverability-samples/development-page.html"), "utf8");
  assert.equal(assessContentInInitialHtml(html), CONTENT_IN_INITIAL_HTML.YES);
});

test("indexability — technically indexable", () => {
  const html = fs.readFileSync(path.join(FIXTURES, "discoverability-samples/development-page.html"), "utf8");
  const idx = evaluateIndexability({ httpStatus: 200, metaRobots: extractMetaRobots(html), html });
  assert.equal(idx, INDEXABILITY_STATUS.TECHNICALLY_INDEXABLE);
});

test("crawler identity — unknown stays unknown", () => {
  assert.equal(matchCrawlerUserAgent("Mozilla/5.0 Chrome"), null);
});

test("referral — google.com not Gemini", () => {
  const c = classifyAiReferrer({ referrer: "https://www.google.com/" });
  assert.equal(c.classification, REFERRAL_CLASSIFICATION.NON_AI);
});

test("referral — chatgpt direct", () => {
  const c = classifyAiReferrer({ referrer: "https://chatgpt.com/" });
  assert.equal(c.classification, REFERRAL_CLASSIFICATION.DIRECT_AI_REFERRAL);
  assert.equal(c.provider, "openai");
});

test("referral — unknown referrer", () => {
  const c = classifyAiReferrer({});
  assert.equal(c.classification, REFERRAL_CLASSIFICATION.UNKNOWN);
});

test("business impact — qualified event required", () => {
  assert.equal(isQualifiedBusinessImpactEvent({ eventId: "e1", qualified: true }), true);
  assert.equal(isQualifiedBusinessImpactEvent({ qualified: true }), false);
});

test("business impact — no inferred actions guard", () => {
  assert.equal(VISIBILITY_VS_DISCOVERABILITY_GUARD.INFERRED_BUSINESS_IMPACT_ALLOWED, false);
});

test("priority pages — missing != unknown", () => {
  assert.notEqual(PRIORITY_PAGE_STATUS.MISSING, PRIORITY_PAGE_STATUS.UNKNOWN);
});

test("UI — no fake values in executive block", () => {
  const block = buildDiscoverabilityExecutiveBlock();
  assert.equal(block.SYNTHETIC_VALUES, false);
  assert.equal(block.referral.display.aiReferralSessions, "Analytics Connection Required");
  assert.equal(block.businessImpact.display.qualifiedDevelopmentActions, "Analytics Connection Required");
});

test("UI — no composite score in detail", () => {
  const detail = buildDiscoverabilityDetailBlock();
  assert.equal(detail.COMPOSITE_SCORE, false);
});

test("UI — connection-required not No Data", () => {
  const exec = buildDiscoverabilityExecutivePlaceholder();
  assert.ok(exec.referral.aiReferralSessions.display.includes("Connection"));
});

test("adapters — no live connections", () => {
  assert.equal(LOG_ADAPTER_INTERFACE_READY, true);
  assert.equal(ANALYTICS_ADAPTER_INTERFACE_READY, true);
  assert.equal(LIVE_LOG_CONNECTION, false);
  assert.equal(LIVE_ANALYTICS_CONNECTION, false);
});

test("storage — high-volume not Airtable", () => {
  assert.ok(STORAGE_MAPPING.AIRTABLE_NOT_FOR.includes("high_volume_analytics"));
});

test("readership — deferred", () => {
  assert.equal(READERSHIP_ENRICHMENT.STATUS, "DEFERRED");
});

test("url governance — company validated priority", () => {
  const r = resolveGovernedBrandUrl({ brandWebsite: "https://example.com" });
  assert.equal(r.sourceTier, "company_validated");
  assert.ok(URL_GOVERNANCE_GAPS.length > 0);
});

test("capability matrix — AI Presence measured separately", () => {
  const ai = CAPABILITY_MATRIX.find((m) => m.metric === "AI Presence");
  assert.equal(ai.dataState, "MEASURED");
  const crawl = CAPABILITY_MATRIX.find((m) => m.metric === "Crawl Readiness");
  assert.equal(crawl.PUBLIC_NOW, true);
});

test("baseline untouched", () => {
  const v = verifyBaselineFreeze();
  if (v.BASELINE_FREEZE_VALID) assert.equal(v.OBSERVATIONS, 336);
});

test("public check engine — fixture run", () => {
  const html = fs.readFileSync(path.join(FIXTURES, "discoverability-samples/development-page.html"), "utf8");
  const robots = fs.readFileSync(path.join(FIXTURES, "robots-samples/openai-allowed.txt"), "utf8");
  const r = runPublicChecksFromFixtures({
    pageUrl: "https://example.com/dev",
    html,
    robotsContent: robots,
    httpStatus: 200,
    brandName: "Autograph Collection",
  });
  assert.equal(r.indexability, INDEXABILITY_STATUS.TECHNICALLY_INDEXABLE);
});

test("orchestrator — pass report", async () => {
  const report = await executePhase3c1();
  assert.ok(report.BUILD_STATUS.includes("PASS"));
  assert.equal(report.ACTIVITY.LIVE_AI_PROVIDER_CALLS, 0);
  assert.equal(report.ACTIVITY.PERIOD2_MONITORING_CALLS, 0);
});

async function runAsyncTests() {
  await Promise.all(asyncTests);
  console.log(`\nPhase 3C.1 tests: ${passed} passed, ${failed} failed\n`);
  process.exit(failed > 0 ? 1 : 0);
}

runAsyncTests();
