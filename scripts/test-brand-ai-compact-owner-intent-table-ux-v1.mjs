#!/usr/bin/env node
/**
 * Brand AI compact Owner Intent table UX V1.
 * Presentation-only. PROVIDER_CALLS = 0.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { IDS, SCENARIO_IDS as S } from "../lib/ai-visibility/competitive-moat/benchmark-brand-ids.js";
import { buildUnifiedOwnerIntentCoverage } from "../lib/ai-visibility/competitive-moat/unified-owner-intent-coverage.js";
import {
  BENCHMARK_SCOPES,
  lookupScopeCertification,
} from "../lib/ai-visibility/competitive-moat/provider-scoped-benchmark-certification.js";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import { loadObservationsFromBatchSummary } from "../lib/ai-visibility/cohort-observations.js";
import { findMatchingSummaries } from "../lib/ai-visibility/brand-read-service.js";
import { auditPayloadForCanonicalPromptLeaks } from "../lib/ai-visibility/customer-prompt-disclosure.js";
import { loadShowcaseCompaniesConfig } from "../lib/ai-visibility/brand-ai-showcase-companies.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const BRAND_JS = fs.readFileSync(
  path.join(ROOT, "public/js/ai-visibility/ai-visibility-brand.js"),
  "utf8"
);
const BRAND_HTML = fs.readFileSync(
  path.join(ROOT, "public/ai-visibility-brand.html"),
  "utf8"
);
const BRAND_CSS = fs.readFileSync(
  path.join(ROOT, "public/js/ai-visibility/ai-visibility-shared.css"),
  "utf8"
);

let passed = 0;
let failed = 0;

async function test(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log("  PASS", name);
  } catch (err) {
    failed += 1;
    console.log("  FAIL", name + ":", err.message);
  }
}

async function loadObsForProvider(providerId) {
  const store = createBrandAiVisibilityReadStore();
  const geo = { geographyScope: "Region", commercialRegion: "CALA", key: "CALA" };
  const summary = (await findMatchingSummaries(store, geo, providerId, { language: "en" }))[0];
  if (!summary) return [];
  const { observations } = await loadObservationsFromBatchSummary(store, summary, {
    language: "en",
  });
  return observations || [];
}

function softRow(payload) {
  return (payload.rows || []).find((r) => r.scenarioId === S.SOFT_BRAND);
}

function unifiedFn() {
  const start = BRAND_JS.indexOf("function renderIntentCoverage");
  const end = BRAND_JS.indexOf("function renderLegacyIntentCoverage");
  assert.ok(start > 0 && end > start, "renderIntentCoverage bounds");
  return BRAND_JS.slice(start, end);
}

console.log("\nBrand AI Compact Owner Intent Table UX V1\n");

await test("collapsed default — no inline context, peers, competitors, or watchlist CTA", () => {
  const fn = unifiedFn();
  assert.match(fn, /aiv-intent-expand/);
  assert.match(fn, /aria-expanded="false"/);
  assert.match(fn, /aiv-unified-intent-detail/);
  assert.match(fn, /hidden/);
  assert.doesNotMatch(fn, /View in watchlist/);
  assert.doesNotMatch(fn, /aiv-intent-watchlist-link/);
  assert.match(fn, /aiv-intent-decision-context--expanded/);
  assert.match(BRAND_JS, /Core Peers/);
  assert.match(BRAND_JS, /Observed Competitors/);
  assert.match(BRAND_JS, /ONE_ROW_AT_A_TIME/);
  assert.match(BRAND_JS, /collapseAll/);
});

await test("watchlist section preserved; zero row CTAs in Coverage Diagnostics", () => {
  assert.match(BRAND_HTML, /Questions Missing Watchlist/);
  assert.match(BRAND_HTML, /id="aivDetailWatchlistSection"/);
  assert.equal((BRAND_JS.match(/View in watchlist/g) || []).length, 0);
});

await test("shared compact CSS — chevron, one-row detail, compact developing copy", () => {
  assert.match(BRAND_CSS, /aiv-intent-expand/);
  assert.match(BRAND_CSS, /aiv-intent-chevron/);
  assert.match(BRAND_CSS, /aiv-unified-intent-detail/);
  assert.match(BRAND_CSS, /aiv-intent-detail-panel/);
  assert.match(BRAND_CSS, /table-layout:\s*fixed/);
  assert.match(BRAND_HTML, /Presence, gaps and competitive position across monitored owner decisions/);
});

await test("no Autograph-specific production render branch", () => {
  const fn = unifiedFn();
  assert.doesNotMatch(fn, /Autograph/);
  assert.doesNotMatch(fn, /recEJCTDj1zrsjPM6/);
});

await test("Autograph Perplexity Soft Brand values unchanged", async () => {
  const cert = lookupScopeCertification(IDS.AUTOGRAPH, S.SOFT_BRAND, BENCHMARK_SCOPES.PERPLEXITY);
  const obs = (await loadObsForProvider("perplexity")).filter(
    (o) => String(o.provider || "").toLowerCase() === "perplexity"
  );
  const payload = buildUnifiedOwnerIntentCoverage(IDS.AUTOGRAPH, {
    allProvidersMode: false,
    provider: "perplexity",
    observations: obs,
  });
  const row = softRow(payload);
  assert.ok(row);
  assert.equal(row.subjectPresenceDisplay, "90%");
  assert.equal(row.presenceDenominatorDisplay, "9 of 10 observations");
  assert.equal(row.missingCount, 1);
  assert.equal(row.peerPresentGapCount, 0);
  assert.equal(row.indexValue, cert.certifiedIndex);
  assert.equal(row.indexValue, 114);
  assert.match(row.position, /14% above benchmark/);
});

await test("Autograph All Providers / OpenAI values unchanged", async () => {
  const obs = await loadObsForProvider("openai");
  const all = buildUnifiedOwnerIntentCoverage(IDS.AUTOGRAPH, {
    allProvidersMode: true,
    observations: obs,
  });
  const openai = buildUnifiedOwnerIntentCoverage(IDS.AUTOGRAPH, {
    allProvidersMode: false,
    provider: "openai",
    observations: obs.filter((o) => String(o.provider || "").toLowerCase() === "openai"),
  });
  assert.equal(softRow(all).indexValue, 103);
  assert.equal(softRow(all).subjectPresenceDisplay, "100%");
  assert.equal(softRow(openai).indexValue, 100);
  assert.equal(softRow(openai).position, "At parity");
});

await test("cross-parent showcase companies present for shared table", () => {
  const showcase = loadShowcaseCompaniesConfig();
  const keys = new Set((showcase.companies || []).map((c) => c.companyKey));
  for (const parent of ["marriott", "hilton", "ihg", "choice"]) {
    assert.ok(keys.has(parent), parent);
    const co = (showcase.companies || []).find((c) => c.companyKey === parent);
    assert.ok(co?.brandIds?.length, parent + " brands");
  }
});

await test("prompt moat on unified payload", async () => {
  const obs = await loadObsForProvider("openai");
  const payload = buildUnifiedOwnerIntentCoverage(IDS.AUTOGRAPH, {
    allProvidersMode: true,
    observations: obs,
  });
  const leak = auditPayloadForCanonicalPromptLeaks(payload);
  assert.equal(leak.leakCount, 0, leak.leaks?.join(", "));
  const fn = unifiedFn();
  assert.doesNotMatch(fn, /questionText|promptText|canonicalPrompt/);
});

await test("accessibility contract on expand control", () => {
  const fn = unifiedFn();
  assert.match(fn, /aria-controls/);
  assert.match(fn, /Show details for/);
  assert.match(fn, /type="button"/);
  assert.match(BRAND_JS, /Hide details for/);
});

console.log("\n" + passed + " passed, " + failed + " failed\n");
if (failed) {
  console.log("BRAND_AI_COMPACT_OWNER_INTENT_TABLE_UX_REMEDIATION_REQUIRED");
  process.exit(1);
}
console.log("BRAND_AI_COMPACT_OWNER_INTENT_TABLE_UX_PASS");
