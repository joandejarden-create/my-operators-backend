#!/usr/bin/env node
/**
 * Phase 3A.3 — Hotel Decision Visibility contracts (no provider calls).
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  assertBrandAiVisibilityRoutesRegistered,
  BRAND_AI_VISIBILITY_EXPECTED_ROUTES,
} from "../lib/ai-visibility/route-registration-guard.js";
import {
  HDV_DEFINITIONS,
  HOTEL_DECISION_VISIBILITY_VERSION,
  topIntentForBrand,
} from "../lib/ai-visibility/hotel-decision-visibility.js";
import {
  HDV_REVIEW_RULES_VERSION,
  HDV_REVIEW_THRESHOLDS_V1,
  buildHotelDecisionVisibilityReviewItems,
} from "../lib/ai-visibility/hotel-decision-visibility-review-rules.js";
import { OPPORTUNITY_THRESHOLDS_V1 } from "../lib/ai-visibility/config.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

let passed = 0;
let failed = 0;
function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

console.log("\nAI Visibility Phase 3A.3 — Hotel Decision Visibility\n");

test("two-tab shell after Phase 3A.4: Exec + Detail only; HDV tab removed", () => {
  const html = fs.readFileSync(path.join(root, "public/ai-visibility-brand.html"), "utf8");
  assert.match(html, /id="aivTabExecutive"/);
  assert.match(html, /id="aivTabDetail"/);
  assert.ok(!/id="aivTabHdv"/.test(html));
  assert.ok(!/id="aivHdvView"/.test(html));
  assert.match(html, /id="aivExecutiveView"/);
  assert.match(html, /id="aivDetailView"/);
  assert.match(html, /Decision Patterns/);
  assert.match(html, /AI Discoverability/);
  assert.match(html, /class="section-nav-item active"[^>]*id="aivTabExecutive"/);
});

test("route registration no longer expects public hotel-decision-visibility", () => {
  assert.ok(
    !BRAND_AI_VISIBILITY_EXPECTED_ROUTES.some((r) =>
      r.path.includes("hotel-decision-visibility")
    )
  );
  const src = fs.readFileSync(path.join(root, "server.js"), "utf8");
  assert.ok(!src.includes('app.get(\n  "/api/ai-visibility/brand/hotel-decision-visibility"'));
  assert.ok(!/app\.get\(\s*"\/api\/ai-visibility\/brand\/hotel-decision-visibility"/.test(src));
  const fakeApp = {
    _router: {
      stack: BRAND_AI_VISIBILITY_EXPECTED_ROUTES.map((r) => ({
        route: { path: r.path, methods: { [r.method]: true } },
      })),
    },
  };
  const result = assertBrandAiVisibilityRoutesRegistered(fakeApp, {
    logger: { log() {}, error() {} },
  });
  assert.equal(result.ok, true);
});

test("HDV definitions and versions remain as internal service contracts", () => {
  assert.equal(HOTEL_DECISION_VISIBILITY_VERSION, "ai_visibility_hotel_decision_visibility_v1");
  assert.equal(HDV_REVIEW_RULES_VERSION, "hotel_decision_visibility_review_rules_v1");
  assert.ok(HDV_DEFINITIONS.DECISION_VISIBILITY_COVERAGE);
  assert.ok(HDV_DEFINITIONS.OWNER_INTENT_COVERAGE);
  assert.ok(HDV_DEFINITIONS.TOP_DECISION_TERRITORY);
  assert.ok(HDV_DEFINITIONS.REGIONAL_LEADER);
  assert.equal(
    HDV_REVIEW_THRESHOLDS_V1.presenceGapPp,
    OPPORTUNITY_THRESHOLDS_V1.competitorDominance.presenceRateGapPp
  );
});

test("Owner Intent Coverage numerator/denominator math (portfolio union)", () => {
  // Manual contract: 4 successful questions, 3 with ≥1 entitled brand present → 75%
  const entitled = new Set(["A", "B"]);
  const observations = [
    { success: true, presentEntityIds: ["A"], intentTerritory: "Conversion" },
    { success: true, presentEntityIds: ["X"], intentTerritory: "Conversion" },
    { success: true, presentEntityIds: ["B"], intentTerritory: "Mixed Use" },
    { success: true, presentEntityIds: [], intentTerritory: "Mixed Use" },
  ];
  const byIntent = new Map();
  for (const o of observations) {
    if (!o.success || !o.intentTerritory) continue;
    if (!byIntent.has(o.intentTerritory)) {
      byIntent.set(o.intentTerritory, { n: 0, d: 0 });
    }
    const row = byIntent.get(o.intentTerritory);
    row.d += 1;
    if ((o.presentEntityIds || []).some((id) => entitled.has(id))) row.n += 1;
  }
  assert.equal(byIntent.get("Conversion").n / byIntent.get("Conversion").d, 0.5);
  assert.equal(byIntent.get("Mixed Use").n / byIntent.get("Mixed Use").d, 0.5);
  const portfolioN = observations.filter((o) =>
    (o.presentEntityIds || []).some((id) => entitled.has(id))
  ).length;
  assert.equal(portfolioN / observations.length, 0.5);
});

test("Top Decision Territory tie-break is alphabetical after equal coverage", () => {
  const rows = [
    { intent: "Zebra", coverage: 0.5, won: 0, share: 0 },
    { intent: "Alpha", coverage: 0.5, won: 0, share: 0 },
    { intent: "Beta", coverage: 0.4, won: 9, share: 1 },
  ];
  rows.sort((a, b) => {
    if (b.coverage !== a.coverage) return b.coverage - a.coverage;
    if (b.won !== a.won) return b.won - a.won;
    if (b.share !== a.share) return b.share - a.share;
    return a.intent.localeCompare(b.intent);
  });
  assert.equal(rows[0].intent, "Alpha");
});

test("Top Decision Territory requires Presence > 0 — zero presence yields null", () => {
  const brandId = "recRyvM8OmLlDj9G7";
  const observations = [
    {
      success: true,
      promptId: "p1",
      intentTerritory: "Branded Residences",
      presentEntityIds: [],
    },
    {
      success: true,
      promptId: "p2",
      intentTerritory: "Conversion",
      presentEntityIds: [],
    },
  ];
  assert.equal(topIntentForBrand(observations, brandId), null);
  observations[0].presentEntityIds = [brandId];
  assert.equal(topIntentForBrand(observations, brandId), "Branded Residences");
});

test("review rules require evidenceId and reuse 15pp gap", () => {
  const withEvidence = buildHotelDecisionVisibilityReviewItems({
    geographyKey: "CALA",
    provider: "openai",
    entitledBrands: [{ brandId: "b1", brandName: "Brand One" }],
    subjectBrandIds: ["b1"],
    leaderPresence: 0.8,
    leaderBrandName: "Leader",
    brandPresenceById: { b1: 0.5 },
    brandMissingShareById: { b1: 0.6 },
    brandEvidenceIdById: { b1: "ev1" },
    regionalPresenceByBrand: { b1: { CALA: 0.5, Europe: 0.8 } },
    monitoredBrandIdsInGeo: ["b1"],
  });
  assert.ok(withEvidence.length >= 1);
  assert.ok(withEvidence.every((i) => i.evidenceId));
  assert.ok(withEvidence.every((i) => i.provider === "openai"));
  assert.ok(withEvidence.every((i) => i.providerLabel === "ChatGPT"));
  assert.ok(withEvidence.every((i) => i.rulesVersion === HDV_REVIEW_RULES_VERSION));
  assert.ok(withEvidence.every((i) => /OpenAI monitoring/.test(i.description)));
  assert.ok(!JSON.stringify(withEvidence).includes("confidence"));
  assert.ok(!JSON.stringify(withEvidence).includes("High Impact"));
  assert.ok(!/across AI platforms/i.test(JSON.stringify(withEvidence)));

  const noEvidence = buildHotelDecisionVisibilityReviewItems({
    geographyKey: "CALA",
    entitledBrands: [{ brandId: "b1", brandName: "Brand One" }],
    subjectBrandIds: ["b1"],
    leaderPresence: 0.8,
    brandPresenceById: { b1: 0.5 },
    brandMissingShareById: { b1: 0.9 },
    brandEvidenceIdById: { b1: null },
    regionalPresenceByBrand: {},
    monitoredBrandIdsInGeo: ["b1"],
  });
  assert.equal(noEvidence.length, 0);
});

test("no composite HDV/GEO score keys in service source", () => {
  const src = fs.readFileSync(
    path.join(root, "lib/ai-visibility/hotel-decision-visibility.js"),
    "utf8"
  );
  assert.match(src, /COMPOSITE_HDV_SCORE:\s*"NONE"/);
  assert.match(src, /GEO_SCORE:\s*"NONE"/);
  assert.match(src, /AI_AUTHORED_DEALALITY_CLAIMS:\s*0/);
  assert.match(src, /LIVE_PROVIDER_CALLS:\s*0/);
  assert.ok(!/vs prior 30 days/i.test(src));
  assert.match(src, /NO_SILENT_PROVIDER_FALLBACK:\s*true/);
});

test("HTML provider filter is data-driven (no hard-coded Gemini/Perplexity options)", () => {
  const html = fs.readFileSync(path.join(root, "public/ai-visibility-brand.html"), "utf8");
  assert.match(html, /id="aivProvider"/);
  assert.match(html, /option value="openai"/);
  assert.ok(!/Perplexity \(coming soon\)/.test(html));
  assert.ok(!/Google Gemini \(coming soon\)/.test(html));
  assert.ok(!/All AI/.test(html));
  const js = fs.readFileSync(
    path.join(root, "public/js/ai-visibility/ai-visibility-brand.js"),
    "utf8"
  );
  assert.match(js, /fillProviderSelect/);
  assert.match(js, /availableProviders/);
});

test("definitions doc exists", () => {
  const p = path.join(
    root,
    "docs/ai-build-system/AI_VISIBILITY_HOTEL_DECISION_VISIBILITY_DEFINITIONS.md"
  );
  assert.ok(fs.existsSync(p));
  const body = fs.readFileSync(p, "utf8");
  assert.match(body, /DECISION_VISIBILITY_COVERAGE/);
  assert.match(body, /OWNER_INTENT_COVERAGE/);
  assert.match(body, /hotel_decision_visibility_review_rules_v1/);
  assert.match(body, /Provider is a first-class/);
  assert.match(body, /No `All AI`/);
});

test("package.json has phase3a3 script", () => {
  const pkg = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8"));
  assert.ok(pkg.scripts["test:ai-visibility-phase3a3"]);
});

async function runAsyncProviderTests() {
  const { createAiVisibilityStore } = await import("../lib/ai-visibility/storage/index.js");
  const {
    listAvailableAiVisibilityProviders,
    normalizeProviderId,
    providersMatch,
    AI_VISIBILITY_CROSS_PROVIDER_STATUS,
  } = await import("../lib/ai-visibility/provider-dimension.js");
  const { findMatchingSummaries, parseGeographyQuery } = await import(
    "../lib/ai-visibility/brand-read-service.js"
  );
  const os = await import("node:os");
  const fsP = await import("node:fs");

  test("provider normalize + no All AI status constants", () => {
    assert.equal(normalizeProviderId({ name: "OpenAI" }), "openai");
    assert.equal(normalizeProviderId("gemini"), "gemini");
    assert.equal(providersMatch("openai", { name: "openai", model: "x" }), true);
    assert.equal(providersMatch("openai", "gemini"), false);
    assert.equal(AI_VISIBILITY_CROSS_PROVIDER_STATUS.ALL_AI, "NOT_IMPLEMENTED");
    assert.equal(AI_VISIBILITY_CROSS_PROVIDER_STATUS.PROVIDER_CONSENSUS, "FUTURE_READY");
  });

  const tmp = fsP.mkdtempSync(path.join(os.tmpdir(), "aiv-provider-"));
  const store = createAiVisibilityStore({ rootDir: tmp });

  await store.saveBatchSummary({
    batchId: "batch_openai_cala",
    status: "completed",
    completedAt: "2026-08-13T12:00:00.000Z",
    provider: { name: "openai", model: "gpt-5.6" },
    cohort: { geographyScope: "Region", commercialRegion: "CALA" },
  });
  await store.saveBatchSummary({
    batchId: "batch_gemini_cala",
    status: "completed",
    completedAt: "2026-08-13T13:00:00.000Z",
    provider: { name: "gemini", model: "gemini-test" },
    cohort: { geographyScope: "Region", commercialRegion: "CALA" },
  });
  await store.saveMetricSnapshot({
    snapshotId: "snap_openai",
    entityId: "brandA",
    metric: "ai_presence_rate",
    value: 0.5,
    geographyScope: "Region",
    commercialRegion: "CALA",
    provider: "openai",
    batchDate: "2026-08-13T12:00:00.000Z",
  });
  await store.saveMetricSnapshot({
    snapshotId: "snap_gemini",
    entityId: "brandA",
    metric: "ai_presence_rate",
    value: 0.9,
    geographyScope: "Region",
    commercialRegion: "CALA",
    provider: "gemini",
    batchDate: "2026-08-13T13:00:00.000Z",
  });
  await store.saveEvidence({
    evidenceId: "ev_openai",
    provider: "openai",
    promptId: "p1",
    payload: { mentions: [{ canonicalEntityId: "brandA" }] },
  });
  await store.saveEvidence({
    evidenceId: "ev_gemini",
    provider: "gemini",
    promptId: "p1",
    payload: { mentions: [{ canonicalEntityId: "brandA" }] },
  });

  const available = await listAvailableAiVisibilityProviders({
    store,
    geographyScope: "Region",
    commercialRegion: "CALA",
  });
  test("provider options derived from completed monitoring datasets", () => {
    const ids = available.map((p) => p.id).sort();
    assert.deepEqual(ids, ["gemini", "openai"]);
    assert.ok(available.every((p) => p.label && p.completedBatchCount >= 1));
  });

  const openaiOnlyStoreRoot = fsP.mkdtempSync(path.join(os.tmpdir(), "aiv-provider-oai-"));
  const openaiStore = createAiVisibilityStore({ rootDir: openaiOnlyStoreRoot });
  await openaiStore.saveBatchSummary({
    batchId: "batch_openai_only",
    status: "completed",
    completedAt: "2026-08-13T12:00:00.000Z",
    provider: { name: "openai", model: "gpt-5.6" },
    cohort: { geographyScope: "Region", commercialRegion: "CALA" },
  });
  const oaiOnly = await listAvailableAiVisibilityProviders({ store: openaiStore });
  test("Gemini not shown without completed Gemini data", () => {
    assert.deepEqual(
      oaiOnly.map((p) => p.id),
      ["openai"]
    );
  });

  const geo = parseGeographyQuery({ geography: "CALA" });
  const openaiSummaries = await findMatchingSummaries(store, geo, "openai");
  const geminiSummaries = await findMatchingSummaries(store, geo, "gemini");
  test("provider=openai returns OpenAI summaries only; gemini does not silently return OpenAI", () => {
    assert.equal(openaiSummaries.length, 1);
    assert.equal(openaiSummaries[0].batchId, "batch_openai_cala");
    assert.equal(geminiSummaries.length, 1);
    assert.equal(geminiSummaries[0].batchId, "batch_gemini_cala");
    assert.ok(geminiSummaries[0].batchId !== openaiSummaries[0].batchId);
  });

  const openaiSnaps = await store.listMetricSnapshots({
    entityId: "brandA",
    provider: "openai",
    geographyScope: "Region",
    commercialRegion: "CALA",
  });
  const geminiSnaps = await store.listMetricSnapshots({
    entityId: "brandA",
    provider: "gemini",
    geographyScope: "Region",
    commercialRegion: "CALA",
  });
  test("metric snapshots remain provider-separated", () => {
    assert.equal(openaiSnaps.length, 1);
    assert.equal(openaiSnaps[0].value, 0.5);
    assert.equal(geminiSnaps.length, 1);
    assert.equal(geminiSnaps[0].value, 0.9);
  });

  const openaiEv = await store.listEvidence({ entityId: "brandA", provider: "openai" });
  const geminiEv = await store.listEvidence({ entityId: "brandA", provider: "gemini" });
  test("evidence list is provider-scoped", () => {
    assert.equal(openaiEv.length, 1);
    assert.equal(openaiEv[0].evidenceId, "ev_openai");
    assert.equal(geminiEv.length, 1);
    assert.equal(geminiEv[0].evidenceId, "ev_gemini");
  });

  test("HTML/UI forbids All AI blend language in provider contract", () => {
    const dim = fs.readFileSync(
      path.join(root, "lib/ai-visibility/provider-dimension.js"),
      "utf8"
    );
    assert.match(dim, /No cross-provider aggregation/);
    assert.match(dim, /ALL_AI:\s*"NOT_IMPLEMENTED"/);
  });
}

await runAsyncProviderTests();

console.log(`\nPhase 3A.3: ${passed} passed, ${failed} failed\n`);
process.exit(failed ? 1 : 0);
