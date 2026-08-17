#!/usr/bin/env node
/**
 * Phase 3A.1 — Brand AI Visibility Executive Summary | Detailed View.
 * No paid provider calls. No Airtable writes.
 */
import assert from "node:assert/strict";
import fs from "fs";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildFixtureViewerContext,
  buildFixtureEntitlementGraph,
  createAiVisibilityStore,
  ACCESS_DEPTH,
  AVAILABILITY,
  getBrandExecutiveSummaryPayload,
  getBrandOverviewPayload,
  getBrandPortfolioPayload,
} from "../lib/ai-visibility/index.js";

let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    const ret = fn();
    if (ret && typeof ret.then === "function") {
      return ret
        .then(() => {
          passed += 1;
          console.log(`  PASS ${name}`);
        })
        .catch((err) => {
          failed += 1;
          console.error(`  FAIL ${name}: ${err.message}`);
        });
    }
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

const ASCEND = "reclkgOzvAcBheUSo";
const COMFORT = "recOzH5iAE1xEjyD0";
const RADISSON = "recmKqo7M7mLZgRqQ";
const PEER = "recPeerBrand00001";
const COMPANY = "reciQEtqmxz6ZroVc";

function viewerBrand() {
  return buildFixtureViewerContext({
    viewerUserId: "recUserBrandDemo",
    viewerCompanyId: COMPANY,
    viewerCompanyName: "Dealality Brand Demo",
    isBrand: true,
    workspaceAccess: ["Brand"],
  });
}

function graphDemo() {
  return buildFixtureEntitlementGraph({
    entitledBrandIds: [ASCEND, COMFORT, RADISSON],
    peerBrandIds: [ASCEND, PEER],
    source: "fixture_phase3a1",
  });
}

async function seedStore(rootDir) {
  const store = createAiVisibilityStore({ rootDir });
  const batchId = "aiv_batch_20260813_phase3a1_cala";
  await store.saveBatch({
    batchId,
    status: "completed",
    startedAt: "2026-08-13T10:00:00.000Z",
    completedAt: "2026-08-13T10:10:00.000Z",
    provider: "openai",
    model: "gpt-5.6",
  });
  await store.saveBatchSummary({
    batchId,
    status: "completed",
    completedAt: "2026-08-13T10:10:00.000Z",
    cohort: {
      geographyScope: "Region",
      commercialRegion: "CALA",
      stakeholder: "brand",
      promptCount: 5,
      entityIds: [ASCEND, PEER],
    },
    provider: { name: "openai", model: "gpt-5.6" },
    peerSet: { peerSetId: "peers_upper_upscale_brands_global_v1", canonicalValid: true },
    metrics: {
      byEntity: {
        Ascend: {
          id: ASCEND,
          presence: 0.2,
          recommendationShare: 0.05,
          firstRecommendationRate: 0,
          questionsWon: 0,
          questionsMissing: 4,
          citationRate: null,
        },
        Peer: {
          id: PEER,
          presence: 0.4,
          recommendationShare: 0.1,
          firstRecommendationRate: 0.2,
          questionsWon: 1,
          questionsMissing: 3,
        },
      },
      competitivePosition: {
        peers: [
          { entityId: PEER, name: "Peer Brand", rank: 1, presence: 0.4 },
          { entityId: ASCEND, name: "Ascend Hotel Collection", rank: 2, presence: 0.2 },
        ],
      },
      citationRateReadiness: "PARTIAL",
    },
  });
  await store.saveMetricSnapshot({
    batchId,
    batchDate: "2026-08-13T10:00:00.000Z",
    entityId: ASCEND,
    entityName: "Ascend Hotel Collection",
    metric: "ai_presence_rate",
    value: 0.2,
    geographyScope: "Region",
    commercialRegion: "CALA",
    provider: "openai",
  });

  // Europe batch — Ascend only in universe
  const euBatch = "aiv_batch_20260813_phase3a1_eu";
  await store.saveBatch({
    batchId: euBatch,
    status: "completed",
    startedAt: "2026-08-13T11:00:00.000Z",
    completedAt: "2026-08-13T11:10:00.000Z",
    provider: "openai",
    model: "gpt-5.6",
  });
  await store.saveBatchSummary({
    batchId: euBatch,
    status: "completed",
    completedAt: "2026-08-13T11:10:00.000Z",
    cohort: {
      geographyScope: "Region",
      commercialRegion: "Europe",
      stakeholder: "brand",
      promptCount: 5,
      entityIds: [ASCEND, PEER],
    },
    provider: { name: "openai", model: "gpt-5.6" },
    peerSet: { peerSetId: "peers_upper_upscale_brands_global_v1", canonicalValid: true },
    metrics: {
      byEntity: {
        Ascend: {
          id: ASCEND,
          presence: 0.2,
          recommendationShare: 0.05,
          firstRecommendationRate: 0,
          questionsWon: 0,
          questionsMissing: 4,
        },
      },
      competitivePosition: {
        peers: [
          { entityId: PEER, name: "Peer Brand", rank: 1, presence: 0.5 },
          { entityId: ASCEND, name: "Ascend Hotel Collection", rank: 2, presence: 0.2 },
        ],
      },
    },
  });
  await store.saveMetricSnapshot({
    batchId: euBatch,
    batchDate: "2026-08-13T11:00:00.000Z",
    entityId: ASCEND,
    entityName: "Ascend Hotel Collection",
    metric: "ai_presence_rate",
    value: 0.2,
    geographyScope: "Region",
    commercialRegion: "Europe",
    provider: "openai",
  });

  return store;
}

async function main() {
  console.log("\nAI Visibility Phase 3A.1 — Executive Summary\n");

  const htmlPath = path.join(
    path.dirname(fileURLToPath(import.meta.url)),
    "..",
    "public",
    "ai-visibility-brand.html"
  );
  const html = fs.readFileSync(htmlPath, "utf8");
  await test("HTML has Executive Summary | Detailed View tabs", () => {
    assert.match(html, /data-tab="executive"/);
    assert.match(html, /data-tab="detail"/);
    assert.match(html, /Executive/);
    assert.match(html, /Detailed/);
    assert.match(html, /bdd-section-nav|section-nav-item/);
    assert.match(html, /Your Brands|Portfolio Overview/);
    assert.match(html, /Portfolio Snapshot|Current Position/);
    assert.match(html, /aiv-theme-group/);
    assert.match(html, /aiv-theme-label/);
    assert.match(html, /aiv-theme-card/);
    assert.match(html, /Markets|&amp; Movement|Markets &amp; Movement/);
    assert.match(html, /aivMarketTrendChart/);
    assert.match(html, /chart\.js|Chart\.js/i);
    assert.match(html, /data-sort="aiPresence"/);
    assert.match(html, /sort-indicator/);
    assert.match(html, /Δ AI Presence|aiPresenceChange/);
    assert.match(html, /aivNotableMoves/);
    assert.doesNotMatch(html, /id="aivExecChanges"/);
    assert.doesNotMatch(html, /AI Visibility Score|GEO score|portfolio composite/i);
  });

  const jsPath = path.join(
    path.dirname(fileURLToPath(import.meta.url)),
    "..",
    "public",
    "js",
    "ai-visibility",
    "ai-visibility-brand.js"
  );
  const js = fs.readFileSync(jsPath, "utf8");
  await test("JS defaults to executive tab and supports drill-through", () => {
    assert.match(js, /tab:\s*"executive"/);
    assert.match(js, /drillToDetail/);
    assert.match(js, /executive-summary/);
    assert.match(js, /STORAGE_BRAND_KEY/);
    assert.match(js, /renderMarketMovementChart/);
    assert.match(js, /MARKET_TREND_COLORS/);
    assert.match(js, /portfolioSortKey/);
    assert.match(js, /bindPortfolioSort|sortedPortfolioBrands/);
  });

  const rootDir = fs.mkdtempSync(path.join(os.tmpdir(), "aiv-3a1-"));
  const store = await seedStore(rootDir);
  const names = {
    [ASCEND]: "Ascend Hotel Collection",
    [COMFORT]: "Comfort Inn & Suites",
    [RADISSON]: "Radisson RED by Choice",
  };

  await test("Executive summary — entitled only; Ascend observed; Comfort/Radisson Not Monitored", async () => {
    const summary = await getBrandExecutiveSummaryPayload({
      viewerContext: viewerBrand(),
      entitlementGraph: graphDemo(),
      store,
      geography: "CALA",
      brandNamesById: names,
    });
    assert.equal(summary.ok, true);
    assert.equal(summary.portfolioCompositeScore, null);
    assert.equal(summary.currentPosition.portfolioCompositeScore, null);
    const brands = summary.portfolioOverview.brands;
    assert.equal(brands.length, 3);
    const ascend = brands.find((b) => b.brandId === ASCEND);
    const comfort = brands.find((b) => b.brandId === COMFORT);
    const red = brands.find((b) => b.brandId === RADISSON);
    assert.ok(ascend);
    assert.ok(
      ascend.aiPresence.availability === AVAILABILITY.OBSERVED ||
        ascend.aiPresence.availability === AVAILABILITY.ZERO
    );
    assert.equal(comfort.aiPresence.availability, AVAILABILITY.NOT_MONITORED);
    assert.equal(red.aiPresence.availability, AVAILABILITY.NOT_MONITORED);
    assert.notEqual(comfort.aiPresence.value, 0);
    assert.equal(comfort.aiPresence.value, null);
    assert.equal(summary.currentPosition.brandsMonitored.monitored, 1);
    assert.equal(summary.currentPosition.brandsMonitored.entitled, 3);
    assert.equal(summary.currentPosition.topBrandByAiPresence.brandId, ASCEND);
  });

  await test("Geography — CALA/Europe observed; Global + NA Not Monitored; no fake averages", async () => {
    const summary = await getBrandExecutiveSummaryPayload({
      viewerContext: viewerBrand(),
      entitlementGraph: graphDemo(),
      store,
      geography: "CALA",
      brandNamesById: names,
    });
    const byKey = Object.fromEntries(summary.geographySummary.map((g) => [g.geography, g]));
    assert.equal(byKey.CALA.brandsMonitored, 1);
    assert.equal(byKey.Europe.brandsMonitored, 1);
    assert.equal(byKey.Global.availability, AVAILABILITY.NOT_MONITORED);
    assert.equal(byKey["North America"].availability, AVAILABILITY.NOT_MONITORED);
    assert.ok(byKey.MEA);
    assert.equal(byKey.MEA.availability, AVAILABILITY.NOT_MONITORED);
    assert.ok(byKey["Asia Pacific"]);
    assert.equal(byKey["Asia Pacific"].availability, AVAILABILITY.NOT_MONITORED);
    assert.deepEqual(
      summary.geographySummary.map((g) => g.geography),
      ["Global", "North America", "CALA", "Europe", "MEA", "Asia Pacific"]
    );
    assert.ok(!("portfolioAiPresenceAverage" in summary));
  });

  await test("What Changed — no fake deltas when single period", async () => {
    const summary = await getBrandExecutiveSummaryPayload({
      viewerContext: viewerBrand(),
      entitlementGraph: graphDemo(),
      store,
      geography: "CALA",
      brandNamesById: names,
    });
    assert.equal(summary.whatChanged.items.length, 0);
    assert.equal((summary.whatChanged.notableItems || []).length, 0);
    assert.match(summary.whatChanged.emptyMessage, /No prior comparable/i);
    assert.equal(summary.whatChanged.primarySurface, "portfolio_overview_column");
    for (const b of summary.portfolioOverview.brands) {
      assert.ok(b.aiPresenceChange, `missing aiPresenceChange on ${b.brandId}`);
      assert.equal(b.aiPresenceChange.display, "—");
      assert.equal(b.aiPresenceChange.availability, AVAILABILITY.NOT_MONITORED);
      assert.equal(b.aiPresenceChange.value, null);
    }
    assert.ok(summary.marketMovement);
    assert.equal(summary.marketMovement.FAKE_INTERMEDIATE_POINTS, "NONE");
    assert.equal(summary.marketMovement.portfolioCompositeScore, null);
    assert.equal(summary.marketMovement.chartReady, false);
    assert.ok(summary.marketMovement.periodCount <= 1);
    assert.match(summary.marketMovement.emptyMessage || "", /Trend will develop|Not Monitored/i);
  });

  await test("Market movement chartReady when two comparable periods exist", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "aiv-phase3a1-trend-"));
    const trendStore = await seedStore(root);
    const priorBatch = "aiv_batch_20260801_phase3a1_cala_prior";
    await trendStore.saveBatch({
      batchId: priorBatch,
      status: "completed",
      startedAt: "2026-08-01T10:00:00.000Z",
      completedAt: "2026-08-01T10:10:00.000Z",
      provider: "openai",
      model: "gpt-5.6",
    });
    await trendStore.saveMetricSnapshot({
      batchId: priorBatch,
      batchDate: "2026-08-01T10:00:00.000Z",
      entityId: ASCEND,
      entityName: "Ascend Hotel Collection",
      metric: "ai_presence_rate",
      value: 0.4,
      geographyScope: "Region",
      commercialRegion: "CALA",
      provider: "openai",
    });
    const summary = await getBrandExecutiveSummaryPayload({
      viewerContext: viewerBrand(),
      entitlementGraph: graphDemo(),
      store: trendStore,
      geography: "CALA",
      brandNamesById: names,
    });
    assert.equal(summary.marketMovement.chartReady, true);
    assert.ok(summary.marketMovement.periodCount >= 2);
    assert.equal(summary.marketMovement.FAKE_INTERMEDIATE_POINTS, "NONE");
    assert.ok(summary.marketMovement.series.some((s) => s.brandId === ASCEND));
    const ascend = summary.marketMovement.series.find((s) => s.brandId === ASCEND);
    assert.ok(ascend.data.filter((v) => typeof v === "number").length >= 2);
    assert.ok(summary.whatChanged.items.some((i) => i.brandId === ASCEND));
  });

  await test("Market movement chartReady for same-day distinct monitoring batches", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "aiv-phase3a1-sameday-"));
    const trendStore = await seedStore(root);
    const sameDayPrior = "aiv_batch_20260813_phase3a1_cala_am";
    await trendStore.saveBatch({
      batchId: sameDayPrior,
      status: "completed",
      startedAt: "2026-08-13T08:00:00.000Z",
      completedAt: "2026-08-13T08:10:00.000Z",
      provider: "openai",
      model: "gpt-5.6",
    });
    await trendStore.saveMetricSnapshot({
      batchId: sameDayPrior,
      batchDate: "2026-08-13T08:00:00.000Z",
      entityId: ASCEND,
      entityName: "Ascend Hotel Collection",
      metric: "ai_presence_rate",
      value: 0.4,
      geographyScope: "Region",
      commercialRegion: "CALA",
      provider: "openai",
    });
    const summary = await getBrandExecutiveSummaryPayload({
      viewerContext: viewerBrand(),
      entitlementGraph: graphDemo(),
      store: trendStore,
      geography: "CALA",
      brandNamesById: names,
    });
    assert.equal(summary.marketMovement.chartReady, true);
    assert.ok(
      summary.marketMovement.periodCount >= 2,
      "same calendar day must still count as two monitoring periods"
    );
    assert.equal(summary.marketMovement.labelKeys.length, summary.marketMovement.periodCount);
    assert.ok(summary.marketMovement.labels.every((l) => typeof l === "string" && l.length > 0));
  });

  await test("Competitive context is comparative-safe; no deep competitor diagnostics", async () => {
    const summary = await getBrandExecutiveSummaryPayload({
      viewerContext: viewerBrand(),
      entitlementGraph: graphDemo(),
      store,
      geography: "CALA",
      brandNamesById: names,
    });
    assert.equal(summary.competitiveContext.FAKE_COMPETITOR_DEEP_DIAGNOSTICS, "NONE");
    if (summary.competitiveContext.leadingPeer) {
      assert.equal(summary.competitiveContext.leadingPeer.deepLinkAllowed, false);
      assert.ok(
        summary.competitiveContext.leadingPeer.accessDepth === ACCESS_DEPTH.COMPARATIVE ||
          summary.competitiveContext.leadingPeer.accessDepth === ACCESS_DEPTH.DEEP
      );
      // Peer is not entitled → comparative
      assert.equal(summary.competitiveContext.leadingPeer.entityId, PEER);
      assert.equal(summary.competitiveContext.leadingPeer.accessDepth, ACCESS_DEPTH.COMPARATIVE);
    }
  });

  await test("Cross-tenant brand excluded from executive portfolio", async () => {
    const foreign = "recForeignBrand999";
    const summary = await getBrandExecutiveSummaryPayload({
      viewerContext: viewerBrand(),
      entitlementGraph: graphDemo(),
      store,
      geography: "CALA",
      brandNamesById: { ...names, [foreign]: "Foreign" },
    });
    assert.ok(!summary.portfolioOverview.brands.some((b) => b.brandId === foreign));
  });

  await test("No synthetic opportunities / confidence; Priority Review honest", async () => {
    const summary = await getBrandExecutiveSummaryPayload({
      viewerContext: viewerBrand(),
      entitlementGraph: graphDemo(),
      store,
      geography: "CALA",
      brandNamesById: names,
    });
    assert.equal(summary.opportunityQueue.status, AVAILABILITY.FUTURE_READY);
    assert.ok(!("confidence" in summary));
    assert.ok(!("compositeScore" in summary));
    // Factual coverage items may appear; never fake impact/confidence labels
    const priJson = JSON.stringify(summary.priorityReviewItems);
    assert.doesNotMatch(priJson, /High Impact|High Confidence|Why You're Losing/i);
  });

  await test("Detailed overview still works for Ascend", async () => {
    const overview = await getBrandOverviewPayload({
      viewerContext: viewerBrand(),
      entitlementGraph: graphDemo(),
      store,
      brandId: ASCEND,
      geography: "CALA",
      brandNamesById: names,
    });
    assert.equal(overview.allowed, true);
    assert.ok(overview.kpis?.aiPresence);
    assert.ok(
      overview.kpis.aiPresence.availability === AVAILABILITY.OBSERVED ||
        overview.kpis.aiPresence.availability === AVAILABILITY.ZERO
    );
  });

  await test("Portfolio endpoint still entitled-only", async () => {
    const port = await getBrandPortfolioPayload({
      viewerContext: viewerBrand(),
      entitlementGraph: graphDemo(),
      store,
      geography: "CALA",
      brandNamesById: names,
    });
    assert.equal(port.brands.length, 3);
  });

  // Drain sequential tests that returned promises
  await new Promise((r) => setTimeout(r, 50));

  fs.rmSync(rootDir, { recursive: true, force: true });

  console.log(`\nPhase 3A.1 results: ${passed} passed, ${failed} failed\n`);
  if (failed) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
