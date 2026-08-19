#!/usr/bin/env node
/**
 * Phase 3A tests — Brand AI Visibility authorized reads + availability honesty.
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
  ACCESS_REASON,
  AVAILABILITY,
  classifyMetricAvailability,
  mapRecommendationRoleToBrandStatus,
  parseGeographyQuery,
  getBrandPortfolioPayload,
  getBrandOverviewPayload,
  getBrandTrendPayload,
  getBrandQuestionsPayload,
  getBrandCompetitorsPayload,
  getBrandSourcesPayload,
  getBrandEvidencePayload,
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

const BRAND_A = "recBrandA10000001";
const BRAND_B = "recBrandB10000002";
const BRAND_C = "recBrandC10000003";
const COMPANY_A = "recCompanyA000001";

function viewerBrand() {
  return buildFixtureViewerContext({
    viewerUserId: "recUserBrandA",
    viewerCompanyId: COMPANY_A,
    viewerCompanyName: "Company A",
    isBrand: true,
    workspaceAccess: ["Brand"],
  });
}

function graphA() {
  return buildFixtureEntitlementGraph({
    entitledBrandIds: [BRAND_A],
    peerBrandIds: [BRAND_A, BRAND_B],
    source: "fixture_phase3a",
  });
}

async function seedStore(rootDir) {
  const store = createAiVisibilityStore({ rootDir });
  const batchId = "aiv_batch_20260813_testcala";
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
    },
    provider: { name: "openai", model: "gpt-5.6" },
    peerSet: { peerSetId: "peers_upper_upscale_brands_global_v1", canonicalValid: true },
    metrics: {
      byEntity: {
        "Brand A": {
          id: BRAND_A,
          presence: 0.8,
          recommendationShare: 0.2,
          firstRecommendationRate: 0.4,
          questionsWon: 2,
          questionsMissing: 1,
          citationRate: 0.1,
        },
        "Brand B": {
          id: BRAND_B,
          presence: 0.6,
          recommendationShare: 0.15,
          firstRecommendationRate: 0.2,
          questionsWon: 1,
          questionsMissing: 2,
          citationRate: 0,
        },
      },
      competitivePosition: {
        peers: [
          { entityId: BRAND_A, name: "Brand A", rank: 1, presence: 0.8, recommendationShare: 0.2 },
          { entityId: BRAND_B, name: "Brand B", rank: 2, presence: 0.6, recommendationShare: 0.15 },
        ],
      },
      citationRateReadiness: "PARTIAL",
    },
  });

  await store.saveMetricSnapshot({
    batchId,
    batchDate: "2026-08-13T10:00:00.000Z",
    entityId: BRAND_A,
    entityName: "Brand A",
    metric: "ai_presence_rate",
    value: 0.8,
    geographyScope: "Region",
    commercialRegion: "CALA",
    provider: "openai",
  });
  await store.saveMetricSnapshot({
    batchId,
    batchDate: "2026-08-13T10:00:00.000Z",
    entityId: BRAND_A,
    entityName: "Brand A",
    metric: "recommendation_share",
    value: 0.2,
    geographyScope: "Region",
    commercialRegion: "CALA",
    provider: "openai",
  });
  await store.saveMetricSnapshot({
    batchId,
    batchDate: "2026-08-13T10:00:00.000Z",
    entityId: BRAND_A,
    entityName: "Brand A",
    metric: "first_recommendation_rate",
    value: 0.4,
    geographyScope: "Region",
    commercialRegion: "CALA",
    provider: "openai",
  });
  await store.saveMetricSnapshot({
    batchId,
    batchDate: "2026-08-13T10:00:00.000Z",
    entityId: BRAND_A,
    entityName: "Brand A",
    metric: "citation_rate",
    value: 0.1,
    geographyScope: "Region",
    commercialRegion: "CALA",
    provider: "openai",
    citationReadiness: "PARTIAL",
  });
  // True zero presence for Brand B snapshot (Europe cohort)
  await store.saveMetricSnapshot({
    batchId: "aiv_batch_20260813_testeurope",
    batchDate: "2026-08-13T10:00:00.000Z",
    entityId: BRAND_B,
    entityName: "Brand B",
    metric: "ai_presence_rate",
    value: 0,
    geographyScope: "Region",
    commercialRegion: "Europe",
    provider: "openai",
  });
  await store.saveBatchSummary({
    batchId: "aiv_batch_20260813_testeurope",
    status: "completed",
    completedAt: "2026-08-13T10:12:00.000Z",
    cohort: {
      geographyScope: "Region",
      commercialRegion: "Europe",
      stakeholder: "brand",
      promptCount: 5,
    },
    provider: { name: "openai", model: "gpt-5.6" },
    peerSet: { peerSetId: "peers_upper_upscale_brands_global_v1", canonicalValid: true },
    metrics: {
      byEntity: {
        "Brand B": {
          id: BRAND_B,
          presence: 0,
          presenceDetail: { denominator: 5, numerator: 0, value: 0 },
          recommendationShare: 0,
          firstRecommendationRate: 0,
          questionsWon: 0,
          questionsMissing: 5,
          citationRate: 0,
        },
      },
      competitivePosition: {
        peers: [{ entityId: BRAND_B, name: "Brand B", rank: 7, presence: 0 }],
      },
      citationRateReadiness: "PARTIAL",
    },
  });

  await store.saveRun({
    runId: "run_test_1",
    batchId,
    status: "completed",
    evidenceId: "ev_test_1",
    promptId: "p_cala_conversion_v1",
  });
  await store.saveEvidence({
    evidenceId: "ev_test_1",
    batchId,
    promptId: "p_cala_conversion_v1",
    promptText: "Best upper-upscale conversion brands in Mexico",
    intentTerritory: "Conversion",
    geographyScope: "Region",
    regionName: "CALA",
    provider: "openai",
    model: "gpt-5.6",
    timestamp: "2026-08-13T10:05:00.000Z",
    payload: {
      rawResponseText: "1. Brand A\n2. Brand B\nSources include example.com",
      mentions: [
        {
          entityId: BRAND_A,
          entityName: "Brand A",
          role: "first_recommendation",
        },
        {
          entityId: BRAND_B,
          entityName: "Brand B",
          role: "recommendation",
        },
      ],
      citations: [{ url: "https://example.com/dev", domain: "example.com" }],
    },
  });

  return store;
}

async function run() {
  console.log("AI Visibility Phase 3A tests\n");
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "aiv-3a-"));
  const store = await seedStore(root);
  const viewer = viewerBrand();
  const graph = graphA();

  console.log("Availability");
  await test("missing != zero; zero preserved; partial preserved", () => {
    assert.equal(classifyMetricAvailability({ monitored: false }).availability, AVAILABILITY.NOT_MONITORED);
    assert.equal(classifyMetricAvailability({ monitored: true, value: 0 }).availability, AVAILABILITY.ZERO);
    assert.equal(
      classifyMetricAvailability({ monitored: true, value: 0.1, partial: true }).availability,
      AVAILABILITY.PARTIAL
    );
    assert.notEqual(
      classifyMetricAvailability({ monitored: false }).value,
      0
    );
  });

  await test("role copy mapping", () => {
    assert.equal(mapRecommendationRoleToBrandStatus("first_recommendation"), "First Recommended");
    assert.equal(mapRecommendationRoleToBrandStatus("associated_option"), "Associated Option");
    assert.equal(mapRecommendationRoleToBrandStatus("missing"), "Missing");
  });

  await test("geography parse isolates scopes", () => {
    assert.equal(parseGeographyQuery({ geography: "Global" }).geographyScope, "Global");
    assert.equal(parseGeographyQuery({ geography: "CALA" }).commercialRegion, "CALA");
    assert.equal(parseGeographyQuery({ geography: "Mexico" }).geographyScope, "Country");
  });

  console.log("\nAuthorization");
  await test("own brand deep overview", async () => {
    const o = await getBrandOverviewPayload({
      viewerContext: viewer,
      entitlementGraph: graph,
      store,
      brandId: BRAND_A,
      geography: "CALA",
      brandNamesById: { [BRAND_A]: "Brand A" },
    });
    assert.equal(o.allowed, true);
    assert.equal(o.accessDepth, ACCESS_DEPTH.DEEP);
    assert.equal(o.kpis.aiPresence.availability, AVAILABILITY.OBSERVED);
    assert.equal(o.kpis.aiPresence.value, 0.8);
    assert.equal(o.secondary.citationRate.readiness, "PARTIAL");
    assert.equal(o.portfolioCompositeScore, undefined);
    assert.equal(o.opportunityQueue.FAKE_OPPORTUNITIES_CREATED, false);
  });

  await test("peer brand deep denied; comparative competitors ok", async () => {
    const deep = await getBrandOverviewPayload({
      viewerContext: viewer,
      entitlementGraph: graph,
      store,
      brandId: BRAND_B,
      geography: "CALA",
    });
    assert.equal(deep.allowed, true);
    assert.equal(deep.accessDepth, ACCESS_DEPTH.COMPARATIVE);

    const comps = await getBrandCompetitorsPayload({
      viewerContext: viewer,
      entitlementGraph: graph,
      store,
      brandId: BRAND_A,
      geography: "CALA",
    });
    assert.equal(comps.ok, true);
    const peer = comps.competitors.find((c) => c.entityId === BRAND_B);
    assert.ok(peer);
    assert.equal(peer.opportunityQueue, undefined);
    assert.equal(peer.deepLinkAllowed, undefined);
  });

  await test("unrelated brand denied; ID swap portfolio denied", async () => {
    const u = await getBrandOverviewPayload({
      viewerContext: viewer,
      entitlementGraph: graph,
      store,
      brandId: BRAND_C,
      geography: "CALA",
    });
    assert.equal(u.allowed, false);
    assert.equal(u.reasonCode, ACCESS_REASON.SUBJECT_NOT_ENTITLED);

    const emptyGraph = buildFixtureEntitlementGraph({
      entitledBrandIds: [],
      peerBrandIds: [BRAND_B],
      source: "empty_entitlement",
    });
    const portfolio = await getBrandPortfolioPayload({
      viewerContext: viewer,
      entitlementGraph: emptyGraph,
      store,
      geography: "CALA",
    });
    assert.equal(portfolio.brands.length, 0);
    assert.equal(portfolio.emptyReason, "NO_ENTITLED_BRANDS");
  });

  console.log("\nGeography + trend");
  await test("CALA observed; Global/NA not monitored; no region substitution", async () => {
    const cala = await getBrandOverviewPayload({
      viewerContext: viewer,
      entitlementGraph: graph,
      store,
      brandId: BRAND_A,
      geography: "CALA",
    });
    assert.equal(cala.kpis.aiPresence.availability, AVAILABILITY.OBSERVED);

    const global = await getBrandOverviewPayload({
      viewerContext: viewer,
      entitlementGraph: graph,
      store,
      brandId: BRAND_A,
      geography: "Global",
    });
    assert.equal(global.kpis.aiPresence.availability, AVAILABILITY.NOT_MONITORED);
    assert.equal(global.kpis.aiPresence.value, null);

    const na = await getBrandOverviewPayload({
      viewerContext: viewer,
      entitlementGraph: graph,
      store,
      brandId: BRAND_A,
      geography: "North America",
    });
    assert.equal(na.kpis.aiPresence.availability, AVAILABILITY.NOT_MONITORED);

    const regional = cala.regionalPosition.find((r) => r.geography === "Global");
    assert.equal(regional.aiPresence.availability, AVAILABILITY.NOT_MONITORED);
  });

  await test("trend one real point; no fabricated intermediates; ranges gated", async () => {
    const trend = await getBrandTrendPayload({
      viewerContext: viewer,
      entitlementGraph: graph,
      store,
      brandId: BRAND_A,
      geography: "CALA",
    });
    assert.equal(trend.points.length, 1);
    assert.equal(trend.FAKE_INTERMEDIATE_POINTS, "NONE");
    assert.match(String(trend.timeRanges["30D"]), /NOT YET AVAILABLE/);
  });

  console.log("\nQuestions / sources / evidence");
  await test("questions won/missing + associated role copy", async () => {
    const all = await getBrandQuestionsPayload({
      viewerContext: viewer,
      entitlementGraph: graph,
      store,
      brandId: BRAND_A,
      geography: "CALA",
      filter: "all",
    });
    assert.ok(all.questions.length >= 1);
    assert.equal(all.questions[0].brandStatus, "First Recommended");

    const won = await getBrandQuestionsPayload({
      viewerContext: viewer,
      entitlementGraph: graph,
      store,
      brandId: BRAND_A,
      geography: "CALA",
      filter: "won",
    });
    assert.ok(won.questions.every((q) => q.brandStatus === "First Recommended"));
  });

  await test("sources non-causal; citation readiness partial", async () => {
    const sources = await getBrandSourcesPayload({
      viewerContext: viewer,
      entitlementGraph: graph,
      store,
      brandId: BRAND_A,
      geography: "CALA",
    });
    assert.equal(sources.CAUSAL_LANGUAGE_USED, false);
    assert.equal(sources.INFLUENCE_SCORE_CREATED, false);
    assert.equal(sources.citationRateReadiness, "PARTIAL");
    assert.ok(sources.sources.some((s) => s.domain === "example.com"));
  });

  await test("deep evidence ok; comparative limited; unauthorized denied", async () => {
    const deep = await getBrandEvidencePayload({
      viewerContext: viewer,
      entitlementGraph: graph,
      store,
      brandId: BRAND_A,
      evidenceId: "ev_test_1",
    });
    assert.equal(deep.allowed, true);
    assert.ok(deep.evidence.length >= 1);
    assert.ok(!JSON.stringify(deep.evidence).includes("raw provider JSON"));

    const peerEv = await getBrandEvidencePayload({
      viewerContext: viewer,
      entitlementGraph: graph,
      store,
      brandId: BRAND_B,
      evidenceId: "ev_test_1",
    });
    assert.equal(peerEv.accessDepth, ACCESS_DEPTH.COMPARATIVE);
    assert.ok(peerEv.evidence.length <= 3);

    const denied = await getBrandEvidencePayload({
      viewerContext: viewer,
      entitlementGraph: graph,
      store,
      brandId: BRAND_C,
      evidenceId: "ev_test_1",
    });
    assert.equal(denied.allowed, false);
  });

  await test("Europe zero presence distinguishable from not monitored", async () => {
    // Brand B has Europe presence 0 for peer comparative path
    const graphBOwner = buildFixtureEntitlementGraph({
      entitledBrandIds: [BRAND_B],
      peerBrandIds: [BRAND_A, BRAND_B],
      source: "fixture_b",
    });
    const o = await getBrandOverviewPayload({
      viewerContext: buildFixtureViewerContext({
        viewerUserId: "recUserB",
        viewerCompanyId: "recCompanyB",
        isBrand: true,
        workspaceAccess: ["Brand"],
      }),
      entitlementGraph: graphBOwner,
      store,
      brandId: BRAND_B,
      geography: "Europe",
    });
    assert.equal(o.kpis.aiPresence.availability, AVAILABILITY.ZERO);
    assert.equal(o.kpis.aiPresence.value, 0);
  });

  await test("UI module contracts present on overview", async () => {
    const o = await getBrandOverviewPayload({
      viewerContext: viewer,
      entitlementGraph: graph,
      store,
      brandId: BRAND_A,
      geography: "CALA",
    });
    assert.ok(o.kpis.aiPresence);
    assert.ok(o.kpis.competitivePosition);
    assert.ok(o.kpis.questionsWon);
    assert.ok(o.kpis.questionsMissing);
    assert.ok(o.kpis.recommendationRate);
    assert.ok(o.kpis.recommendationShare);
    assert.ok(o.kpis.top3RecommendationRate);
    assert.ok(o.kpis.firstRecommendationRate);
    assert.ok(o.secondary.citationRate);
    assert.ok(o.decisionPatterns);
    assert.ok(o.openAiDiscoverability);
    assert.ok(o.regionalPosition.length >= 4);
    assert.equal(o.opportunityQueue.status, AVAILABILITY.FUTURE_READY);
    assert.equal(o.evidenceStrength.status, AVAILABILITY.FUTURE_READY);
  });

  // UI smoke: required public files exist
  await test("UI assets exist", () => {
    const rootRepo = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
    const files = [
      "public/ai-visibility-brand.html",
      "public/js/ai-visibility/ai-visibility-brand.js",
      "public/js/ai-visibility/ai-visibility-shared.js",
      "public/js/ai-visibility/ai-visibility-shared.css",
    ];
    for (const f of files) {
      assert.ok(fs.existsSync(path.join(rootRepo, f)), `missing ${f}`);
    }
    const html = fs.readFileSync(path.join(rootRepo, "public/ai-visibility-brand.html"), "utf8");
    assert.match(html, /Brand AI|Hotel Brand AI/);
    assert.match(html, /Presence Over Time/);
    assert.match(html, /Owner Questions/);
    assert.match(html, /Opportunity Engine/);
    assert.match(html, /Decision Patterns|OpenAI Discoverability|aivDetailIntentCoverage/);
    assert.ok(!/id="aivTabHdv"/.test(html));
    assert.doesNotMatch(html, /Summit Hotels/);
    assert.doesNotMatch(html, /Strong fit/);
    // Product title must be stakeholder-prefixed
    assert.match(html, /<h1>Brand AI Intelligence<\/h1>/);
    assert.match(html, /dashboard-container/);
    assert.match(html, /filters-section/);
    assert.match(html, /filter-select/);
    assert.doesNotMatch(html, /aiv-shell/);
    const css = fs.readFileSync(
      path.join(rootRepo, "public/js/ai-visibility/ai-visibility-shared.css"),
      "utf8"
    );
    assert.match(css, /max-width:\s*100%/);
    assert.doesNotMatch(css, /max-width:\s*1280px/);
    const brandJs = fs.readFileSync(
      path.join(rootRepo, "public/js/ai-visibility/ai-visibility-brand.js"),
      "utf8"
    );
    assert.match(brandJs, /\/api\/ai-visibility\/brand\/portfolio/);
    assert.match(brandJs, /api_route_missing/);
    assert.doesNotMatch(brandJs, /may have completed monitoring cohorts/);
  });

  console.log("\nMonitoring state semantics + store root");
  await test("store root recovers phase2e when present", async () => {
    const { resolveAiVisibilityStoreRoot, PHASE2E_ROOT } = await import(
      "../lib/ai-visibility/storage/resolve-store-root.js"
    );
    const resolved = resolveAiVisibilityStoreRoot({});
    assert.equal(resolved.rootDir, PHASE2E_ROOT);
    assert.equal(resolved.recoveredPhase2e, true);
    const storeDefault = createAiVisibilityStore();
    assert.equal(storeDefault.rootDir, PHASE2E_ROOT);
  });

  await test("relevant batch + entity observed", async () => {
    const overview = await getBrandOverviewPayload({
      viewerContext: viewer,
      entitlementGraph: graph,
      store,
      brandId: BRAND_A,
      geography: "CALA",
      provider: "openai",
      brandNamesById: { [BRAND_A]: "Brand A" },
    });
    assert.equal(overview.ok, true);
    assert.equal(overview.availabilityReason, "observed");
    assert.equal(overview.kpis.aiPresence.availability, AVAILABILITY.OBSERVED);
    assert.equal(overview.kpis.aiPresence.value, 0.8);
  });

  await test("relevant batch + monitored entity absent = valid zero", async () => {
    const zeroRoot = fs.mkdtempSync(path.join(os.tmpdir(), "aiv-zero-"));
    const zeroStore = createAiVisibilityStore({ rootDir: zeroRoot });
    await zeroStore.saveBatchSummary({
      batchId: "aiv_batch_zero_scope",
      status: "completed",
      completedAt: "2026-08-13T12:00:00.000Z",
      cohort: {
        geographyScope: "Region",
        commercialRegion: "CALA",
        promptCount: 5,
      },
      provider: { name: "openai" },
      peerSet: { peerSetId: "peers_upper_upscale_brands_global_v1" },
      metrics: {
        byEntity: {
          "Brand A": {
            id: BRAND_A,
            presence: 0,
            presenceDetail: { denominator: 5, numerator: 0, value: 0 },
            recommendationShare: 0,
            firstRecommendationRate: 0,
            questionsWon: 0,
            questionsMissing: 5,
          },
        },
        competitivePosition: {
          peers: [{ entityId: BRAND_A, name: "Brand A", rank: 7, presence: 0 }],
        },
      },
    });
    const overview = await getBrandOverviewPayload({
      viewerContext: viewer,
      entitlementGraph: graph,
      store: zeroStore,
      brandId: BRAND_A,
      geography: "CALA",
      provider: "openai",
    });
    assert.equal(overview.ok, true);
    assert.ok(["zero", "observed"].includes(overview.availabilityReason));
    assert.equal(overview.kpis.aiPresence.availability, AVAILABILITY.ZERO);
    assert.equal(overview.kpis.aiPresence.value, 0);
    assert.equal(overview.kpis.questionsWon.value, 0);
    assert.equal(overview.kpis.questionsMissing.value, 5);
  });

  await test("out-of-scope entitled brand stays Not Monitored (no synthetic metrics)", async () => {
    const outsider = "recBrandOutside999";
    const graphOut = buildFixtureEntitlementGraph({
      entitledBrandIds: [outsider],
      peerBrandIds: [BRAND_A, BRAND_B],
    });
    const overview = await getBrandOverviewPayload({
      viewerContext: viewerBrand(),
      entitlementGraph: graphOut,
      store,
      brandId: outsider,
      geography: "CALA",
      provider: "openai",
      brandNamesById: { [outsider]: "Outsider Brand" },
    });
    assert.equal(overview.ok, true);
    assert.equal(overview.availabilityReason, "out_of_scope");
    assert.equal(overview.kpis.aiPresence.availability, AVAILABILITY.NOT_MONITORED);
    assert.equal(overview.kpis.aiPresence.value, null);
    assert.match(overview.availabilityMessage || "", /not yet been included/i);
  });

  await test("no relevant batch = Not Monitored", async () => {
    const emptyRoot = fs.mkdtempSync(path.join(os.tmpdir(), "aiv-empty-"));
    const emptyStore = createAiVisibilityStore({ rootDir: emptyRoot });
    const overview = await getBrandOverviewPayload({
      viewerContext: viewer,
      entitlementGraph: graph,
      store: emptyStore,
      brandId: BRAND_A,
      geography: "CALA",
      provider: "openai",
    });
    assert.equal(overview.ok, true);
    assert.equal(overview.availabilityReason, "no_batch");
    assert.equal(overview.kpis.aiPresence.availability, AVAILABILITY.NOT_MONITORED);
    assert.match(overview.availabilityMessage || "", /No monitoring data is available/i);
  });

  console.log("\nExpress route registration (harness)");
  await test("server.js registers portfolio before :brandId and before API 404", () => {
    const rootRepo = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
    const src = fs.readFileSync(path.join(rootRepo, "server.js"), "utf8");
    const portfolioIdx = src.indexOf('app.get("/api/ai-visibility/brand/portfolio"');
    const execIdx = src.indexOf('app.get("/api/ai-visibility/brand/executive-summary"');
    const overviewIdx = src.indexOf('app.get("/api/ai-visibility/brand/:brandId/overview"');
    const api404Idx = src.indexOf('error: "API route not found"');
    assert.ok(portfolioIdx > 0, "portfolio route missing");
    assert.ok(execIdx > portfolioIdx, "executive-summary must follow portfolio");
    assert.ok(overviewIdx > execIdx, "overview must follow executive-summary");
    assert.ok(api404Idx > overviewIdx, "API 404 fallback must be after AI Visibility routes");
    assert.match(src, /requireBrandAiVisibilityAccess/);
  });

  await test("express mini-app: portfolio / overview / unauthorized", async () => {
    const express = (await import("express")).default;
    const {
      getBrandPortfolio,
      getBrandOverview,
    } = await import("../api/ai-visibility-brand.js");

    const app = express();
    const mockUser = {
      userRecordId: "recUserBrandA",
      companyId: COMPANY_A,
      companyIds: [COMPANY_A],
      companyName: "Company A",
      isBrand: true,
      canAccessBrandWorkspace: true,
      workspaceAccess: ["Brand"],
      isAdmin: false,
      isOwner: false,
      isOperator: false,
    };
    app.use((req, _res, next) => {
      req.dealalityUser = mockUser;
      req.aiVisibilityStore = store;
      req.aiVisibilityEntitlementGraph = graph;
      req.aiVisibilityBrandNamesById = { [BRAND_A]: "Brand A" };
      next();
    });
    app.get("/api/ai-visibility/brand/portfolio", getBrandPortfolio);
    app.get("/api/ai-visibility/brand/:brandId/overview", getBrandOverview);
    app.use("/api", (_req, res) => {
      res.status(404).json({ success: false, error: "API route not found" });
    });

    const server = await new Promise((resolve) => {
      const s = app.listen(0, () => resolve(s));
    });
    const port = server.address().port;
    try {
      const portfolioRes = await fetch(
        `http://127.0.0.1:${port}/api/ai-visibility/brand/portfolio?geography=CALA&provider=openai`
      );
      const portfolio = await portfolioRes.json();
      assert.equal(portfolioRes.status, 200);
      assert.equal(portfolio.success, true);
      assert.ok(Array.isArray(portfolio.brands));
      assert.notEqual(portfolio.error, "API route not found");

      const overviewRes = await fetch(
        `http://127.0.0.1:${port}/api/ai-visibility/brand/${BRAND_A}/overview?geography=CALA&provider=openai`
      );
      const overview = await overviewRes.json();
      assert.equal(overviewRes.status, 200);
      assert.equal(overview.success, true);
      assert.equal(overview.allowed, true);

      const deniedRes = await fetch(
        `http://127.0.0.1:${port}/api/ai-visibility/brand/${BRAND_C}/overview?geography=CALA&provider=openai`
      );
      const denied = await deniedRes.json();
      assert.equal(deniedRes.status, 403);
      assert.equal(denied.success, false);
      assert.notEqual(denied.error, "API route not found");

      const emptyApp = express();
      emptyApp.use((req, _res, next) => {
        req.dealalityUser = mockUser;
        req.aiVisibilityStore = store;
        req.aiVisibilityEntitlementGraph = buildFixtureEntitlementGraph({
          entitledBrandIds: [],
          peerBrandIds: [],
          source: "empty",
        });
        next();
      });
      emptyApp.get("/api/ai-visibility/brand/portfolio", getBrandPortfolio);
      const emptyServer = await new Promise((resolve) => {
        const s = emptyApp.listen(0, () => resolve(s));
      });
      try {
        const emptyPort = emptyServer.address().port;
        const emptyRes = await fetch(
          `http://127.0.0.1:${emptyPort}/api/ai-visibility/brand/portfolio?geography=CALA`
        );
        const emptyBody = await emptyRes.json();
        assert.equal(emptyRes.status, 200);
        assert.equal(emptyBody.emptyReason, "NO_ENTITLED_BRANDS");
        assert.equal(emptyBody.brands.length, 0);
      } finally {
        await new Promise((r) => emptyServer.close(r));
      }
    } finally {
      await new Promise((r) => server.close(r));
    }
  });

  console.log(`\nPhase 3A: ${passed} passed, ${failed} failed`);
  if (failed) process.exit(1);
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
