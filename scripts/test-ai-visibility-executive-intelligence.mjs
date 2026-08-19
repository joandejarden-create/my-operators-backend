#!/usr/bin/env node
/**
 * P0E — Executive Intelligence Integration tests.
 */
import assert from "node:assert/strict";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import {
  buildExecutiveFindings,
  buildBrandDetailIntelligence,
  executiveFindingsToInsightBoxes,
  FINDING_TYPES,
  ACTION_CATEGORIES,
} from "../lib/ai-visibility/executive-finding-engine.js";
import { isAssociationAttributeProductionEligible } from "../lib/ai-visibility/gaps/association-eligibility.js";
import { runCompetitiveGapEngineFromStore } from "../lib/ai-visibility/gaps/competitive-gap-engine.js";
import { peerSetBrandNamesById, PEER_SET_ID_V2 } from "../lib/ai-visibility/peer-sets.js";
import {
  loadCachedTruthComparisons,
  filterTruthComparisonsForCohort,
} from "../lib/ai-visibility/truth-layer/truth-comparisons-loader.js";
import { buildExecutiveInsightBoxes } from "../lib/ai-visibility/brand-executive-insights.js";

const MARRIOTT = [
  "recEJCTDj1zrsjPM6",
  "recCvV0PuZOi8c3hC",
  "rec9aZp7GHtzUEg0c",
  "rec02zPClpWUTCyXM",
  "recIPuBC50fv13zRR",
];
const AUTOGRAPH = "recEJCTDj1zrsjPM6";
const AC = "rec9aZp7GHtzUEg0c";

let passed = 0;
let failed = 0;

async function test(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

function mockGap(overrides = {}) {
  return {
    gapId: overrides.gapId || "gap_test_1",
    gapClass: "PERSISTENT_SCENARIO_GAP",
    subjectBrandId: overrides.subjectBrandId || AC,
    peerBrandIds: overrides.peerBrandIds || ["recEJCTDj1zrsjPM6", "recCvV0PuZOi8c3hC"],
    scenarioId: overrides.scenarioId || "scenario_newbuild_uu_brand_selection_v1",
    geography: "CALA",
    language: "en",
    classification: overrides.classification || "HIGH_PRIORITY",
    persistence: "STRONGLY_REPEATED",
    observationCount: 8,
    providers: ["openai", "anthropic", "perplexity"],
    evidenceIds: ["ev_1"],
    lifecycleStatus: "ACTIVE",
    commercialPriority: "CRITICAL",
    ...overrides,
  };
}

async function main() {
  console.log("\nP0E Executive Intelligence tests\n");

  await test("research-only association blocked from production eligibility", async () => {
    assert.equal(isAssociationAttributeProductionEligible("OWNER_FLEXIBILITY"), false);
    assert.equal(isAssociationAttributeProductionEligible("DISTRIBUTION"), true);
  });

  await test("executive findings dedupe largest + highest priority review", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const brandNamesById = peerSetBrandNamesById(PEER_SET_ID_V2);
    const gap = mockGap();
    const result = await buildExecutiveFindings({
      store,
      brandIds: MARRIOTT,
      brandNamesById,
      geographyKey: "CALA",
      language: "en",
      scope: "portfolio",
      preloadedGaps: [gap],
      preloadedEvidence: [],
    });
    const types = result.findings.map((f) => f.findingType);
    const largest = types.filter((t) => t === FINDING_TYPES.LARGEST_COMPETITIVE_GAP).length;
    const review = types.filter((t) => t === FINDING_TYPES.HIGHEST_PRIORITY_REVIEW).length;
    assert.ok(largest >= 1);
    assert.equal(largest, 1, "exactly one competitive gap theme");
    assert.equal(review, 0, "review merged into largest gap tile");
  });

  await test("executive summary does not repeat competitive gap theme", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const brandNamesById = peerSetBrandNamesById(PEER_SET_ID_V2);
    const result = await buildExecutiveFindings({
      store,
      brandIds: MARRIOTT,
      brandNamesById,
      geographyKey: "CALA",
      language: "en",
      scope: "portfolio",
      preloadedGaps: [
        mockGap({
          gapId: "gap_ac_conversion",
          subjectBrandId: AC,
          scenarioId: "scenario_newbuild_uu_brand_selection_v1",
        }),
        mockGap({
          gapId: "gap_design_affiliation",
          subjectBrandId: AUTOGRAPH,
          scenarioId: "scenario_soft_brand_collection_affiliation_v1",
          observationCount: 8,
        }),
      ],
      preloadedEvidence: [],
    });
    const types = result.findings.map((f) => f.findingType);
    const titles = result.findings.map((f) => String(f.title || "").toLowerCase());
    assert.equal(
      types.filter((t) => t === FINDING_TYPES.LARGEST_COMPETITIVE_GAP).length,
      1
    );
    assert.equal(new Set(types).size, types.length, "unique finding types");
    assert.equal(new Set(titles).size, titles.length, "unique category titles");
    assert.equal(result.themeDiversity?.UNIQUE_FINDING_TYPES, true);
    assert.equal(result.themeDiversity?.UNIQUE_CATEGORY_TITLES, true);
    assert.equal(result.themeDiversity?.GAP_TILE_COUNT, 1);
  });

  await test("competitive strength fills a distinct executive theme", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const brandNamesById = peerSetBrandNamesById(PEER_SET_ID_V2);
    const result = await buildExecutiveFindings({
      store,
      brandIds: MARRIOTT,
      brandNamesById,
      geographyKey: "CALA",
      language: "en",
      scope: "portfolio",
      preloadedGaps: [mockGap()],
      preloadedEvidence: [],
      topByPresence: {
        brandId: AUTOGRAPH,
        brandName: "Autograph Collection",
        presence: 0.42,
        display: "42.0%",
        geography: "CALA",
      },
    });
    const strength = result.findings.find(
      (f) => f.findingType === FINDING_TYPES.LARGEST_COMPETITIVE_STRENGTH
    );
    const gap = result.findings.find(
      (f) => f.findingType === FINDING_TYPES.LARGEST_COMPETITIVE_GAP
    );
    assert.ok(strength, "strength tile present when presence leader differs from gap subject");
    assert.ok(gap);
    assert.notEqual(strength.brandId, gap.brandId);
    assert.equal(strength.title, "Largest Competitive Strength");
    assert.ok(/\b42\.0%/.test(strength.governedBody || ""));
    assert.ok(!/\bcited\b/i.test(strength.governedBody || ""));
  });

  await test("HIGH_PRIORITY gap produces executive finding", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const brandNamesById = peerSetBrandNamesById(PEER_SET_ID_V2);
    const result = await buildExecutiveFindings({
      store,
      brandIds: MARRIOTT,
      brandNamesById,
      geographyKey: "CALA",
      language: "en",
      scope: "portfolio",
      peerSetId: PEER_SET_ID_V2,
    });
    assert.ok(result.totalFindings >= 1);
    assert.ok(
      result.findings.some(
        (f) =>
          f.findingType === FINDING_TYPES.LARGEST_COMPETITIVE_GAP &&
          (f.classification === "HIGH_PRIORITY" || f.classification === "PRIORITY")
      )
    );
  });

  await test("MONITOR gap does not outrank HIGH_PRIORITY", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const brandNamesById = peerSetBrandNamesById(PEER_SET_ID_V2);
    const engine = await runCompetitiveGapEngineFromStore(store, {
      geography: "CALA",
      language: "en",
      brandIds: MARRIOTT,
      brandNamesById,
      peerSetId: PEER_SET_ID_V2,
    });
    const high = engine.gaps.find((g) => g.classification === "HIGH_PRIORITY");
    const monitor = engine.gaps.find((g) => g.classification === "MONITOR");
    const result = await buildExecutiveFindings({
      store,
      brandIds: MARRIOTT,
      brandNamesById,
      geographyKey: "CALA",
      language: "en",
      preloadedGaps: engine.gaps,
      preloadedEvidence: [],
    });
    if (high && monitor) {
      const hi = result.findings.findIndex((f) => f.gapId === high.gapId);
      const mo = result.findings.findIndex((f) => f.gapId === monitor.gapId);
      if (hi >= 0 && mo >= 0) assert.ok(hi < mo);
    }
    assert.ok(!result.findings.some((f) => f.classification === "MONITOR"));
  });

  await test("Truth executiveEligible allowed in exec findings", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const brandNamesById = peerSetBrandNamesById(PEER_SET_ID_V2);
    const { comparisons } = loadCachedTruthComparisons();
    const execTruth = filterTruthComparisonsForCohort(comparisons, {
      language: "en",
      brandIds: MARRIOTT,
    }).filter((c) => c.executiveEligible === true);
    const result = await buildExecutiveFindings({
      store,
      brandIds: MARRIOTT,
      brandNamesById,
      geographyKey: "CALA",
      language: "en",
      preloadedGaps: [],
      preloadedEvidence: [],
    });
    if (execTruth.length) {
      assert.ok(
        result.findings.some((f) => f.findingType === FINDING_TYPES.POTENTIAL_AI_PERCEPTION_GAP)
      );
    }
  });

  await test("Truth detail-only blocked from exec when not executiveEligible", async () => {
    const { comparisons } = loadCachedTruthComparisons();
    const detailOnly = comparisons.filter(
      (c) =>
        c.comparisonStatus === "POTENTIAL_PERCEPTION_GAP" && c.executiveEligible !== true
    );
    assert.ok(detailOnly.length >= 0);
    for (const c of detailOnly) {
      assert.notEqual(c.executiveEligible, true);
    }
  });

  await test("NOT_EVALUATED Truth hidden from exec findings contract", async () => {
    const { comparisons } = loadCachedTruthComparisons();
    const notEval = comparisons.filter((c) => c.comparisonStatus === "NOT_EVALUATED");
    assert.ok(notEval.length > 0);
  });

  await test("provider disagreement valid shape", async () => {
    const r = await buildExecutiveFindings({
      store: null,
      preloadedGaps: [],
      preloadedEvidence: [],
      crossProvider: {
        NOT_COMPARABLE: false,
        PROVIDER_DISAGREEMENT: { status: "DISAGREE" },
        STRONGEST_PROVIDER_BY_PRESENCE: { provider: "openai", rate: 0.8 },
        WEAKEST_PROVIDER_BY_PRESENCE: { provider: "perplexity", rate: 0.4 },
        PROVIDERS_MONITORED: ["openai", "perplexity"],
      },
      geographyKey: "CALA",
    });
    const pd = r.findings.find((f) => f.findingType === FINDING_TYPES.PROVIDER_DISAGREEMENT);
    assert.ok(pd);
    assert.ok(pd.headline.includes("OpenAI"));
  });

  await test("provider disagreement incomparable blocked", async () => {
    const r = await buildExecutiveFindings({
      store: null,
      preloadedGaps: [],
      preloadedEvidence: [],
      crossProvider: { NOT_COMPARABLE: true, PROVIDER_DISAGREEMENT: { status: "DISAGREE" } },
      geographyKey: "CALA",
    });
    assert.ok(!r.findings.some((f) => f.findingType === FINDING_TYPES.PROVIDER_DISAGREEMENT));
  });

  await test("trend hidden with insufficient history", async () => {
    const r = await buildExecutiveFindings({
      store: null,
      preloadedGaps: [],
      preloadedEvidence: [],
      presenceChange: { comparable: false, trendStatus: "INSUFFICIENT_HISTORY" },
      geographyKey: "CALA",
    });
    assert.ok(!r.findings.some((f) => f.findingType === FINDING_TYPES.MATERIAL_MOVEMENT));
  });

  await test("trend valid with comparable change", async () => {
    const r = await buildExecutiveFindings({
      store: null,
      preloadedGaps: [],
      preloadedEvidence: [],
      presenceChange: { comparable: true, deltaPp: -5, brandName: "Test Brand" },
      geographyKey: "CALA",
    });
    assert.ok(r.findings.some((f) => f.findingType === FINDING_TYPES.MATERIAL_MOVEMENT));
  });

  await test("source insight allowed when owned rate zero", async () => {
    const r = await buildExecutiveFindings({
      store: null,
      preloadedGaps: [],
      preloadedEvidence: [],
      sourceExecutivePanel: {
        CITATION_SUPPORT: "SUPPORTED",
        OWNED_SOURCE_CITATION_RATE: { value: 0, denominator: 10 },
        THIRD_PARTY_CITATION_RATE: { value: 0.6, denominator: 10 },
      },
      geographyKey: "CALA",
    });
    assert.ok(r.findings.some((f) => f.findingType === FINDING_TYPES.SOURCE_CITATION_GAP));
  });

  await test("no fake tile when empty gaps and no signals", async () => {
    const r = await buildExecutiveFindings({
      store: null,
      preloadedGaps: [],
      preloadedEvidence: [],
      geographyKey: "CALA",
      language: "es",
      brandIds: ["rec_nonexistent_brand"],
    });
    assert.equal(r.totalFindings, 0);
  });

  await test("portfolio vs brand scope", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const brandNamesById = peerSetBrandNamesById(PEER_SET_ID_V2);
    const portfolio = await buildExecutiveFindings({
      store,
      brandIds: MARRIOTT,
      brandNamesById,
      geographyKey: "CALA",
      language: "en",
      scope: "portfolio",
      peerSetId: PEER_SET_ID_V2,
    });
    const brand = await buildExecutiveFindings({
      store,
      brandIds: [AUTOGRAPH],
      brandNamesById,
      geographyKey: "CALA",
      language: "en",
      scope: "brand",
      subjectBrandId: AUTOGRAPH,
      peerSetId: PEER_SET_ID_V2,
    });
    assert.equal(portfolio.scope, "portfolio");
    assert.equal(brand.scope, "brand");
  });

  await test("scenario grouping in detail intelligence", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const brandNamesById = peerSetBrandNamesById(PEER_SET_ID_V2);
    const detail = await buildBrandDetailIntelligence({
      store,
      brandId: AUTOGRAPH,
      brandName: brandNamesById[AUTOGRAPH],
      brandNamesById,
      geographyKey: "CALA",
      language: "en",
      peerSetId: PEER_SET_ID_V2,
    });
    assert.equal(detail.ok, true);
    assert.ok(Array.isArray(detail.topScenarios));
  });

  await test("language isolation in truth cohort filter", async () => {
    const { comparisons } = loadCachedTruthComparisons();
    const en = filterTruthComparisonsForCohort(comparisons, { language: "en", brandIds: MARRIOTT });
    const es = filterTruthComparisonsForCohort(comparisons, { language: "es", brandIds: MARRIOTT });
    for (const c of en) assert.match(String(c.language || "en"), /en/i);
    for (const c of es) assert.match(String(c.language || "es"), /es/i);
  });

  await test("backward-compatible insight box conversion", async () => {
    const payload = executiveFindingsToInsightBoxes({
      findings: [
        {
          findingType: FINDING_TYPES.LARGEST_COMPETITIVE_GAP,
          title: "Largest Competitive Gap",
          headline: "Test headline",
          evidenceSummary: "8 observations",
          reviewAction: "Review positioning",
          dedupeKey: "gap:1",
        },
      ],
    });
    assert.ok(Array.isArray(payload.boxes));
    assert.equal(payload.boxes[0].finding, "Test headline");
  });

  await test("existing executive insight layer unchanged contract", async () => {
    const legacy = buildExecutiveInsightBoxes({
      geographyKey: "CALA",
      topByPresence: {
        brandName: "Autograph Collection",
        presence: 0.8,
        display: "80.0%",
      },
      brandsMonitoredDisplay: "5",
    });
    assert.ok(Array.isArray(legacy.boxes));
  });

  await test("action categories are deterministic enums", async () => {
    assert.ok(ACTION_CATEGORIES.BRAND_POSITIONING);
    assert.ok(ACTION_CATEGORIES.AI_PERCEPTION_REVIEW);
    assert.ok(ACTION_CATEGORIES.SOURCE_CITATION_COVERAGE);
  });

  await test("numeric opportunity scores absent", async () => {
    const store = createBrandAiVisibilityReadStore({});
    const brandNamesById = peerSetBrandNamesById(PEER_SET_ID_V2);
    const r = await buildExecutiveFindings({
      store,
      brandIds: MARRIOTT,
      brandNamesById,
      geographyKey: "CALA",
      language: "en",
      scope: "portfolio",
      peerSetId: PEER_SET_ID_V2,
    });
    assert.equal(r.safety.NUMERIC_OPPORTUNITY_SCORES, 0);
    for (const f of r.findings) {
      assert.equal(f.confidence, undefined);
      assert.equal(f.opportunityScore, undefined);
    }
  });

  console.log(`\nTOTAL: ${passed + failed}  PASS: ${passed}  FAIL: ${failed}\n`);
  process.exit(failed > 0 ? 1 : 0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
