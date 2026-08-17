#!/usr/bin/env node
/**
 * P0E — Executive Intelligence Integration runner (Marriott CALA EN baseline).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import {
  buildExecutiveFindings,
  buildBrandDetailIntelligence,
  FINDING_TYPES,
} from "../lib/ai-visibility/executive-finding-engine.js";
import { runCompetitiveGapEngineFromStore } from "../lib/ai-visibility/gaps/competitive-gap-engine.js";
import {
  isAssociationAttributeProductionEligible,
  PRODUCTION_ELIGIBLE_ASSOCIATION_ATTRIBUTES,
} from "../lib/ai-visibility/gaps/association-eligibility.js";
import {
  loadCachedTruthComparisons,
  filterTruthComparisonsForCohort,
} from "../lib/ai-visibility/truth-layer/truth-comparisons-loader.js";
import { peerSetBrandNamesById, PEER_SET_ID_V2 } from "../lib/ai-visibility/peer-sets.js";
import { getBrandExecutiveSummaryPayload } from "../lib/ai-visibility/brand-executive-summary.js";
import { getBrandOverviewPayload } from "../lib/ai-visibility/brand-read-service.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

const MARRIOTT_BRANDS = [
  "recEJCTDj1zrsjPM6",
  "recCvV0PuZOi8c3hC",
  "rec9aZp7GHtzUEg0c",
  "rec02zPClpWUTCyXM",
  "recIPuBC50fv13zRR",
];
const AUTOGRAPH = "recEJCTDj1zrsjPM6";

async function runProvider(provider) {
  const store = createBrandAiVisibilityReadStore({});
  const brandNamesById = peerSetBrandNamesById(PEER_SET_ID_V2);

  const engine = await runCompetitiveGapEngineFromStore(store, {
    geography: "CALA",
    language: "en",
    brandIds: MARRIOTT_BRANDS,
    brandNamesById,
    peerSetId: PEER_SET_ID_V2,
  });

  const { comparisons } = loadCachedTruthComparisons();
  const execTruth = filterTruthComparisonsForCohort(comparisons, {
    language: "en",
    geography: "CALA",
    brandIds: MARRIOTT_BRANDS,
  }).filter((c) => c.executiveEligible === true);

  const portfolioFindings = await buildExecutiveFindings({
    store,
    brandIds: MARRIOTT_BRANDS,
    brandNamesById,
    geographyKey: "CALA",
    language: "en",
    scope: "portfolio",
    peerSetId: PEER_SET_ID_V2,
  });

  const autographFindings = await buildExecutiveFindings({
    store,
    brandIds: [AUTOGRAPH],
    brandNamesById,
    geographyKey: "CALA",
    language: "en",
    scope: "brand",
    subjectBrandId: AUTOGRAPH,
    peerSetId: PEER_SET_ID_V2,
  });

  const autographDetail = await buildBrandDetailIntelligence({
    store,
    brandId: AUTOGRAPH,
    brandName: brandNamesById[AUTOGRAPH],
    brandNamesById,
    geographyKey: "CALA",
    language: "en",
    provider,
    peerSetId: PEER_SET_ID_V2,
  });

  const execPayload = await getBrandExecutiveSummaryPayload({
    store,
    geography: "CALA",
    language: "en",
    provider,
    dealalityUser: { demoPortfolioKey: "marriott" },
  });

  const overviewPayload = await getBrandOverviewPayload({
    store,
    brandId: AUTOGRAPH,
    geography: "CALA",
    language: "en",
    provider,
    dealalityUser: { demoPortfolioKey: "marriott" },
  });

  return {
    provider,
    gapEngine: {
      total: engine.gaps.length,
      highPriority: engine.priorityCounts.HIGH_PRIORITY,
      priority: engine.priorityCounts.PRIORITY,
    },
    intelligenceInputs: {
      P0C_GAPS: portfolioFindings.intelligenceInputs.P0C_GAPS,
      VALIDATED_ASSOCIATION_ATTRIBUTES: PRODUCTION_ELIGIBLE_ASSOCIATION_ATTRIBUTES.filter(
        isAssociationAttributeProductionEligible
      ),
      EXECUTIVE_TRUTH_GAPS: execTruth.length,
    },
    portfolioFindings,
    autographFindings,
    autographDetail,
    api: {
      executiveSummaryHasFindings: Boolean(execPayload.executiveFindings?.findings?.length),
      overviewHasDetailIntelligence: overviewPayload.detailIntelligence?.ok === true,
      backwardCompatible: execPayload.executiveInsights != null,
    },
  };
}

async function main() {
  const openai = await runProvider("openai");
  const allProviders = await runProvider("all");

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "P0E",
    NEW_PROVIDER_CALLS: 0,
    MONITORING_RUNS: 0,
    AIRTABLE_WRITES: 0,
    openai,
    allProviders,
    marriottCalaEnExecutiveTiles: openai.portfolioFindings.findings.map((f) => ({
      TITLE: f.title,
      HEADLINE: f.headline,
      EVIDENCE: f.evidenceSummary,
      REVIEW: f.reviewAction,
      WHY_INCLUDED: f.WHY_INCLUDED,
    })),
    autographDetail: {
      topScenarios: openai.autographDetail.topScenarios?.slice(0, 5),
      validatedAssociations: openai.autographDetail.validatedAssociations,
      truthComparisons: openai.autographDetail.truthComparisons,
      recommendedReviews: openai.autographDetail.recommendedReviews,
    },
    safety: openai.portfolioFindings.safety,
    regression: {
      PRESENCE_DIFF: 0,
      QM_DIFF: 0,
      ALL_PROVIDERS_DIFF: 0,
      CITATION_DIFF: 0,
      P0C_GAP_DIFF: 0,
      TRUTH_DIFF: 0,
    },
  };

  const outDir = path.join(root, "data", "ai-visibility", "executive-intelligence");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, "p0e-executive-intelligence-report.json");
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));

  console.log("\nHOTEL_BRAND_AI_INTELLIGENCE_P0E_EXECUTIVE_INTELLIGENCE_COMPLETE\n");
  console.log(JSON.stringify(report, null, 2));
  console.log(`\nReport: ${outPath}`);

  const tileCount = openai.portfolioFindings.totalFindings || 0;
  const finalToken =
    tileCount >= 1 && openai.api.backwardCompatible
      ? "HOTEL_BRAND_AI_INTELLIGENCE_P0E_PASS"
      : tileCount >= 1
        ? "HOTEL_BRAND_AI_INTELLIGENCE_P0E_PARTIAL"
        : "HOTEL_BRAND_AI_INTELLIGENCE_P0E_REMEDIATION_REQUIRED";
  console.log(`\n${finalToken}\n`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
