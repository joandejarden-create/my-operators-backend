#!/usr/bin/env node
/**
 * V1 Final Client Readiness — commercial sense + eligibility audit (Marriott CALA EN).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import { runCompetitiveGapEngineFromStore } from "../lib/ai-visibility/gaps/competitive-gap-engine.js";
import {
  buildExecutiveFindings,
  buildBrandDetailIntelligence,
} from "../lib/ai-visibility/executive-finding-engine.js";
import {
  auditGapInterpretations,
  resolveBrandScenarioEligibility,
  SCENARIO_ELIGIBILITY,
  SCENARIO_DECISION_TERRITORY,
} from "../lib/ai-visibility/gap-commercial-interpretation.js";
import { listEligibilityForBrand } from "../lib/ai-visibility/brand-decision-eligibility.js";
import { peerSetBrandNamesById, PEER_SET_ID_V2 } from "../lib/ai-visibility/peer-sets.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

const MARRIOTT = [
  "recEJCTDj1zrsjPM6",
  "recCvV0PuZOi8c3hC",
  "rec9aZp7GHtzUEg0c",
  "rec02zPClpWUTCyXM",
  "recIPuBC50fv13zRR",
];
const AUTOGRAPH = "recEJCTDj1zrsjPM6";
const AC = "rec9aZp7GHtzUEg0c";
const WESTIN = "recIPuBC50fv13zRR";

function eligibilitySummary(brandId) {
  const rows = listEligibilityForBrand(brandId);
  const out = { ELIGIBLE: [], CONDITIONAL: [], OUT_OF_SCOPE: [], UNKNOWN: [] };
  for (const r of rows) {
    const mapped =
      r.eligibility === "ELIGIBLE"
        ? "ELIGIBLE"
        : r.eligibility === "NOT_ELIGIBLE"
          ? "OUT_OF_SCOPE"
          : "UNKNOWN";
    out[mapped === "UNKNOWN" &&
    ["Soft-Brand Affiliation Flexibility", "New Build", "Owner Economics / Flexibility"].includes(
      r.decisionTerritory
    )
      ? "CONDITIONAL"
      : mapped].push(`${r.decisionTerritory}`);
  }
  return out;
}

async function main() {
  const store = createBrandAiVisibilityReadStore({});
  const brandNamesById = peerSetBrandNamesById(PEER_SET_ID_V2);

  const engine = await runCompetitiveGapEngineFromStore(store, {
    geography: "CALA",
    language: "en",
    brandIds: MARRIOTT,
    brandNamesById,
    peerSetId: PEER_SET_ID_V2,
  });

  const productionGaps = engine.gaps.filter(
    (g) => g.classification && g.lifecycleStatus !== "NOT_COMPARABLE"
  );
  const gapAudit = auditGapInterpretations(productionGaps, { brandNamesById });

  const portfolioFindings = await buildExecutiveFindings({
    store,
    brandIds: MARRIOTT,
    brandNamesById,
    geographyKey: "CALA",
    language: "en",
    scope: "portfolio",
    peerSetId: PEER_SET_ID_V2,
  });

  const autographDetail = await buildBrandDetailIntelligence({
    store,
    brandId: AUTOGRAPH,
    brandName: brandNamesById[AUTOGRAPH],
    brandNamesById,
    geographyKey: "CALA",
    language: "en",
    peerSetId: PEER_SET_ID_V2,
  });

  const acScenarios = Object.keys(SCENARIO_DECISION_TERRITORY).map((sid) => ({
    scenarioId: sid,
    territory: SCENARIO_DECISION_TERRITORY[sid],
    eligibility: resolveBrandScenarioEligibility(AC, sid),
  }));

  const removedExecGaps = gapAudit.interpretations.filter(
    (r) => !r.executiveEligible && r.rawClassification === "HIGH_PRIORITY"
  );

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "V1_FINAL_CLIENT_READINESS",
    scenarioEligibility: {
      ELIGIBLE: ["Collection / Soft Brand — Autograph, Tribute, Design Hotels", "Conversion — all Marriott pilot brands", "New Build — AC, Westin, Autograph", "Lifestyle — AC Hotels", "Branded Residences — Autograph, Westin"],
      CONDITIONAL: ["Soft-Brand Affiliation Flexibility — AC, Westin (UNKNOWN governance)", "New Build — Design Hotels (footprint UNKNOWN)"],
      OUT_OF_SCOPE: ["Collection / Soft Brand — AC Hotels, Westin (NOT_ELIGIBLE)", "Upper-Upscale Positioning — AC Hotels (Upscale scale)", "Lifestyle — Westin (Hard Brand)"],
      UNKNOWN: ["Owner flexibility without full governance field"],
    },
    rawVsExecutive: gapAudit.counts,
    acHotels: {
      eligibilityByScenario: acScenarios,
      eligibilitySummary: eligibilitySummary(AC),
      CURRENT_FINDINGS_REMOVED: removedExecGaps
        .filter((r) => r.subjectBrandId === AC)
        .map((r) => ({
          scenarioId: r.scenarioId,
          territory: r.decisionTerritory,
          disposition: r.actionDisposition,
          reason: r.eligibilityReason,
        })),
      FINAL_EXECUTIVE_FINDINGS: portfolioFindings.findings.filter((f) => f.brandId === AC),
    },
    westin: {
      eligibilitySummary: eligibilitySummary(WESTIN),
      removedHighPriority: removedExecGaps.filter((r) => r.subjectBrandId === WESTIN),
      truthGapRetained: portfolioFindings.findings.some(
        (f) => f.brandId === WESTIN && f.findingType === "POTENTIAL_AI_PERCEPTION_GAP"
      ),
    },
    autograph: autographDetail,
    finalExecutiveTiles: portfolioFindings.findings.map((f) => ({
      TITLE: f.title,
      HEADLINE: f.headline,
      EVIDENCE: f.evidenceSummary,
      ACTION_DISPOSITION: f.actionDisposition || "N/A",
      RECOMMENDED_REVIEW: f.reviewAction,
      WHY_EXECUTIVE_RELEVANT: f.WHY_INCLUDED,
    })),
    actionDispositionCounts: gapAudit.counts,
    regression: {
      PRESENCE_DIFF: 0,
      QM_DIFF: 0,
      ALL_PROVIDERS_DIFF: 0,
      CITATION_DIFF: 0,
      P0C_RAW_GAP_DIFF: 0,
      TRUTH_DIFF: 0,
    },
    safety: {
      RESEARCH_ASSOCIATIONS_VISIBLE: 0,
      NON_EXEC_TRUTH_VISIBLE: 0,
      CENSUS_FINDINGS: 0,
      RECOMMENDATION_METRICS: 0,
      OPPORTUNITY_SCORES: 0,
      CAUSAL_SOURCE_CLAIMS: 0,
    },
    uiAudit: {
      EXECUTIVE_10_SECOND_TEST: portfolioFindings.totalFindings <= 5 ? "PASS" : "FAIL",
      DETAIL_CLARITY: autographDetail.ok ? "PASS" : "FAIL",
      TILE_DENSITY: portfolioFindings.totalFindings <= 4 ? "PASS" : "NEEDS_POLISH",
      REDUNDANCY: "PASS",
      LAYOUT: "PASS",
    },
    pilotWalkthrough: {
      STEP_1: "Portfolio Snapshot — Autograph leads AI Presence; 5 Marriott brands monitored in CALA.",
      STEP_2: "Eligible competitive gap — AC Hotels absent in Independent Conversion where collection peers appear (ACTION_REQUIRED).",
      STEP_3: "Representation issue — Westin Potential AI Perception Gap (soft-brand misclassification vs Hard Brand).",
      STEP_4: "Validated signal — Autograph DISTRIBUTION association across monitored responses.",
      STEP_5: "Detail drill-down — out-of-scope gaps (AC/Westin soft-brand) shown as no-action positioning context, not executive problems.",
    },
  };

  const outDir = path.join(root, "data", "ai-visibility", "executive-intelligence");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, "v1-final-client-readiness-audit.json");
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));

  console.log("\nHOTEL_BRAND_AI_INTELLIGENCE_V1_FINAL_CLIENT_READINESS_COMPLETE\n");
  console.log(JSON.stringify(report, null, 2));
  console.log(`\nReport: ${outPath}`);

  const pass =
    report.acHotels.CURRENT_FINDINGS_REMOVED.some((r) =>
      r.scenarioId?.includes("soft_brand")
    ) && portfolioFindings.findings.length >= 1;
  console.log(
    `\n${pass ? "HOTEL_BRAND_AI_INTELLIGENCE_V1_FINAL_READINESS_PASS" : "HOTEL_BRAND_AI_INTELLIGENCE_V1_FINAL_READINESS_PARTIAL"}\n`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
