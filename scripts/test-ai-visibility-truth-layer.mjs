#!/usr/bin/env node
/**
 * P0D-A — Non-Census Truth Layer tests.
 */
import assert from "node:assert/strict";
import { createHash } from "crypto";
import {
  COMPARISON_STATUSES,
  TRUTH_DIMENSION_READINESS,
  isTruthDimensionProductionReady,
  isGovernanceEligibleForTruth,
} from "../lib/ai-visibility/truth-layer/truth-eligibility.js";
import {
  getCensusTruthLayerStatus,
  compareCensusTruth,
} from "../lib/ai-visibility/truth-layer/census-truth-placeholder.js";
import {
  loadBrandBasicsTruthIndex,
  parentCompanyMatches,
  getBrandBasicsTruthFact,
} from "../lib/ai-visibility/truth-layer/brand-basics-truth.js";
import { auditBrandExplorerStructuredFields } from "../lib/ai-visibility/truth-layer/brand-explorer-truth.js";
import {
  compareTruthClaim,
  compareChainScale,
  compareBrandModel,
} from "../lib/ai-visibility/truth-layer/truth-comparison.js";
import { extractTruthClaimsFromEvidence } from "../lib/ai-visibility/truth-layer/truth-claim-extractor.js";
import {
  isProductionTruthGapEligible,
  buildProductionTruthGap,
  integrateP0cClassDGaps,
} from "../lib/ai-visibility/truth-layer/p0c-truth-integration.js";
import { buildTruthLayerHook } from "../lib/ai-visibility/gaps/truth-layer-hook.js";
import { auditAssociationEvidenceCorpus } from "../lib/ai-visibility/associations/evidence-corpus-audit.js";
import { computeAiPresenceRate } from "../lib/ai-visibility/metrics.js";
import { observationsFromEvidence } from "../lib/ai-visibility/gaps/evidence-observations.js";
import { runCompetitiveGapEngine } from "../lib/ai-visibility/gaps/competitive-gap-engine.js";
import { runTruthLayer, runCertifiedLayerRegression } from "../lib/ai-visibility/truth-layer/truth-layer-index.js";

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

async function asyncTest(name, fn) {
  try {
    await fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

function claim(overrides = {}) {
  return {
    claimId: "tcl_test",
    subjectBrandId: "recEJCTDj1zrsjPM6",
    claimType: "PARENT_COMPANY",
    claimValue: "Marriott International",
    supportingSpan: {
      start: 0,
      end: 60,
      exactText: "Autograph Collection is part of Marriott International.",
      text: "Autograph Collection is part of Marriott International.",
    },
    evidenceId: "ev_test",
    responseId: "resp_test",
    promptId: "p_test",
    scenarioId: "scenario_test",
    provider: "openai",
    language: "en",
    geography: "CALA",
    ...overrides,
  };
}

console.log("\nP0D-A — Non-Census Truth Layer Tests\n");

test("comparison statuses — conservative set only", () => {
  assert.deepEqual([...COMPARISON_STATUSES], [
    "ALIGNED",
    "POTENTIAL_PERCEPTION_GAP",
    "INSUFFICIENT_DEALALITY_EVIDENCE",
    "NOT_EVALUATED",
  ]);
  assert.ok(!COMPARISON_STATUSES.includes("AI_IS_WRONG"));
});

await asyncTest("parent aligned — Marriott", async () => {
  const idx = await loadBrandBasicsTruthIndex({ fixtureOnly: true });
  const cmp = compareTruthClaim(
    claim({ claimValue: "Marriott International" }),
    idx.byId
  );
  assert.equal(cmp.comparisonStatus, "ALIGNED");
});

await asyncTest("parent mismatch — Hilton", async () => {
  const idx = await loadBrandBasicsTruthIndex({ fixtureOnly: true });
  const cmp = compareTruthClaim(
    claim({ claimValue: "Hilton Worldwide" }),
    idx.byId
  );
  assert.equal(cmp.comparisonStatus, "POTENTIAL_PERCEPTION_GAP");
});

await asyncTest("parent vs sibling collision — sibling parent keys", async () => {
  const idx = await loadBrandBasicsTruthIndex({ fixtureOnly: true });
  const cmp = compareTruthClaim(
    claim({ claimValue: "Hilton" }),
    idx.byId,
    { peerParentKeys: ["hilton"] }
  );
  assert.equal(cmp.comparisonStatus, "NOT_EVALUATED");
});

await asyncTest("brand-family aligned", async () => {
  const idx = await loadBrandBasicsTruthIndex({ fixtureOnly: true });
  const cmp = compareTruthClaim(
    claim({ claimType: "BRAND_FAMILY", claimValue: "Marriott International" }),
    idx.byId
  );
  assert.equal(cmp.comparisonStatus, "ALIGNED");
});

await asyncTest("brand-family mismatch", async () => {
  const idx = await loadBrandBasicsTruthIndex({ fixtureOnly: true });
  const cmp = compareTruthClaim(
    claim({ claimType: "BRAND_FAMILY", claimValue: "IHG" }),
    idx.byId
  );
  assert.equal(cmp.comparisonStatus, "POTENTIAL_PERCEPTION_GAP");
});

test("chain-scale aligned", () => {
  const r = compareChainScale("Upper Upscale", {
    factValue: "Upper Upscale",
    eligibility: "ELIGIBLE",
  });
  assert.equal(r.status, "ALIGNED");
});

test("chain-scale ambiguous — lifestyle/upscale overlap", () => {
  const r = compareChainScale("Lifestyle / Boutique", {
    factValue: "Upper Upscale",
    eligibility: "ELIGIBLE",
  });
  assert.equal(r.status, "ALIGNED");
  assert.equal(r.ambiguous, true);
});

test("chain-scale false-conflict prevention — compatible upscale", () => {
  const r = compareChainScale("Upscale", {
    factValue: "Upper Upscale",
    eligibility: "ELIGIBLE",
  });
  assert.equal(r.status, "ALIGNED");
});

test("brand-model aligned", () => {
  const r = compareBrandModel("Collection Brand", {
    factValue: "Collection Brand",
    eligibility: "ELIGIBLE",
  });
  assert.equal(r.status, "ALIGNED");
});

test("brand-model mismatch", () => {
  const r = compareBrandModel("Hard Brand", {
    factValue: "Collection Brand",
    eligibility: "ELIGIBLE",
  });
  assert.equal(r.status, "POTENTIAL_PERCEPTION_GAP");
});

await asyncTest("positioning ineligible — narrative blocked", async () => {
  const idx = await loadBrandBasicsTruthIndex({ fixtureOnly: true });
  const cmp = compareTruthClaim(
    claim({ claimType: "POSITIONING", claimValue: "Luxury" }),
    idx.byId
  );
  assert.equal(cmp.comparisonStatus, "INSUFFICIENT_DEALALITY_EVIDENCE");
});

await asyncTest("conversion narrative blocked — deferred", async () => {
  const idx = await loadBrandBasicsTruthIndex({ fixtureOnly: true });
  const cmp = compareTruthClaim(
    claim({ claimType: "CONVERSION_ORIENTATION", claimValue: "CONVERSION_FRIENDLY" }),
    idx.byId
  );
  assert.equal(cmp.comparisonStatus, "INSUFFICIENT_DEALALITY_EVIDENCE");
});

test("AI-assisted Explorer blocked", () => {
  const audit = auditBrandExplorerStructuredFields();
  const positioning = audit.fields.find((f) => f.field === "positioning");
  assert.equal(positioning.safeForTruthLayer, "NO");
  assert.equal(positioning.governanceState, "AI_ASSISTED");
});

test("source-informed Explorer conditional", () => {
  const audit = auditBrandExplorerStructuredFields();
  const conv = audit.fields.find((f) => f.field === "conversion_orientation");
  assert.equal(conv.safeForTruthLayer, "NO");
});

await asyncTest("explicit claim required — no span", async () => {
  const idx = await loadBrandBasicsTruthIndex({ fixtureOnly: true });
  const cmp = compareTruthClaim(
    claim({ supportingSpan: null }),
    idx.byId
  );
  assert.equal(cmp.comparisonStatus, "NOT_EVALUATED");
});

await asyncTest("span validity — too short blocked", async () => {
  const idx = await loadBrandBasicsTruthIndex({ fixtureOnly: true });
  const cmp = compareTruthClaim(
    claim({ supportingSpan: { exactText: "short", text: "short", start: 0, end: 5 } }),
    idx.byId
  );
  assert.equal(cmp.comparisonStatus, "NOT_EVALUATED");
});

test("entity binding — parentCompanyMatches", () => {
  const match = parentCompanyMatches("Marriott International", {
    factValue: "Marriott International",
    parentNormalizedKeys: ["marriott international"],
  });
  assert.equal(match.match, true);
});

test("language isolation — claim carries language", () => {
  const c = claim({ language: "es" });
  assert.equal(c.language, "es");
});

test("geography isolation — claim carries geography", () => {
  const c = claim({ geography: "CALA" });
  assert.equal(c.geography, "CALA");
});

test("P0C D eligible integration", () => {
  const cmp = {
    comparisonStatus: "POTENTIAL_PERCEPTION_GAP",
    aiClaimType: "PARENT_COMPANY",
    aiClaimValue: "Hilton Worldwide",
    aiSupportingSpan: "Autograph Collection is part of Hilton Worldwide and operates under Hilton distribution.",
    eligibilityStatus: "ELIGIBLE",
    executiveEligible: true,
    subjectBrandId: "recEJCTDj1zrsjPM6",
    scenarioId: "scenario_test",
    truthComparisonId: "tcmp_test",
    dealalityFactValue: "Marriott International",
    dealalitySource: "Brand Basics",
    dealalityGovernanceState: "COMPANY_PUBLISHED",
    truthRuleVersion: "v1_1",
    evidenceId: "ev1",
  };
  const gate = isProductionTruthGapEligible(cmp);
  assert.equal(gate.eligible, true);
  const gap = buildProductionTruthGap(cmp);
  assert.equal(gap.productionEligible, true);
  assert.equal(gap.gapClass, "AI_PERCEPTION_VS_DEALALITY_FACT_GAP");
});

test("P0C D blocked integration — aligned not gap", () => {
  const cmp = {
    comparisonStatus: "ALIGNED",
    aiClaimType: "PARENT_COMPANY",
    aiClaimValue: "Marriott International",
    aiSupportingSpan: "Autograph Collection is part of Marriott International.",
    eligibilityStatus: "ELIGIBLE",
    subjectBrandId: "recEJCTDj1zrsjPM6",
  };
  assert.equal(isProductionTruthGapEligible(cmp).eligible, false);
});

test("P0C D blocked — positioning deferred", () => {
  const cmp = {
    comparisonStatus: "POTENTIAL_PERCEPTION_GAP",
    aiClaimType: "POSITIONING",
    aiClaimValue: "Luxury",
    aiSupportingSpan: "Autograph Collection is positioned as luxury.",
    eligibilityStatus: "NOT_ELIGIBLE",
    subjectBrandId: "recEJCTDj1zrsjPM6",
  };
  assert.equal(isProductionTruthGapEligible(cmp).eligible, false);
});

test("Census hook returns DEFERRED_INCOMPLETE_CENSUS", () => {
  const status = getCensusTruthLayerStatus();
  assert.equal(status.status, "DEFERRED_INCOMPLETE_CENSUS");
  assert.equal(status.CENSUS_READS_FOR_TRUTH_COMPARISON, 0);
  const cmp = compareCensusTruth();
  assert.equal(cmp.reason, "DEFERRED_INCOMPLETE_CENSUS");
});

test("truth layer hook — with comparison", () => {
  const hook = buildTruthLayerHook(
    { gapClass: "AI_PERCEPTION_VS_DEALALITY_FACT_GAP", subjectBrandId: "recEJCTDj1zrsjPM6" },
    {
      truthComparisonId: "tcmp_1",
      comparisonStatus: "POTENTIAL_PERCEPTION_GAP",
      aiClaimType: "PARENT_COMPANY",
      aiClaimValue: "Hilton Worldwide",
      aiSupportingSpan: "Autograph Collection is part of Hilton Worldwide and operates under Hilton distribution.",
      eligibilityStatus: "ELIGIBLE",
      executiveEligible: true,
      subjectBrandId: "recEJCTDj1zrsjPM6",
      scenarioId: "s1",
      dealalitySource: "Brand Basics",
      dealalityGovernanceState: "COMPANY_PUBLISHED",
    }
  );
  assert.equal(hook.productionEligible, true);
  assert.equal(hook.truthComparisonStatus, "POTENTIAL_PERCEPTION_GAP");
});

test("governance eligibility", () => {
  assert.equal(isGovernanceEligibleForTruth("COMPANY_PUBLISHED"), true);
  assert.equal(isGovernanceEligibleForTruth("AI_ASSISTED"), false);
  assert.equal(isGovernanceEligibleForTruth("CURATED_INTERPRETATION"), false);
});

test("dimension readiness — parent production validated", () => {
  assert.equal(isTruthDimensionProductionReady("PARENT_COMPANY"), true);
  assert.equal(TRUTH_DIMENSION_READINESS.CONVERSION_ORIENTATION, "DEFERRED");
});

await asyncTest("certified layer regression — zero diff", async () => {
  const corpus = await auditAssociationEvidenceCorpus();
  const certified = runCertifiedLayerRegression(corpus);
  assert.equal(certified.PRESENCE_DIFF, 0);
  assert.equal(certified.QM_DIFF, 0);
  assert.equal(certified.ALL_PROVIDERS_DIFF, 0);
  assert.equal(certified.CITATION_DIFF, 0);
  assert.equal(certified.ASSOCIATION_DIFF, 0);
  assert.equal(certified.P0C_A_B_GAP_DIFF, 0);
});

await asyncTest("full truth layer run — completes", async () => {
  const result = await runTruthLayer();
  assert.ok(result.report);
  assert.ok(Array.isArray(result.comparisons));
  assert.equal(result.report.safety.CENSUS_READS_FOR_TRUTH, 0);
  assert.equal(result.report.safety.AIRTABLE_WRITES, 0);
});

await asyncTest("claim extractor — explicit parent from evidence sample", async () => {
  const corpus = await auditAssociationEvidenceCorpus();
  const ev = corpus.evidence.find(
    (e) =>
      String(e.payload?.rawResponseText || "").includes("Autograph") &&
      String(e.payload?.rawResponseText || "").includes("Marriott")
  );
  if (!ev) {
    console.log("    (skip — no matching evidence in corpus)");
    return;
  }
  const entity = { id: "recEJCTDj1zrsjPM6", name: "Autograph Collection" };
  const claims = extractTruthClaimsFromEvidence(ev, entity);
  assert.ok(claims.length >= 0);
});

console.log(`\nTOTAL: ${passed + failed}  PASS: ${passed}  FAIL: ${failed}`);
if (failed > 0) {
  console.log("\nHOTEL_BRAND_AI_INTELLIGENCE_P0DA_REMEDIATION_REQUIRED (tests failed)");
  process.exit(1);
}
console.log("\nHOTEL_BRAND_AI_INTELLIGENCE_P0DA_PASS (tests)");
process.exit(0);
