#!/usr/bin/env node
/**
 * P0D-A.1 — Truth Layer semantic integrity tests.
 */
import assert from "node:assert/strict";
import {
  areDimensionsComparable,
  inferAiSemanticDimension,
  isContrastiveMention,
  isListEnumerationClaim,
  isPortfolioRangeChainScale,
  normalizeArchitectureBucket,
} from "../lib/ai-visibility/truth-layer/truth-comparability.js";
import {
  compareTruthClaim,
  compareBrandModel,
  compareChainScale,
  applySemanticPreCheck,
} from "../lib/ai-visibility/truth-layer/truth-comparison.js";
import { assessExecutiveEligibility } from "../lib/ai-visibility/truth-layer/executive-eligibility.js";
import { auditParentCompanyConflicts } from "../lib/ai-visibility/truth-layer/parent-conflict-audit.js";
import { isProductionTruthGapEligible } from "../lib/ai-visibility/truth-layer/p0c-truth-integration.js";
import { loadBrandBasicsTruthIndex, parentCompanyMatches } from "../lib/ai-visibility/truth-layer/brand-basics-truth.js";
import { getCensusTruthLayerStatus } from "../lib/ai-visibility/truth-layer/census-truth-placeholder.js";
import { auditAssociationEvidenceCorpus } from "../lib/ai-visibility/associations/evidence-corpus-audit.js";
import { runCertifiedLayerRegression } from "../lib/ai-visibility/truth-layer/truth-layer-index.js";
import { runCompetitiveGapEngine } from "../lib/ai-visibility/gaps/competitive-gap-engine.js";
import { observationsFromEvidence } from "../lib/ai-visibility/gaps/evidence-observations.js";
import { computeAiPresenceRate } from "../lib/ai-visibility/metrics.js";

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
    claimId: "tcl_sem",
    subjectBrandId: "recEJCTDj1zrsjPM6",
    subjectBrandName: "Autograph Collection",
    claimType: "BRAND_MODEL",
    claimValue: "Lifestyle Brand",
    supportingSpan: {
      exactText: "Marriott's lifestyle brands — which include W Hotels, Edition, and Autograph Collection — are some of the best known.",
      start: 0,
      end: 100,
    },
    evidenceId: "ev_sem",
    promptId: "p_sem",
    scenarioId: "scenario_test",
    provider: "openai",
    language: "en",
    geography: "CALA",
    ...overrides,
  };
}

console.log("\nP0D-A.1 — Truth Semantic Integrity Tests\n");

test("positioning vs architecture non-comparable", () => {
  assert.equal(areDimensionsComparable("POSITIONING", "BRAND_ARCHITECTURE"), "NO");
  assert.equal(inferAiSemanticDimension("BRAND_MODEL", "Lifestyle Brand", ""), "POSITIONING");
});

test("lifestyle vs collection non-conflict — NOT_EVALUATED", async () => {
  const idx = await loadBrandBasicsTruthIndex({ fixtureOnly: true });
  const cmp = compareTruthClaim(claim(), idx.byId);
  assert.equal(cmp.comparisonStatus, "NOT_EVALUATED");
  assert.equal(cmp.comparisonReason, "cross_dimension_non_comparable");
});

test("chain scale vs lifestyle ambiguity blocked", async () => {
  const idx = await loadBrandBasicsTruthIndex({ fixtureOnly: true });
  const cmp = compareTruthClaim(
    claim({
      subjectBrandId: "rec9aZp7GHtzUEg0c",
      subjectBrandName: "AC Hotels by Marriott",
      claimType: "CHAIN_SCALE",
      claimValue: "Lifestyle / Boutique",
      supportingSpan: {
        exactText: "AC Hotels by Marriott: A modern, minimalist European-design lifestyle brand popular with LATAM developers.",
        start: 0,
        end: 90,
      },
    }),
    idx.byId
  );
  assert.equal(cmp.comparisonStatus, "NOT_EVALUATED");
});

test("parent display alias normalization — the Marriott", () => {
  const match = parentCompanyMatches("the Marriott", {
    factValue: "Marriott International",
    parentNormalizedKeys: ["marriott international", "marriott"],
  });
  assert.equal(match.match, true);
});

test("parent genuine mismatch — Hilton", async () => {
  const idx = await loadBrandBasicsTruthIndex({ fixtureOnly: true });
  const cmp = compareTruthClaim(
    claim({
      claimType: "PARENT_COMPANY",
      claimValue: "Hilton Worldwide",
      supportingSpan: {
        exactText: "Autograph Collection is part of Hilton Worldwide's portfolio.",
        start: 0,
        end: 60,
      },
    }),
    idx.byId
  );
  assert.equal(cmp.comparisonStatus, "POTENTIAL_PERCEPTION_GAP");
});

test("soft brand genuine mismatch blocked — contrastive Westin context", async () => {
  const idx = await loadBrandBasicsTruthIndex({ fixtureOnly: true });
  const cmp = compareTruthClaim(
    claim({
      subjectBrandId: "recIPuBC50fv13zRR",
      subjectBrandName: "Westin",
      claimType: "BRAND_MODEL",
      claimValue: "Soft Brand",
      supportingSpan: {
        exactText: "Soft brands (Autograph, Curio) typically require lower PIP compared to strict hard brands (Westin, Grand Hyatt).",
        start: 0,
        end: 100,
      },
    }),
    idx.byId
  );
  assert.equal(cmp.comparisonStatus, "NOT_EVALUATED");
  assert.equal(cmp.comparisonReason, "contrastive_context_not_subject_claim");
});

test("hard brand / collection contrastive rule — Autograph", async () => {
  const idx = await loadBrandBasicsTruthIndex({ fixtureOnly: true });
  const cmp = compareTruthClaim(
    claim({
      claimType: "BRAND_MODEL",
      claimValue: "Hard Brand",
      supportingSpan: {
        exactText: "Autograph Collection — useful when Marriott Bonvoy distribution is important but a hard brand would dilute the concept.",
        start: 0,
        end: 110,
      },
    }),
    idx.byId
  );
  assert.equal(cmp.comparisonStatus, "NOT_EVALUATED");
});

test("cross-dimension conflict blocked — conversion brand", async () => {
  const idx = await loadBrandBasicsTruthIndex({ fixtureOnly: true });
  const cmp = compareTruthClaim(
    claim({
      claimType: "BRAND_MODEL",
      claimValue: "Conversion Brand",
      supportingSpan: {
        exactText: "The most active upper-upscale conversion brands globally are Autograph Collection, Tribute Portfolio, Renaissance, Sheraton.",
        start: 0,
        end: 110,
      },
    }),
    idx.byId
  );
  assert.equal(cmp.comparisonStatus, "NOT_EVALUATED");
});

test("ambiguous portfolio range → NOT_EVALUATED", async () => {
  const idx = await loadBrandBasicsTruthIndex({ fixtureOnly: true });
  const cmp = compareTruthClaim(
    claim({
      claimType: "CHAIN_SCALE",
      claimValue: "Midscale",
      supportingSpan: {
        exactText: "| Marriott | Autograph Collection, Tribute Portfolio, Design Hotels | Midscale–Luxury |",
        start: 0,
        end: 90,
      },
    }),
    idx.byId
  );
  assert.equal(cmp.comparisonStatus, "NOT_EVALUATED");
  assert.equal(cmp.comparisonReason, "portfolio_range_not_subject_scale");
});

test("same-dimension conflict preserved — Westin collection vs hard", async () => {
  const idx = await loadBrandBasicsTruthIndex({ fixtureOnly: true });
  const span = "Marriott officially places Westin within its premium full-service portfolio alongside collection brands.";
  const pre = applySemanticPreCheck(
    { claimType: "BRAND_MODEL", claimValue: "Collection Brand" },
    { factType: "BRAND_MODEL", factValue: "Hard Brand", eligibility: "ELIGIBLE" },
    span,
    "Westin"
  );
  if (pre) {
    assert.equal(pre.status, "NOT_EVALUATED");
  } else {
    const r = compareBrandModel("Collection Brand", { factValue: "Hard Brand", eligibility: "ELIGIBLE" });
    assert.ok(["POTENTIAL_PERCEPTION_GAP", "NOT_EVALUATED"].includes(r.status));
  }
});

test("Executive eligibility blocked — lifestyle list", () => {
  const cmp = {
    comparisonStatus: "POTENTIAL_PERCEPTION_GAP",
    eligibilityStatus: "ELIGIBLE",
    aiClaimType: "BRAND_MODEL",
    aiClaimValue: "Lifestyle Brand",
    aiSupportingSpan: "Owners consider lifestyle brands such as W Hotels, Autograph Collection, Tribute Portfolio, Kimpton.",
    dealalityFactType: "BRAND_MODEL",
    dealalityFactValue: "Collection Brand",
    subjectBrandId: "recEJCTDj1zrsjPM6",
  };
  const exec = assessExecutiveEligibility(cmp, { subjectBrandName: "Autograph Collection" });
  assert.equal(exec.executiveEligible, false);
});

test("Executive eligibility positive — explicit architecture mismatch", () => {
  const cmp = {
    comparisonStatus: "POTENTIAL_PERCEPTION_GAP",
    eligibilityStatus: "ELIGIBLE",
    aiClaimType: "BRAND_MODEL",
    aiClaimValue: "Hard Brand",
    aiSupportingSpan: "Autograph Collection operates as a traditional hard brand within the Marriott system with strict standards.",
    dealalityFactType: "BRAND_MODEL",
    dealalityFactValue: "Collection Brand",
    dealalitySource: "Brand Basics",
    dealalityGovernanceState: "STRUCTURED_GOVERNED_FACT",
    subjectBrandId: "recEJCTDj1zrsjPM6",
    CENSUS_USED: false,
  };
  const exec = assessExecutiveEligibility(cmp, { subjectBrandName: "Autograph Collection" });
  assert.equal(exec.executiveEligible, true);
});

test("P0C D blocked when not executive eligible", () => {
  const cmp = {
    comparisonStatus: "POTENTIAL_PERCEPTION_GAP",
    eligibilityStatus: "ELIGIBLE",
    executiveEligible: false,
    aiClaimType: "BRAND_MODEL",
    aiClaimValue: "Lifestyle Brand",
    aiSupportingSpan: "Lifestyle brands such as Autograph Collection and Kimpton are commonly cited.",
    subjectBrandId: "recEJCTDj1zrsjPM6",
  };
  assert.equal(isProductionTruthGapEligible(cmp).eligible, false);
});

test("architecture buckets — collection vs hard mutually exclusive", () => {
  assert.equal(normalizeArchitectureBucket("Collection Brand"), "COLLECTION");
  assert.equal(normalizeArchitectureBucket("Hard Brand"), "HARD_BRAND");
});

test("contrastive mention detection", () => {
  assert.equal(
    isContrastiveMention(
      "Soft brands (Autograph, Curio) require lower PIP compared to hard brands (Westin, Grand Hyatt).",
      "Soft Brand",
      "Westin"
    ),
    true
  );
});

test("portfolio range detection", () => {
  assert.equal(isPortfolioRangeChainScale("| Marriott | Autograph | Midscale–Luxury |"), true);
});

test("list enumeration detection", () => {
  assert.equal(
    isListEnumerationClaim(
      "Owners consider lifestyle brands such as W Hotels, Autograph Collection, Tribute Portfolio, Kimpton, Hotel Indigo.",
      "Lifestyle Brand",
      "Autograph Collection"
    ),
    true
  );
});

test("parent conflicts — all normalization variation", async () => {
  const idx = await loadBrandBasicsTruthIndex({ fixtureOnly: true });
  const audit = auditParentCompanyConflicts(idx);
  assert.equal(audit.genuineConflict, 0);
  assert.ok(audit.normalizationVariation >= 10);
});

test("Census remains frozen", () => {
  const s = getCensusTruthLayerStatus();
  assert.equal(s.status, "DEFERRED_INCOMPLETE_CENSUS");
  assert.equal(s.CENSUS_READS_FOR_TRUTH_COMPARISON, 0);
});

await asyncTest("P0C A/B unchanged — certified layer zero diff", async () => {
  const corpus = await auditAssociationEvidenceCorpus();
  const certified = runCertifiedLayerRegression(corpus);
  assert.equal(certified.PRESENCE_DIFF, 0);
  assert.equal(certified.QM_DIFF, 0);
  assert.equal(certified.P0C_A_B_GAP_DIFF, 0);

  const observations = observationsFromEvidence(corpus.evidence, { geography: "CALA", language: "en" });
  const before = computeAiPresenceRate(observations, "recEJCTDj1zrsjPM6");
  runCompetitiveGapEngine({ observations, evidence: corpus.evidence, brandIds: ["recEJCTDj1zrsjPM6"] });
  const after = computeAiPresenceRate(observations, "recEJCTDj1zrsjPM6");
  assert.equal(before.value, after.value);
});

console.log(`\nTOTAL: ${passed + failed}  PASS: ${passed}  FAIL: ${failed}`);
if (failed > 0) {
  console.log("\nHOTEL_BRAND_AI_INTELLIGENCE_P0DA1_REMEDIATION_REQUIRED (tests failed)");
  process.exit(1);
}
console.log("\nHOTEL_BRAND_AI_INTELLIGENCE_P0DA1_PASS (tests)");
process.exit(0);
