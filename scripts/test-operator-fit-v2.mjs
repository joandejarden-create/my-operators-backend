#!/usr/bin/env node
/**
 * Operator Fit Engine v2 — pure unit + scenario tests (no Airtable writes).
 *   node scripts/test-operator-fit-v2.mjs
 */
import {
  isOperatorFitEngineV2Enabled,
  getOperatorFitEngineFlagState,
  OPERATOR_FIT_ENGINE_VERSION,
} from "../lib/operator-fit/feature-flag.js";
import {
  EVIDENCE_CLASSES,
  EVIDENCE_CONFIDENCE,
  TABLE_STAKES_CAPABILITY_TOKENS,
} from "../lib/operator-fit/config.js";
import { adaptProjectFromDealContext } from "../lib/operator-fit/adapters/project-from-deal.js";
import {
  adaptOperatorFromPrefill,
  isTableStakesToken,
} from "../lib/operator-fit/adapters/operator-from-prefill.js";
import { evaluateEligibility } from "../lib/operator-fit/eligibility.js";
import {
  scoreAllOperatorProjectFactors,
  aggregateOperatorProjectAlignment,
} from "../lib/operator-fit/alignment-factors.js";
import { evaluateCandidate } from "../lib/operator-fit/evaluate-candidate.js";
import { selectTop5OperatorAlignment } from "../lib/operator-fit/top5-selector.js";
import { evaluateOperatorFitForDeal } from "../lib/operator-fit/evaluate-deal.js";
import { mapOperatingStructureValue } from "../lib/operator-fit/structure-mapping.js";
import { FIT_V2_SCENARIOS, FIT_V2_OPERATORS } from "../lib/operator-fit/fixtures/scenarios.js";
import { scoreOperatorMatchForDeal } from "../api/my-deals.js";

let failed = 0;
function ok(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    failed += 1;
  } else {
    console.log("ok:", msg);
  }
}

function opById(id) {
  const row = FIT_V2_OPERATORS.find((o) => o.id === id);
  return adaptOperatorFromPrefill(row.prefill, {
    operatorId: row.id,
    companyName: row.companyName,
  });
}

function projectFromScenario(s) {
  return adaptProjectFromDealContext({
    dealId: "recTEST" + s.id,
    dealFields: s.dealFields,
    locationData: s.locationData,
    mpData: s.mpData,
    siData: s.siData,
  });
}

// --- Feature flag default off ---
{
  const prev = process.env.OPERATOR_FIT_ENGINE_V2;
  delete process.env.OPERATOR_FIT_ENGINE_V2;
  ok(isOperatorFitEngineV2Enabled() === false, "flag default off");
  ok(getOperatorFitEngineFlagState().defaultOff === true, "flag state defaultOff");
  process.env.OPERATOR_FIT_ENGINE_V2 = "1";
  ok(isOperatorFitEngineV2Enabled() === true, "flag on when OPERATOR_FIT_ENGINE_V2=1");
  if (prev == null) delete process.env.OPERATOR_FIT_ENGINE_V2;
  else process.env.OPERATOR_FIT_ENGINE_V2 = prev;
}

// --- Structure mapping preserves labels ---
{
  const m = mapOperatingStructureValue("Full third-party management");
  ok(m.canonicalKey === "third_party_management", "maps full third-party to canonical");
  ok(m.preservedLabel === "Third-Party Management", "preserved owner-facing label");
  const f = mapOperatingStructureValue("Franchise Only");
  ok(f.preservedLabel === "Franchise Only", "Franchise Only preserved");
}

// --- Table-stakes detection ---
{
  ok(isTableStakesToken("Revenue management"), "RM is table-stakes");
  ok(isTableStakesToken("Sales"), "Sales is table-stakes");
  ok(!isTableStakesToken("Branded residences hospitality services"), "residences not table-stakes");
  ok(TABLE_STAKES_CAPABILITY_TOKENS.length >= 8, "table-stakes list present");
}

// --- Unknown vs no / factor dropout ---
{
  const project = projectFromScenario(FIT_V2_SCENARIOS[0]);
  const sparse = opById("sparse-data-operator");
  const factors = scoreAllOperatorProjectFactors(project, sparse);
  const agg = aggregateOperatorProjectAlignment(factors);
  const unknownFactors = factors.filter((f) => f.applicable && f.state === "unknown");
  ok(unknownFactors.length >= 2, "sparse profile has unknown applicable factors");
  ok(agg.unknownWeight > 0, "unknown weight counted in denominator");
  ok(agg.applicableWeight >= agg.knownWeight + agg.unknownWeight - 0.01, "denom includes unknowns");
  // Removing data must not inflate: raw should be modest
  ok(agg.rawScore < 70, "sparse operator-project raw not inflated via reweighting");
}

// --- Fee default not 75 ---
{
  const project = projectFromScenario(FIT_V2_SCENARIOS[0]);
  const sparse = opById("sparse-data-operator");
  const evaled = evaluateCandidate(project, sparse);
  ok(evaled.layers.economics.numericScore == null, "unknown economics have no numeric fee score");
  ok(
    !String(evaled.layers.economics.note || "").includes("75"),
    "economics note does not invent 75"
  );
}

// --- Sparse displayed ceiling ---
{
  const project = projectFromScenario(FIT_V2_SCENARIOS[5]); // lifestyle
  const sparse = opById("sparse-data-operator");
  const evaled = evaluateCandidate(project, sparse);
  ok(evaled.evidenceConfidence === EVIDENCE_CONFIDENCE.LIMITED.label, "sparse → Limited confidence");
  ok(evaled.displayedOperatorAlignment <= 69, "sparse displayed ≤ Limited ceiling 69");
  ok(evaled.displayedOperatorAlignment < 94.9, "legacy sparse 94.9 impossible");
}

// --- Evidence quality: operator-reported only not Strong ---
{
  const project = projectFromScenario(FIT_V2_SCENARIOS[0]);
  const generic = opById("generic-full-service-claims");
  const evaled = evaluateCandidate(project, generic);
  ok(evaled.evidenceConfidence !== EVIDENCE_CONFIDENCE.STRONG.label, "general claims ≠ Strong");
}

// --- Verified evidence can be Strong ---
{
  const project = projectFromScenario(FIT_V2_SCENARIOS[1]);
  const luxury = opById("cala-resort-luxury");
  const evaled = evaluateCandidate(project, luxury);
  ok(
    evaled.evidenceConfidence === EVIDENCE_CONFIDENCE.STRONG.label ||
      evaled.evidenceConfidence === EVIDENCE_CONFIDENCE.MODERATE.label,
    "verified resort evidence is Moderate or Strong"
  );
  if (evaled.evidenceConfidence === EVIDENCE_CONFIDENCE.STRONG.label) {
    ok(evaled.confidenceCeilingApplied == null, "Strong has no ceiling");
  }
}

// --- Eligibility hard conflict ---
{
  const project = projectFromScenario(FIT_V2_SCENARIOS[0]);
  const wrongGeo = opById("wrong-geo-broad-claims");
  const elig = evaluateEligibility(project, wrongGeo);
  ok(elig.status === "Not Currently Eligible", "geo hard conflict → Not Currently Eligible");
  const { top5 } = selectTop5OperatorAlignment(project, [
    wrongGeo,
    opById("yucatan-select-specialist"),
  ]);
  ok(
    !top5.some((t) => t.candidateId === "wrong-geo-broad-claims"),
    "ineligible not in Top-5"
  );
}

// --- Generic claims must not auto-rank #1 on niche conversion ---
{
  const project = projectFromScenario(
    FIT_V2_SCENARIOS.find((s) => s.id === "select-service-conversion")
  );
  const ops = [
    opById("generic-full-service-claims"),
    opById("yucatan-select-specialist"),
    opById("sparse-data-operator"),
    opById("wrong-geo-broad-claims"),
  ];
  const { top5 } = selectTop5OperatorAlignment(project, ops);
  ok(top5.length >= 1, "conversion scenario returns candidates");
  ok(
    top5[0].candidateId !== "generic-full-service-claims",
    "generic claims not #1 on select-service conversion"
  );
  ok(
    top5[0].candidateId === "yucatan-select-specialist",
    "comparable specialist can outrank generic claims"
  );
}

// --- Comparable relevance: mixed-use specialist vs generic ---
{
  const project = projectFromScenario(
    FIT_V2_SCENARIOS.find((s) => s.id === "mixed-use-branded-residences")
  );
  const { top5 } = selectTop5OperatorAlignment(project, [
    opById("generic-full-service-claims"),
    opById("mixed-use-residences-specialist"),
  ]);
  ok(
    top5[0].candidateId === "mixed-use-residences-specialist",
    "smaller relevant specialist outranks broad generalist on mixed-use residences"
  );
}

// --- Determinism ---
{
  const project = projectFromScenario(FIT_V2_SCENARIOS[0]);
  const ops = FIT_V2_OPERATORS.map((o) =>
    adaptOperatorFromPrefill(o.prefill, { operatorId: o.id, companyName: o.companyName })
  );
  const a = selectTop5OperatorAlignment(project, ops);
  const b = selectTop5OperatorAlignment(project, ops);
  ok(
    JSON.stringify(a.top5.map((t) => [t.candidateId, t.displayedOperatorAlignment, t.whyItMatches])) ===
      JSON.stringify(b.top5.map((t) => [t.candidateId, t.displayedOperatorAlignment, t.whyItMatches])),
    "identical inputs → identical ranking/scores/explanations"
  );
}

// --- All 8 scenarios run ---
{
  for (const s of FIT_V2_SCENARIOS) {
    const result = evaluateOperatorFitForDeal({
      dealId: "recSCENARIO_" + s.id,
      dealFields: s.dealFields,
      locationData: s.locationData,
      mpData: s.mpData,
      siData: s.siData,
      operatorPrefills: FIT_V2_OPERATORS.map((o) => ({
        operatorId: o.id,
        companyName: o.companyName,
        prefill: o.prefill,
      })),
      brandManagedCandidates:
        s.id === "luxury-leisure-resort"
          ? [
              {
                brandName: "Four Seasons",
                offersBrandManagement: true,
                markets: ["Dominican Republic"],
                scales: ["Luxury"],
                evidenceClasses: [EVIDENCE_CLASSES.PORTFOLIO_LEVEL],
              },
            ]
          : [],
    });
    ok(result.featureVersion === OPERATOR_FIT_ENGINE_VERSION, `scenario ${s.id} has version`);
    ok(Array.isArray(result.top5), `scenario ${s.id} top5 array`);
    ok(result.top5.length <= 5, `scenario ${s.id} ≤5 results`);
    for (const t of result.top5) {
      ok(t.displayedOperatorAlignment <= 100, `${s.id} score ≤100`);
      if (t.evidenceConfidence === "Limited") {
        ok(t.displayedOperatorAlignment <= 69, `${s.id} Limited ceiling`);
      }
      if (t.evidenceConfidence === "Moderate") {
        ok(t.displayedOperatorAlignment <= 84, `${s.id} Moderate ceiling`);
      }
      ok(t.eligibilityStatus !== "Not Currently Eligible", `${s.id} top5 not ineligible`);
    }
  }
}

// --- Legacy OAS unchanged smoke (still callable; formula not modified by this suite) ---
{
  const legacy = scoreOperatorMatchForDeal(
    FIT_V2_SCENARIOS[0].dealFields,
    FIT_V2_SCENARIOS[0].locationData,
    FIT_V2_SCENARIOS[0].mpData,
    FIT_V2_SCENARIOS[0].siData,
    FIT_V2_OPERATORS[0].prefill
  );
  ok(typeof legacy.score === "number", "legacy OAS still returns numeric score");
  ok(legacy.breakdownDetails && legacy.breakdownDetails.feeCommercial, "legacy fee factor still present");
}

console.log(failed ? `\n${failed} failure(s)` : "\nAll Operator Fit v2 tests passed.");
process.exit(failed ? 1 : 0);
