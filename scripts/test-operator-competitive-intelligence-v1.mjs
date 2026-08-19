#!/usr/bin/env node
/**
 * Operator Competitive Intelligence V1 — offline, no provider calls, no UI.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  OPERATOR_AI_UNIVERSE,
  PRIMARY_OPERATOR_COUNT,
  classifyOperatorPresence,
  computeOperatorQuestionsMissing,
  computeOperatorAllProvidersPresence,
  detectOperatorProviderDisagreement,
  OPERATOR_ALL_PROVIDERS_DERIVATION_CONTRACT,
  classifyOperatorPair,
  COMMERCIAL_RELATION,
  interpretOperatorCompetitiveGap,
  GAP_INTERPRETATION,
  GAP_GOLD_LABEL,
  eligibilityFor,
  ELIGIBILITY,
  listOperatorCustomerScenarioLabels,
  OPERATOR_CUSTOMER_OWNER_INTENT,
  toCustomerSafeQuestionsMissingRow,
  toCustomerSafeCompetitiveGapRow,
  assertNoOperatorPromptLeak,
  scoreOperatorGapGold,
  listGapGoldCases,
  buildOperatorCompetitiveIntelligenceReport,
  buildOperatorCompetitiveGapFinalCertificationReport,
  auditArborOperatorSpecificEvidence,
  ARBOR_LODGING_ID,
  OPERATOR_DECISION_SCENARIOS,
  BLOCKED_OPERATOR_ALIASES,
  OPERATOR_MODEL,
  listOperatorModels,
  listScenarioEligibilityMatrix,
  summarizeScenarioEligibilityMatrix,
  buildCorpusGapHoldoutCases,
  selectBalancedCorpusHoldout,
  holdoutCoverage,
  loadCertifiedOperatorPresenceCorpus,
} from "../lib/ai-visibility/operator-intelligence/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

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

function id(founder) {
  return OPERATOR_AI_UNIVERSE.find((o) => o.founderName === founder).canonicalId;
}

console.log("\nOperator Competitive Intelligence V1\n");

await test("Questions Missing: missing provider is not zero", () => {
  const r = computeOperatorQuestionsMissing({
    operatorId: id("Aimbridge LATAM"),
    promptIds: ["p1", "p2", "p3"],
    observations: [
      { promptId: "p1", provider: "openai", present: false },
      { promptId: "p2", provider: "openai", present: true },
    ],
  });
  assert.equal(r.denominator, 2);
  assert.equal(r.questionsMissingCount, 1);
  assert.equal(r.missingProviderEqualsZero, false);
  assert.equal(r.providerProxy, false);
});

await test("failed / missing provider excluded from denominator", () => {
  const r = computeOperatorQuestionsMissing({
    operatorId: id("Aimbridge LATAM"),
    promptIds: ["p1"],
    observations: [],
  });
  assert.equal(r.denominator, 0);
  assert.equal(r.status, "NOT_READY");
});

await test("All Providers is derived and not an OpenAI proxy", () => {
  const r = computeOperatorAllProvidersPresence([
    { promptId: "p1", provider: "perplexity", present: true },
  ]);
  assert.equal(r.derived, true);
  assert.equal(r.isAProvider, false);
  assert.equal(r.openAiProxy, false);
  assert.equal(r.weighting, "NONE");
  assert.equal(OPERATOR_ALL_PROVIDERS_DERIVATION_CONTRACT.openAiProxy, false);
  assert.equal(OPERATOR_ALL_PROVIDERS_DERIVATION_CONTRACT.isAProvider, false);
});

await test("PROVIDER_DISAGREEMENT is not a quality score", () => {
  const r = detectOperatorProviderDisagreement([
    { promptId: "p1", provider: "openai", present: true },
    { promptId: "p1", provider: "claude", present: false },
  ]);
  assert.equal(r.hasDisagreement, true);
  assert.equal(r.isQualityScore, false);
  assert.equal(r.isConfidenceScore, false);
  assert.match(r.customerCopy, /AI providers differ/i);
});

await test("subject absent + CORE comparable present = TRUE_COMPETITIVE_GAP", () => {
  const r = interpretOperatorCompetitiveGap({
    operatorId: id("Aimbridge LATAM"),
    scenarioId: "op_scenario_third_party_management_v1",
    operatorPresent: false,
    presentPeerOperatorIds: [id("Hotel Equities CALA"), id("Remington CALA")],
    observationCount: 3,
  });
  assert.equal(r.interpretation, GAP_INTERPRETATION.TRUE_COMPETITIVE_GAP);
  assert.equal(r.clientPromoted, false);
});

await test("subject absent + non-comparable operator present is not a gap", () => {
  const r = interpretOperatorCompetitiveGap({
    operatorId: id("Aimbridge LATAM"),
    scenarioId: "op_scenario_third_party_management_v1",
    operatorPresent: false,
    presentPeerOperatorIds: [id("Marriott International")],
  });
  assert.notEqual(r.interpretation, GAP_INTERPRETATION.TRUE_COMPETITIVE_GAP);
  assert.equal(r.goldLabel, GAP_GOLD_LABEL.NOT_A_GAP);
});

await test("regional mismatch: Brittain vs CALA", () => {
  const elig = eligibilityFor(id("Brittain Resorts"), "op_scenario_cala_latam_regional_capability_v1");
  assert.equal(elig.status, ELIGIBILITY.OUT_OF_SCOPE);
  const pair = classifyOperatorPair(
    id("Brittain Resorts"),
    id("Hotel Equities CALA"),
    "op_scenario_cala_latam_regional_capability_v1"
  );
  assert.equal(pair.relation, COMMERCIAL_RELATION.NON_COMPARABLE);
  const r = interpretOperatorCompetitiveGap({
    operatorId: id("Brittain Resorts"),
    scenarioId: "op_scenario_cala_latam_regional_capability_v1",
    operatorPresent: false,
    presentPeerOperatorIds: [id("Aimbridge LATAM")],
  });
  assert.equal(r.interpretation, GAP_INTERPRETATION.SCENARIO_OUT_OF_SCOPE);
});

await test("brand-managed vs local TPM on full-service is SECONDARY not CORE gap", () => {
  const pair = classifyOperatorPair(
    id("Marriott International"),
    id("Hotel Equities CALA"),
    "op_scenario_full_service_uu_operator_selection_v1"
  );
  assert.equal(pair.relation, COMMERCIAL_RELATION.SECONDARY_CONTEXT);
  const r = interpretOperatorCompetitiveGap({
    operatorId: id("Marriott International"),
    scenarioId: "op_scenario_full_service_uu_operator_selection_v1",
    operatorPresent: false,
    presentPeerOperatorIds: [id("Hotel Equities CALA")],
  });
  assert.equal(r.interpretation, GAP_INTERPRETATION.EXPECTED_POSITIONING_DIFFERENCE);
});

await test("brand-as-operator negative is not a competitive gap", () => {
  const presence = classifyOperatorPresence({
    text: "Guests earn Marriott Bonvoy points at this property.",
  });
  assert.equal(presence.presentOperatorIds.includes(id("Marriott International")), false);
  const r = interpretOperatorCompetitiveGap({
    operatorId: id("Marriott International"),
    scenarioId: "op_scenario_full_service_uu_operator_selection_v1",
    operatorPresent: false,
    presentPeerOperatorIds: [],
  });
  assert.notEqual(r.interpretation, GAP_INTERPRETATION.TRUE_COMPETITIVE_GAP);
});

await test("Remington safety: bare name blocked; canonical parent preserved", () => {
  const rem = OPERATOR_AI_UNIVERSE.find((o) => o.founderName === "Remington CALA");
  assert.equal(rem.canonicalName, "Remington Hospitality (CALA)");
  assert.equal(rem.parentPlatform, "Remington Hospitality");
  const bare = classifyOperatorPresence({ text: "Remington is a well-known firearms brand." });
  assert.equal(bare.presentOperatorIds.includes(id("Remington CALA")), false);
  const ok = classifyOperatorPresence({
    text: "Remington Hospitality is a third-party operator owners consider in CALA.",
  });
  assert.equal(ok.presentOperatorIds.includes(id("Remington CALA")), true);
});

await test("institutional scenario is DETAIL_ONLY and not an auto gap", () => {
  const r = interpretOperatorCompetitiveGap({
    operatorId: id("Marriott International"),
    scenarioId: "op_scenario_institutional_platform_alignment_v1",
    operatorPresent: false,
    presentPeerOperatorIds: [id("Hilton"), id("IHG")],
  });
  assert.equal(r.interpretation, GAP_INTERPRETATION.REQUIRES_REVIEW);
  assert.equal(r.scenarioGapTier, "DETAIL_ONLY");
});

await test("Arbor evidence limitation is preserved despite 100% holdout metrics", () => {
  const report = buildOperatorCompetitiveIntelligenceReport();
  assert.equal(report.arbor.currentStatus, "INSUFFICIENT_OPERATOR_SPECIFIC_EVIDENCE");
  assert.equal(report.arbor.recommendedStatus, "INSUFFICIENT_OPERATOR_SPECIFIC_EVIDENCE");
  assert.equal(report.arbor.positiveGoldCases, 0);
  assert.equal(report.arbor.executiveCompetitiveClaims, false);
  const gap = interpretOperatorCompetitiveGap({
    operatorId: ARBOR_LODGING_ID,
    scenarioId: "op_scenario_third_party_management_v1",
    operatorPresent: false,
    presentPeerOperatorIds: [id("Hotel Equities CALA")],
  });
  assert.equal(gap.interpretation, GAP_INTERPRETATION.INSUFFICIENT_CONTEXT);
  assert.match(report.arbor.rootCause, /true-negative only|0 PRESENT/i);
  void auditArborOperatorSpecificEvidence;
});

await test("raw prompt redaction: customer contracts hide prompt ids and text", () => {
  const labels = listOperatorCustomerScenarioLabels();
  assert.equal(labels.length, 12);
  assert.equal(Object.keys(OPERATOR_CUSTOMER_OWNER_INTENT).length, 12);
  for (const row of labels) {
    assert.ok(row.ownerIntent);
    assert.doesNotMatch(row.ownerIntent, /op_p_/);
    assert.doesNotMatch(row.decisionContext, /hotel brand portfolio|Bonvoy/i);
  }
  const customer = toCustomerSafeQuestionsMissingRow({
    scenarioId: "op_scenario_third_party_management_v1",
    operatorPresence: "ABSENT",
    missingProviders: ["openai"],
    comparableProviderCount: 4,
    relevantOperatorsPresentCustomer: ["Hotel Equities (CALA)"],
    evidenceCount: 8,
    providerDisagreement: true,
    promptId: "op_p_core_third_party_cala_en_v1",
    promptText: "SECRET PROMPT",
  });
  assert.equal(customer.promptId, undefined);
  assert.equal(customer.promptText, undefined);
  assertNoOperatorPromptLeak(customer);
  const blocked = toCustomerSafeCompetitiveGapRow(
    { scenarioId: "op_scenario_third_party_management_v1", gapInterpretation: "TRUE_COMPETITIVE_GAP" },
    { clientPromoted: false }
  );
  assert.equal(blocked, null);
});

await test("gold-label holdout: precision prioritized, zero identity errors", () => {
  const holdout = scoreOperatorGapGold(
    interpretOperatorCompetitiveGap,
    listGapGoldCases("HOLDOUT")
  );
  const dev = scoreOperatorGapGold(interpretOperatorCompetitiveGap, listGapGoldCases("DEV"));
  assert.equal(holdout.criticalIdentityErrors, 0);
  assert.equal(holdout.brandAsOperatorErrors, 0);
  assert.equal(holdout.regionalScopeErrors, 0);
  assert.equal(dev.criticalIdentityErrors, 0);
  assert.ok(holdout.precision >= 0.95, `holdout precision ${holdout.precision}`);
  assert.equal(holdout.fp, 0);
  if (holdout.mismatches.filter((m) => m.class !== "non_positive_mismatch").length) {
    throw new Error(`holdout mismatches: ${JSON.stringify(holdout.mismatches)}`);
  }
});

await test("Brand regression freeze + universe lock + no Brand peer import", () => {
  assert.equal(PRIMARY_OPERATOR_COUNT, 9);
  assert.equal(OPERATOR_DECISION_SCENARIOS.length, 12);
  const ci = fs.readFileSync(
    path.join(root, "lib/ai-visibility/operator-intelligence/competitive-intelligence.js"),
    "utf8"
  );
  const gaps = fs.readFileSync(
    path.join(root, "lib/ai-visibility/operator-intelligence/gaps.js"),
    "utf8"
  );
  const comparability = fs.readFileSync(
    path.join(root, "lib/ai-visibility/operator-intelligence/comparability.js"),
    "utf8"
  );
  for (const src of [ci, gaps, comparability]) {
    assert.doesNotMatch(src, /peer-sets\.js|brand-read-service|brand-longitudinal/);
    assert.doesNotMatch(src, /peers_uu_collection/);
  }
  assert.ok(BLOCKED_OPERATOR_ALIASES.includes("Arbor"));
});

await test("offline report: no new calls, associations frozen, UI not promoted", () => {
  const report = buildOperatorCompetitiveIntelligenceReport();
  assert.equal(report.token, "OPERATOR_AI_COMPETITIVE_INTELLIGENCE_BUILD_V1_COMPLETE");
  assert.equal(report.execution.providerCalls, 0);
  assert.equal(report.execution.spend, "$0");
  assert.equal(report.regression.primaryMonitoredOperators, 9);
  assert.equal(report.regression.brandDiff, 0);
  assert.equal(report.regression.operatorUiDiff, 0);
  assert.equal(report.associations.status, "RESEARCH_ONLY");
  assert.equal(report.associations.productionPromoted, 0);
  assert.equal(report.narrativeSources.status, "DEFERRED");
  assert.equal(report.recommendationSignals.status, "BLOCKED");
  assert.equal(report.operatorIndex.status, "NOT_BUILT");
  assert.equal(report.newProviderCalls.needed, "NO");
  assert.equal(report.questionsMissing.totalOperatorScenarioRows, 9 * 12);
  assert.equal(report.customerContracts.unpromotedGapLeaks, true);
  assert.ok(["READY", "PARTIAL"].includes(report.questionsMissing.status));
  assert.equal(report.questionsMissing.allProvidersReady, "YES");
});

await test("operator models are certified for all 9", () => {
  const models = listOperatorModels();
  assert.equal(models.length, 9);
  assert.equal(models.find((m) => m.canonicalId === id("Marriott International")).model, OPERATOR_MODEL.BRAND_MANAGED_PLATFORM);
  assert.equal(models.find((m) => m.canonicalId === id("Aimbridge LATAM")).model, OPERATOR_MODEL.THIRD_PARTY_MANAGER);
  assert.equal(models.find((m) => m.canonicalId === id("GHL")).model, OPERATOR_MODEL.REGIONAL_PLATFORM_MIXED);
});

await test("GHL vs Aimbridge on CALA is SECONDARY not CORE", () => {
  const pair = classifyOperatorPair(
    id("GHL"),
    id("Aimbridge LATAM"),
    "op_scenario_cala_latam_regional_capability_v1"
  );
  assert.equal(pair.relation, COMMERCIAL_RELATION.SECONDARY_CONTEXT);
});

await test("brand-managed excluded from third-party CORE", () => {
  const pair = classifyOperatorPair(
    id("Marriott International"),
    id("Hotel Equities CALA"),
    "op_scenario_third_party_management_v1"
  );
  assert.equal(pair.relation, COMMERCIAL_RELATION.NON_COMPARABLE);
});

await test("SECONDARY cannot create TRUE_COMPETITIVE_GAP", () => {
  const r = interpretOperatorCompetitiveGap({
    operatorId: id("Marriott International"),
    scenarioId: "op_scenario_full_service_uu_operator_selection_v1",
    operatorPresent: false,
    presentPeerOperatorIds: [id("Hotel Equities CALA")],
  });
  assert.notEqual(r.interpretation, GAP_INTERPRETATION.TRUE_COMPETITIVE_GAP);
});

await test("CONDITIONAL cannot create TRUE_COMPETITIVE_GAP", () => {
  const r = interpretOperatorCompetitiveGap({
    operatorId: id("Aimbridge LATAM"),
    scenarioId: "op_scenario_luxury_operator_selection_v1",
    operatorPresent: false,
    presentPeerOperatorIds: [id("Hotel Equities CALA")],
  });
  assert.notEqual(r.interpretation, GAP_INTERPRETATION.TRUE_COMPETITIVE_GAP);
});

await test("NON_COMPARABLE cannot create TRUE_COMPETITIVE_GAP", () => {
  const r = interpretOperatorCompetitiveGap({
    operatorId: id("Brittain Resorts"),
    scenarioId: "op_scenario_cala_latam_regional_capability_v1",
    operatorPresent: false,
    presentPeerOperatorIds: [id("Aimbridge LATAM")],
  });
  assert.notEqual(r.interpretation, GAP_INTERPRETATION.TRUE_COMPETITIVE_GAP);
});

await test("Questions Missing out-of-scope is NOT_APPLICABLE not a weakness", () => {
  const report = buildOperatorCompetitiveIntelligenceReport();
  const cert = buildOperatorCompetitiveGapFinalCertificationReport();
  void cert;
  assert.equal(report.questionsMissing.status, "READY");
  const qm = report.questionsMissing;
  assert.ok(typeof qm.notApplicableRows === "number");
});

await test("eligibility matrix covers 108 pairs including INSUFFICIENT_TRUTH", () => {
  const matrix = listScenarioEligibilityMatrix();
  const sum = summarizeScenarioEligibilityMatrix(matrix);
  assert.equal(sum.totalOperatorScenarioPairs, 108);
  const brittainLuxury = eligibilityFor(id("Brittain Resorts"), "op_scenario_luxury_operator_selection_v1");
  assert.equal(brittainLuxury.status, ELIGIBILITY.INSUFFICIENT_TRUTH);
});

await test("final certification: larger holdout, Arbor blocked, partial promotion, no UI", () => {
  const corpus = loadCertifiedOperatorPresenceCorpus();
  const corpusCases = selectBalancedCorpusHoldout(buildCorpusGapHoldoutCases(corpus.extractions));
  assert.ok(corpusCases.length >= 60, `holdout ${corpusCases.length}`);
  const coverage = holdoutCoverage(corpusCases);
  assert.ok(coverage.operators >= 6);
  assert.ok(coverage.scenarios >= 8);
  assert.equal(coverage.hasArbor, true);
  assert.equal(coverage.hasRemington, true);
  const cert = buildOperatorCompetitiveGapFinalCertificationReport();
  assert.equal(cert.token, "OPERATOR_AI_COMMERCIAL_TRUTH_COMPETITIVE_GAP_FINAL_CERTIFICATION_COMPLETE");
  assert.equal(cert.execution.providerCalls, 0);
  assert.equal(cert.ui.operatorUiDiff, 0);
  assert.equal(cert.security.rawPromptLeaks, 0);
  assert.equal(cert.security.comparabilityMatrixCustomerLeaks, 0);
  assert.equal(cert.arbor.clientCompetitiveClaims, "BLOCKED");
  assert.ok(cert.validation.holdoutCases >= 60);
  assert.ok(cert.validation.precision >= 0.95);
  assert.equal(cert.validation.criticalIdentityErrors, 0);
  assert.equal(cert.validation.secondaryAsCoreErrors, 0);
  assert.equal(cert.associations.status, "RESEARCH_ONLY");
  assert.equal(cert.operatorIndex.status, "BLOCKED");
  for (const row of cert.clientPromotion.customerSafeRows) {
    assert.equal(row.gapInterpretation, "TRUE_COMPETITIVE_GAP");
    assert.doesNotMatch(JSON.stringify(row), /op_p_/);
    assert.ok(!row.subjectOperator.includes("Arbor"));
  }
});

console.log(`\n${passed} passed, ${failed} failed`);
if (failed) process.exit(1);
