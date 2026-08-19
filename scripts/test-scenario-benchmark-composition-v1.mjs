#!/usr/bin/env node
/**
 * Scenario benchmark composition remediation + tab integration contract tests.
 * No provider calls. No UI activation.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "url";
import {
  runScenarioBenchmarkCompositionRemediation,
  RECOMMENDED_POLICY,
  CUSTOMER_INDEX_RENDERING,
  BRANDED_RESIDENCES_BENCHMARK_STATUS,
  CORE_FIRST_GATES_CANDIDATE,
  LIFESTYLE_PEER_REVIEW,
} from "../lib/ai-visibility/competitive-moat/scenario-benchmark-composition.js";
import { classifyScenarioPeerRelation } from "../lib/ai-visibility/competitive-moat/scenario-peer-eligibility.js";
import { IDS, SCENARIO_IDS } from "../lib/ai-visibility/competitive-moat/benchmark-brand-ids.js";
import {
  getTabIntegrationContract,
  buildFutureOwnerIntentBenchmarkRow,
  buildFutureQuestionsMissingRow,
  redactFutureCompetitivePeerPayload,
  auditFutureCustomerPayload,
  CUSTOMER_INDEX_RENDERING as TAB_RENDERING,
  NEW_TAB,
} from "../lib/ai-visibility/competitive-moat/scenario-benchmark-tab-integration.js";
import { CUSTOMER_PAYLOAD_ALLOWLIST } from "../lib/ai-visibility/competitive-moat/customer-payload.js";
import { PRIMARY_OPERATOR_COUNT } from "../lib/ai-visibility/operator-intelligence/universe.js";
import { listShowcaseMonitoringBrandIds, loadShowcaseCompaniesConfig } from "../lib/ai-visibility/brand-ai-showcase-companies.js";
import { HEADLINE_AI_PRESENCE_INDEX_STATUS } from "../lib/ai-visibility/competitive-moat/scenario-benchmark-validation.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

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

console.log("\nBrand AI Scenario Benchmark Composition Remediation V1\n");

const report = runScenarioBenchmarkCompositionRemediation({ writeReport: true });
const contract = getTabIntegrationContract();

await test("CORE-only benchmark candidate", () => {
  assert.equal(RECOMMENDED_POLICY, "CORE_BENCHMARK_PLUS_SECONDARY_CONTEXT");
  assert.equal(report.benchmarkPolicy.SECONDARY_IN_DENOMINATOR, false);
  const indigo = report.lifestyleReview.INDIGO;
  assert.equal(typeof indigo.CORE_ONLY, "number");
});

await test("CORE + SECONDARY comparison", () => {
  const indigo = report.lifestyleReview.INDIGO;
  assert.equal(typeof indigo.MIXED, "number");
  assert.ok(report.policyComparison.CORE_ONLY.FRAGILE <= report.policyComparison.CORE_PLUS_SECONDARY.FRAGILE);
});

await test("secondary context not denominator under candidate policy", () => {
  const indigo = report.rows.find(
    (r) => r.subjectId === IDS.INDIGO && r.scenarioId === SCENARIO_IDS.LIFESTYLE
  );
  assert.ok(indigo);
  assert.equal(indigo.policies.CORE_BENCHMARK_PLUS_SECONDARY_CONTEXT.index, indigo.policies.CORE_ONLY.index);
  assert.ok(Array.isArray(indigo.secondaryContext));
  for (const ctx of indigo.secondaryContext) {
    assert.equal(ctx.role, "ADDITIONAL_OBSERVED_CONTEXT");
  }
});

await test("lifestyle questionable peers", () => {
  assert.equal(LIFESTYLE_PEER_REVIEW.DESIGN_HOTELS, "KEEP_SECONDARY");
  assert.equal(LIFESTYLE_PEER_REVIEW.RADISSON_RED, "KEEP_SECONDARY");
  assert.equal(LIFESTYLE_PEER_REVIEW.PREFERRED, "MOVE_CONDITIONAL");
  assert.equal(LIFESTYLE_PEER_REVIEW.EVEN, "NON_COMPARABLE");
  assert.equal(report.lifestyleReview.DESIGN_HOTELS, "SECONDARY");
  assert.equal(report.lifestyleReview.RADISSON_RED, "SECONDARY");
  assert.equal(report.lifestyleReview.PREFERRED, "CONDITIONAL");
  assert.equal(report.lifestyleReview.EVEN, "NON_COMPARABLE");
  assert.equal(classifyScenarioPeerRelation(IDS.INDIGO, IDS.EVEN, SCENARIO_IDS.LIFESTYLE).commercialRelation, "NON_COMPARABLE");
  assert.equal(classifyScenarioPeerRelation(IDS.INDIGO, IDS.PREFERRED, SCENARIO_IDS.LIFESTYLE).commercialRelation, "CONDITIONAL");
});

await test("soft-brand secondary peers", () => {
  assert.ok(report.softBrandReview.CORE_SET.includes("Curio Collection by Hilton"));
  assert.ok(report.softBrandReview.CORE_SET.includes("Vignette Collection"));
  assert.ok(report.softBrandReview.SECONDARY_CONTEXT.includes("Preferred Hotels & Resorts"));
  assert.ok(report.softBrandReview.CONDITIONAL.includes("Design Hotels"));
});

await test("branded residences suppressed/research", () => {
  assert.equal(BRANDED_RESIDENCES_BENCHMARK_STATUS, "REDESIGN_REQUIRED");
  assert.equal(report.brandedResidences.STATUS, "REDESIGN_REQUIRED");
  for (const r of report.rows.filter((x) => x.scenarioId === SCENARIO_IDS.BRANDED_RESIDENCES)) {
    assert.equal(r.productionClass, "SUPPRESSED");
  }
});

await test("no FRAGILE production classification", () => {
  assert.equal(report.FRAGILE_PRODUCTION, 0);
  for (const r of report.rows) {
    if (r.productionClass === "PRODUCTION_VALIDATED" || r.productionClass === "PRODUCTION_VALIDATED_NARROW") {
      assert.notEqual(r.stabilityCore, "FRAGILE");
    }
  }
});

await test("provider conflict gating", () => {
  for (const r of report.rows) {
    if (r.providerAgreementCore === "PROVIDER_CONFLICT") {
      assert.notEqual(r.productionClass, "PRODUCTION_VALIDATED");
    }
  }
});

await test("Competitive / Peer Analysis future payload redaction", () => {
  const row = buildFutureOwnerIntentBenchmarkRow(
    {
      scenarioId: SCENARIO_IDS.SOFT_BRAND,
      intentLabel: "Soft-brand affiliation",
      subjectPresence: 1,
      indexValue: 129,
      relativeGapPct: 29,
      productionClass: "DETAIL_ONLY",
      selectedCorePeers: ["Curio Collection by Hilton", "Tribute Portfolio", "Vignette Collection", "Secret Fourth"],
      selectedObservedCompetitors: ["Handwritten Collection"],
    },
    { customerIndexRendering: false }
  );
  assert.equal(row.indexValue, null);
  assert.equal(row.selectedCorePeers.length, 3);
  const leaky = redactFutureCompetitivePeerPayload({
    ownerIntentBenchmarks: [{ ...row, benchmarkMembers: ["should-not-survive"], peerPresenceValues: [0.9] }],
    CUSTOMER_INDEX_RENDERING: "OFF",
  });
  assert.equal(leaky.ownerIntentBenchmarks[0].benchmarkMembers, undefined);
  assert.equal(leaky.CUSTOMER_INDEX_RENDERING, "OFF");
});

await test("Questions Missing peer context", () => {
  const qm = buildFutureQuestionsMissingRow({
    scenarioId: SCENARIO_IDS.OWNER_FLEXIBILITY,
    intentLabel: "Owner flexibility / control",
    missingProviderCount: 4,
    comparableProviderCount: 4,
    corePeersPresent: ["Ascend Hotel Collection", "Vignette Collection", "Radisson Individuals by Choice", "Extra"],
    observedCompetitors: ["Handwritten Collection"],
    priority: "PRIORITY",
    competitiveContext: "Peers appear across this owner-decision scenario while Autograph is absent.",
  });
  assert.equal(qm.corePeersPresent.length, 3);
  assert.equal(qm.INDEX_REQUIRED, undefined);
  assert.equal(qm.priority, "PRIORITY");
  assert.doesNotMatch(qm.competitiveContext, /lost|beat|displaced/i);
});

await test("no full peer matrix customer exposure", () => {
  assert.equal(CUSTOMER_PAYLOAD_ALLOWLIST.includes("benchmarkMembers"), false);
  assert.equal(CUSTOMER_PAYLOAD_ALLOWLIST.includes("allCompetitorScores"), false);
  assert.equal(CUSTOMER_PAYLOAD_ALLOWLIST.includes("ownerIntentBenchmarks"), false);
  const audit = auditFutureCustomerPayload({
    ownerIntentBenchmarks: [{ intentLabel: "Soft-brand", selectedCorePeers: ["Curio"] }],
  });
  assert.equal(audit.ok, true);
  const leak = auditFutureCustomerPayload({ benchmarkMembers: ["x"], winCount: 1 });
  assert.equal(leak.ok, false);
});

await test("no new tab", () => {
  assert.equal(NEW_TAB, "NO");
  assert.equal(contract.NEW_MAJOR_SECTION, "NO");
  const html = fs.readFileSync(path.join(ROOT, "public", "ai-visibility-brand.html"), "utf8");
  assert.match(html, /Competitive \/ Peer Analysis/);
  assert.match(html, /Questions Missing Watchlist/);
  assert.doesNotMatch(html, /id="aivDetailThemeBenchmark"/);
  assert.doesNotMatch(html, /AI Presence Index tab/);
});

await test("no UI activation", () => {
  assert.equal(CUSTOMER_INDEX_RENDERING, "OFF");
  assert.equal(TAB_RENDERING, "OFF");
  assert.equal(report.CUSTOMER_INDEX_RENDERING, "OFF");
  assert.equal(report.uiChanges, 0);
  const certifiedHidden = buildFutureOwnerIntentBenchmarkRow(
    {
      productionClass: "PRODUCTION_VALIDATED",
      indexValue: 129,
      relativeGapPct: 29,
    },
    { customerIndexRendering: false }
  );
  assert.equal(certifiedHidden.indexValue, null);
});

await test("no provider calls", () => {
  assert.equal(report.providerCalls, 0);
  assert.equal(report.spend, 0);
});

await test("Brand regression", () => {
  assert.equal(report.regression.BRAND_UI_DIFF, 0);
  assert.equal(listShowcaseMonitoringBrandIds(undefined, loadShowcaseCompaniesConfig()).length, 19);
  assert.equal(HEADLINE_AI_PRESENCE_INDEX_STATUS, "DEFERRED");
  assert.equal(CORE_FIRST_GATES_CANDIDATE.MIN_CORE_PEERS_CUSTOMER, 3);
});

await test("Operator regression", () => {
  assert.equal(PRIMARY_OPERATOR_COUNT, 9);
  assert.equal(report.regression.OPERATOR_DIFF, 0);
});

console.log(`\nTOTAL: ${passed + failed}  PASS: ${passed}  FAIL: ${failed}\n`);
process.exit(failed ? 1 : 0);
