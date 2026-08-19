#!/usr/bin/env node
/**
 * Benchmark cohort remediation V1 tests — no provider calls.
 */
import assert from "node:assert/strict";
import {
  runBenchmarkCohortRemediation,
  HEADLINE_INDEX_AGGREGATION,
  CUSTOMER_INDEX_STATUS,
  BENCHMARK_AGGREGATION_STATUS,
} from "../lib/ai-visibility/competitive-moat/benchmark-cohort-remediation.js";
import { IDS, SCENARIO_IDS } from "../lib/ai-visibility/competitive-moat/benchmark-brand-ids.js";
import {
  isBenchmarkEligible,
  auditCustomerVisibleBenchmarkEligibility,
  listBenchmarkEligibleMembers,
} from "../lib/ai-visibility/competitive-moat/benchmark-eligible-universe.js";
import {
  classifyScenarioPeerRelation,
  resolveScenarioCommercialPeers,
  auditRelationSymmetry,
  NO_FULL_SET_FALLBACK,
} from "../lib/ai-visibility/competitive-moat/scenario-peer-eligibility.js";
import {
  UNION_GRAIN_BENCHMARK,
  COMMON_GRAIN_METHOD,
  computePairwiseScenarioPresence,
} from "../lib/ai-visibility/competitive-moat/intersection-grains.js";
import { classifyBenchmarkCohortValidityV2 } from "../lib/ai-visibility/competitive-moat/benchmark-cohort-validity-v2.js";
import { resolveContextualPeerIds, NO_FULL_SET_FALLBACK as COHORT_NO_FALLBACK } from "../lib/ai-visibility/competitive-moat/contextual-cohort-v1.js";
import { HEADLINE_INDEX_AGGREGATION as ENGINE_HEADLINE } from "../lib/ai-visibility/competitive-moat/benchmark-engine-v1.js";
import { listShowcaseMonitoringBrandIds, loadShowcaseCompaniesConfig } from "../lib/ai-visibility/brand-ai-showcase-companies.js";
import { PEER_SET_ID_V2, PEER_SET_ID_V5, resolvePeerSetMembership } from "../lib/ai-visibility/peer-sets.js";
import { PRIMARY_OPERATOR_COUNT } from "../lib/ai-visibility/operator-intelligence/universe.js";
import { CUSTOMER_PAYLOAD_ALLOWLIST } from "../lib/ai-visibility/competitive-moat/customer-payload.js";

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

console.log("\nBrand AI Benchmark Cohort Remediation V1\n");

const report = runBenchmarkCohortRemediation({ writeReport: true });

await test("customer-visible remains 19", () => {
  assert.equal(listShowcaseMonitoringBrandIds(undefined, loadShowcaseCompaniesConfig()).length, 19);
  assert.equal(report.customerVisibleBrands, 19);
});

await test("peer v2/v5 not overwritten", () => {
  assert.equal(resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V2 }).effectiveCount, 15);
  assert.equal(resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V5 }).effectiveCount, 22);
});

await test("Vignette benchmark eligibility", () => {
  assert.equal(isBenchmarkEligible(IDS.VIGNETTE), true);
  assert.equal(report.vignetteBenchmarkEligible, true);
  assert.equal(report.autograph.SOFT_BRAND_AFFILIATION.VIGNETTE_INCLUDED, "YES");
});

await test("Voco benchmark eligibility", () => {
  assert.equal(isBenchmarkEligible(IDS.VOCO), true);
  assert.equal(report.vocoBenchmarkEligible, true);
  assert.equal(report.hotelIndigo.LIFESTYLE.VOCO, "CORE");
});

await test("Radisson benchmark eligibility", () => {
  assert.equal(isBenchmarkEligible(IDS.RADISSON), true);
  assert.equal(isBenchmarkEligible(IDS.RAD_BLU), true);
  assert.equal(report.radissonBenchmarkEligible, true);
});

await test("all 19 visible brands are benchmark eligible", () => {
  const audit = auditCustomerVisibleBenchmarkEligibility();
  assert.equal(audit.ok, true, audit.missingFromEligible.join(","));
});

await test("no full-set fallback", () => {
  assert.equal(NO_FULL_SET_FALLBACK, true);
  assert.equal(COHORT_NO_FALLBACK, true);
  assert.equal(report.FULL_SET_FALLBACK_REMOVED, "YES");
  const westin = resolveContextualPeerIds(IDS.WESTIN);
  assert.equal(westin.usedBroaderFallback, false);
  assert.ok(westin.peerIds.length < 21, `westin fallback size ${westin.peerIds.length}`);
});

await test("Westin does not fall back to all peers", () => {
  assert.equal(report.westin.FULL_SET_FALLBACK, "NO");
  const eligible = listBenchmarkEligibleMembers().length;
  assert.ok(report.westin.peerCount < eligible - 5, `westin peers ${report.westin.peerCount}`);
  const peers = resolveScenarioCommercialPeers(IDS.WESTIN, SCENARIO_IDS.CHAIN_SCALE);
  assert.ok(!peers.core.some((p) => p.peerBrandId === IDS.AUTOGRAPH));
  assert.ok(peers.core.some((p) => p.peerBrandId === IDS.RAD_BLU));
});

await test("Autograph ↔ Curio symmetry", () => {
  const ab = classifyScenarioPeerRelation(IDS.AUTOGRAPH, IDS.CURIO, SCENARIO_IDS.SOFT_BRAND);
  const ba = classifyScenarioPeerRelation(IDS.CURIO, IDS.AUTOGRAPH, SCENARIO_IDS.SOFT_BRAND);
  assert.equal(ab.commercialRelation, "CORE");
  assert.equal(ba.commercialRelation, "CORE");
  assert.equal(report.autograph.SOFT_BRAND_AFFILIATION.CURIO_INCLUDED, "YES");
  assert.equal(report.curio.SOFT_BRAND_AFFILIATION.AUTOGRAPH_INCLUDED, "YES");
});

await test("Autograph ↔ Vignette commercially eligible", () => {
  const ab = classifyScenarioPeerRelation(IDS.AUTOGRAPH, IDS.VIGNETTE, SCENARIO_IDS.SOFT_BRAND);
  assert.equal(ab.commercialRelation, "CORE");
});

await test("Indigo ↔ Voco lifestyle eligible", () => {
  const ab = classifyScenarioPeerRelation(IDS.INDIGO, IDS.VOCO, SCENARIO_IDS.LIFESTYLE);
  assert.equal(ab.commercialRelation, "CORE");
});

await test("union denominator prohibited", () => {
  assert.equal(UNION_GRAIN_BENCHMARK, "PROHIBITED");
  assert.equal(report.UNION_GRAIN_REMOVED, "YES");
  assert.equal(COMMON_GRAIN_METHOD, "PAIRWISE");
});

await test("intersection/common-grain logic", () => {
  const fake = {
    grainsByScenario: new Map([[SCENARIO_IDS.SOFT_BRAND, new Set(["g1", "g2", "g3", "g4"])]]),
    presentByBrandScenario: new Map([
      [IDS.AUTOGRAPH, new Map([[SCENARIO_IDS.SOFT_BRAND, new Set(["g1", "g2"])]])],
      [IDS.CURIO, new Map([[SCENARIO_IDS.SOFT_BRAND, new Set(["g2", "g4"])]])],
    ]),
  };
  const pair = computePairwiseScenarioPresence(IDS.AUTOGRAPH, IDS.CURIO, SCENARIO_IDS.SOFT_BRAND, fake);
  assert.equal(pair.commonGrains, 4);
  assert.equal(pair.subjectPresenceCommon, 0.5);
  assert.equal(pair.peerPresenceCommon, 0.5);
  assert.equal(pair.unionGrainUsed, false);
});

await test("commercial peer but no data = MEASUREMENT_COVERAGE_GAP", () => {
  const v = classifyBenchmarkCohortValidityV2({
    totalValidPeers: 0,
    corePeers: 4,
    coreWithData: 0,
    commonGrains: 0,
    scenarioHasPrompts: false,
  });
  assert.equal(v.status, "SUPPRESSED_INSUFFICIENT_DATA");
  const gapRow = report.measurementCoverageGaps.gaps[0];
  if (gapRow) {
    assert.equal(gapRow.classify, "MEASUREMENT_COVERAGE_GAP");
  }
});

await test("non-comparable remains distinct", () => {
  const rel = classifyScenarioPeerRelation(IDS.AUTOGRAPH, IDS.WESTIN, SCENARIO_IDS.SOFT_BRAND);
  assert.equal(rel.commercialRelation, "NON_COMPARABLE");
  const life = classifyScenarioPeerRelation(IDS.INDIGO, IDS.AUTOGRAPH, SCENARIO_IDS.LIFESTYLE);
  assert.equal(life.commercialRelation, "NON_COMPARABLE");
});

await test("scenario-specific cohort", () => {
  const autoSoft = resolveScenarioCommercialPeers(IDS.AUTOGRAPH, SCENARIO_IDS.SOFT_BRAND);
  const autoLife = resolveScenarioCommercialPeers(IDS.AUTOGRAPH, SCENARIO_IDS.LIFESTYLE);
  assert.ok(autoSoft.core.length >= 4);
  assert.equal(autoLife.core.length, 0);
  assert.equal(report.SCENARIO_SPECIFIC, "PASS");
});

await test("no headline aggregate", () => {
  assert.equal(HEADLINE_INDEX_AGGREGATION, "DEFERRED");
  assert.equal(ENGINE_HEADLINE, "DEFERRED");
  assert.equal(report.headlineIndex.STATUS, "DEFERRED");
});

await test("aggregation method deferred", () => {
  assert.equal(BENCHMARK_AGGREGATION_STATUS, "DEFERRED_UNTIL_COHORT_CERTIFIED");
  assert.equal(report.aggregationMethod.MEDIAN_VS_MEAN, "DEFERRED");
});

await test("no customer promotion", () => {
  assert.equal(CUSTOMER_INDEX_STATUS, "INTERNAL_REVIEW_ONLY");
  assert.equal(report.customer.CUSTOMER_INDEX_STATUS, "INTERNAL_REVIEW_ONLY");
  assert.equal(report.customer.UI_CHANGES, 0);
  assert.equal(CUSTOMER_PAYLOAD_ALLOWLIST.includes("benchmarkMembers"), false);
});

await test("Brand regression except cohort engine", () => {
  assert.equal(report.regression.BRAND_UI_DIFF, 0);
  assert.equal(report.regression.BRAND_PRESENCE_CLASSIFIER_DIFF, 0);
  assert.equal(report.uiChanges, 0);
});

await test("Operator regression", () => {
  assert.equal(PRIMARY_OPERATOR_COUNT, 9);
  assert.equal(report.regression.OPERATOR_DIFF, 0);
});

await test("symmetry unjustified is zero", () => {
  const sym = auditRelationSymmetry();
  assert.equal(sym.ASYMMETRIC_UNJUSTIFIED, 0, JSON.stringify(sym.unjustifiedPairs.slice(0, 3)));
  assert.equal(report.symmetry.ASYMMETRIC_UNJUSTIFIED, 0);
});

await test("Radisson family not assumed identical", () => {
  const bluRed = classifyScenarioPeerRelation(IDS.RAD_BLU, IDS.RAD_RED, SCENARIO_IDS.CHAIN_SCALE);
  const bluRad = classifyScenarioPeerRelation(IDS.RAD_BLU, IDS.RADISSON, SCENARIO_IDS.CHAIN_SCALE);
  assert.equal(bluRad.commercialRelation, "CORE");
  assert.equal(bluRed.commercialRelation, "NON_COMPARABLE");
  const indBlu = classifyScenarioPeerRelation(IDS.RAD_IND, IDS.RAD_BLU, SCENARIO_IDS.SOFT_BRAND);
  assert.equal(indBlu.commercialRelation, "NON_COMPARABLE");
});

await test("no provider calls", () => {
  assert.equal(report.providerCalls, 0);
  assert.equal(report.futureCalls.EXECUTED, "NO");
});

await test("seven internal additions preserved in universe", () => {
  const internals = listBenchmarkEligibleMembers().filter((m) => m.internalBenchmarkOnly && m.brandId !== IDS.MGALLERY);
  assert.equal(internals.length, 7);
});

console.log(`\nTOTAL: ${passed + failed}  PASS: ${passed}  FAIL: ${failed}\n`);
process.exit(failed ? 1 : 0);
