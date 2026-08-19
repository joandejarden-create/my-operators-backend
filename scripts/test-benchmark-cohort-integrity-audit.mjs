#!/usr/bin/env node
/**
 * Cohort integrity audit tests — inspect only, no provider calls, no engine mutation.
 */
import assert from "node:assert/strict";
import {
  runBenchmarkCohortIntegrityAudit,
  IDS,
} from "../lib/ai-visibility/competitive-moat/benchmark-cohort-integrity-audit.js";
import { listShowcaseMonitoringBrandIds, loadShowcaseCompaniesConfig } from "../lib/ai-visibility/brand-ai-showcase-companies.js";
import { PEER_SET_ID_V2, PEER_SET_ID_V5, resolvePeerSetMembership } from "../lib/ai-visibility/peer-sets.js";
import { CUSTOMER_PAYLOAD_ALLOWLIST, INTERNAL_ONLY_FIELDS } from "../lib/ai-visibility/competitive-moat/customer-payload.js";
import { PRIMARY_OPERATOR_COUNT } from "../lib/ai-visibility/operator-intelligence/universe.js";
import { BENCHMARK_AGGREGATION } from "../lib/ai-visibility/competitive-moat/benchmark-engine-v1.js";

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

console.log("\nBenchmark Cohort Integrity Audit V1\n");

const report = runBenchmarkCohortIntegrityAudit({ writeReport: false });

await test("customer-visible remains 19", () => {
  assert.equal(listShowcaseMonitoringBrandIds(undefined, loadShowcaseCompaniesConfig()).length, 19);
  assert.equal(report.customerVisibleBrands, 19);
});

await test("peer v2 frozen; v5 still 22", () => {
  assert.equal(resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V2 }).effectiveCount, 15);
  assert.equal(resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V5 }).effectiveCount, 22);
});

await test("Autograph includes Curio mathematically", () => {
  assert.equal(report.autographDeepDive.curioIncluded, "YES");
  const curio = report.autographDeepDive.includedPeersWithPresence.find((p) => p.peerId === IDS.CURIO);
  assert.ok(curio);
  assert.equal(typeof curio.presenceValueUsed, "number");
});

await test("Curio includes Autograph mathematically", () => {
  assert.equal(report.curioDeepDive.autographIncluded, "YES");
});

await test("Vignette is a core Autograph peer but missing from peer v5", () => {
  const missing = report.autographDeepDive.importantPeersExcluded.find((m) => m.peerId === IDS.VIGNETTE);
  assert.ok(missing);
  assert.equal(missing.classify, "GOVERNANCE_GAP");
});

await test("Unbound Collection not governed", () => {
  const unbound = report.autographDeepDive.namedChecks.find((c) => c.name.startsWith("Unbound"));
  assert.equal(unbound.status, "EXCLUDED");
  assert.match(unbound.why, /IDENTITY_NOT_GOVERNED|NOT_IN_INTERNAL/);
});

await test("union denominator is documented", () => {
  const auto = report.subjectsDetail.find((s) => s.subjectEntityId === IDS.AUTOGRAPH);
  assert.equal(auto.denominatorConstruction, "UNION_OF_SUBJECT_AND_PEER_GRAINS");
});

await test("false benchmark confidence risk flagged", () => {
  assert.equal(report.expansionAudit.sampleSizeImproved, "YES");
  assert.equal(report.expansionAudit.falseBenchmarkConfidenceRisk, "YES");
});

await test("no methodology change — median still production method", () => {
  assert.equal(BENCHMARK_AGGREGATION, "MEDIAN");
  assert.equal(report.methodologyChanged, false);
});

await test("customer allowlist still hides benchmarkMembers", () => {
  assert.ok(!CUSTOMER_PAYLOAD_ALLOWLIST.includes("benchmarkMembers"));
  assert.ok(INTERNAL_ONLY_FIELDS.includes("benchmarkMembers"));
});

await test("all 19 subjects present in founder table", () => {
  assert.equal(report.founderTable.length, 19);
});

await test("Autograph↔Curio commercially YES both ways", () => {
  assert.equal(report.autographCurioTest.autographToCurio.softBrandAffiliation, "YES");
  assert.equal(report.autographCurioTest.curioToAutograph.softBrandAffiliation, "YES");
  assert.equal(report.autographCurioTest.mathematicallyIncludedBothWays, true);
});

await test("no provider calls", () => {
  assert.equal(report.providerCalls, 0);
  assert.equal(report.uiChanges, 0);
});

await test("operator regression", () => {
  assert.equal(PRIMARY_OPERATOR_COUNT, 9);
});

await test("readiness downgraded to internal review", () => {
  assert.equal(report.indexReadiness, "READY_FOR_INTERNAL_REVIEW");
  assert.equal(report.next, "BENCHMARK_COHORT_REMEDIATION");
});

await test("recommended architecture is scenario-specific not static", () => {
  assert.equal(report.architecture.staticCohort, "FAIL");
  assert.match(report.architecture.recommendedArchitecture, /scenario-specific/i);
});

console.log(`\nTOTAL: ${passed + failed}  PASS: ${passed}  FAIL: ${failed}\n`);
process.exit(failed ? 1 : 0);
