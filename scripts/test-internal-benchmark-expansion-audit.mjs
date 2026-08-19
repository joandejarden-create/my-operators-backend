#!/usr/bin/env node
/**
 * Internal Benchmark Expansion Audit tests — no provider calls.
 */
import assert from "node:assert/strict";
import {
  runInternalBenchmarkExpansionAudit,
  CANDIDATE_DEFINITIONS,
} from "../lib/ai-visibility/competitive-moat/internal-benchmark-expansion-audit.js";
import {
  classifyBenchmarkSampleSize,
  computeAiPresenceIndex,
} from "../lib/ai-visibility/competitive-moat/benchmark-engine-v1.js";
import { listShowcaseMonitoringBrandIds, loadShowcaseCompaniesConfig } from "../lib/ai-visibility/brand-ai-showcase-companies.js";
import { auditCustomerPayloadForBlockedSignals } from "../lib/ai-visibility/competitive-moat/blocked-signals.js";
import { redactToCustomerAllowlist, buildCustomerBenchmarkPayload } from "../lib/ai-visibility/competitive-moat/customer-payload.js";
import { PRIMARY_OPERATOR_COUNT } from "../lib/ai-visibility/operator-intelligence/universe.js";

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

console.log("\nInternal Benchmark Expansion Audit V1\n");

await test("customer-visible 19 brands unchanged", () => {
  assert.equal(listShowcaseMonitoringBrandIds(undefined, loadShowcaseCompaniesConfig()).length, 19);
  const r = runInternalBenchmarkExpansionAudit();
  assert.equal(r.customerVisibleBrands, 19);
});

await test("internal candidate universe separate from customer visible", () => {
  const r = runInternalBenchmarkExpansionAudit();
  const visible = new Set(listShowcaseMonitoringBrandIds(undefined, loadShowcaseCompaniesConfig()));
  for (const c of r.candidates) {
    if (c.canonicalId) assert.equal(visible.has(c.canonicalId), c.inCustomerVisible);
  }
});

await test("no candidate auto-added to config", () => {
  const r = runInternalBenchmarkExpansionAudit();
  assert.ok(r.minimumRecommendedSet.count >= 0);
  assert.equal(r.regression.BENCHMARK_ENGINE_DIFF, 0);
});

await test("candidate identity safety gate exists", () => {
  const r = runInternalBenchmarkExpansionAudit();
  for (const c of r.addNow) {
    const full = r.candidates.find((x) => x.brand === c.brand);
    assert.equal(full.identitySafe, "YES");
  }
});

await test("benchmark cohort size calculation LIMITED -> VALID", () => {
  assert.equal(classifyBenchmarkSampleSize(4), "LIMITED_BENCHMARK");
  assert.equal(classifyBenchmarkSampleSize(5), "VALID_BENCHMARK");
});

await test("leave-one-out sensitivity structure", () => {
  const r = runInternalBenchmarkExpansionAudit();
  assert.ok(r.stability.length >= 3);
  for (const s of r.stability) {
    assert.ok(["STABLE", "MODERATELY_SENSITIVE", "FRAGILE"].includes(s.leaveOneOutState));
  }
});

await test("no provider calls", () => {
  const r = runInternalBenchmarkExpansionAudit();
  assert.equal(r.providerCalls, 0);
  assert.equal(r.spend, 0);
  for (const c of r.candidates) {
    assert.equal(c.incrementalProviderCalls, 0);
  }
});

await test("no Recommendation metrics in audit output", () => {
  const r = runInternalBenchmarkExpansionAudit();
  const audit = auditCustomerPayloadForBlockedSignals(r);
  assert.equal(audit.ok, true);
});

await test("customer payload still redacted", () => {
  const p = buildCustomerBenchmarkPayload({
    subjectEntityId: "rec1",
    indexResult: computeAiPresenceIndex(0.7, 0.5),
    benchmarkMembers: ["secret"],
  });
  const redacted = redactToCustomerAllowlist(p);
  assert.equal(redacted.benchmarkMembers, undefined);
});

await test("Brand regression unchanged", () => {
  const r = runInternalBenchmarkExpansionAudit();
  assert.equal(r.regression.BRAND_LOGIC_DIFF, 0);
  assert.equal(r.regression.BRAND_UI_DIFF, 0);
});

await test("Operator regression unchanged", () => {
  assert.equal(PRIMARY_OPERATOR_COUNT, 9);
  const r = runInternalBenchmarkExpansionAudit();
  assert.equal(r.regression.OPERATOR_DIFF, 0);
});

await test("minimum 10 candidates defined", () => {
  assert.ok(CANDIDATE_DEFINITIONS.length >= 10);
  const r = runInternalBenchmarkExpansionAudit();
  assert.ok(r.candidatesAudited >= 10);
});

await test("founder approval required", () => {
  const r = runInternalBenchmarkExpansionAudit();
  assert.equal(r.founderGate.status, "FOUNDER_APPROVAL_REQUIRED");
});

console.log(`\nTOTAL: ${passed + failed}  PASS: ${passed}  FAIL: ${failed}\n`);
process.exit(failed > 0 ? 1 : 0);
