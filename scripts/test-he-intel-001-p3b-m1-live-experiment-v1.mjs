#!/usr/bin/env node
/**
 * No-spend regressions for P3B-M1 live ownership experiment readiness.
 */
import assert from "node:assert/strict";
import {
  P3B_M1_HARD_CAP_USD,
  P3B_M1_HPC_IDS,
  RESEARCH_METHODS,
  buildLiveExperimentManifest,
  buildExactPaidExecutionCommand,
  runLiveExperimentReadinessValidation,
  scoreOwnershipQuality,
  adjudicateFinding,
  toDiscoveryCandidate,
} from "../lib/hotel-intelligence/research-engine-v1/benchmark-p3b-m1/index.js";
import { OWNERSHIP_SUCCESS_TIER } from "../lib/hotel-intelligence/research-engine-v1/ownership-research-objective.js";

let passed = 0;
function pass(name) {
  console.log(`PASS ${name}`);
  passed += 1;
}
function test(name, fn) {
  fn();
  pass(name);
}
async function testAsync(name, fn) {
  await fn();
  pass(name);
}

test("three_hotel_cohort_exact", () => {
  const m = buildLiveExperimentManifest();
  assert.equal(m.ok, true);
  assert.deepEqual(m.cohort.hpcIds, [...P3B_M1_HPC_IDS]);
  assert.equal(m.cohort.hotels.length, 3);
  assert.ok(m.cohort.hotels[0].name.includes("Bethesda"));
  assert.ok(m.cohort.hotels[1].name.includes("Wyndham"));
  assert.ok(m.cohort.hotels[2].name.includes("Ritz-Carlton"));
});

test("paid_not_authorized_and_caps", () => {
  const m = buildLiveExperimentManifest();
  assert.equal(m.paid_execution_authorized, false);
  assert.equal(m.proposed_budget_not_authorized, true);
  assert.equal(m.financial_controls.proposed_hard_cap_usd, P3B_M1_HARD_CAP_USD);
  assert.equal(m.financial_controls.proposed_per_hotel_cap_usd, 2.5);
  assert.equal(m.financial_controls.expected_cost_is_not_max_guarantee, true);
  assert.equal(
    m.financial_controls.authorization.this_prompt_is_not_spend_authorization,
    true
  );
});

test("native_and_hybrid_not_executed_as_paid_by_default", () => {
  const m = buildLiveExperimentManifest();
  assert.equal(
    m.methods[RESEARCH_METHODS.D_DEALALITY_NATIVE].executed_when_authorized,
    false
  );
  assert.equal(
    m.methods[RESEARCH_METHODS.D_DEALALITY_NATIVE].status_if_not_run,
    "NOT_EXECUTED_COST_UNKNOWN"
  );
  assert.equal(
    m.methods[RESEARCH_METHODS.E_HYBRID_WATERFALL].default_status,
    "NOT_EXECUTED_LABELED_SIMULATION_ONLY"
  );
});

test("mode_a_priority_and_isolation", () => {
  const m = buildLiveExperimentManifest();
  assert.equal(m.comparison_protocol.primary_mode, "INDEPENDENT_DISCOVERY");
  assert.equal(m.comparison_protocol.mode_b.do_not_sacrifice_mode_a, true);
  assert.equal(m.comparison_protocol.arm_isolation, true);
  assert.equal(m.hotel_research_instructions[0].seed_mode_a.may_not_use_other_arms_discoveries, true);
});

test("scorecard_weights_sum_to_one", () => {
  const m = buildLiveExperimentManifest();
  const sum = Object.values(m.quality_scorecard.weights).reduce((a, b) => a + b, 0);
  assert.ok(Math.abs(sum - 1) < 1e-9);
  assert.equal(m.quality_scorecard.numerical_pass_is_not_full_actionable, true);
});

test("url_alone_not_verified_and_not_full_actionable", () => {
  const c = toDiscoveryCandidate({
    url: "https://example.com/x",
    title: "Owner?",
  });
  const adj = adjudicateFinding(c, {});
  assert.equal(adj.accepted, false);
  const scored = scoreOwnershipQuality({
    classifierInput: {
      ultimateOwner: {
        name: "Acme",
        verified: false,
        evidenceUrl: "https://example.com/x",
        linkedToHpc: true,
      },
      stagedOnly: true,
    },
    actual_ownership_accuracy: 0.5,
  });
  assert.equal(scored.full_actionable, false);
  assert.notEqual(scored.ownership_success_tier, OWNERSHIP_SUCCESS_TIER.FULL_ACTIONABLE);
});

test("exact_paid_command_requires_auth_and_network", () => {
  const cmd = buildExactPaidExecutionCommand();
  assert.ok(cmd.phase2_paid_only_after_explicit_founder_approval.includes("--enable-live-network"));
  assert.ok(
    cmd.phase2_paid_only_after_explicit_founder_approval.includes(
      "FOUNDER_AUTH_HE_INTEL_001_P3B_M1_PAID_V1"
    )
  );
  assert.ok(
    cmd.refuse_without.some((s) => String(s).includes("founder-spend-authorized"))
  );
});

await testAsync("full_readiness_validation", async () => {
  const report = await runLiveExperimentReadinessValidation({ memoryOnly: true });
  assert.equal(report.ok, true, JSON.stringify({
    ok: report.ok,
    phaseA: report.phaseASafety,
    adj: report.adjudicationSmoke,
  }));
  assert.equal(report.stop, "STOP_BEFORE_PAID_EXECUTION");
  assert.equal(report.paid_execution_authorized, false);
  assert.equal(report.manifest.cohort.hpcIds.length, 3);
});

console.log(`\nOK ${passed} he-intel-001-p3b-m1-live-experiment readiness tests passed`);
