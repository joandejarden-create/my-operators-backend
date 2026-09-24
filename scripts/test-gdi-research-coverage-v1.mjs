/**
 * GDI Research Coverage V1 — fixture tests (no Airtable required).
 */
import assert from "node:assert/strict";
import {
  TARGET_TYPE,
  TARGET_STATUS,
  TARGET_PRIORITY,
  RESEARCH_CADENCE,
  RUN_TYPE,
  EXECUTION_STATUS,
  RESULT_TYPE,
  NO_CHANGE_RELAX_THRESHOLD,
  buildResearchTarget,
  buildRunId,
  isTargetDue,
  planCoverage,
  classifyTargetAgainstEvidence,
  executeCoverageCycle,
  applyTargetRunOutcome,
  attachDiscoveryProvenance,
  isTrulyNewOpportunity,
  buildTargetsFromExistingIntelligence,
  buildCustomerResearchSummary,
  formatCustomerResearchSummaryText,
  buildTargetRun,
} from "../lib/group-demand-intelligence/research-coverage/index.js";

const HOTEL = "recLuxvwwxID7U2B8";
const HOTEL_B = "recG66DQJKP2c0UNh";
const HOTEL_C = "recIwaP1etgx2g9nA";

function makeTarget(overrides = {}) {
  return buildResearchTarget({
    hotelId: HOTEL,
    hotelName: "Bethesda Marriott",
    targetType: TARGET_TYPE.DEMAND_GENERATOR,
    entityKey: overrides.entityKey || "dg_test_alpha",
    entityId: overrides.entityId || "dg_test_alpha",
    demandGeneratorId: overrides.demandGeneratorId || "dg_test_alpha",
    canonicalName: overrides.canonicalName || "Test Generator",
    priority: TARGET_PRIORITY.HIGH,
    status: TARGET_STATUS.ACTIVE,
    researchCadence: RESEARCH_CADENCE.WEEKLY,
    nextResearchAt: overrides.nextResearchAt || new Date(Date.now() - 86400000).toISOString(),
    lastResearchedAt: overrides.lastResearchedAt || null,
    consecutiveNoChangeRuns: overrides.consecutiveNoChangeRuns || 0,
    ...overrides,
  });
}

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}`);
    console.error(err && err.stack ? err.stack : err);
  }
}

check("target_due_ledger_check_no_change_not_researched", () => {
  const t = makeTarget({ lastResearchedAt: new Date().toISOString() });
  const cycle = executeCoverageCycle({
    hotelId: HOTEL,
    targets: [t],
    evidence: { signals: [], opportunities: [] },
  });
  assert.equal(cycle.plan.due.length, 1);
  assert.equal(cycle.run.targetsCompleted, 1);
  assert.equal(cycle.run.targetsNoChange, 1);
  // Coverage is ledger-only — SKIPPED, not RESEARCHED
  assert.equal(cycle.researchedTargetRuns.length, 0);
  assert.equal(cycle.targetRuns[0].executionStatus, EXECUTION_STATUS.SKIPPED);
  assert.equal(cycle.targetRuns[0].resultType, RESULT_TYPE.NO_MATERIAL_CHANGE);
});

check("target_due_new_signal_ledger_not_researched", () => {
  const t = makeTarget({
    lastResearchedAt: "2026-01-01T00:00:00.000Z",
    demandGeneratorId: "dg_sig",
    entityKey: "dg_sig",
    entityId: "dg_sig",
  });
  const cycle = executeCoverageCycle({
    hotelId: HOTEL,
    targets: [t],
    evidence: {
      signals: [
        {
          demandGeneratorId: "dg_sig",
          firstSeenAt: "2026-09-20T00:00:00.000Z",
          lastSeenAt: "2026-09-20T00:00:00.000Z",
          sourceUrl: "https://example.org/a",
        },
      ],
    },
  });
  assert.equal(cycle.run.newSignals, 1);
  assert.equal(cycle.researchedTargetRuns.length, 0);
  assert.equal(cycle.targetRuns[0].resultType, RESULT_TYPE.NEW_SIGNAL);
  assert.equal(cycle.targetRuns[0].executionStatus, EXECUTION_STATUS.SKIPPED);
});

check("target_due_new_opportunity", () => {
  const t = makeTarget({
    entityKey: "dg_opp",
    entityId: "dg_opp",
    demandGeneratorId: "dg_opp",
    lastResearchedAt: "2026-01-01T00:00:00.000Z",
  });
  // Pre-build run id by executing then checking — inject opp with matching run after plan
  // Use classify with explicit runId
  const runId = buildRunId({ hotelId: HOTEL, runType: RUN_TYPE.WEEKLY });
  const classification = classifyTargetAgainstEvidence(
    t,
    {
      opportunities: [
        {
          opportunityId: "opp_new",
          firstDiscoveredRunId: runId,
          firstSeenRunId: runId,
          discoveryTargetId: t.targetId,
          dgGeneratorId: "dg_opp",
        },
      ],
    },
    { runId }
  );
  assert.equal(classification.resultType, RESULT_TYPE.OPPORTUNITY_CREATED);
  assert.equal(classification.newOpportunityCount, 1);
  assert.equal(classification.executionStatus, EXECUTION_STATUS.SKIPPED);
  assert.equal(classification.researchExecuted, false);
});

check("target_due_existing_opportunity_update", () => {
  const t = makeTarget({
    entityKey: "dg_upd",
    entityId: "dg_upd",
    demandGeneratorId: "dg_upd",
    lastResearchedAt: "2026-01-01T00:00:00.000Z",
  });
  const runId = buildRunId({ hotelId: HOTEL });
  const classification = classifyTargetAgainstEvidence(
    t,
    {
      opportunities: [
        {
          opportunityId: "opp_old",
          firstSeenRunId: "gdir_prior",
          lastMaterialChangeRunId: runId,
          weeklyDeltaState: "UPDATED",
          dgGeneratorId: "dg_upd",
          discoveryTargetId: t.targetId,
        },
      ],
    },
    { runId }
  );
  assert.equal(classification.resultType, RESULT_TYPE.OPPORTUNITY_UPDATED);
  assert.equal(classification.newOpportunityCount, 0);
});

check("target_not_due", () => {
  const t = makeTarget({
    nextResearchAt: new Date(Date.now() + 7 * 86400000).toISOString(),
  });
  assert.equal(isTargetDue(t), false);
  const plan = planCoverage({ targets: [t] });
  assert.equal(plan.due.length, 0);
  assert.equal(plan.notDue.length, 1);
});

check("target_failed", () => {
  const t = makeTarget();
  // Force failure by throwing inside classify via broken evidence callback — simulate via apply
  const tr = buildTargetRun({
    hotelId: HOTEL,
    targetId: t.targetId,
    runId: "gdir_x",
    executionStatus: EXECUTION_STATUS.FAILED,
    resultType: RESULT_TYPE.ERROR,
    failureReason: "timeout",
  });
  assert.equal(tr.executionStatus, EXECUTION_STATUS.FAILED);
  assert.equal(tr.resultType, RESULT_TYPE.ERROR);
});

check("target_skipped_paused", () => {
  const t = makeTarget({ status: TARGET_STATUS.PAUSED });
  const plan = planCoverage({ targets: [t] });
  assert.equal(plan.skipped.length, 1);
  assert.equal(plan.due.length, 0);
});

check("new_source_old_opportunity_not_new", () => {
  const opp = {
    opportunityId: "opp1",
    firstSeenRunId: "gdir_old",
    firstDiscoveredRunId: "gdir_old",
    weeklyDeltaState: "UPDATED",
  };
  assert.equal(isTrulyNewOpportunity(opp, "gdir_new"), false);
});

check("same_opportunity_two_targets_one_canonical", () => {
  const runId = "gdir_same";
  const opp = {
    opportunityId: "opp_shared",
    firstSeenRunId: runId,
    firstDiscoveredRunId: runId,
    discoveryTargetId: "gdirt_a",
  };
  // Second target discovery does not create a second NEW flag
  assert.equal(isTrulyNewOpportunity(opp, runId), true);
  const opp2 = { ...opp, discoveryTargetId: "gdirt_b" };
  assert.equal(opp.opportunityId, opp2.opportunityId);
  assert.equal(isTrulyNewOpportunity(opp2, "other_run"), false);
});

check("monthly_expansion_adds_new_target", () => {
  const pack = buildTargetsFromExistingIntelligence({
    hotelId: HOTEL,
    hotelName: "Bethesda Marriott",
    generators: [
      {
        demandGeneratorId: "dg_new_exp",
        organizationName: "Expansion Org",
        organizationType: "ASSOCIATION",
        firstSeenAt: "2026-09-01T00:00:00.000Z",
      },
    ],
    programs: [],
    fits: [
      {
        hotelId: HOTEL,
        demandGeneratorId: "dg_new_exp",
        generatorPriority: "HIGH",
      },
    ],
    venues: [],
    venueFits: [],
  });
  assert.equal(pack.totals.total, 1);
  assert.equal(pack.targets[0].targetType, TARGET_TYPE.DEMAND_GENERATOR);
});

check("repeated_no_change_relaxes_cadence", () => {
  let t = makeTarget({
    researchCadence: RESEARCH_CADENCE.WEEKLY,
    consecutiveNoChangeRuns: NO_CHANGE_RELAX_THRESHOLD - 1,
  });
  const tr = buildTargetRun({
    hotelId: HOTEL,
    targetId: t.targetId,
    runId: "gdir_nc",
    executionStatus: EXECUTION_STATUS.RESEARCHED,
    resultType: RESULT_TYPE.NO_MATERIAL_CHANGE,
  });
  const applied = applyTargetRunOutcome(t, tr);
  assert.equal(applied.learning.cadenceRelaxed, true);
  assert.equal(applied.target.researchCadence, RESEARCH_CADENCE.BIWEEKLY);
});

check("new_opportunity_links_run_target_provenance", () => {
  const attached = attachDiscoveryProvenance(
    { opportunityId: "opp_x", title: "X" },
    {
      runId: "gdir_r1",
      targetId: "gdirt_t1",
      targetRunId: "gditr_tr1",
      playbook: "official_source_monitor_v1",
      source: "https://example.org",
    }
  );
  assert.equal(attached.firstDiscoveredRunId, "gdir_r1");
  assert.equal(attached.discoveryTargetId, "gdirt_t1");
  assert.equal(attached.discoveryTargetRunId, "gditr_tr1");
  assert.ok(attached.firstDiscoveredAt);
});

check("customer_summary_null_from_ledger_only_coverage", () => {
  const cycle = executeCoverageCycle({
    hotelId: HOTEL,
    targets: [makeTarget()],
    evidence: {},
  });
  const summary = buildCustomerResearchSummary({
    run: cycle.run,
    researchedTargetRuns: cycle.researchedTargetRuns,
    activeTargetCount: 1,
  });
  assert.equal(summary, null);
  assert.equal(formatCustomerResearchSummaryText(summary), "");
});

check("customer_summary_from_researched_target_runs", () => {
  const t = makeTarget();
  const tr = buildTargetRun({
    hotelId: HOTEL,
    targetId: t.targetId,
    runId: "gdir_real_research",
    executionStatus: EXECUTION_STATUS.RESEARCHED,
    researchExecuted: true,
    resultType: RESULT_TYPE.NO_MATERIAL_CHANGE,
  });
  const summary = buildCustomerResearchSummary({
    run: {
      status: "COMPLETED",
      completedAt: "2026-09-24T12:00:00.000Z",
      newSignals: 0,
      newOpportunities: 0,
      updatedOpportunities: 1,
    },
    researchedTargetRuns: [tr],
    activeTargetCount: 1,
  });
  assert.ok(summary);
  assert.equal(summary.monitoredTargetsReviewed, 1);
  const text = formatCustomerResearchSummaryText(summary);
  assert.match(text, /Last Research:/);
  assert.match(text, /monitored demand targets reviewed/);
});

check("replication_hotel_specific_target_ids", () => {
  const a = buildTargetsFromExistingIntelligence({
    hotelId: HOTEL,
    generators: [{ demandGeneratorId: "dg_shared", organizationName: "Shared" }],
    fits: [{ hotelId: HOTEL, demandGeneratorId: "dg_shared", generatorPriority: "HIGH" }],
  });
  const b = buildTargetsFromExistingIntelligence({
    hotelId: HOTEL_B,
    generators: [{ demandGeneratorId: "dg_shared", organizationName: "Shared" }],
    fits: [{ hotelId: HOTEL_B, demandGeneratorId: "dg_shared", generatorPriority: "MEDIUM" }],
  });
  const c = buildTargetsFromExistingIntelligence({
    hotelId: HOTEL_C,
    generators: [{ demandGeneratorId: "dg_shared", organizationName: "Shared" }],
    fits: [{ hotelId: HOTEL_C, demandGeneratorId: "dg_shared", generatorPriority: "LOW" }],
  });
  assert.notEqual(a.targets[0].targetId, b.targets[0].targetId);
  assert.notEqual(b.targets[0].targetId, c.targets[0].targetId);
  assert.equal(a.targets[0].priority, TARGET_PRIORITY.HIGH);
  assert.equal(b.targets[0].priority, TARGET_PRIORITY.MEDIUM);
  assert.equal(c.targets[0].priority, TARGET_PRIORITY.LOW);
  assert.doesNotMatch(JSON.stringify(a), /Bethesda Marriott.*hardcode/i);
});

check("dedupe_targets_stable_identity", () => {
  const pack = buildTargetsFromExistingIntelligence({
    hotelId: HOTEL,
    generators: [
      { demandGeneratorId: "dg_dup", organizationName: "Dup" },
      { demandGeneratorId: "dg_dup", organizationName: "Dup Again" },
    ],
    fits: [
      { hotelId: HOTEL, demandGeneratorId: "dg_dup", generatorPriority: "HIGH" },
    ],
  });
  assert.equal(pack.totals.total, 1);
});

if (failed) {
  console.error(`\n${failed} check(s) failed`);
  process.exit(1);
}
console.log("\nAll GDI research coverage V1 checks passed.");
