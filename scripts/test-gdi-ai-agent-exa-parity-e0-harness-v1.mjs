#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  assertE0FairnessCapability,
  assertE0LiveSpendDenied,
  runBudgetRefusalSelfTests,
  runE0Experiment,
  assessE1Readiness,
  E0_HARNESS_VERSION,
} from "../lib/group-demand-intelligence/experiments/gdi-ai-agent-exa-parity-e0-harness-v1.js";
import {
  createArmBudgetLedger,
  E0_LIMITS,
} from "../lib/group-demand-intelligence/experiments/gdi-ai-agent-exa-parity-e0-budget-ledger-v1.js";
import {
  buildMockExaCorpus,
  createMockExaExecutor,
} from "../lib/group-demand-intelligence/experiments/gdi-ai-agent-exa-parity-e0-mock-exa-v1.js";
import { evaluateMaterialCommercialLift } from "../lib/group-demand-intelligence/experiments/gdi-ai-agent-wave1-evidence-grounded-evaluator-v1.js";
import {
  runArmAExistingExaPoweredPolicy,
  runArmBAdaptiveExaPlanner,
} from "../lib/group-demand-intelligence/experiments/gdi-ai-agent-exa-parity-e0-arms-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

// Fairness capability
const fairness = assertE0FairnessCapability();
assert.equal(fairness.ok, true, JSON.stringify(fairness.missing));
assert.equal(E0_LIMITS.perArmPerCaseUsd, 1.25);
assert.equal(E0_LIMITS.maxRetrievalsPerCase, 12);
assert.equal(E0_LIMITS.e1TotalCeilingUsd, 20);

// Live spend denied in E0
const liveGate = assertE0LiveSpendDenied({
  GDI_AI_AGENT_EXA_PARITY_AUTH: "1",
  GDI_EXA_AGENT_LIVE: "1",
});
assert.equal(liveGate.forceMockOnly, true);
assert.equal(liveGate.actualPaidUsd, 0);

// Budget refusal path
const budgetTests = runBudgetRefusalSelfTests();
assert.equal(budgetTests.ok, true, JSON.stringify(budgetTests, null, 2));

// Denied provider + live HTTP on mock executor
{
  const ledger = createArmBudgetLedger({
    arm: "t",
    opportunityId: "x",
    forceMockOnly: true,
  });
  const exa = createMockExaExecutor({
    ledger,
    corpus: buildMockExaCorpus(
      { slot: "incomplete_demand", opportunityId: "x", hotelName: "H" },
      { organizationName: "Org" },
    ),
    arm: "t",
  });
  const live = await exa.invoke({
    operation: "search",
    query: "q",
    attemptLiveHttp: true,
  });
  assert.equal(live.ok, false);
  assert.equal(live.reason, "E0_LIVE_EXA_HTTP_REFUSED");
}

// DrawerReady-only cannot materialLift
{
  const lift = evaluateMaterialCommercialLift(
    {
      commercial: {
        verifiedNamedCount: 0,
        genuinelyNewVerifiedCount: 1,
        drawerReady: false,
      },
    },
    {
      commercial: {
        verifiedNamedCount: 0,
        genuinelyNewVerifiedCount: 1,
        drawerReady: true,
      },
    },
  );
  assert.equal(lift.materialLift, false);
}

// Arm A is Exa-powered deterministic; Arm B adaptive — planner differs
{
  const focusCase = {
    opportunityId: "disp_recLuxvwwxID7U2B8_aledade",
    slot: "incomplete_demand",
    hotelId: "recLuxvwwxID7U2B8",
    hotelName: "Bethesda Marriott",
    focusIndex: 1,
  };
  const frozen = {
    id: focusCase.opportunityId,
    organizationName: "Aledade",
    hotelName: focusCase.hotelName,
    hotelId: focusCase.hotelId,
    unresolvedResearchTasks: [
      { question: "What lodging demand thesis exists for Aledade near Bethesda Marriott?" },
    ],
  };
  const meta = {
    opportunityId: focusCase.opportunityId,
    hotelId: focusCase.hotelId,
    hotelName: focusCase.hotelName,
    slot: focusCase.slot,
  };
  const armA = await runArmAExistingExaPoweredPolicy({
    frozenOpportunity: frozen,
    focusCase,
    meta,
  });
  const armB = await runArmBAdaptiveExaPlanner({
    frozenOpportunity: frozen,
    focusCase,
    meta,
  });
  assert.equal(armA.exaPoweredPolicy, true);
  assert.equal(armA.adaptivePlanner, false);
  assert.equal(armB.adaptivePlanner, true);
  assert.equal(armA.costs.actualPaidUsd, 0);
  assert.equal(armB.costs.actualPaidUsd, 0);
  assert.ok(armA.decisions.some((d) => d.decision === "EXISTING_POLICY_SEARCH_FIRST"));
  assert.ok(armB.decisions.some((d) => d.adaptive === true));
  assert.ok(armB.decisions.some((d) => d.decision === "ADAPTIVE_TOOL_SELECT"));
  assert.ok(
    armB.decisions.some((d) => d.decision === "REFORMULATE_SEARCH" || d.reformulation === true),
    "Arm B must reformulate after insufficient first search",
  );
  assert.ok(
    armB.decisions.some((d) => d.tool === "agent" || d.decision === "EXECUTED_EXA_AGENT"),
    "Arm B must be able to select Exa Agent for commercial gaps",
  );
  assert.ok(!armB.decisions.some((d) => d.decision === "STOP_SALES_READY"));
}

// Competitor slot: Arm B correct demotion stop
{
  const focusCase = {
    opportunityId: "camp_ivirma_2027_castillo",
    slot: "competitor_cross_hotel",
    hotelId: "rec82D9zpB8fede1I",
    hotelName: "Castillo Hotel Son Vida",
    focusIndex: 8,
  };
  const frozen = {
    id: focusCase.opportunityId,
    organizationName: "IVIRMA",
    hotelName: focusCase.hotelName,
  };
  const armB = await runArmBAdaptiveExaPlanner({
    frozenOpportunity: frozen,
    focusCase,
    meta: focusCase,
  });
  assert.equal(armB.stopReason, "STOP_CORRECT_DEMOTION");
  assert.ok(armB.decisions.some((d) => d.decision === "STOP_CORRECT_DEMOTION"));
}

// Full E0 run
const report = await runE0Experiment({ writeReports: true });
assert.equal(report.wave, "e0");
assert.equal(report.actualPaidUsd, 0);
assert.equal(report.focusCaseCount, 8);
assert.equal(report.airtableWrites, false);
assert.equal(report.customerPublication, false);
assert.equal(report.e1Readiness.status, "E1_READY", JSON.stringify(report.e1Readiness, null, 2));
assert.equal(report.decision.commercialVerdict, "NOT_APPLICABLE_E0_MOCK");
for (const row of report.results) {
  assert.equal(row.ok, true);
  assert.equal(row.yieldLabel, "MOCK_SIMULATED");
  assert.equal(row.armA.exaPoweredPolicy, true);
  assert.equal(row.armB.adaptivePlanner, true);
  assert.equal(row.deltas.materialLift, false);
  assert.ok(row.plannerDiff.prematureSalesReadyStopBanned);
}

const readiness = assessE1Readiness(report);
assert.equal(readiness.status, "E1_READY");
assert.equal(readiness.e1SpendAuthorized, false);

const out = path.join(
  ROOT,
  "reports/gdi-ai-agent-exa-powered-parity-experiment-v1/E0_HARNESS_RESULTS.json",
);
assert.ok(fs.existsSync(out));

console.log(
  JSON.stringify(
    {
      ok: true,
      harnessVersion: E0_HARNESS_VERSION,
      e1Readiness: readiness.status,
      focusCases: 8,
      actualPaidUsd: 0,
      drawerReadyAloneRejected: true,
      liveHttpRefused: true,
      prematureStopSalesReadyBanned: true,
    },
    null,
    2,
  ),
);
