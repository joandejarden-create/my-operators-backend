#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  loadFocusEight,
  assertFocusExcludesCmaaDdaa,
  resolveWaveMode,
  sharedExecutionLimits,
  runExperiment,
  WAVE1_AUTH_ENV,
  CONCLUSION_SCOPE,
} from "../lib/group-demand-intelligence/experiments/gdi-ai-agent-incremental-value-experiment-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const doc = path.join(
  ROOT,
  "docs/data-intelligence/gdi-ai-agent-incremental-value-experiment-v1.md",
);
const summary = path.join(
  ROOT,
  "reports/gdi-ai-agent-incremental-value-experiment-v1/PROTOCOL_SUMMARY.json",
);
const focusPath = path.join(
  ROOT,
  "fixtures/gdi-experiments/gdi-ai-agent-incremental-value-experiment-v1/focus-eight-cases.json",
);

assert.ok(fs.existsSync(doc), "protocol doc missing");
assert.ok(fs.existsSync(summary), "protocol summary missing");
assert.ok(fs.existsSync(focusPath), "focus-eight lock missing");

const j = JSON.parse(fs.readFileSync(summary, "utf8"));
assert.equal(j.schema, "gdi_ai_agent_incremental_value_experiment_v1");
assert.equal(j.status, "PROTOCOL_LOCKED");
assert.equal(j.pr414Role, "limitations_memo_only_not_product_authorization");
assert.equal(j.fairnessContract.forbidden, "compare_active_agent_to_frozen_only_baseline_in_wave1");
assert.equal(
  j.conclusionScope.primary,
  "A_incremental_research_on_existing_opportunities",
);
assert.equal(
  j.conclusionScope.doesNotEstablish,
  "B_incremental_discovery_of_genuinely_new_opportunities",
);
assert.match(
  j.adoptInterpretation,
  /further_validation_or_selective_integration/,
);
assert.equal(j.budgetParity.wave0_offline.paidUsd, 0);
assert.equal(j.budgetParity.wave1_live_public.paidUsd, 0);
assert.equal(j.budgetParity.wave1_live_public.armAActiveBaselineRequired, true);
assert.equal(j.budgetParity.wave2_paid.defaultAuthorized, false);
assert.equal(j.decisionFramework.wave0CannotAdopt, true);
assert.equal(j.decisionFramework.wave1NotAuthorizedByThisPr, true);
assert.equal(j.writes.airtable, false);
assert.equal(j.cohort.focusSubsetCases, 8);
assert.equal(j.cohort.focusOpportunityIds.length, 8);

const focus = loadFocusEight().data;
assert.equal(focus.cases.length, 8);
assertFocusExcludesCmaaDdaa(focus);
assert.deepEqual(
  focus.cases.map((c) => c.opportunityId),
  j.cohort.focusOpportunityIds,
);

const md = fs.readFileSync(doc, "utf8");
assert.match(md, /Fairness principle/);
assert.match(md, /both arms actively research/i);
assert.match(md, /ADOPT/);
assert.match(md, /ITERATE/);
assert.match(md, /REJECT/);
assert.match(md, /further validation or selective integration/i);
assert.match(md, /Conclusion A/i);
assert.match(md, /Conclusion B/i);
assert.match(md, /automatic production-wide deployment/i);
assert.match(md, /further validation or selective integration/i);
assert.doesNotMatch(md, /Authorize broad discovery architecture/i);

// Wave 1 blocked without founder auth
const blocked = resolveWaveMode({ wave: "wave1", env: {} });
assert.equal(blocked.wave, "wave0");
assert.equal(blocked.blockedWave1, true);

const allowed = resolveWaveMode({
  wave: "wave1",
  env: { [WAVE1_AUTH_ENV]: "1" },
});
assert.equal(allowed.wave, "wave1");

const limits = sharedExecutionLimits("wave1");
assert.equal(limits.maxFetchesPerCase, 12);
assert.equal(limits.maxPaidUsd, 0);
assert.equal(sharedExecutionLimits("wave0").maxFetchesPerCase, 0);

// Wave 0 harness smoke (no network, no Wave 1)
const report = await runExperiment({ wave: "wave0", writeReports: true });
assert.equal(report.wave, "wave0");
assert.equal(report.paidUsd, 0);
assert.equal(report.wave1Executed, false);
assert.equal(report.focusCaseCount, 8);
assert.equal(report.conclusionScope, CONCLUSION_SCOPE.A_EXISTING_OPPORTUNITY_RESEARCH);
assert.equal(report.doesNotEstablish, CONCLUSION_SCOPE.B_NEW_OPPORTUNITY_DISCOVERY);
assert.equal(report.fairnessContract.frozenOnlyBaselineForbiddenInWave1, true);
assert.ok(["ITERATE", "REJECT"].includes(report.decision.decision));
assert.notEqual(report.decision.decision, "ADOPT");

console.log(
  JSON.stringify(
    {
      ok: true,
      status: j.status,
      focusIds: j.cohort.focusOpportunityIds.length,
      wave0Decision: report.decision.decision,
      wave1BlockedWithoutAuth: true,
    },
    null,
    2,
  ),
);
