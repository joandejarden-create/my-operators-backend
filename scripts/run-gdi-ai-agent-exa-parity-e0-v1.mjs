#!/usr/bin/env node
/**
 * E0 Exa-parity harness — mock Exa only. Does not authorize or execute paid Exa.
 */
import {
  runE0Experiment,
  assertE0FairnessCapability,
  runBudgetRefusalSelfTests,
} from "../lib/group-demand-intelligence/experiments/gdi-ai-agent-exa-parity-e0-harness-v1.js";

const fairness = assertE0FairnessCapability();
if (!fairness.ok) {
  console.error(JSON.stringify({ ok: false, fairness }, null, 2));
  process.exit(3);
}

const budgetTests = runBudgetRefusalSelfTests();
if (!budgetTests.ok) {
  console.error(JSON.stringify({ ok: false, budgetTests }, null, 2));
  process.exit(4);
}

const report = await runE0Experiment({ writeReports: true });
console.log(
  JSON.stringify(
    {
      ok: report.ok === true,
      wave: report.wave,
      actualPaidUsd: report.actualPaidUsd,
      focusCaseCount: report.focusCaseCount,
      e1Readiness: report.e1Readiness?.status,
      blockers: report.e1Readiness?.blockers || [],
      plausibleAdaptiveValueCaseIds: report.e1Readiness?.plausibleAdaptiveValueCaseIds || [],
      reportFile: report.reportFile || null,
      disclaimer: report.disclaimer,
    },
    null,
    2,
  ),
);
process.exitCode = report.e1Readiness?.status === "E1_READY" ? 0 : 5;
